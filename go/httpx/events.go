package httpx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Event is one server-sent event of an http.v1 event stream.
type Event struct {
	// ID is the resume cursor. The client sends the last one it saw back as
	// Last-Event-ID. Its meaning belongs to the domain; XRPC treats it as an
	// opaque string without CR, LF or NUL. An empty ID adds no id line.
	ID string
	// Type is the event type; empty means the default "message". The types
	// "reset" and "closing" are reserved for the transport.
	Type string
	// Data is the payload, JSON by convention. It must not be empty.
	Data []byte
	// Err is never set by a server. SubscribeEvents sets it on the final event
	// of a subscription that ended with a failure retrying cannot fix.
	Err error
}

// Reserved event types the transport emits itself.
const (
	// EventReset tells the client that its cursor is unknown to the source:
	// state it derived from earlier events is stale and the stream continues
	// from the source's fresh start. SubscribeEvents delivers it to the consumer.
	EventReset = "reset"
	// EventClosing announces that the server is draining. The stream ends and
	// the client reconnects with backoff and resumes. SubscribeEvents consumes it.
	EventClosing = "closing"
)

// ErrCursorGone is returned by EventSource.Subscribe when the cursor is not
// (or no longer) known to the source, for example because its journal was
// truncated or restarted. ServeEvents answers it with an EventReset and a fresh
// subscription.
var ErrCursorGone = errors.New("xrpc: event cursor unknown to the source")

// ErrInvalidEvent is returned by the emit callback for an event that cannot be
// framed: an empty payload, a reserved type or a line break in the id or type.
var ErrInvalidEvent = errors.New("xrpc: invalid event")

// EventSource is the domain journal behind an event stream.
type EventSource interface {
	// Subscribe emits every event that follows cursor after, in order, and then
	// live events, until ctx ends or the source has nothing more to say. An
	// empty cursor is a fresh subscription. When the cursor is unknown it
	// returns ErrCursorGone before emitting anything. Subscribe must return
	// promptly once ctx ends and must call emit from one goroutine at a time;
	// an error from emit means the stream is over and must be returned.
	Subscribe(ctx context.Context, after string, emit func(Event) error) error
}

// EventsOptions are plain limits for ServeEvents; zero fields select the default.
type EventsOptions struct {
	// Heartbeat is the interval of ": hb" comments on an idle stream, which keep
	// intermediaries from closing it, make a dead client fail a write and let
	// SubscribeEvents tell a live stream from a dead one: keep it well below
	// EventIdleTimeout (default DefaultEventHeartbeat).
	Heartbeat time.Duration
	// WriteTimeout bounds each frame write. It is pushed forward on every frame
	// with http.ResponseController, so a stream outlives the server's
	// WriteTimeout and ends when one write stalls this long (default
	// DefaultEventWriteTimeout).
	WriteTimeout time.Duration
	// Drain ends the stream with an EventClosing frame once it is closed. A
	// request served by a Host from this package carries its host's drain
	// signal already; set this only for streams served by another server.
	Drain <-chan struct{}
}

// Defaults for EventsOptions.
const (
	DefaultEventHeartbeat    = 15 * time.Second
	DefaultEventWriteTimeout = 10 * time.Second
	// MaxCursorBytes bounds a resume cursor.
	MaxCursorBytes = 1024
)

type drainKey struct{}

// drainSignal returns the drain channel of the Host serving r, or nil.
func drainSignal(r *http.Request) <-chan struct{} {
	signal, _ := r.Context().Value(drainKey{}).(<-chan struct{})
	return signal
}

// ValidCursor reports whether cursor can travel as Last-Event-ID or ?after=.
func ValidCursor(cursor string) bool {
	return len(cursor) <= MaxCursorBytes && !strings.ContainsAny(cursor, "\x00\r\n")
}

// ServeEvents serves an event stream over w: SSE frames
// "id: <cursor>\nevent: <type>\ndata: <json>\n\n", heartbeat comments, resume
// from Last-Event-ID (or ?after= where a header is impossible; the header wins)
// and EventReset when the source does not know the cursor. When the serving
// Host drains it sends EventClosing and returns, so a graceful shutdown never
// waits for a stream. Every frame extends the write deadline, so a stream
// lives as long as the client and the source do.
//
// It is meant for edge hosts (ServeEdge, RunEdge) or any net/http server: an
// internal Serve host bounds every response by the call budget and size. Each
// stream holds one admission slot of its host for its lifetime, so size
// HostOptions.MaxInFlight and MaxConnections for the expected subscribers.
// Authenticate and validate the request before calling ServeEvents: the
// response is committed as 200 on entry. ServeEvents returns nil when the
// client left, the stream was drained or the source finished, and an error when
// the source failed or emitted an invalid event; the response is over either way.
func ServeEvents(w http.ResponseWriter, r *http.Request, source EventSource, options EventsOptions) error {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "invalid_argument", "event streams are served with GET")
		return nil
	}
	if accept := r.Header.Get("Accept"); accept != "" && !acceptsEventStream(accept) {
		writeError(w, http.StatusNotAcceptable, "invalid_argument", "this resource produces text/event-stream")
		return nil
	}
	cursor := r.URL.Query().Get("after")
	if header := r.Header.Values("Last-Event-ID"); len(header) > 1 {
		writeError(w, http.StatusBadRequest, "invalid_argument", "at most one Last-Event-ID is allowed")
		return nil
	} else if len(header) == 1 {
		cursor = header[0]
	}
	if !ValidCursor(cursor) {
		writeError(w, http.StatusBadRequest, "invalid_argument", "event cursor is too long or contains control characters")
		return nil
	}
	heartbeat, writeTimeout := options.Heartbeat, options.WriteTimeout
	if heartbeat <= 0 {
		heartbeat = DefaultEventHeartbeat
	}
	if writeTimeout <= 0 {
		writeTimeout = DefaultEventWriteTimeout
	}
	drain := options.Drain
	if drain == nil {
		drain = drainSignal(r)
	}
	if !canFlush(w) {
		writeError(w, http.StatusInternalServerError, "internal", "response does not support streaming")
		return nil
	}
	controller := http.NewResponseController(w)

	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(nil)
	stream := &eventWriter{w: w, controller: controller, timeout: writeTimeout}
	stream.header()
	if drain != nil {
		go func() {
			select {
			case <-drain:
				cancel(errDraining)
			case <-ctx.Done():
			}
		}()
	}
	var beats sync.WaitGroup
	beats.Add(1)
	go func() {
		defer beats.Done()
		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := stream.comment("hb"); err != nil {
					cancel(err)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	emit := func(event Event) error {
		if err := stream.event(event); err != nil {
			cancel(err)
			return err
		}
		return nil
	}
	err := source.Subscribe(ctx, cursor, emit)
	if errors.Is(err, ErrCursorGone) && ctx.Err() == nil {
		if werr := stream.frame("id:\nevent: " + EventReset + "\ndata: {}\n\n"); werr != nil {
			cancel(werr)
		} else {
			err = source.Subscribe(ctx, "", emit)
		}
	}
	cause := context.Cause(ctx)
	cancel(nil)
	beats.Wait()
	if errors.Is(cause, errDraining) && !stream.broken() {
		_ = stream.frame("event: " + EventClosing + "\ndata: {}\n\n")
		stream.finish()
		return nil
	}
	stream.finish()
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !stream.broken() {
		return err
	}
	return nil
}

var errDraining = errors.New("xrpc: host is draining")

// canFlush mirrors http.ResponseController: a writer streams when it, or a
// writer it wraps, can flush.
func canFlush(w http.ResponseWriter) bool {
	for {
		switch t := w.(type) {
		case interface{ FlushError() error }:
			return true
		case http.Flusher:
			return true
		case interface{ Unwrap() http.ResponseWriter }:
			w = t.Unwrap()
		default:
			return false
		}
	}
}

func acceptsEventStream(accept string) bool {
	for _, part := range strings.Split(accept, ",") {
		media, _, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err == nil && (media == "text/event-stream" || media == "text/*" || media == "*/*") {
			return true
		}
	}
	return false
}

// eventWriter serializes frames from the source and the heartbeat and pushes
// the write deadline forward before each of them.
type eventWriter struct {
	w          http.ResponseWriter
	controller *http.ResponseController
	timeout    time.Duration

	mu     sync.Mutex
	failed bool
	done   bool
}

func (s *eventWriter) header() {
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	s.w.WriteHeader(http.StatusOK)
	_ = s.controller.Flush()
}

func (s *eventWriter) frame(text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done || s.failed {
		return errors.New("xrpc: event stream closed")
	}
	_ = s.controller.SetWriteDeadline(time.Now().Add(s.timeout))
	if _, err := s.w.Write([]byte(text)); err != nil {
		s.failed = true
		return err
	}
	if err := s.controller.Flush(); err != nil {
		s.failed = true
		return err
	}
	return nil
}

func (s *eventWriter) comment(text string) error { return s.frame(": " + text + "\n\n") }

func (s *eventWriter) event(event Event) error {
	frame, err := formatEvent(event)
	if err != nil {
		return err
	}
	return s.frame(frame)
}

func (s *eventWriter) broken() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}

// finish stops later writes; the handler is about to return.
func (s *eventWriter) finish() {
	s.mu.Lock()
	s.done = true
	s.mu.Unlock()
}

func formatEvent(event Event) (string, error) {
	if len(event.Data) == 0 || strings.ContainsAny(event.ID, "\x00\r\n") || strings.ContainsAny(event.Type, "\r\n") || event.Type == EventReset || event.Type == EventClosing || len(event.ID) > MaxCursorBytes {
		return "", fmt.Errorf("%w: id %q type %q with %d data bytes", ErrInvalidEvent, truncate(event.ID), truncate(event.Type), len(event.Data))
	}
	var frame strings.Builder
	if event.ID != "" {
		frame.WriteString("id: " + event.ID + "\n")
	}
	if event.Type != "" {
		frame.WriteString("event: " + event.Type + "\n")
	}
	data := bytes.ReplaceAll(bytes.ReplaceAll(event.Data, []byte("\r\n"), []byte("\n")), []byte("\r"), []byte("\n"))
	for _, line := range bytes.Split(data, []byte("\n")) {
		frame.WriteString("data: ")
		frame.Write(line)
		frame.WriteString("\n")
	}
	frame.WriteString("\n")
	return frame.String(), nil
}

func truncate(s string) string {
	if len(s) > 32 {
		return s[:32] + "..."
	}
	return s
}
