package httpx

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

// Reconnect backoff of SubscribeEvents: full jitter, uniform in [0, bound],
// where the bound doubles from EventBackoffInitial to EventBackoffMax.
const (
	EventBackoffInitial = 500 * time.Millisecond
	EventBackoffMax     = 5 * time.Second

	maxEventLine = 1 << 20
)

// SubscribeEvents opens the event stream at path on ref and delivers its
// events on the returned channel, resuming from after (empty: from the
// source's start). When the connection drops, the server announces EventClosing
// or a transient error occurs, it reconnects with full-jitter backoff
// (500 ms .. 5 s) and resumes from the last event id it saw. EventReset is
// delivered to the consumer and clears the cursor; EventClosing is consumed.
// The channel is closed when ctx ends. A failure that retrying cannot fix (the
// server answers 4xx, or its certificate does not verify) is returned by
// SubscribeEvents itself when it happens on the first connection, and later
// as the final event, whose Err is set. The consumer must keep receiving or
// cancel ctx: a slow consumer pauses the stream.
//
// ref is an http.v1 reference with a unix or https endpoint; path is an absolute
// request URI, query allowed. client carries the transport: nil derives one
// from ref (the Unix socket, or HTTPS with system roots and TLS 1.2+), and a
// caller-supplied client must have no Timeout, which would cut the stream.
// Event streams are unfenced: neither the instance nor a call budget applies.
func SubscribeEvents(ctx context.Context, client *http.Client, ref xrpc.ServiceRef, path, after string) (<-chan Event, error) {
	return subscribeEvents(ctx, client, ref, path, after, randomBelow)
}

// subscribeEvents is SubscribeEvents with an injectable jitter source.
func subscribeEvents(ctx context.Context, client *http.Client, ref xrpc.ServiceRef, path, after string, draw func(bound int64) int64) (<-chan Event, error) {
	if err := ref.Validate(); err != nil {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, err)
	}
	if ref.Profile != xrpc.HTTP {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: http.v1 reference required"))
	}
	if err := requestPath(path); err != nil {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, err)
	}
	if !ValidCursor(after) {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: invalid event cursor"))
	}
	if err := ctx.Err(); err != nil {
		return nil, xrpc.Failure(xrpc.Code(err), xrpc.NotSent, err)
	}
	s := &subscription{path: path, cursor: after, out: make(chan Event), draw: draw}
	switch ref.Endpoint.Kind {
	case "unix":
		s.base = "http://unix"
	case "https":
		s.base = strings.TrimSuffix(ref.Endpoint.Address, "/")
	default:
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: event streams need a unix or https endpoint"))
	}
	if client == nil {
		s.client, s.owned = defaultEventClient(ref), true
	} else if client.Timeout != 0 {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: an event stream client must not set Timeout"))
	} else {
		s.client = client
	}
	response, err := s.connect(ctx)
	if err != nil && !retryable(err) {
		if s.owned {
			s.client.CloseIdleConnections()
		}
		return nil, err
	}
	go s.run(ctx, response, err)
	return s.out, nil
}

func defaultEventClient(ref xrpc.ServiceRef) *http.Client {
	transport := &http.Transport{Proxy: nil, DisableCompression: true, ForceAttemptHTTP2: false, IdleConnTimeout: 30 * time.Second, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	if ref.Endpoint.Kind == "unix" {
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", ref.Endpoint.Address)
		}
	}
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

type subscription struct {
	client *http.Client
	owned  bool // client was derived here and is closed with the subscription
	base   string
	path   string
	cursor string
	out    chan Event
	draw   func(bound int64) int64
}

// transient marks a connect failure worth retrying.
type transient struct{ error }

func (t transient) Unwrap() error { return t.error }

func retryable(err error) bool {
	var t transient
	return errors.As(err, &t)
}

// connect opens one stream.
func (s *subscription) connect(ctx context.Context) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+s.path, nil)
	if err != nil {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, err)
	}
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Cache-Control", "no-cache")
	var id [16]byte
	if _, err := rand.Read(id[:]); err == nil {
		request.Header.Set(RequestIDHeader, hex.EncodeToString(id[:]))
	}
	if s.cursor != "" {
		request.Header.Set("Last-Event-ID", s.cursor)
	}
	response, err := s.client.Do(request)
	if err != nil {
		var verification *tls.CertificateVerificationError
		if errors.As(err, &verification) {
			return nil, xrpc.Failure("unavailable", xrpc.NotSent, err)
		}
		return nil, transient{xrpc.Failure(xrpc.Code(err), xrpc.NotSent, err)}
	}
	if response.StatusCode == http.StatusOK {
		if media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type")); err == nil && media == "text/event-stream" {
			return response, nil
		}
		_ = response.Body.Close()
		return nil, xrpc.Failure("invalid_argument", xrpc.ResponseReceived, errors.New("xrpc: the response is not an event stream"))
	}
	excerpt, _ := io.ReadAll(io.LimitReader(response.Body, 256))
	_ = response.Body.Close()
	failure := xrpc.Failure(statusCode(response.StatusCode), xrpc.ResponseReceived, fmt.Errorf("HTTP status %d: %s", response.StatusCode, strings.TrimSpace(string(excerpt))))
	switch {
	case response.StatusCode == http.StatusRequestTimeout, response.StatusCode == http.StatusTooEarly, response.StatusCode == http.StatusTooManyRequests, response.StatusCode >= 500:
		return nil, transient{failure}
	}
	return nil, failure
}

// run reads the first connection, if any, then reconnects until ctx ends or a
// failure retrying cannot fix.
func (s *subscription) run(ctx context.Context, response *http.Response, failure error) {
	defer close(s.out)
	if s.owned {
		defer s.client.CloseIdleConnections()
	}
	attempt := 0
	for {
		if response != nil {
			attempt = 0
			err := s.read(ctx, response.Body)
			_ = response.Body.Close()
			if err != nil {
				s.fail(ctx, err)
				return
			}
		} else if !retryable(failure) {
			s.fail(ctx, failure)
			return
		}
		timer := time.NewTimer(reconnectDelay(attempt, s.draw))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		attempt++
		response, failure = s.connect(ctx)
		if ctx.Err() != nil {
			if response != nil {
				_ = response.Body.Close()
			}
			return
		}
	}
}

func (s *subscription) fail(ctx context.Context, err error) {
	select {
	case s.out <- Event{Err: err}:
	case <-ctx.Done():
	}
}

// read delivers the events of one connection until it ends. A nil result means
// reconnect and resume; an error means the stream cannot continue.
func (s *subscription) read(ctx context.Context, body io.Reader) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 4096), maxEventLine)
	id := s.cursor // the last event id persists across events and connections
	var eventType string
	var data []string
	hasData := false
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			if hasData {
				event := Event{ID: id, Type: eventType, Data: []byte(strings.Join(data, "\n"))}
				switch eventType {
				case EventClosing:
					return nil
				case EventReset:
					id = ""
					event.ID = ""
				}
				s.cursor = id
				select {
				case s.out <- event:
				case <-ctx.Done():
					return nil
				}
			}
			eventType, data, hasData = "", nil, false
		case strings.HasPrefix(line, ":"):
			// A comment: the heartbeat.
		default:
			field, value, _ := strings.Cut(line, ":")
			value = strings.TrimPrefix(value, " ")
			switch field {
			case "event":
				eventType = value
			case "data":
				data, hasData = append(data, value), true
			case "id":
				if !strings.Contains(value, "\x00") {
					id = value
				}
			}
		}
	}
	if errors.Is(scanner.Err(), bufio.ErrTooLong) {
		return xrpc.Failure("resource_exhausted", xrpc.ResponseReceived, fmt.Errorf("xrpc: event line exceeds %d bytes", maxEventLine))
	}
	return nil
}

// reconnectDelay is the full-jitter wait before reconnect attempt number
// attempt: uniform in [0, min(EventBackoffMax, EventBackoffInitial << attempt)].
// draw returns a uniform value in [0, bound).
func reconnectDelay(attempt int, draw func(bound int64) int64) time.Duration {
	bound := EventBackoffMax
	if attempt < 8 {
		bound = min(EventBackoffMax, EventBackoffInitial<<attempt)
	}
	return time.Duration(draw(int64(bound) + 1))
}

func randomBelow(bound int64) int64 {
	n, err := rand.Int(rand.Reader, big.NewInt(bound))
	if err != nil {
		return bound - 1
	}
	return n.Int64()
}
