package httpx

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

func httpRef(kind, address string) xrpc.ServiceRef {
	return xrpc.ServiceRef{TargetID: "agent", Service: "agent.events", APIVersion: "v1", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: kind, Address: address}}
}

func httpsRef(server *httptest.Server) xrpc.ServiceRef { return httpRef("https", server.URL) }

// fastJitter replaces the random reconnect delay: tests wait a millisecond.
func fastJitter(int64) int64 { return int64(time.Millisecond) }

func subscribe(t *testing.T, ctx context.Context, server *httptest.Server, after string) <-chan Event {
	t.Helper()
	events, err := subscribeEvents(ctx, server.Client(), httpsRef(server), "/events", after, fastJitter)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func next(t *testing.T, events <-chan Event) Event {
	t.Helper()
	select {
	case event, open := <-events:
		if !open {
			t.Fatal("event channel closed")
		}
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("no event within 3s")
		return Event{}
	}
}

func closed(t *testing.T, events <-chan Event) {
	t.Helper()
	select {
	case event, open := <-events:
		if open {
			t.Fatalf("unexpected event %+v", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("channel not closed")
	}
}

// limited ends each connection after max events, as a server restart or a
// dropped link would.
type limited struct {
	EventSource
	max int
}

func (l limited) Subscribe(ctx context.Context, after string, emit func(Event) error) error {
	sent := 0
	return l.EventSource.Subscribe(ctx, after, func(event Event) error {
		if sent == l.max {
			return context.Canceled
		}
		sent++
		return emit(event)
	})
}

// recording remembers the Last-Event-ID of every connection.
func recording(source EventSource, seen *[]string, mu *sync.Mutex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		*seen = append(*seen, r.Header.Get("Last-Event-ID"))
		mu.Unlock()
		_ = ServeEvents(w, r, source, EventsOptions{})
	}
}

func TestSubscribeEventsResumesAcrossConnections(t *testing.T) {
	j := newJournal()
	for i := 1; i <= 6; i++ {
		j.append("tick", `{"n":`+strconv.Itoa(i)+`}`)
	}
	var mu sync.Mutex
	var cursors []string
	server := httptest.NewTLSServer(recording(limited{j, 2}, &cursors, &mu))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := subscribe(t, ctx, server, "")
	for i := 1; i <= 6; i++ {
		event := next(t, events)
		if event.ID != strconv.Itoa(i) || event.Type != "tick" || string(event.Data) != `{"n":`+strconv.Itoa(i)+`}` || event.Err != nil {
			t.Fatalf("event %d: %+v", i, event)
		}
	}
	cancel()
	closed(t, events)
	mu.Lock()
	defer mu.Unlock()
	if len(cursors) < 3 || cursors[0] != "" || cursors[1] != "2" || cursors[2] != "4" {
		t.Fatalf("connections resumed from %q", cursors)
	}
}

func TestSubscribeEventsStartsFromTheGivenCursor(t *testing.T) {
	j := newJournal()
	for i := 1; i <= 3; i++ {
		j.append("tick", "{}")
	}
	var mu sync.Mutex
	var cursors []string
	server := httptest.NewTLSServer(recording(j, &cursors, &mu))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := subscribe(t, ctx, server, "2")
	if event := next(t, events); event.ID != "3" {
		t.Fatalf("%+v", event)
	}
	mu.Lock()
	defer mu.Unlock()
	if cursors[0] != "2" {
		t.Fatalf("first connection sent %q", cursors[0])
	}
}

func TestSubscribeEventsDeliversResetAndForgetsTheCursor(t *testing.T) {
	j := newJournal()
	for i := 1; i <= 4; i++ {
		j.append("tick", "{}")
	}
	j.forget(3) // only id 4 remains; "1" is gone
	snapshot := Event{Type: "snapshot", Data: []byte(`{"fresh":true}`)}
	j.fresh = &snapshot
	var mu sync.Mutex
	var cursors []string
	server := httptest.NewTLSServer(recording(limited{j, 2}, &cursors, &mu))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := subscribe(t, ctx, server, "1")
	reset := next(t, events)
	if reset.Type != EventReset || reset.ID != "" || string(reset.Data) != "{}" {
		t.Fatalf("reset: %+v", reset)
	}
	if event := next(t, events); event.Type != "snapshot" || event.ID != "" {
		t.Fatalf("snapshot: %+v", event)
	}
	// The limited source ends the connection after the snapshot and event 4;
	// the client reconnects from event 4, not from the stale cursor "1".
	if event := next(t, events); event.ID != "4" {
		t.Fatalf("event after reset: %+v", event)
	}
	j.append("tick", "{}")
	if event := next(t, events); event.ID != "5" {
		t.Fatalf("event after reconnect: %+v", event)
	}
	mu.Lock()
	defer mu.Unlock()
	if cursors[0] != "1" || cursors[1] == "1" {
		t.Fatalf("the stale cursor survived the reset: %q", cursors)
	}
}

func TestSubscribeEventsConsumesClosingAndReconnects(t *testing.T) {
	var connections atomic.Int32
	var mu sync.Mutex
	var cursors []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		cursors = append(cursors, r.Header.Get("Last-Event-ID"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		if connections.Add(1) == 1 {
			w.Write([]byte("id: 1\ndata: first\n\n: hb\n\nevent: closing\ndata: {}\n\n"))
			return
		}
		w.Write([]byte("id: 2\ndata: second\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := subscribe(t, ctx, server, "")
	if event := next(t, events); event.ID != "1" || string(event.Data) != "first" {
		t.Fatalf("%+v", event)
	}
	if event := next(t, events); event.ID != "2" || string(event.Data) != "second" || event.Type == EventClosing {
		t.Fatalf("closing leaked to the consumer or the stream did not resume: %+v", event)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(cursors) != 2 || cursors[1] != "1" {
		t.Fatalf("resumed from %q", cursors)
	}
}

func TestSubscribeEventsParsesLikeAnEventSource(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(200)
		w.Write([]byte(": comment\nretry: 99\nid: 7\nevent: a\ndata: one\ndata: two\n\n" + // two data lines
			"data:no-space\n\n" + // the id persists; the type does not
			"id: bad\x00id\ndata: kept-7\n\n" + // an id with NUL is ignored
			"data\n\n" + // a field without a colon has an empty value
			"event: nodata\n\n" + // no data: nothing is dispatched
			"id: 9\r\ndata: crlf\r\n\r\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := subscribe(t, ctx, server, "")
	for i, want := range []Event{
		{ID: "7", Type: "a", Data: []byte("one\ntwo")},
		{ID: "7", Data: []byte("no-space")},
		{ID: "7", Data: []byte("kept-7")},
		{ID: "7", Data: []byte("")},
		{ID: "9", Data: []byte("crlf")},
	} {
		got := next(t, events)
		if got.ID != want.ID || got.Type != want.Type || string(got.Data) != string(want.Data) {
			t.Errorf("event %d: got %+v want %+v", i, got, want)
		}
	}
}

func TestSubscribeEventsPermanentFailures(t *testing.T) {
	var mu sync.Mutex
	status := http.StatusForbidden
	drop := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		code := status
		mu.Unlock()
		if code != http.StatusOK {
			writeError(w, code, "permission_denied", "token expired")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.Write([]byte("id: 1\ndata: hello\n\n"))
		w.(http.Flusher).Flush()
		select {
		case <-drop:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A refusal on the first connection is returned by SubscribeEvents.
	_, err := subscribeEvents(ctx, server.Client(), httpsRef(server), "/events", "", fastJitter)
	var failure *xrpc.CallError
	if !errors.As(err, &failure) || failure.Code != "permission_denied" || failure.Disposition != xrpc.ResponseReceived || !strings.Contains(failure.Message, "token expired") {
		t.Fatalf("first connection: %v", err)
	}
	// A refusal after a good start arrives as the final event.
	mu.Lock()
	status = http.StatusOK
	mu.Unlock()
	events := subscribe(t, ctx, server, "")
	if event := next(t, events); event.ID != "1" {
		t.Fatalf("%+v", event)
	}
	mu.Lock()
	status = http.StatusUnauthorized
	mu.Unlock()
	close(drop) // the connection ends; the reconnect is refused
	final := next(t, events)
	if final.Err == nil || xrpc.Code(final.Err) != "unauthenticated" {
		t.Fatalf("final event %+v", final)
	}
	closed(t, events)
}

func TestSubscribeEventsRetriesTransientFailures(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch attempts.Add(1) {
		case 1:
			writeError(w, 503, "unavailable", "starting")
		case 2:
			writeError(w, 429, "resource_exhausted", "busy")
		case 3:
			hijacked, _, _ := w.(http.Hijacker).Hijack() // a dropped connection
			hijacked.Close()
		default:
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			w.Write([]byte("id: 1\ndata: up\n\n"))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := subscribe(t, ctx, server, "")
	if event := next(t, events); string(event.Data) != "up" || attempts.Load() != 4 {
		t.Fatalf("%+v after %d attempts", event, attempts.Load())
	}
}

func TestSubscribeEventsRejectsWhatIsNotAnEventStream(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	if _, err := subscribeEvents(context.Background(), server.Client(), httpsRef(server), "/events", "", fastJitter); xrpc.Code(err) != "invalid_argument" {
		t.Fatal(err)
	}
}

func TestSubscribeEventsStopsWithItsContext(t *testing.T) {
	j := newJournal()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = ServeEvents(w, r, j, EventsOptions{}) }))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	events := subscribe(t, ctx, server, "")
	waitFor(t, "the subscription", func() bool { return j.subscribers.Load() == 1 })
	cancel()
	closed(t, events)
	waitFor(t, "the server side to be released", func() bool { return j.subscribers.Load() == 0 })
}

func TestSubscribeEventsOverUnixSocketWithoutAClient(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "events.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	j := newJournal()
	j.append("tick", `{"unix":true}`)
	host, err := ServeEdge(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = ServeEvents(w, r, j, EventsOptions{}) }), HostOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		host.Shutdown(ctx)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := SubscribeEvents(ctx, nil, httpRef("unix", socket), "/events?topic=a", "")
	if err != nil {
		t.Fatal(err)
	}
	if event := next(t, events); event.ID != "1" || string(event.Data) != `{"unix":true}` {
		t.Fatalf("%+v", event)
	}
}

func TestSubscribeEventsVerifiesServerCertificates(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("a stream was served to a client that cannot verify the server")
	}))
	defer server.Close()
	// The derived client trusts system roots only, not the test certificate.
	_, err := SubscribeEvents(context.Background(), nil, httpsRef(server), "/events", "")
	if xrpc.Code(err) != "unavailable" {
		t.Fatalf("%v", err)
	}
}

func TestSubscribeEventsValidatesArguments(t *testing.T) {
	ctx := context.Background()
	bad := func(name string, client *http.Client, ref xrpc.ServiceRef, path, after string) {
		t.Helper()
		if _, err := SubscribeEvents(ctx, client, ref, path, after); xrpc.Code(err) != "invalid_argument" {
			t.Errorf("%s: %v", name, err)
		}
	}
	good := httpRef("https", "https://agent.example")
	bad("grpc profile", nil, xrpc.ServiceRef{TargetID: "t", Service: "s", APIVersion: "v", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/run/x.sock"}}, "/events", "")
	bad("invalid reference", nil, xrpc.ServiceRef{}, "/events", "")
	bad("relative path", nil, good, "events", "")
	bad("scheme-relative path", nil, good, "//host/events", "")
	bad("cursor with newline", nil, good, "/events", "a\nb")
	bad("cursor too long", nil, good, "/events", strings.Repeat("x", MaxCursorBytes+1))
	bad("client with a timeout", &http.Client{Timeout: time.Second}, good, "/events", "")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := SubscribeEvents(cancelled, nil, good, "/events", ""); xrpc.Code(err) != "cancelled" {
		t.Errorf("cancelled context: %v", err)
	}
}

func TestReconnectBackoffIsFullJitterFrom500msTo5s(t *testing.T) {
	highest := func(bound int64) int64 { return bound - 1 }
	lowest := func(int64) int64 { return 0 }
	for attempt, want := range []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second, 5 * time.Second, 5 * time.Second, 5 * time.Second, 5 * time.Second} {
		if got := reconnectDelay(attempt, highest); got != want {
			t.Errorf("attempt %d: upper bound %s, want %s", attempt, got, want)
		}
		if got := reconnectDelay(attempt, lowest); got != 0 {
			t.Errorf("attempt %d: lower bound %s, want 0 (full jitter)", attempt, got)
		}
	}
	if got := reconnectDelay(1<<30, highest); got != 5*time.Second {
		t.Errorf("huge attempt: %s", got)
	}
	var below, above int
	for i := 0; i < 400; i++ {
		d := reconnectDelay(2, randomBelow)
		if d < 0 || d > 2*time.Second {
			t.Fatalf("delay %s outside [0, 2s]", d)
		}
		if d < time.Second {
			below++
		} else {
			above++
		}
	}
	if below < 100 || above < 100 {
		t.Fatalf("delays are not spread over the interval: %d below 1s, %d above", below, above)
	}
}

// A host drains (the stream ends with "closing"), a replacement starts on the
// same socket, and the subscriber resumes without a gap or a duplicate.
func TestSubscribeEventsSurvivesAHostRestart(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "events.sock")
	j := newJournal()
	serve := func() *Host {
		listener, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		host, err := ServeEdge(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = ServeEvents(w, r, j, EventsOptions{}) }), HostOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return host
	}
	first := serve()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := subscribeEvents(ctx, nil, httpRef("unix", socket), "/events", "", fastJitter)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		j.append("tick", "{}")
		if event := next(t, events); event.ID != strconv.Itoa(i) {
			t.Fatalf("event %d: %+v", i, event)
		}
	}
	shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	started := time.Now()
	if err := first.Shutdown(shutdown); err != nil || time.Since(started) > 2*time.Second {
		t.Fatalf("Shutdown: %v after %s", err, time.Since(started))
	}
	j.append("tick", "{}") // happens while no host is listening
	second := serve()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		second.Shutdown(ctx)
	}()
	for i := 4; i <= 5; i++ {
		if event := next(t, events); event.ID != strconv.Itoa(i) {
			t.Fatalf("event %d after the restart: %+v", i, event)
		}
		j.append("tick", "{}")
	}
}
