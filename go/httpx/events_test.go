package httpx

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// journal is a small domain journal: events carry the ids "1", "2", ... and
// the first `first` ids can be forgotten, which makes older cursors unknown.
type journal struct {
	mu     sync.Mutex
	events []Event // events[i] has id first+i
	first  int
	wake   chan struct{}
	fresh  *Event // sent first to a subscriber that starts without a cursor
	// subscribers counts live Subscribe calls.
	subscribers atomic.Int32
}

func newJournal() *journal { return &journal{first: 1, wake: make(chan struct{})} }

func (j *journal) append(kind, data string) Event {
	j.mu.Lock()
	defer j.mu.Unlock()
	event := Event{ID: strconv.Itoa(j.first + len(j.events)), Type: kind, Data: []byte(data)}
	j.events = append(j.events, event)
	close(j.wake)
	j.wake = make(chan struct{})
	return event
}

// forget drops the oldest events: cursors before the remaining ones are gone.
func (j *journal) forget(n int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = j.events[n:]
	j.first += n
}

func (j *journal) Subscribe(ctx context.Context, after string, emit func(Event) error) error {
	j.subscribers.Add(1)
	defer j.subscribers.Add(-1)
	next := 0 // index into events of the next event to send
	j.mu.Lock()
	if after == "" && j.fresh != nil {
		if err := emitUnlocked(&j.mu, emit, *j.fresh); err != nil {
			j.mu.Unlock()
			return err
		}
	}
	if after != "" {
		id, err := strconv.Atoi(after)
		if err != nil || id < j.first-1 || id > j.first+len(j.events)-1 {
			j.mu.Unlock()
			return ErrCursorGone
		}
		next = id - j.first + 1
	}
	j.mu.Unlock()
	for {
		j.mu.Lock()
		pending := append([]Event(nil), j.events[min(next, len(j.events)):]...)
		next += len(pending)
		wake := j.wake
		j.mu.Unlock()
		for _, event := range pending {
			if err := emit(event); err != nil {
				return err
			}
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func emitUnlocked(mu *sync.Mutex, emit func(Event) error, event Event) error {
	mu.Unlock()
	defer mu.Lock()
	return emit(event)
}

func eventServer(t *testing.T, source EventSource, options EventsOptions) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = ServeEvents(w, r, source, options) }))
	t.Cleanup(server.Close)
	return server
}

// frames reads count raw frames (up to the blank line) from body.
func frames(t *testing.T, body io.Reader, count int) []string {
	t.Helper()
	reader, ok := body.(*bufio.Reader)
	if !ok {
		reader = bufio.NewReader(body)
	}
	var out []string
	var current strings.Builder
	for len(out) < count {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("stream ended after %d frames (%q): %v", len(out), current.String(), err)
		}
		current.WriteString(line)
		if line == "\n" {
			out = append(out, current.String())
			current.Reset()
		}
	}
	return out
}

func open(t *testing.T, server *httptest.Server, header map[string]string, query string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, server.URL+"/events"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range header {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func TestServeEventsFramesAndResume(t *testing.T) {
	j := newJournal()
	j.append("tick", `{"n":1}`)
	j.append("", `{"n":2}`)
	j.append("tick", "line one\nline two")
	server := eventServer(t, j, EventsOptions{})

	response := open(t, server, map[string]string{"Accept": "text/event-stream"}, "")
	if response.StatusCode != 200 || response.Header.Get("Content-Type") != "text/event-stream" || response.Header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("status %d headers %v", response.StatusCode, response.Header)
	}
	got := frames(t, response.Body, 3)
	want := []string{
		"id: 1\nevent: tick\ndata: {\"n\":1}\n\n",
		"id: 2\ndata: {\"n\":2}\n\n",
		"id: 3\nevent: tick\ndata: line one\ndata: line two\n\n",
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("frame %d\n got %q\nwant %q", i, got[i], want[i])
		}
	}
	j.append("tick", `{"n":4}`)
	if live := frames(t, response.Body, 1); live[0] != "id: 4\nevent: tick\ndata: {\"n\":4}\n\n" {
		t.Fatalf("live frame %q", live[0])
	}

	resumed := frames(t, open(t, server, map[string]string{"Last-Event-ID": "2"}, "").Body, 2)
	if !strings.HasPrefix(resumed[0], "id: 3\n") || !strings.HasPrefix(resumed[1], "id: 4\n") {
		t.Fatalf("resume from header: %q", resumed)
	}
	byQuery := frames(t, open(t, server, nil, "?after=3").Body, 1)
	if !strings.HasPrefix(byQuery[0], "id: 4\n") {
		t.Fatalf("resume from ?after=: %q", byQuery)
	}
	// A browser repeats its original URL on reconnect: the header is newer.
	both := frames(t, open(t, server, map[string]string{"Last-Event-ID": "3"}, "?after=1").Body, 1)
	if !strings.HasPrefix(both[0], "id: 4\n") {
		t.Fatalf("header must win over ?after=: %q", both)
	}
}

func TestServeEventsResetsAnUnknownCursor(t *testing.T) {
	j := newJournal()
	for i := 0; i < 5; i++ {
		j.append("tick", `{}`)
	}
	j.forget(3) // ids 4 and 5 remain
	snapshot := Event{Type: "snapshot", Data: []byte(`{"state":"fresh"}`)}
	j.fresh = &snapshot
	server := eventServer(t, j, EventsOptions{})
	for _, cursor := range []string{"1", "99", "garbage"} {
		got := frames(t, open(t, server, map[string]string{"Last-Event-ID": cursor}, "").Body, 4)
		if got[0] != "id:\nevent: reset\ndata: {}\n\n" || got[1] != "event: snapshot\ndata: {\"state\":\"fresh\"}\n\n" || !strings.HasPrefix(got[2], "id: 4\n") || !strings.HasPrefix(got[3], "id: 5\n") {
			t.Fatalf("cursor %q: %q", cursor, got)
		}
	}
	// The newest retained cursor and the one before the oldest are still known.
	if got := frames(t, open(t, server, map[string]string{"Last-Event-ID": "3"}, "").Body, 1); !strings.HasPrefix(got[0], "id: 4\n") {
		t.Fatalf("cursor at the journal edge: %q", got)
	}
	if got := frames(t, open(t, server, map[string]string{"Last-Event-ID": "5"}, "").Body, 0); len(got) != 0 {
		t.Fatal("cursor at the head must replay nothing")
	}
}

func TestServeEventsHeartbeat(t *testing.T) {
	server := eventServer(t, newJournal(), EventsOptions{Heartbeat: 30 * time.Millisecond})
	response := open(t, server, nil, "")
	started := time.Now()
	for _, frame := range frames(t, response.Body, 3) {
		if frame != ": hb\n\n" {
			t.Fatalf("frame %q", frame)
		}
	}
	if elapsed := time.Since(started); elapsed < 60*time.Millisecond || elapsed > time.Second {
		t.Fatalf("three heartbeats in %s", elapsed)
	}
}

func TestServeEventsRejectsBadRequests(t *testing.T) {
	server := eventServer(t, newJournal(), EventsOptions{})
	cases := []struct {
		name   string
		method string
		header map[string]string
		query  string
		status int
	}{
		{"POST", http.MethodPost, nil, "", http.StatusMethodNotAllowed},
		{"JSON only", http.MethodGet, map[string]string{"Accept": "application/json"}, "", http.StatusNotAcceptable},
		{"long cursor", http.MethodGet, map[string]string{"Last-Event-ID": strings.Repeat("x", MaxCursorBytes+1)}, "", http.StatusBadRequest},
		{"long query cursor", http.MethodGet, nil, "?after=" + strings.Repeat("x", MaxCursorBytes+1), http.StatusBadRequest},
		{"NUL in cursor", http.MethodGet, nil, "?after=a%00b", http.StatusBadRequest},
	}
	for _, c := range cases {
		request, _ := http.NewRequest(c.method, server.URL+"/events"+c.query, nil)
		for name, value := range c.header {
			request.Header.Set(name, value)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != c.status || !strings.Contains(string(body), `"code":"invalid_argument"`) {
			t.Errorf("%s: status %d body %q", c.name, response.StatusCode, body)
		}
	}
	// Both Accept forms that include the media type are fine.
	for _, accept := range []string{"text/event-stream", "text/*", "*/*", "application/json, text/event-stream;q=0.9"} {
		response := open(t, server, map[string]string{"Accept": accept}, "")
		if response.StatusCode != 200 {
			t.Errorf("Accept %q: status %d", accept, response.StatusCode)
		}
	}
}

// bareWriter is a ResponseWriter that cannot stream.
type bareWriter struct {
	header http.Header
	status int
	body   strings.Builder
}

func (w *bareWriter) Header() http.Header         { return w.header }
func (w *bareWriter) Write(p []byte) (int, error) { return w.body.Write(p) }
func (w *bareWriter) WriteHeader(status int)      { w.status = status }

func TestServeEventsNeedsAStreamingWriter(t *testing.T) {
	w := &bareWriter{header: http.Header{}}
	err := ServeEvents(w, httptest.NewRequest(http.MethodGet, "/events", nil), newJournal(), EventsOptions{})
	if err != nil || w.status != http.StatusInternalServerError || !strings.Contains(w.body.String(), "does not support streaming") {
		t.Fatalf("status %d body %q err %v", w.status, w.body.String(), err)
	}
}

type invalidSource struct{ event Event }

func (s invalidSource) Subscribe(ctx context.Context, after string, emit func(Event) error) error {
	return emit(s.event)
}

func TestServeEventsRejectsInvalidEvents(t *testing.T) {
	for name, event := range map[string]Event{
		"empty data":       {ID: "1"},
		"newline in id":    {ID: "1\n2", Data: []byte("{}")},
		"NUL in id":        {ID: "a\x00b", Data: []byte("{}")},
		"newline in type":  {Type: "a\nb", Data: []byte("{}")},
		"reserved reset":   {Type: EventReset, Data: []byte("{}")},
		"reserved closing": {Type: EventClosing, Data: []byte("{}")},
		"long id":          {ID: strings.Repeat("x", MaxCursorBytes+1), Data: []byte("{}")},
	} {
		recorder := httptest.NewRecorder()
		err := ServeEvents(recorder, httptest.NewRequest(http.MethodGet, "/events", nil), invalidSource{event}, EventsOptions{})
		if !errors.Is(err, ErrInvalidEvent) || strings.Contains(recorder.Body.String(), "data:") {
			t.Errorf("%s: err %v body %q", name, err, recorder.Body.String())
		}
	}
}

// A stream must outlive the server's WriteTimeout: every frame pushes the
// write deadline forward. The control handler streams without that and is cut.
func TestStreamOutlivesTwiceTheServerWriteTimeout(t *testing.T) {
	const writeTimeout = 150 * time.Millisecond
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	j := newJournal()
	mux := http.NewServeMux()
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		_ = ServeEvents(w, r, j, EventsOptions{Heartbeat: 40 * time.Millisecond, WriteTimeout: time.Second})
	})
	mux.HandleFunc("/plain", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < 20; i++ {
			if _, err := w.Write([]byte(": plain\n\n")); err != nil {
				return
			}
			http.NewResponseController(w).Flush()
			time.Sleep(40 * time.Millisecond)
		}
	})
	host, err := ServeEdge(listener, mux, HostOptions{WriteTimeout: writeTimeout})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		host.Shutdown(ctx)
	})
	base := "http://" + listener.Addr().String()

	control, err := http.Get(base + "/plain")
	if err != nil {
		t.Fatal(err)
	}
	cut := time.Now()
	_, readErr := io.ReadAll(control.Body)
	control.Body.Close()
	if readErr == nil || time.Since(cut) > 2*time.Second {
		t.Fatalf("control stream was not cut by WriteTimeout (%v after %s)", readErr, time.Since(cut))
	}

	response, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	ids := make(chan string, 64)
	go func() {
		defer close(ids)
		reader := bufio.NewReader(response.Body)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if id, ok := strings.CutPrefix(line, "id: "); ok {
				ids <- strings.TrimSpace(id)
			}
		}
	}()
	started := time.Now()
	const events = 16
	for i := 1; i <= events; i++ {
		j.append("tick", "{}")
		time.Sleep(50 * time.Millisecond)
	}
	for i := 1; i <= events; i++ {
		select {
		case id, open := <-ids:
			if !open || id != strconv.Itoa(i) {
				t.Fatalf("event %d: got %q (stream open: %v) after %s", i, id, open, time.Since(started))
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("event %d never arrived", i)
		}
	}
	if lived := time.Since(started); lived < 5*writeTimeout {
		t.Fatalf("stream lived only %s", lived)
	}
}

func TestDrainSendsClosingAndShutdownDoesNotWaitForStreams(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	j := newJournal()
	j.append("tick", "{}")
	host, err := ServeEdge(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = ServeEvents(w, r, j, EventsOptions{})
	}), HostOptions{})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Get("http://" + listener.Addr().String() + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	frames(t, reader, 1)
	shutdown := make(chan error, 1)
	started := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdown <- host.Shutdown(ctx)
	}()
	closing := frames(t, reader, 1)
	if closing[0] != "event: closing\ndata: {}\n\n" {
		t.Fatalf("frame %q", closing[0])
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		t.Fatalf("stream must end after closing: %v", err)
	}
	if err := <-shutdown; err != nil || time.Since(started) > 2*time.Second {
		t.Fatalf("Shutdown: %v after %s", err, time.Since(started))
	}
	select {
	case <-host.Drained():
	case <-time.After(time.Second):
		t.Fatal("host not drained")
	}
	waitFor(t, "the source to be released", func() bool { return j.subscribers.Load() == 0 })
}

// An explicit Drain channel serves hosts that are not from this package.
func TestExplicitDrainChannel(t *testing.T) {
	drain := make(chan struct{})
	server := eventServer(t, newJournal(), EventsOptions{Drain: drain})
	response := open(t, server, nil, "")
	reader := bufio.NewReader(response.Body)
	close(drain)
	if got := frames(t, reader, 1); got[0] != "event: closing\ndata: {}\n\n" {
		t.Fatalf("frame %q", got[0])
	}
}

func TestDisconnectReleasesTheSource(t *testing.T) {
	j := newJournal()
	server := eventServer(t, j, EventsOptions{Heartbeat: 20 * time.Millisecond})
	response := open(t, server, nil, "")
	waitFor(t, "the subscription", func() bool { return j.subscribers.Load() == 1 })
	response.Body.Close()
	waitFor(t, "the source to be released after the client left", func() bool { return j.subscribers.Load() == 0 })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}
