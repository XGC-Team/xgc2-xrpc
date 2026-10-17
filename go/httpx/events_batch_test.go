package httpx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type batchWriterProbe struct {
	header     http.Header
	writes     [][]byte
	flushes    int
	deadlines  []time.Time
	writeError error
	hook       func(int)
}

func (p *batchWriterProbe) Header() http.Header {
	if p.header == nil {
		p.header = make(http.Header)
	}
	return p.header
}
func (*batchWriterProbe) WriteHeader(int) {}
func (p *batchWriterProbe) Write(data []byte) (int, error) {
	p.writes = append(p.writes, bytes.Clone(data))
	if p.hook != nil {
		p.hook(len(p.writes))
	}
	if p.writeError != nil {
		return 0, p.writeError
	}
	return len(data), nil
}
func (p *batchWriterProbe) FlushError() error { p.flushes++; return nil }
func (p *batchWriterProbe) SetWriteDeadline(at time.Time) error {
	p.deadlines = append(p.deadlines, at)
	return nil
}
func probeEventWriter(p *batchWriterProbe) *eventWriter {
	return &eventWriter{w: p, controller: http.NewResponseController(p), timeout: time.Second}
}
func (p *batchWriterProbe) body() string { return string(bytes.Join(p.writes, nil)) }

type readyEventSource struct{ batches [][]Event }

func (s readyEventSource) Subscribe(ctx context.Context, _ string, emit func([]Event) error) error {
	for _, batch := range s.batches {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := emit(batch); err != nil {
			return err
		}
	}
	return nil
}

func TestEventBatchCoalescesInOrderAndReusesBoundedBuffer(t *testing.T) {
	p := &batchWriterProbe{}
	s := probeEventWriter(p)
	batch := []Event{{ID: "1", Type: "health", Data: []byte(`{"healthy":false}`)}, {ID: "2", Type: "health", Data: []byte(`{"healthy":true}`)}, {ID: "3", Type: "text", Data: []byte("first\r\nsecond\rlast\n")}}
	expected := "id: 1\nevent: health\ndata: {\"healthy\":false}\n\nid: 2\nevent: health\ndata: {\"healthy\":true}\n\nid: 3\nevent: text\ndata: first\ndata: second\ndata: last\ndata: \n\n"
	if err := s.events(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	if p.body() != expected || len(p.writes) != 1 || p.flushes != 1 {
		t.Fatalf("writes=%d flushes=%d body=%q", len(p.writes), p.flushes, p.body())
	}
	if len(s.batchBuffer) != 0 || cap(s.batchBuffer) != eventBatchBytes {
		t.Fatalf("buffer len/cap=%d/%d", len(s.batchBuffer), cap(s.batchBuffer))
	}
	storage := &s.batchBuffer[:cap(s.batchBuffer)][0]
	if err := s.events(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	if &s.batchBuffer[:cap(s.batchBuffer)][0] != storage || p.body() != expected+expected {
		t.Fatal("coalescing buffer not reused or payload changed")
	}
	if len(p.deadlines) != 2 {
		t.Fatalf("write deadlines=%d", len(p.deadlines))
	}
	for _, deadline := range p.deadlines {
		if !deadline.After(time.Now().Add(-time.Second)) {
			t.Fatalf("stale write deadline %v", deadline)
		}
	}
}

func TestEventBatchValidatesEntireMetadataBeforeWriting(t *testing.T) {
	p := &batchWriterProbe{}
	s := probeEventWriter(p)
	valid := Event{ID: "prefix", Type: "health", Data: []byte("not JSON: still opaque bytes")}
	if err := s.events(t.Context(), []Event{valid}); err != nil {
		t.Fatal(err)
	}
	previous := p.body()
	writes := len(p.writes)
	for name, bad := range map[string]Event{
		"empty payload": {ID: "2"}, "bad id": {ID: "bad\nid", Data: []byte("{}")}, "bad type": {Type: "bad\rkind", Data: []byte("{}")}, "reset": {Type: EventReset, Data: []byte("{}")}, "closing": {Type: EventClosing, Data: []byte("{}")},
	} {
		t.Run(name, func(t *testing.T) {
			err := s.events(t.Context(), []Event{valid, bad})
			if !errors.Is(err, ErrInvalidEvent) || len(p.writes) != writes || p.body() != previous {
				t.Fatalf("late invalid metadata wrote partial batch: %v writes=%d", err, len(p.writes))
			}
		})
	}
	// Validation is before any ServeEvents data write too, after the initial header flush.
	q := &batchWriterProbe{}
	err := ServeEvents(q, httptest.NewRequest(http.MethodGet, "/events", nil), readyEventSource{[][]Event{{valid, {ID: "bad\n", Data: []byte("{}")}}}}, EventsOptions{Heartbeat: time.Hour})
	if !errors.Is(err, ErrInvalidEvent) || len(q.writes) != 0 || q.flushes != 1 {
		t.Fatalf("ServeEvents: err=%v writes=%d flushes=%d", err, len(q.writes), q.flushes)
	}
}

func TestEventBatchByteBoundAndLargeSingleFrame(t *testing.T) {
	p := &batchWriterProbe{}
	s := probeEventWriter(p)
	batch := []Event{{ID: "a", Type: "sample", Data: bytes.Repeat([]byte("a"), eventBatchBytes/2)}, {ID: "b", Type: "sample", Data: bytes.Repeat([]byte("b"), eventBatchBytes/2)}, {ID: "large", Type: "sample", Data: bytes.Repeat([]byte("x"), eventBatchBytes+100)}, {ID: "after", Type: "sample", Data: []byte("done")}}
	var expected strings.Builder
	for _, event := range batch {
		fmt.Fprintf(&expected, "id: %s\nevent: sample\ndata: %s\n\n", event.ID, event.Data)
	}
	if err := s.events(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	if p.body() != expected.String() || len(p.writes) != 4 || p.flushes != 4 {
		t.Fatalf("writes=%d flushes=%d", len(p.writes), p.flushes)
	}
	for i, write := range p.writes {
		if i != 2 && len(write) > eventBatchBytes {
			t.Fatalf("normal write%d grew to%d", i, len(write))
		}
	}
	if len(p.writes[2]) <= eventBatchBytes || cap(s.batchBuffer) != eventBatchBytes {
		t.Fatalf("large frame/buffer=%d/%d", len(p.writes[2]), cap(s.batchBuffer))
	}
	q := probeEventWriter(&batchWriterProbe{})
	if err := q.events(t.Context(), []Event{batch[2]}); err != nil {
		t.Fatal(err)
	}
	if cap(q.batchBuffer) != 0 {
		t.Fatalf("large frame retained in coalescing buffer: %d", cap(q.batchBuffer))
	}
}

func TestEventBatchFramingSizeMatchesNormalizedWire(t *testing.T) {
	cases := map[string]string{"plain": "data: a\n\n", "lf": "data: a\ndata: b\n\n", "cr": "data: a\ndata: b\n\n", "crlf": "data: a\ndata: b\n\n", "trailing": "data: a\ndata: \n\n", "empty lines": "data: \ndata: \ndata: \n\n"}
	payloads := map[string]string{"plain": "a", "lf": "a\nb", "cr": "a\rb", "crlf": "a\r\nb", "trailing": "a\r\n", "empty lines": "\r\n\r"}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			e := Event{Data: []byte(payloads[name])}
			got := appendEventFrame(nil, e)
			if string(got) != want || eventFrameSize(e) != len(want) || formatValidatedEvent(e) != want {
				t.Fatalf("wire=%q size=%d want=%q", got, eventFrameSize(e), want)
			}
		})
	}
}

func TestEventBatchCancellationAndWriteFailureStopSuffix(t *testing.T) {
	batch := []Event{{ID: "1", Data: bytes.Repeat([]byte("a"), 40000)}, {ID: "2", Data: bytes.Repeat([]byte("b"), 40000)}, {ID: "3", Data: []byte("tail")}}
	ctx, cancel := context.WithCancel(t.Context())
	p := &batchWriterProbe{hook: func(int) { cancel() }}
	s := probeEventWriter(p)
	if err := s.events(ctx, batch); !errors.Is(err, context.Canceled) || len(p.writes) != 1 {
		t.Fatalf("cancel: %v writes=%d", err, len(p.writes))
	}
	if !strings.HasPrefix(p.body(), "id: 1\n") || strings.Contains(p.body(), "id: 2\n") || len(s.batchBuffer) != 0 {
		t.Fatal("canceled batch wrote an unaccepted suffix")
	}
	failed := errors.New("socket write failed")
	q := &batchWriterProbe{writeError: failed}
	writer := probeEventWriter(q)
	if err := writer.events(t.Context(), batch); !errors.Is(err, failed) || len(q.writes) != 1 || !writer.broken() {
		t.Fatalf("write failure: %v writes=%d", err, len(q.writes))
	}
	writer.finish()
	if err := writer.events(t.Context(), []Event{{ID: "late", Data: []byte("{}")}}); err == nil || len(q.writes) != 1 {
		t.Fatal("finished stream wrote late data")
	}
	empty := &batchWriterProbe{}
	if err := probeEventWriter(empty).events(t.Context(), nil); err != nil || len(empty.writes) != 0 || empty.flushes != 0 {
		t.Fatalf("empty batch wrote: %v", err)
	}
}

func TestEventBatchHeartbeatCanInterleaveOnlyCompleteWrites(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	p := &batchWriterProbe{hook: func(n int) {
		if n == 1 {
			close(entered)
			<-release
		}
	}}
	s := probeEventWriter(p)
	batchDone, hbDone := make(chan error, 1), make(chan error, 1)
	go func() {
		batchDone <- s.events(t.Context(), []Event{{ID: "1", Data: bytes.Repeat([]byte("a"), 40000)}, {ID: "2", Data: bytes.Repeat([]byte("b"), 40000)}})
	}()
	<-entered
	go func() { hbDone <- s.comment("hb") }()
	once.Do(func() { close(release) })
	if err := <-batchDone; err != nil {
		t.Fatal(err)
	}
	if err := <-hbDone; err != nil {
		t.Fatal(err)
	}
	body := p.body()
	if strings.Count(body, ": hb\n\n") != 1 || strings.Index(body, "id: 1\n") > strings.Index(body, "id: 2\n") || len(p.writes) != 3 {
		t.Fatalf("writes=%d body order invalid", len(p.writes))
	}
	if !reflect.DeepEqual([]int{bytes.Count(p.writes[0], []byte("\n\n")), bytes.Count(p.writes[1], []byte("\n\n")), bytes.Count(p.writes[2], []byte("\n\n"))}, []int{1, 1, 1}) {
		t.Fatal("heartbeat split an event frame")
	}
}

type canceledAfterPrewriteCheck struct {
	context.Context
	checks int
	cancel context.CancelFunc
}

func (c *canceledAfterPrewriteCheck) Err() error {
	c.checks++
	if c.checks == 3 {
		c.cancel()
		return nil
	}
	return c.Context.Err()
}

func TestEventBatchRechecksCancellationAtTheLockedWriteBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	raced := &canceledAfterPrewriteCheck{Context: ctx, cancel: cancel}
	p := &batchWriterProbe{}
	err := probeEventWriter(p).events(raced, []Event{{ID: "1", Data: []byte("{}")}})
	if !errors.Is(err, context.Canceled) || len(p.writes) != 0 || p.flushes != 0 {
		t.Fatalf("err=%v checks=%d writes=%d flushes=%d", err, raced.checks, len(p.writes), p.flushes)
	}
}
