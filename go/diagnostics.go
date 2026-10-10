package xrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Diagnostic contains only bounded transport metadata. Payloads, headers,
// credentials and arbitrary configuration are deliberately not accepted.
type Diagnostic struct {
	Time       time.Time `json:"time"`
	Level      string    `json:"level"`
	Event      string    `json:"event"`
	Service    string    `json:"service,omitempty"`
	InstanceID string    `json:"instance_id,omitempty"`
	RequestID  string    `json:"request_id,omitempty"`
	Operation  string    `json:"operation,omitempty"`
	Category   string    `json:"error_category,omitempty"`
	ElapsedMS  int64     `json:"elapsed_ms,omitempty"`
}

type DiagnosticOptions struct {
	Sink    io.Writer
	OwnSink bool
	// Level is the most verbose level emitted: error, warn, info, debug or trace.
	// The default is info; SetLevel changes it while the owner runs.
	Level string
	// Format is json or text. The default is json.
	Format           string
	MaxRecordBytes   int
	MaxQueuedRecords int
}

type DiagnosticStatus struct {
	Time           time.Time `json:"time"`
	QueuedRecords  int       `json:"queued_records"`
	QueueCapacity  int       `json:"queue_capacity"`
	MaxRecordBytes int       `json:"max_record_bytes"`
	Written        uint64    `json:"written_records"`
	Dropped        uint64    `json:"dropped_records"`
	SinkErrors     uint64    `json:"sink_errors"`
	Drained        bool      `json:"drained"`
	Level          string    `json:"level"`
	Format         string    `json:"format"`
}

// Diagnostics has one explicitly owned writer and a finite queue. Emit never
// waits for IO. Close can report failed quiescence while retaining the writer
// and sink; Done closes only when the real writer has finished.
type Diagnostics struct {
	mu                           sync.Mutex
	level                        atomic.Int32
	format                       string
	options                      DiagnosticOptions
	queue                        chan []byte
	done                         chan struct{}
	closed                       bool
	written, dropped, sinkErrors atomic.Uint64
	err                          error
	rate                         [4]struct {
		start time.Time
		count uint32
	}
}

var levelNames = [...]string{"error", "warn", "info", "debug", "trace"}

func severity(level string) int32 {
	for i, name := range levelNames {
		if level == name {
			return int32(i)
		}
	}
	return -1
}

// NewDiagnostics starts the single writer that owns options.Sink.
func NewDiagnostics(options DiagnosticOptions) (*Diagnostics, error) {
	if options.Sink == nil {
		return nil, errors.New("xrpc: explicit diagnostic sink required")
	}
	if options.Level == "" {
		options.Level = "info"
	}
	if options.Format == "" {
		options.Format = "json"
	}
	if severity(options.Level) < 0 {
		return nil, errors.New("xrpc: diagnostic level must be error, warn, info, debug or trace")
	}
	if options.Format != "json" && options.Format != "text" {
		return nil, errors.New("xrpc: diagnostic format must be json or text")
	}
	if options.MaxRecordBytes == 0 {
		options.MaxRecordBytes = 2048
	}
	if options.MaxQueuedRecords == 0 {
		options.MaxQueuedRecords = 256
	}
	if options.MaxRecordBytes < 256 || options.MaxRecordBytes > 1<<20 || options.MaxQueuedRecords < 1 || options.MaxQueuedRecords > 65536 {
		return nil, errors.New("xrpc: diagnostic queue/record bounds invalid")
	}
	if options.OwnSink {
		if _, ok := options.Sink.(io.Closer); !ok {
			return nil, errors.New("xrpc: owned diagnostic sink must be closeable")
		}
	}
	d := &Diagnostics{format: options.Format, options: options, queue: make(chan []byte, options.MaxQueuedRecords), done: make(chan struct{})}
	d.level.Store(severity(options.Level))
	go d.write()
	return d, nil
}

func boundedDiagnosticField(value string, limit int) string {
	if len(value) > limit {
		value = value[:limit]
	}
	return strings.Map(func(c rune) rune {
		if c < ' ' || c == 127 {
			return '_'
		}
		return c
	}, value)
}

// Emit keeps bounded metadata and silently accounts for saturation through
// Status. Event and category should be stable codes, never arbitrary error text.
func (d *Diagnostics) Emit(record Diagnostic) {
	if d == nil {
		return
	}
	level := severity(record.Level)
	if level < 0 || level > d.level.Load() {
		return
	}
	if record.Time.IsZero() {
		record.Time = time.Now().UTC()
	}
	// Stable repeated transport warnings share four fixed buckets. No path,
	// request identity or arbitrary category ever creates a rate-limit entry.
	bucket := -1
	switch record.Event {
	case "admission_rejected":
		bucket = 0
	case "admission_recovered":
		bucket = 1
	case "peer_error":
		bucket = 2
	case "call_deadline":
		bucket = 3
	}
	if bucket >= 0 {
		d.mu.Lock()
		rate := &d.rate[bucket]
		now := time.Now()
		if now.Sub(rate.start) >= time.Second {
			rate.start = now
			rate.count = 0
		}
		if rate.count >= 8 {
			d.mu.Unlock()
			d.dropped.Add(1)
			return
		}
		rate.count++
		d.mu.Unlock()
	}
	record.Event = boundedDiagnosticField(record.Event, 64)
	record.Service = boundedDiagnosticField(record.Service, 128)
	record.InstanceID = boundedDiagnosticField(record.InstanceID, 128)
	record.RequestID = boundedDiagnosticField(record.RequestID, 128)
	record.Operation = boundedDiagnosticField(record.Operation, 128)
	record.Category = boundedDiagnosticField(record.Category, 64)
	var encoded []byte
	if d.format == "json" {
		encoded, _ = json.Marshal(record)
		encoded = append(encoded, '\n')
	} else {
		encoded = []byte(fmt.Sprintf("%s %s event=%s service=%s instance=%s request=%s operation=%s category=%s elapsed_ms=%d\n", record.Time.Format(time.RFC3339Nano), record.Level, record.Event, record.Service, record.InstanceID, record.RequestID, record.Operation, record.Category, record.ElapsedMS))
	}
	if len(encoded) > d.options.MaxRecordBytes {
		d.dropped.Add(1)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		d.dropped.Add(1)
		return
	}
	select {
	case d.queue <- encoded:
	default:
		d.dropped.Add(1)
	}
}

func (d *Diagnostics) write() {
	defer close(d.done)
	for record := range d.queue {
		if err := writeAll(d.options.Sink, record); err != nil {
			d.sinkErrors.Add(1)
			d.dropped.Add(1)
			d.mu.Lock()
			d.err = err
			d.mu.Unlock()
		} else {
			d.written.Add(1)
		}
	}
	if d.options.OwnSink {
		if err := d.options.Sink.(io.Closer).Close(); err != nil {
			d.sinkErrors.Add(1)
			d.mu.Lock()
			d.err = err
			d.mu.Unlock()
		}
	}
}

func writeAll(writer io.Writer, body []byte) error {
	for len(body) > 0 {
		n, err := writer.Write(body)
		if n < 0 || n > len(body) {
			return errors.New("xrpc: invalid sink write count")
		}
		body = body[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (d *Diagnostics) Done() <-chan struct{} { return d.done }
func (d *Diagnostics) Close(ctx context.Context) error {
	if d == nil {
		return nil
	}
	if _, err := Remaining(ctx); err != nil {
		return err
	}
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		close(d.queue)
	}
	d.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.done:
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.err
	}
}

func (d *Diagnostics) Status() DiagnosticStatus {
	if d == nil {
		return DiagnosticStatus{Time: time.Now().UTC()}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	status := DiagnosticStatus{Time: time.Now().UTC(), QueuedRecords: len(d.queue), QueueCapacity: cap(d.queue), MaxRecordBytes: d.options.MaxRecordBytes, Written: d.written.Load(), Dropped: d.dropped.Load(), SinkErrors: d.sinkErrors.Load(), Level: levelNames[d.level.Load()], Format: d.format}
	select {
	case <-d.done:
		status.Drained = true
	default:
	}
	return status
}

// SetLevel changes verbosity while the owner runs. The format and the queue
// bounds are fixed at construction.
func (d *Diagnostics) SetLevel(level string) error {
	if d == nil {
		return errors.New("xrpc: diagnostics owner required")
	}
	value := severity(level)
	if value < 0 {
		return errors.New("xrpc: diagnostic level must be error, warn, info, debug or trace")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return errors.New("xrpc: diagnostics owner closed")
	}
	d.level.Store(value)
	return nil
}

// Metrics is a fixed-cardinality process/transport counter set. It contains no
// request IDs, robot IDs, paths, arbitrary error text, or network probes.
type Metrics struct {
	inFlight                                                       atomic.Int64
	peak                                                           atomic.Int64
	admitted, rejected, completed, deadline, cancelled, peerErrors atomic.Uint64
}
type MetricsSnapshot struct {
	Time         time.Time `json:"time"`
	InFlight     int64     `json:"in_flight"`
	PeakInFlight int64     `json:"peak_in_flight"`
	Admitted     uint64    `json:"admitted"`
	Rejected     uint64    `json:"admission_rejected"`
	Completed    uint64    `json:"completed"`
	Deadlines    uint64    `json:"deadlines"`
	Cancelled    uint64    `json:"cancelled"`
	PeerErrors   uint64    `json:"peer_errors"`
}

func (m *Metrics) Admit() func() {
	if m == nil {
		return func() {}
	}
	m.admitted.Add(1)
	n := m.inFlight.Add(1)
	for old := m.peak.Load(); n > old; old = m.peak.Load() {
		if m.peak.CompareAndSwap(old, n) {
			break
		}
	}
	var once sync.Once
	return func() { once.Do(func() { m.inFlight.Add(-1); m.completed.Add(1) }) }
}
func (m *Metrics) Reject() {
	if m != nil {
		m.rejected.Add(1)
	}
}
func (m *Metrics) Outcome(err error) {
	if m == nil || err == nil {
		return
	}
	switch Code(err) {
	case "deadline_exceeded":
		m.deadline.Add(1)
	case "cancelled":
		m.cancelled.Add(1)
	case "unavailable":
		m.peerErrors.Add(1)
	}
}
func (m *Metrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{Time: time.Now().UTC()}
	}
	return MetricsSnapshot{Time: time.Now().UTC(), InFlight: m.inFlight.Load(), PeakInFlight: m.peak.Load(), Admitted: m.admitted.Load(), Rejected: m.rejected.Load(), Completed: m.completed.Load(), Deadlines: m.deadline.Load(), Cancelled: m.cancelled.Load(), PeerErrors: m.peerErrors.Load()}
}
