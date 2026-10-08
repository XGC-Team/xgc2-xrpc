package xrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/XGC-Team/xgc2-xrpc/go/internal/policy"
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
	Sink             io.Writer
	OwnSink          bool
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
	Revision       uint64    `json:"policy_revision"`
	Level          string    `json:"level"`
	Format         string    `json:"format"`
}

// Diagnostics has one explicitly owned writer and a finite queue. Emit never
// waits for IO. Close can report failed quiescence while retaining the writer
// and sink; Done closes only when the real writer has finished.
type Diagnostics struct {
	mu                           sync.Mutex
	policy                       *Policy
	appliedSnapshot              EffectivePolicy
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

func severity(level string) int32 {
	switch level {
	case "error":
		return 0
	case "warn":
		return 1
	case "info":
		return 2
	case "debug":
		return 3
	case "trace":
		return 4
	}
	return -1
}

func NewDiagnostics(policy *Policy, options DiagnosticOptions) (*Diagnostics, error) {
	if policy == nil || policyIsRole(policy) || options.Sink == nil {
		return nil, errors.New("xrpc: resolved diagnostic policy and explicit sink required")
	}
	effective := policy.Effective()
	level, ok := effective.Fields["LOG_LEVEL"].Value.(string)
	if !ok {
		return nil, errors.New("xrpc: LOG_LEVEL capability is not selected")
	}
	format, ok := effective.Fields["LOG_FORMAT"].Value.(string)
	if !ok {
		return nil, errors.New("xrpc: LOG_FORMAT capability is not selected")
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
	if err := claimDiagnosticPolicy(policy); err != nil {
		return nil, err
	}
	// An earlier owner may have applied verbosity and drained between the
	// initial capability check and this claim. The claimed owner now pins
	// initialization to the actual latest parent snapshot.
	effective = policy.Effective()
	level, _ = effective.Fields["LOG_LEVEL"].Value.(string)
	format, _ = effective.Fields["LOG_FORMAT"].Value.(string)
	d := &Diagnostics{policy: policy, appliedSnapshot: effective, format: format, options: options, queue: make(chan []byte, options.MaxQueuedRecords), done: make(chan struct{})}
	d.level.Store(severity(level))
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
	defer func() { d.mu.Lock(); owner := d.policy; d.mu.Unlock(); releaseDiagnosticPolicy(owner) }()
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
	effective := d.appliedSnapshot
	level, _ := effective.Fields["LOG_LEVEL"].Value.(string)
	status := DiagnosticStatus{Time: time.Now().UTC(), QueuedRecords: len(d.queue), QueueCapacity: cap(d.queue), MaxRecordBytes: d.options.MaxRecordBytes, Written: d.written.Load(), Dropped: d.dropped.Load(), SinkErrors: d.sinkErrors.Load(), Revision: effective.Revision, Level: level, Format: d.format}
	select {
	case <-d.done:
		status.Drained = true
	default:
	}
	return status
}

// UpdatePolicy applies live verbosity with compare-and-swap revision semantics.
// LOG_FORMAT and resource budgets require restart. No values are persisted.
func (d *Diagnostics) UpdatePolicy(expected uint64, updates map[string]string) (*Policy, error) {
	if d == nil {
		return nil, errors.New("xrpc: diagnostics owner required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, errors.New("xrpc: diagnostics owner closed")
	}
	policy, err := d.policy.Update(expected, updates)
	if err != nil {
		return nil, err
	}
	level, _ := policy.Effective().Fields["LOG_LEVEL"].Value.(string)
	oldLevel := d.level.Load()
	d.level.Store(severity(level))
	if err := publishDiagnosticPolicy(policy, expected); err != nil {
		d.level.Store(oldLevel)
		return nil, err
	}
	d.policy = policy
	d.appliedSnapshot = policy.Effective()
	return policy, nil
}

// CheckDiagnosticPolicy prevents a transport from silently accepting explicit
// logging settings while no diagnostic owner is wired into that transport.
func CheckDiagnosticPolicy(p *Policy, diagnostics *Diagnostics) error {
	if p == nil {
		return errors.New("xrpc: resolved runtime policy required")
	}
	if diagnostics == nil {
		snapshot := p.Effective()
		for _, name := range []string{"LOG_LEVEL", "LOG_FORMAT"} {
			field, selected := snapshot.Fields[name]
			if selected && field.Source != "sdk_default" {
				return fmt.Errorf("xrpc: %s requires an explicit diagnostics owner", name)
			}
		}
		return nil
	}
	// Serialize the cold startup/query check with the one actual log owner so a
	// concurrent verbosity publication cannot compare two different revisions.
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	if diagnostics.closed {
		return errors.New("xrpc: diagnostics owner is closing or closed")
	}
	if policyIsRole(p) && !sameDiagnosticPolicyOwner(p, diagnostics.policy) {
		return errors.New("xrpc: role policy requires its parent's diagnostics owner")
	}
	snapshot := p.Effective()
	actual := diagnostics.appliedSnapshot
	for _, name := range []string{"LOG_LEVEL", "LOG_FORMAT"} {
		field, selected := snapshot.Fields[name]
		if !selected {
			return fmt.Errorf("xrpc: diagnostic policy missing %s", name)
		}
		if field.Value != actual.Fields[name].Value {
			return fmt.Errorf("xrpc: diagnostics owner policy conflicts with %s", name)
		}
	}
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

func policyIsRole(p *Policy) bool { return policy.IsRole(p) }
func publishDiagnosticPolicy(p *Policy, expected uint64) error {
	return policy.PublishApplied(p, expected)
}

func claimDiagnosticPolicy(p *Policy) error       { return policy.ClaimDiagnostic(p) }
func releaseDiagnosticPolicy(p *Policy)           { policy.ReleaseDiagnostic(p) }
func sameDiagnosticPolicyOwner(a, b *Policy) bool { return policy.SameOwner(a, b) }
