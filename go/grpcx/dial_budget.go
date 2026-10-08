package grpcx

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc/stats"
)

// grpc-go's connection context is owned by the channel, not an individual RPC.
// This bounded table ties pending dial IO to live admitted callers. The last
// caller's exit cancels that IO; a new caller cannot revive an already canceled
// dial. No endpoint watchdog or extra worker loop is introduced.
type dialBudget struct {
	mu       sync.Mutex
	sequence uint64
	active   map[uint64]time.Time
	dialing  map[uint64]context.CancelFunc
	pending  map[uint64]*setupConn
}

func newDialBudget() *dialBudget {
	return &dialBudget{active: make(map[uint64]time.Time), dialing: make(map[uint64]context.CancelFunc), pending: make(map[uint64]*setupConn)}
}
func (b *dialBudget) admit(ctx context.Context) func() {
	b.mu.Lock()
	b.sequence++
	id := b.sequence
	deadline, _ := ctx.Deadline()
	b.active[id] = deadline
	b.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.active, id)
			var abandoned []*setupConn
			if len(b.active) == 0 {
				for _, cancel := range b.dialing {
					cancel()
				}
				for _, connection := range b.pending {
					abandoned = append(abandoned, connection)
				}
			}
			b.mu.Unlock()
			for _, connection := range abandoned {
				_ = connection.Close()
			}
		})
	}
}

// Connection setup includes TLS and the HTTP/2 server preface after the native
// dial callback returns. Keep those bytes deadline-bound until grpc-go emits a
// real outgoing RPC header, which proves setup has completed. Established
// pooled connections are then independent of any individual caller lifetime.
type setupConn struct {
	net.Conn
	budget *dialBudget
	id     uint64
}

func (c *setupConn) Close() error {
	c.budget.mu.Lock()
	delete(c.budget.pending, c.id)
	c.budget.mu.Unlock()
	return c.Conn.Close()
}

func (b *dialBudget) track(ctx context.Context, connection net.Conn) (net.Conn, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		_ = connection.Close()
		return nil, errors.New("xrpc: connection setup lost caller deadline")
	}
	if err := connection.SetDeadline(deadline); err != nil {
		_ = connection.Close()
		return nil, err
	}
	b.mu.Lock()
	if len(b.active) == 0 || ctx.Err() != nil {
		b.mu.Unlock()
		_ = connection.Close()
		return nil, context.Canceled
	}
	b.sequence++
	id := b.sequence
	tracked := &setupConn{Conn: connection, budget: b, id: id}
	b.pending[id] = tracked
	b.mu.Unlock()
	return tracked, nil
}

func (b *dialBudget) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (b *dialBudget) HandleRPC(_ context.Context, event stats.RPCStats) {
	header, ok := event.(*stats.OutHeader)
	if !ok {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, connection := range b.pending {
		if connection.LocalAddr().String() == header.LocalAddr.String() && connection.RemoteAddr().String() == header.RemoteAddr.String() {
			_ = connection.SetDeadline(time.Time{})
			delete(b.pending, id)
		}
	}
}
func (b *dialBudget) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (b *dialBudget) HandleConn(context.Context, stats.ConnStats)                       {}
func (b *dialBudget) context(parent context.Context) (context.Context, func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var deadline time.Time
	for _, active := range b.active {
		if active.After(deadline) {
			deadline = active
		}
	}
	if deadline.IsZero() || !deadline.After(time.Now()) {
		return nil, nil, errors.New("xrpc: no live caller owns connection setup")
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	b.sequence++
	id := b.sequence
	b.dialing[id] = cancel
	return ctx, func() { b.mu.Lock(); delete(b.dialing, id); b.mu.Unlock(); cancel() }, nil
}
