// Package udptest is a fault-injection harness for UDP protocols: an
// in-process proxy that sits between clients and a server and loses,
// duplicates, reorders and delays datagrams, or swallows a whole direction.
// It is used by the udpx tests and by the cross-language interop test, where
// the server is the C++ implementation.
package udptest

import (
	"errors"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Faults impair one direction. Probabilities are in [0, 1] and independent.
type Faults struct {
	// Loss drops a datagram.
	Loss float64
	// Duplicate delivers a datagram twice.
	Duplicate float64
	// Reorder holds a datagram back for ReorderDelay so that later datagrams
	// overtake it.
	Reorder float64
	// ReorderDelay is how long a reordered datagram is held back (default 50ms).
	ReorderDelay time.Duration
	// Delay and Jitter add a fixed and a uniformly random [0, Jitter) one-way
	// delay to every delivery.
	Delay  time.Duration
	Jitter time.Duration
}

func (f Faults) validate() error {
	for _, p := range []float64{f.Loss, f.Duplicate, f.Reorder} {
		if p < 0 || p > 1 {
			return errors.New("udptest: probabilities must be in [0, 1]")
		}
	}
	if f.ReorderDelay < 0 || f.Delay < 0 || f.Jitter < 0 {
		return errors.New("udptest: delays must not be negative")
	}
	return nil
}

// Config configures a Proxy. Requests travel from clients to the server and
// replies back. Two proxies with the same Seed make the same random choices for
// the same sequence of datagrams.
type Config struct {
	Seed     uint64
	Requests Faults
	Replies  Faults
}

// DirectionStats counts the datagrams of one direction.
type DirectionStats struct {
	Seen       uint64 // read by the proxy
	Dropped    uint64 // lost, blackholed or scripted away
	Duplicated uint64 // delivered a second time
	Reordered  uint64 // held back
	Delivered  uint64 // written to the destination, duplicates included
}

// Stats are the counters of both directions.
type Stats struct {
	Requests DirectionStats
	Replies  DirectionStats
}

// Proxy relays datagrams between clients and one upstream server. Every client
// address gets its own upstream socket, so the server sees distinct sources and
// replies find their way back.
type Proxy struct {
	conn     *net.UDPConn
	upstream netip.AddrPort
	requests *direction
	replies  *direction

	mu       sync.Mutex
	sessions map[netip.AddrPort]*session
	closed   bool
	wg       sync.WaitGroup
}

type session struct {
	up   *net.UDPConn
	last atomic.Int64 // unix nanoseconds
}

type direction struct {
	mu                                              sync.Mutex
	rng                                             *rand.Rand
	faults                                          Faults
	blackhole                                       atomic.Bool
	script                                          atomic.Int64 // datagrams still to drop
	seen, dropped, duplicated, reordered, delivered atomic.Uint64
}

func newDirection(seed, stream uint64, faults Faults) *direction {
	return &direction{rng: rand.New(rand.NewPCG(seed, stream)), faults: faults}
}

// sessionIdle is how long an unused client session is kept.
const sessionIdle = 10 * time.Second

// NewProxy listens on a loopback port and starts relaying to upstream
// ("host:port"). The upstream does not have to be up yet.
func NewProxy(upstream string, config Config) (*Proxy, error) {
	target, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		return nil, err
	}
	for _, faults := range []Faults{config.Requests, config.Replies} {
		if err := faults.validate(); err != nil {
			return nil, err
		}
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		conn: conn, upstream: target.AddrPort(), sessions: make(map[netip.AddrPort]*session),
		requests: newDirection(config.Seed, 1, config.Requests), replies: newDirection(config.Seed, 2, config.Replies),
	}
	p.wg.Add(1)
	go p.readClients()
	return p, nil
}

// Addr is the address clients send to, in host:port form.
func (p *Proxy) Addr() string { return p.conn.LocalAddr().String() }

// SetFaults replaces the impairments of both directions.
func (p *Proxy) SetFaults(requests, replies Faults) error {
	for _, faults := range []Faults{requests, replies} {
		if err := faults.validate(); err != nil {
			return err
		}
	}
	p.requests.set(requests)
	p.replies.set(replies)
	return nil
}

// BlackholeRequests makes the proxy swallow every request while on is true.
func (p *Proxy) BlackholeRequests(on bool) { p.requests.blackhole.Store(on) }

// BlackholeReplies makes the proxy swallow every reply while on is true.
func (p *Proxy) BlackholeReplies(on bool) { p.replies.blackhole.Store(on) }

// DropRequests drops the next n requests, a deterministic scripted loss.
func (p *Proxy) DropRequests(n int) { p.requests.script.Store(int64(n)) }

// DropReplies drops the next n replies.
func (p *Proxy) DropReplies(n int) { p.replies.script.Store(int64(n)) }

// Stats returns a snapshot of both directions.
func (p *Proxy) Stats() Stats {
	return Stats{Requests: p.requests.stats(), Replies: p.replies.stats()}
}

// Close stops relaying and releases every socket.
func (p *Proxy) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	sessions := p.sessions
	p.sessions = nil
	p.mu.Unlock()
	err := p.conn.Close()
	for _, s := range sessions {
		_ = s.up.Close()
	}
	p.wg.Wait()
	return err
}

func (p *Proxy) readClients() {
	defer p.wg.Done()
	buffer := make([]byte, 2048)
	for {
		n, client, err := p.conn.ReadFromUDPAddrPort(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(time.Millisecond)
			continue
		}
		datagram := append([]byte(nil), buffer[:n]...)
		s := p.session(client)
		if s == nil {
			return
		}
		p.requests.relay(datagram, func(data []byte) bool { _, err := s.up.Write(data); return err == nil })
	}
}

// session finds or opens the upstream socket of client, forgetting idle ones.
func (p *Proxy) session(client netip.AddrPort) *session {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	now := time.Now()
	if s, ok := p.sessions[client]; ok {
		s.last.Store(now.UnixNano())
		return s
	}
	for key, s := range p.sessions {
		if now.Sub(time.Unix(0, s.last.Load())) > sessionIdle {
			_ = s.up.Close()
			delete(p.sessions, key)
		}
	}
	up, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(p.upstream))
	if err != nil {
		return nil
	}
	s := &session{up: up}
	s.last.Store(now.UnixNano())
	p.sessions[client] = s
	p.wg.Add(1)
	go p.readUpstream(client, s)
	return s
}

func (p *Proxy) readUpstream(client netip.AddrPort, s *session) {
	defer p.wg.Done()
	buffer := make([]byte, 2048)
	for {
		n, err := s.up.Read(buffer)
		if err != nil {
			// A refused read is the ICMP echo of a datagram sent while the
			// upstream was down; the session stays usable for its restart.
			if errors.Is(err, syscall.ECONNREFUSED) {
				continue
			}
			return
		}
		s.last.Store(time.Now().UnixNano())
		datagram := append([]byte(nil), buffer[:n]...)
		p.replies.relay(datagram, func(data []byte) bool { _, err := p.conn.WriteToUDPAddrPort(data, client); return err == nil })
	}
}

func (d *direction) set(faults Faults) {
	d.mu.Lock()
	d.faults = faults
	d.mu.Unlock()
}

func (d *direction) stats() DirectionStats {
	return DirectionStats{Seen: d.seen.Load(), Dropped: d.dropped.Load(), Duplicated: d.duplicated.Load(), Reordered: d.reordered.Load(), Delivered: d.delivered.Load()}
}

// scripted consumes one scripted drop, if any is left.
func (d *direction) scripted() bool {
	for {
		left := d.script.Load()
		if left <= 0 {
			return false
		}
		if d.script.CompareAndSwap(left, left-1) {
			return true
		}
	}
}

// plan draws the fate of one datagram: the delay of every copy to deliver, or
// none when it is lost. The random draws depend only on the order of datagrams.
func (d *direction) plan() (delays []time.Duration, reordered int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	f := d.faults
	lost := d.rng.Float64() < f.Loss
	copies := 1
	if d.rng.Float64() < f.Duplicate {
		copies = 2
	}
	if lost {
		return nil, 0
	}
	hold := f.ReorderDelay
	if hold == 0 {
		hold = 50 * time.Millisecond
	}
	for i := 0; i < copies; i++ {
		delay := f.Delay
		if f.Jitter > 0 {
			delay += time.Duration(d.rng.Int64N(int64(f.Jitter)))
		}
		if d.rng.Float64() < f.Reorder {
			delay += hold
			reordered++
		}
		delays = append(delays, delay)
	}
	return delays, reordered
}

// relay applies the direction's faults to one datagram and hands the copies
// that survive to send, possibly later.
func (d *direction) relay(datagram []byte, send func([]byte) bool) {
	d.seen.Add(1)
	if d.blackhole.Load() || d.scripted() {
		d.dropped.Add(1)
		return
	}
	delays, reordered := d.plan()
	if len(delays) == 0 {
		d.dropped.Add(1)
		return
	}
	if len(delays) == 2 {
		d.duplicated.Add(1)
	}
	d.reordered.Add(uint64(reordered))
	for _, delay := range delays {
		deliver := func() {
			if send(datagram) {
				d.delivered.Add(1)
			}
		}
		if delay <= 0 {
			deliver()
		} else {
			time.AfterFunc(delay, deliver)
		}
	}
}
