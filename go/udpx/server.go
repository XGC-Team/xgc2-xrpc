package udpx

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults for ServerConfig.
const (
	DefaultCallBudget    = 2 * time.Second
	DefaultCacheTTL      = 120 * time.Second
	DefaultCacheCapacity = 1024
	DefaultMaxInFlight   = 64
	DefaultRateLimit     = 50  // requests per second per source address
	DefaultRateBurst     = 100 // bucket size per source address
)

var (
	// ErrAlreadyReplied is returned by a second answer to the same request.
	ErrAlreadyReplied = errors.New("udpx: request already answered")
	// ErrDeadlineExceeded is returned when a request is answered after its
	// deadline or after the server closed: the request was abandoned, the client
	// reports an unknown outcome and nothing is sent.
	ErrDeadlineExceeded = errors.New("udpx: request abandoned (deadline passed or server closed); no reply sent")
	// ErrReplyTooLarge is returned when a reply did not fit in one datagram. A
	// short resource_exhausted reply was sent in its place.
	ErrReplyTooLarge = errors.New("udpx: reply exceeds the datagram limit; resource_exhausted sent")
)

// ServerConfig holds the plain limits of a Server. Zero fields select the
// documented default.
type ServerConfig struct {
	// Keys authenticates requests; required.
	Keys *KeyRing
	// CallBudget caps the deadline of every request: the server deadline is the
	// receipt time plus the smaller of the request's timeout_ms and this budget
	// (default DefaultCallBudget, at most MaxTimeoutMS).
	CallBudget time.Duration
	// CacheTTL is how long an executed request stays in the reply cache
	// (default DefaultCacheTTL).
	CacheTTL time.Duration
	// CacheCapacity bounds the reply cache (default DefaultCacheCapacity); it
	// must exceed MaxInFlight.
	CacheCapacity int
	// MaxInFlight bounds the requests whose handler has not answered yet;
	// further ones are answered resource_exhausted (default DefaultMaxInFlight).
	MaxInFlight int
	// RateLimit and RateBurst configure the token bucket of each source address.
	// A negative RateLimit disables the limiter (defaults DefaultRateLimit and
	// DefaultRateBurst).
	RateLimit float64
	RateBurst int
}

// Request is one authenticated, deduplicated request handed to a Handler.
type Request struct {
	Method string
	// Body is owned by the handler.
	Body   []byte
	Source netip.AddrPort
	KeyID  uint32
	ID     RequestID
	// Deadline is the server deadline; an answer after it is discarded.
	Deadline time.Time
}

// Handler serves one request. It answers through response, either before it
// returns or later from another goroutine, at most once and before
// request.Deadline. ctx ends at the deadline, once the request is answered or
// when the server closes. A handler that returns without answering leaves the
// request pending until it is answered elsewhere or its deadline passes.
type Handler func(ctx context.Context, request Request, response *Responder)

// Stats are monotonic counters plus two gauges.
type Stats struct {
	Received        uint64 // datagrams read from the socket
	Malformed       uint64 // dropped: not a udp.v1 request
	Unauthenticated uint64 // dropped: unknown key_id or bad tag
	RateLimited     uint64 // dropped: the source's bucket was empty
	Requests        uint64 // authenticated requests that passed the limiter
	Executed        uint64 // handler invocations
	Duplicates      uint64 // retransmissions of an executed request
	Ignored         uint64 // retransmissions of a request still running
	Replies         uint64 // reply datagrams sent, cached resends included
	Refused         uint64 // answered without running a handler
	Abandoned       uint64 // handlers that missed their deadline: no reply
	Panics          uint64 // handlers that panicked and were answered internal
	SendErrors      uint64 // socket write failures
	InFlight        int    // requests pending right now
	CacheEntries    int    // executed requests remembered right now
}

// Server answers udp.v1 requests on one UDP socket.
type Server struct {
	conn     *net.UDPConn
	keys     *KeyRing
	instance [16]byte
	budget   time.Duration
	cache    *replyCache
	limiter  *limiter

	mu      sync.RWMutex
	methods map[string]Handler

	admitMu  sync.Mutex // guards draining and the work.Add that admission performs
	draining bool

	base    context.Context
	cancel  context.CancelFunc
	closing atomic.Bool
	closeIt sync.Once
	work    sync.WaitGroup // handler goroutines and pending responders
	loop    chan struct{}  // closed when the receive loop returned
	drained chan struct{}  // closed when every handler and responder finished

	received, malformed, unauthenticated, rateLimited, requests, executed atomic.Uint64
	duplicates, ignored, replies, refused, abandoned, panics, sendErrors  atomic.Uint64
	inFlight                                                              atomic.Int64
}

// Listen binds address (host:port) and starts serving. Methods may be
// registered at any time; requests for a method not yet registered are
// answered not_found.
func Listen(address string, config ServerConfig) (*Server, error) {
	local, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", local)
	if err != nil {
		return nil, err
	}
	server, err := NewServer(conn, config)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return server, nil
}

// NewServer serves on conn, which it owns from now on.
func NewServer(conn *net.UDPConn, config ServerConfig) (*Server, error) {
	if conn == nil || config.Keys == nil {
		return nil, errors.New("udpx: socket and key ring required")
	}
	if config.CallBudget == 0 {
		config.CallBudget = DefaultCallBudget
	}
	if config.CallBudget < time.Millisecond || config.CallBudget > MaxTimeoutMS*time.Millisecond {
		return nil, errors.New("udpx: call budget must be 1ms..60s")
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = DefaultCacheTTL
	}
	if config.CacheCapacity == 0 {
		config.CacheCapacity = DefaultCacheCapacity
	}
	if config.MaxInFlight == 0 {
		config.MaxInFlight = DefaultMaxInFlight
	}
	if config.CacheTTL < 0 || config.MaxInFlight < 1 || config.CacheCapacity <= config.MaxInFlight {
		return nil, errors.New("udpx: cache ttl must be positive and the cache capacity must exceed the positive in-flight limit")
	}
	s := &Server{conn: conn, keys: config.Keys, budget: config.CallBudget, cache: newReplyCache(config.CacheTTL, config.CacheCapacity, config.MaxInFlight), methods: make(map[string]Handler), loop: make(chan struct{}), drained: make(chan struct{})}
	if config.RateLimit >= 0 {
		if config.RateLimit == 0 {
			config.RateLimit = DefaultRateLimit
		}
		if config.RateBurst == 0 {
			config.RateBurst = DefaultRateBurst
		}
		if config.RateBurst < 1 {
			return nil, errors.New("udpx: rate burst must be positive")
		}
		s.limiter = newLimiter(config.RateLimit, config.RateBurst)
	}
	if _, err := rand.Read(s.instance[:]); err != nil {
		return nil, errors.New("udpx: instance identity unavailable")
	}
	// A burst of requests must not overrun the kernel's default receive buffer.
	_ = conn.SetReadBuffer(1 << 20)
	s.base, s.cancel = context.WithCancel(context.Background())
	go s.serve()
	return s, nil
}

// Handle registers handler for method. A method can be registered once.
func (s *Server) Handle(method string, handler Handler) error {
	if !validMethod(method) || handler == nil {
		return errors.New("udpx: valid method name and handler required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.methods[method]; exists {
		return errors.New("udpx: method already registered")
	}
	s.methods[method] = handler
	return nil
}

// Addr is the local address of the socket.
func (s *Server) Addr() netip.AddrPort {
	local := s.conn.LocalAddr().(*net.UDPAddr).AddrPort()
	return netip.AddrPortFrom(local.Addr().Unmap(), local.Port())
}

// Instance is the random identity of this server process. Replies carry it and
// a caller may pin it.
func (s *Server) Instance() [16]byte { return s.instance }

// InstanceID is Instance in the lowercase hex form of ServiceRef.InstanceID.
func (s *Server) InstanceID() string { return hex.EncodeToString(s.instance[:]) }

// Stats returns a snapshot of the counters.
func (s *Server) Stats() Stats {
	return Stats{
		Received: s.received.Load(), Malformed: s.malformed.Load(), Unauthenticated: s.unauthenticated.Load(), RateLimited: s.rateLimited.Load(),
		Requests: s.requests.Load(), Executed: s.executed.Load(), Duplicates: s.duplicates.Load(), Ignored: s.ignored.Load(), Replies: s.replies.Load(),
		Refused: s.refused.Load(), Abandoned: s.abandoned.Load(), Panics: s.panics.Load(), SendErrors: s.sendErrors.Load(),
		InFlight: int(s.inFlight.Load()), CacheEntries: s.cache.len(),
	}
}

// Drained is closed once the server was shut down or closed and every handler
// and pending request has finished.
func (s *Server) Drained() <-chan struct{} { return s.drained }

// Shutdown stops admitting requests, which are answered unavailable from now on
// (a retransmission of a request already answered still gets its cached reply),
// waits for pending requests to finish within ctx, then closes the socket. When
// ctx ends first the remaining requests are abandoned and ctx's error is
// returned.
func (s *Server) Shutdown(ctx context.Context) error {
	s.admitMu.Lock()
	s.draining = true
	s.admitMu.Unlock()
	finished := make(chan struct{})
	go func() { s.work.Wait(); close(finished) }()
	var err error
	select {
	case <-finished:
	case <-ctx.Done():
		err = ctx.Err()
	}
	s.stopReading()
	<-s.loop
	s.release()
	return err
}

// Close stops the server at once: pending requests are abandoned. Handlers that
// are still running are not interrupted; Drained reports when they returned.
func (s *Server) Close() error {
	s.stopReading()
	<-s.loop
	s.release()
	return nil
}

func (s *Server) stopReading() {
	if s.closing.CompareAndSwap(false, true) {
		_ = s.conn.SetReadDeadline(time.Now())
		go func() {
			<-s.loop
			s.work.Wait()
			close(s.drained)
		}()
	}
}

func (s *Server) release() {
	s.closeIt.Do(func() {
		s.cancel()
		_ = s.conn.Close()
	})
}

func (s *Server) serve() {
	defer close(s.loop)
	buffer := make([]byte, 2*MaxDatagram)
	for {
		n, from, err := s.conn.ReadFromUDPAddrPort(buffer)
		if err != nil {
			if s.closing.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		s.received.Add(1)
		s.dispatch(buffer[:n], netip.AddrPortFrom(from.Addr().Unmap(), from.Port()))
	}
}

func (s *Server) dispatch(datagram []byte, from netip.AddrPort) {
	m, err := parse(datagram)
	if err != nil || m.typ != typeRequest {
		s.malformed.Add(1)
		return
	}
	key, known := s.keys.lookup(m.keyID)
	if !known || !authentic(datagram, key) {
		s.unauthenticated.Add(1)
		return
	}
	now := time.Now()
	if s.limiter != nil && !s.limiter.allow(from.Addr(), now) {
		s.rateLimited.Add(1)
		return
	}
	s.requests.Add(1)
	refuse := func(status Status, message string) {
		s.refused.Add(1)
		reply, _ := s.encodeReply(&m, key, status, errorBody(status, message, nil))
		s.send(from, reply)
	}
	if m.flags&^flagExpectedInstance != 0 || m.word < 1 || m.word > MaxTimeoutMS {
		refuse(StatusInvalidArgument, "reserved flags set or timeout_ms outside 1..60000")
		return
	}
	if m.expectedInstance() && m.instance != s.instance {
		refuse(StatusConflict, "server instance does not match the expected instance")
		return
	}
	id := cacheKey{keyID: m.keyID, id: m.id}
	s.admitMu.Lock()
	if s.draining {
		s.admitMu.Unlock()
		switch outcome, cached := s.cache.peek(id, now); outcome {
		case admitCached:
			s.duplicates.Add(1)
			s.send(from, cached)
		case admitAbandoned:
			s.duplicates.Add(1)
		case admitPending:
			s.ignored.Add(1)
		default:
			refuse(StatusUnavailable, "server is shutting down")
		}
		return
	}
	s.mu.RLock()
	handler := s.methods[string(m.method)]
	s.mu.RUnlock()
	if handler == nil {
		s.admitMu.Unlock()
		refuse(StatusNotFound, "unknown method")
		return
	}
	outcome, entry, cached := s.cache.admit(id, now)
	switch outcome {
	case admitCached:
		s.admitMu.Unlock()
		s.duplicates.Add(1)
		s.send(from, cached)
		return
	case admitAbandoned:
		s.admitMu.Unlock()
		s.duplicates.Add(1)
		return
	case admitPending:
		s.admitMu.Unlock()
		s.ignored.Add(1)
		return
	case admitCacheFull:
		s.admitMu.Unlock()
		refuse(StatusResourceExhausted, "too many requests in flight")
		return
	}
	s.work.Add(2) // the handler goroutine and the pending responder
	s.executed.Add(1)
	s.inFlight.Add(1)
	s.admitMu.Unlock()
	deadline := now.Add(min(time.Duration(m.word)*time.Millisecond, s.budget))
	ctx, cancel := context.WithDeadline(s.base, deadline)
	// The responder outlives the receive buffer that m aliases.
	header := m
	header.method, header.body = nil, nil
	response := &Responder{server: s, entry: entry, request: header, key: key, source: from, deadline: deadline, cancel: cancel}
	// The context ends at the deadline, or sooner when the server closes: either
	// abandons a request nobody answered.
	response.mu.Lock()
	response.watch = context.AfterFunc(ctx, response.expire)
	response.mu.Unlock()
	request := Request{Method: string(m.method), Body: bytes.Clone(m.body), Source: from, KeyID: m.keyID, ID: m.id, Deadline: deadline}
	go s.run(ctx, handler, request, response)
}

func (s *Server) run(ctx context.Context, handler Handler, request Request, response *Responder) {
	defer s.work.Done()
	defer func() {
		if recover() != nil {
			s.panics.Add(1)
			_ = response.Fail(StatusInternal, "handler failed", nil)
		}
	}()
	handler(ctx, request, response)
}

func (s *Server) send(to netip.AddrPort, datagram []byte) {
	if _, err := s.conn.WriteToUDPAddrPort(datagram, to); err != nil {
		s.sendErrors.Add(1)
		return
	}
	s.replies.Add(1)
}

// encodeReply signs a reply to m. A reply that would not fit in one datagram
// is replaced by a short resource_exhausted reply; tooLarge reports that.
func (s *Server) encodeReply(m *message, key []byte, status Status, body []byte) (datagram []byte, tooLarge bool) {
	reply := message{typ: typeReply, keyID: m.keyID, id: m.id, instance: s.instance, word: uint32(status), body: body}
	datagram = appendDatagram(make([]byte, 0, headerLen+len(body)+tagLen), &reply, key)
	if len(datagram) <= MaxDatagram {
		return datagram, false
	}
	reply.word, reply.body = uint32(StatusResourceExhausted), errorBody(StatusResourceExhausted, "reply exceeds the 1200-byte datagram limit", nil)
	return appendDatagram(nil, &reply, key), true
}

// errorBody is the body of a non-zero status: {"code","message","details"}.
// It always fits in one datagram: an oversized message or details are dropped.
func errorBody(status Status, message string, details map[string]any) []byte {
	if details == nil {
		details = map[string]any{}
	}
	body := struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details"`
	}{status.Code(), message, details}
	for _, trim := range []func(){func() {}, func() { body.Details = map[string]any{} }, func() { body.Message = body.Message[:min(len(body.Message), 256)] }} {
		trim()
		if encoded, err := json.Marshal(body); err == nil && len(encoded) <= MaxPayload/2 {
			return encoded
		}
	}
	return []byte(`{"code":"` + status.Code() + `","message":"","details":{}}`)
}

// Responder completes one request: exactly one answer takes effect.
type Responder struct {
	server   *Server
	entry    *cacheEntry
	request  message
	key      []byte
	source   netip.AddrPort
	deadline time.Time
	cancel   context.CancelFunc
	watch    func() bool

	mu      sync.Mutex
	done    bool
	expired bool
}

// Reply answers with status ok and body as the result.
func (r *Responder) Reply(body []byte) error { return r.Respond(StatusOK, body) }

// Fail answers with a non-zero status and an error body carrying the
// status code name, a message and domain details.
func (r *Responder) Fail(status Status, message string, details map[string]any) error {
	if status == StatusOK {
		return errors.New("udpx: Fail needs a non-zero status")
	}
	return r.Respond(status, errorBody(status, message, details))
}

// Respond answers with status and body. Statuses are 0..8 and 10; the
// unauthenticated status is never sent. A reply that does not fit in one
// datagram is replaced by resource_exhausted and ErrReplyTooLarge is returned.
func (r *Responder) Respond(status Status, body []byte) error {
	if status > StatusPermissionDenied || status == StatusUnauthenticated {
		return errors.New("udpx: status cannot be sent")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		if r.expired {
			return ErrDeadlineExceeded
		}
		return ErrAlreadyReplied
	}
	if !time.Now().Before(r.deadline) {
		r.finish(nil)
		return ErrDeadlineExceeded
	}
	reply, tooLarge := r.server.encodeReply(&r.request, r.key, status, body)
	r.finish(reply)
	r.server.send(r.source, reply)
	if tooLarge {
		return ErrReplyTooLarge
	}
	return nil
}

// expire runs when the request context ends: a request nobody answered is
// abandoned, and the client reports an unknown outcome.
func (r *Responder) expire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.done {
		r.finish(nil)
	}
}

// finish records the outcome. reply == nil means the deadline passed. The
// caller holds r.mu.
func (r *Responder) finish(reply []byte) {
	r.done = true
	r.expired = reply == nil
	r.watch()
	r.cancel()
	r.server.cache.complete(r.entry, reply, time.Now())
	if r.expired {
		r.server.abandoned.Add(1)
	}
	r.server.inFlight.Add(-1)
	r.server.work.Done()
}
