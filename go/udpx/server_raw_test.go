package udpx

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Raw-datagram tests: they speak the wire format directly, without Client, to
// pin down what the server does with datagrams no well-behaved client sends.

type raw struct {
	t      *testing.T
	conn   *net.UDPConn
	server *Server
}

func rawServer(t *testing.T, config ServerConfig) (*Server, *raw, *atomic.Int32) {
	t.Helper()
	ring, err := NewKeyRing(map[uint32][]byte{1: testKey()})
	if err != nil {
		t.Fatal(err)
	}
	config.Keys = ring
	server, err := Listen("127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	var runs atomic.Int32
	if err := server.Handle("test.v1/Count", func(ctx context.Context, request Request, response *Responder) {
		_ = response.Reply([]byte(strconv.Itoa(int(runs.Add(1)))))
	}); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(server.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return server, &raw{t: t, conn: conn, server: server}, &runs
}

func (r *raw) send(datagram []byte) {
	r.t.Helper()
	if _, err := r.conn.Write(datagram); err != nil {
		r.t.Fatal(err)
	}
}

func (r *raw) receive(wait time.Duration) (message, []byte, bool) {
	r.t.Helper()
	_ = r.conn.SetReadDeadline(time.Now().Add(wait))
	buffer := make([]byte, 2048)
	n, err := r.conn.Read(buffer)
	if err != nil {
		return message{}, nil, false
	}
	datagram := buffer[:n]
	m, err := parse(datagram)
	if err != nil || !authentic(datagram, testKey()) {
		r.t.Fatalf("server sent an invalid reply: %v", err)
	}
	return m, datagram, true
}

func countRequest(n byte) message {
	return message{typ: typeRequest, keyID: 1, id: RequestID{n}, word: 1000, method: []byte("test.v1/Count")}
}

func TestMalformedAndUnauthenticatedDatagramsAreDroppedSilently(t *testing.T) {
	server, conn, runs := rawServer(t, ServerConfig{})
	valid := appendDatagram(nil, &message{typ: typeRequest, keyID: 1, id: RequestID{1}, word: 1000, method: []byte("test.v1/Count")}, testKey())
	flipped := func(offset int) []byte {
		d := append([]byte(nil), valid...)
		d[offset] ^= 0x01
		return d
	}
	otherKey := append([]byte(nil), testKey()...)
	otherKey[3] ^= 0xff
	reply := appendDatagram(nil, &message{typ: typeReply, keyID: 1, id: RequestID{1}}, testKey())
	malformed := map[string][]byte{
		"empty":                    {},
		"garbage":                  []byte("GET / HTTP/1.1\r\n\r\n"),
		"truncated":                valid[:len(valid)-1],
		"bad magic":                flipped(0),
		"bad version":              flipped(4),
		"a reply sent to a server": reply,
		"oversized":                append(append([]byte(nil), valid...), make([]byte, MaxDatagram)...),
	}
	unauthenticated := map[string][]byte{
		"flipped tag":    flipped(len(valid) - 1),
		"flipped body":   flipped(headerLen),
		"flipped id":     flipped(12),
		"other key":      appendDatagram(nil, &message{typ: typeRequest, keyID: 1, id: RequestID{2}, word: 1000, method: []byte("test.v1/Count")}, otherKey),
		"unknown key id": appendDatagram(nil, &message{typ: typeRequest, keyID: 2, id: RequestID{3}, word: 1000, method: []byte("test.v1/Count")}, testKey()),
	}
	for name, datagram := range malformed {
		conn.send(datagram)
		if _, _, got := conn.receive(40 * time.Millisecond); got {
			t.Errorf("%s: the server answered", name)
		}
	}
	for name, datagram := range unauthenticated {
		conn.send(datagram)
		if _, _, got := conn.receive(40 * time.Millisecond); got {
			t.Errorf("%s: the server answered", name)
		}
	}
	stats := server.Stats()
	if stats.Malformed != uint64(len(malformed)) || stats.Unauthenticated != uint64(len(unauthenticated)) || stats.Executed != 0 || runs.Load() != 0 || stats.Replies != 0 {
		t.Fatalf("stats %+v", stats)
	}
	conn.send(valid)
	if m, _, got := conn.receive(time.Second); !got || Status(m.word) != StatusOK || string(m.body) != "1" {
		t.Fatalf("the server stopped answering valid requests: %+v %v", m, got)
	}
}

func TestReplyIsSignedAndCarriesTheServerInstance(t *testing.T) {
	server, conn, _ := rawServer(t, ServerConfig{})
	request := countRequest(9)
	conn.send(appendDatagram(nil, &request, testKey()))
	m, _, got := conn.receive(time.Second)
	if !got || m.typ != typeReply || m.keyID != 1 || m.id != request.id || m.instance != server.Instance() || m.flags != 0 || len(m.method) != 0 || Status(m.word) != StatusOK {
		t.Fatalf("reply %+v", m)
	}
	other, _, _ := rawServer(t, ServerConfig{})
	if other.Instance() == server.Instance() {
		t.Fatal("two servers share an instance")
	}
	if server.Instance() == ([16]byte{}) {
		t.Fatal("zero instance")
	}
}

func TestRequestFieldsAreValidatedAfterAuthentication(t *testing.T) {
	server, conn, runs := rawServer(t, ServerConfig{})
	for _, timeout := range []uint32{0, MaxTimeoutMS + 1, 1 << 31} {
		request := countRequest(byte(timeout))
		request.word = timeout
		conn.send(appendDatagram(nil, &request, testKey()))
		m, _, got := conn.receive(time.Second)
		if !got || Status(m.word) != StatusInvalidArgument {
			t.Errorf("timeout_ms=%d: %+v %v", timeout, m, got)
		}
	}
	// The expected-instance flag with an instance that is not the server's.
	request := countRequest(100)
	request.flags = flagExpectedInstance
	conn.send(appendDatagram(nil, &request, testKey()))
	if m, _, got := conn.receive(time.Second); !got || Status(m.word) != StatusConflict || m.instance != server.Instance() {
		t.Fatalf("zero pin: %+v %v", m, got)
	}
	// With the flag clear the instance field is ignored.
	request = countRequest(101)
	request.instance = [16]byte{1}
	conn.send(appendDatagram(nil, &request, testKey()))
	if m, _, got := conn.receive(time.Second); !got || Status(m.word) != StatusOK {
		t.Fatalf("unpinned request: %+v %v", m, got)
	}
	if runs.Load() != 1 || server.Stats().Refused != 4 {
		t.Fatalf("runs=%d stats=%+v", runs.Load(), server.Stats())
	}
}

func TestPerSourceRateLimitDropsTheExcess(t *testing.T) {
	server, conn, runs := rawServer(t, ServerConfig{RateLimit: 5, RateBurst: 3})
	for i := byte(1); i <= 10; i++ {
		request := countRequest(i)
		conn.send(appendDatagram(nil, &request, testKey()))
	}
	replies := 0
	for {
		if _, _, got := conn.receive(150 * time.Millisecond); !got {
			break
		}
		replies++
	}
	stats := server.Stats()
	// The burst is 3; a slow machine may earn a token or two while sending.
	if replies < 3 || replies > 5 || int(runs.Load()) != replies || int(stats.RateLimited) != 10-replies || int(stats.Requests) != replies {
		t.Fatalf("replies=%d runs=%d stats=%+v", replies, runs.Load(), stats)
	}
	time.Sleep(250 * time.Millisecond) // 5 tokens per second: one token is back
	request := countRequest(11)
	conn.send(appendDatagram(nil, &request, testKey()))
	if _, _, got := conn.receive(time.Second); !got {
		t.Fatal("the bucket did not refill")
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRateLimitCanBeDisabled(t *testing.T) {
	server, conn, runs := rawServer(t, ServerConfig{RateLimit: -1, MaxInFlight: 400})
	for i := 0; i < 300; i++ {
		request := countRequest(byte(i))
		request.id[1] = byte(i >> 8)
		conn.send(appendDatagram(nil, &request, testKey()))
		if i%25 == 24 {
			time.Sleep(time.Millisecond) // do not overrun the loopback socket buffer
		}
	}
	waitFor(t, "300 executions", func() bool { return runs.Load() == 300 })
	if server.Stats().RateLimited != 0 {
		t.Fatal("the disabled limiter dropped a request")
	}
}

func TestReplayIsServedFromTheCacheOnlyWithinTheWindow(t *testing.T) {
	_, conn, runs := rawServer(t, ServerConfig{CacheTTL: 150 * time.Millisecond})
	request := countRequest(1)
	datagram := appendDatagram(nil, &request, testKey())
	conn.send(datagram)
	_, first, got := conn.receive(time.Second)
	if !got {
		t.Fatal("no reply")
	}
	conn.send(datagram)
	_, second, got := conn.receive(time.Second)
	if !got || string(first) != string(second) || runs.Load() != 1 {
		t.Fatalf("the replay must return the cached reply bytes without executing: runs=%d", runs.Load())
	}
	// The transport does not prevent replay beyond the cache window; domains
	// fence non-idempotent methods with instance and revision.
	time.Sleep(200 * time.Millisecond)
	conn.send(datagram)
	if m, _, got := conn.receive(time.Second); !got || string(m.body) != "2" || runs.Load() != 2 {
		t.Fatalf("after the window the request is new: runs=%d", runs.Load())
	}
}

func TestCacheCapacityBoundsDeduplication(t *testing.T) {
	_, conn, runs := rawServer(t, ServerConfig{CacheCapacity: 3, MaxInFlight: 2})
	var datagrams [4][]byte
	for i := range datagrams {
		request := countRequest(byte(i + 1))
		datagrams[i] = appendDatagram(nil, &request, testKey())
		conn.send(datagrams[i])
		if _, _, got := conn.receive(time.Second); !got {
			t.Fatal("no reply")
		}
	}
	conn.send(datagrams[3]) // still remembered
	conn.receive(time.Second)
	if runs.Load() != 4 {
		t.Fatalf("a remembered request ran again: %d", runs.Load())
	}
	conn.send(datagrams[0]) // evicted by the fourth request
	conn.receive(time.Second)
	if runs.Load() != 5 {
		t.Fatalf("an evicted request is new again: %d", runs.Load())
	}
}

func TestRequestsBeyondTheInFlightLimitAreRefusedNotDropped(t *testing.T) {
	ring, _ := NewKeyRing(map[uint32][]byte{1: testKey()})
	server, err := Listen("127.0.0.1:0", ServerConfig{Keys: ring, MaxInFlight: 1, CacheCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	release := make(chan struct{})
	server.Handle("test.v1/Hold", func(ctx context.Context, request Request, response *Responder) {
		<-release
		_ = response.Reply(nil)
	})
	conn, _ := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(server.Addr()))
	defer conn.Close()
	c := &raw{t: t, conn: conn, server: server}
	hold := message{typ: typeRequest, keyID: 1, id: RequestID{1}, word: 1000, method: []byte("test.v1/Hold")}
	c.send(appendDatagram(nil, &hold, testKey()))
	waitFor(t, "the first request to be pending", func() bool { return server.Stats().InFlight == 1 })
	second := hold
	second.id = RequestID{2}
	c.send(appendDatagram(nil, &second, testKey()))
	if m, _, got := c.receive(time.Second); !got || Status(m.word) != StatusResourceExhausted || m.id != second.id {
		t.Fatalf("a full server must answer resource_exhausted: %+v %v", m, got)
	}
	close(release)
	if m, _, got := c.receive(time.Second); !got || Status(m.word) != StatusOK {
		t.Fatalf("held request: %+v %v", m, got)
	}
}

func TestReservedFlagsAreRefusedAfterAuthentication(t *testing.T) {
	server, conn, runs := rawServer(t, ServerConfig{})
	request := countRequest(1)
	request.flags = 0x0002
	conn.send(appendDatagram(nil, &request, testKey()))
	if m, _, got := conn.receive(time.Second); !got || Status(m.word) != StatusInvalidArgument {
		t.Fatalf("%+v %v", m, got)
	}
	// Unauthenticated, the same datagram is dropped silently.
	conn.send(appendDatagram(nil, &request, make([]byte, KeyLen)))
	if _, _, got := conn.receive(60 * time.Millisecond); got {
		t.Fatal("an unauthenticated request was answered")
	}
	if runs.Load() != 0 || server.Stats().Refused != 1 {
		t.Fatalf("runs=%d stats=%+v", runs.Load(), server.Stats())
	}
}

// While draining the server still answers what it has already answered, ignores
// what is running, and refuses new requests with unavailable.
func TestDrainingServerRefusesNewRequestsButServesTheCache(t *testing.T) {
	ring, _ := NewKeyRing(map[uint32][]byte{1: testKey()})
	server, err := Listen("127.0.0.1:0", ServerConfig{Keys: ring})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	release := make(chan struct{})
	var runs atomic.Int32
	server.Handle("test.v1/Count", func(ctx context.Context, request Request, response *Responder) {
		runs.Add(1)
		_ = response.Reply([]byte("done"))
	})
	server.Handle("test.v1/Hold", func(ctx context.Context, request Request, response *Responder) {
		<-release
		_ = response.Reply([]byte("held"))
	})
	conn, _ := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(server.Addr()))
	defer conn.Close()
	c := &raw{t: t, conn: conn, server: server}
	answered := countRequest(1)
	answeredDatagram := appendDatagram(nil, &answered, testKey())
	c.send(answeredDatagram)
	if _, _, got := c.receive(time.Second); !got {
		t.Fatal("no reply")
	}
	hold := message{typ: typeRequest, keyID: 1, id: RequestID{2}, word: 5000, method: []byte("test.v1/Hold")}
	holdDatagram := appendDatagram(nil, &hold, testKey())
	c.send(holdDatagram)
	waitFor(t, "the held request to run", func() bool { return server.Stats().InFlight == 1 })

	done := make(chan error, 1)
	go func() { done <- server.Shutdown(context.Background()) }()
	// Probe with a method nobody registered: not_found until draining begins.
	probes := byte(10)
	waitFor(t, "the server to start draining", func() bool {
		probes++
		next := message{typ: typeRequest, keyID: 1, id: RequestID{probes}, word: 1000, method: []byte("test.v1/Missing")}
		c.send(appendDatagram(nil, &next, testKey()))
		m, _, got := c.receive(50 * time.Millisecond)
		return got && Status(m.word) == StatusUnavailable
	})
	fresh := countRequest(3)
	c.send(appendDatagram(nil, &fresh, testKey()))
	if m, _, got := c.receive(time.Second); !got || Status(m.word) != StatusUnavailable {
		t.Fatalf("a new request while draining: %+v %v", m, got)
	}
	if runs.Load() != 1 {
		t.Fatalf("a request was executed while draining: %d", runs.Load())
	}
	c.send(answeredDatagram) // answered before: the cache still serves it
	if m, _, got := c.receive(time.Second); !got || Status(m.word) != StatusOK || string(m.body) != "done" {
		t.Fatalf("cached reply while draining: %+v %v", m, got)
	}
	c.send(holdDatagram) // running: ignored
	if _, _, got := c.receive(60 * time.Millisecond); got {
		t.Fatal("a running request was answered twice")
	}
	select {
	case <-done:
		t.Fatal("Shutdown returned with a request still running")
	default:
	}
	close(release)
	if m, _, got := c.receive(time.Second); !got || string(m.body) != "held" {
		t.Fatalf("the held request must finish during the drain: %+v %v", m, got)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// The receive path takes arbitrary bytes from the network: it must neither
// panic nor execute anything for datagrams that are not authentic requests.
func FuzzDispatch(f *testing.F) {
	ring, err := NewKeyRing(map[uint32][]byte{1: testKey()})
	if err != nil {
		f.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		f.Fatal(err)
	}
	server, err := NewServer(conn, ServerConfig{Keys: ring, RateLimit: -1})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { server.Close() })
	var runs atomic.Int32
	server.Handle("test.v1/Count", func(ctx context.Context, request Request, response *Responder) {
		runs.Add(1)
		_ = response.Reply(nil)
	})
	valid := countRequest(1)
	f.Add(appendDatagram(nil, &valid, testKey()))
	f.Add([]byte("garbage"))
	source := netip.MustParseAddrPort("127.0.0.1:9") // discard port: replies go nowhere
	f.Fuzz(func(t *testing.T, datagram []byte) {
		before := runs.Load()
		server.dispatch(append([]byte(nil), datagram...), source)
		if m, err := parse(datagram); (err != nil || !authentic(datagram, testKey())) && runs.Load() != before {
			t.Fatalf("a datagram that is not an authentic request was executed: %+v", m)
		}
	})
}
