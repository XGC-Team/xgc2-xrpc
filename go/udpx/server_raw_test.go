package udpx

import (
	"context"
	"net"
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
	if replies != 3 || runs.Load() != 3 || stats.RateLimited != 7 || stats.Requests != 3 {
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
	server, conn, runs := rawServer(t, ServerConfig{RateLimit: -1})
	for i := 0; i < 300; i++ {
		request := countRequest(byte(i))
		request.id[1] = byte(i >> 8)
		conn.send(appendDatagram(nil, &request, testKey()))
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
	_, conn, runs := rawServer(t, ServerConfig{CacheCapacity: 2})
	var datagrams [3][]byte
	for i := range datagrams {
		request := countRequest(byte(i + 1))
		datagrams[i] = appendDatagram(nil, &request, testKey())
		conn.send(datagrams[i])
		if _, _, got := conn.receive(time.Second); !got {
			t.Fatal("no reply")
		}
	}
	conn.send(datagrams[2]) // still remembered
	conn.receive(time.Second)
	if runs.Load() != 3 {
		t.Fatalf("a remembered request ran again: %d", runs.Load())
	}
	conn.send(datagrams[0]) // evicted by the third request
	conn.receive(time.Second)
	if runs.Load() != 4 {
		t.Fatalf("an evicted request is new again: %d", runs.Load())
	}
}

func TestRequestsBeyondTheCacheCapacityAreRefusedNotDropped(t *testing.T) {
	ring, _ := NewKeyRing(map[uint32][]byte{1: testKey()})
	server, err := Listen("127.0.0.1:0", ServerConfig{Keys: ring, CacheCapacity: 1})
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
		t.Fatalf("a full cache must answer resource_exhausted: %+v %v", m, got)
	}
	close(release)
	if m, _, got := c.receive(time.Second); !got || Status(m.word) != StatusOK {
		t.Fatalf("held request: %+v %v", m, got)
	}
}
