package udptest

import (
	"net"
	"net/netip"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// sink is an upstream that records what reaches it and can answer.
type sink struct {
	conn *net.UDPConn
	mu   sync.Mutex
	got  []received
}

type received struct {
	payload string
	from    netip.AddrPort
	at      time.Time
}

func newSink(t *testing.T, echo bool) *sink {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	s := &sink{conn: conn}
	t.Cleanup(func() { conn.Close() })
	go func() {
		buffer := make([]byte, 2048)
		for {
			n, from, err := conn.ReadFromUDPAddrPort(buffer)
			if err != nil {
				return
			}
			s.mu.Lock()
			s.got = append(s.got, received{string(buffer[:n]), from, time.Now()})
			s.mu.Unlock()
			if echo {
				_, _ = conn.WriteToUDPAddrPort(append([]byte("re:"), buffer[:n]...), from)
			}
		}
	}()
	return s
}

func (s *sink) payloads() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.got))
	for i, r := range s.got {
		out[i] = r.payload
	}
	return out
}

func (s *sink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func (s *sink) addr() string { return s.conn.LocalAddr().String() }

func dial(t *testing.T, proxy *Proxy) *net.UDPConn {
	t.Helper()
	target, err := net.ResolveUDPAddr("udp", proxy.Addr())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func newProxy(t *testing.T, upstream string, config Config) *Proxy {
	t.Helper()
	proxy, err := NewProxy(upstream, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { proxy.Close() })
	return proxy
}

func settle(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRelaysBothDirectionsPerClient(t *testing.T) {
	upstream := newSink(t, true)
	proxy := newProxy(t, upstream.addr(), Config{})
	a, b := dial(t, proxy), dial(t, proxy)
	buffer := make([]byte, 64)
	for name, conn := range map[string]*net.UDPConn{"a": a, "b": b} {
		if _, err := conn.Write([]byte("hello " + name)); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		n, err := conn.Read(buffer)
		if err != nil || string(buffer[:n]) != "re:hello "+name {
			t.Fatalf("client %s: %q %v", name, buffer[:n], err)
		}
	}
	upstream.mu.Lock()
	distinct := map[netip.AddrPort]bool{}
	for _, r := range upstream.got {
		distinct[r.from] = true
	}
	upstream.mu.Unlock()
	if len(distinct) != 2 {
		t.Fatalf("the server must see one source per client, saw %d", len(distinct))
	}
	settle(t, "counters", func() bool { return proxy.Stats().Replies.Delivered == 2 })
	if s := proxy.Stats(); s.Requests.Seen != 2 || s.Requests.Delivered != 2 || s.Replies.Seen != 2 || s.Requests.Dropped != 0 {
		t.Fatalf("stats %+v", s)
	}
	if err := proxy.Close(); err != nil {
		t.Fatal(err)
	}
	if err := proxy.Close(); err != nil {
		t.Fatal("second Close must be harmless")
	}
}

func deliveredSet(t *testing.T, seed uint64, count int) []string {
	t.Helper()
	upstream := newSink(t, false)
	proxy := newProxy(t, upstream.addr(), Config{Seed: seed, Requests: Faults{Loss: 0.4}})
	conn := dial(t, proxy)
	for i := 0; i < count; i++ {
		if _, err := conn.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	settle(t, "the proxy to see every datagram", func() bool { return proxy.Stats().Requests.Seen == uint64(count) })
	settle(t, "the survivors to arrive", func() bool { return uint64(upstream.count()) == proxy.Stats().Requests.Delivered })
	time.Sleep(20 * time.Millisecond)
	got := upstream.payloads()
	sort.Strings(got)
	return got
}

func TestLossIsDeterministicForASeed(t *testing.T) {
	first := deliveredSet(t, 42, 200)
	again := deliveredSet(t, 42, 200)
	other := deliveredSet(t, 43, 200)
	if len(first) < 80 || len(first) > 160 {
		t.Fatalf("40%% loss delivered %d of 200", len(first))
	}
	if len(first) != len(again) {
		t.Fatalf("same seed, %d then %d survivors", len(first), len(again))
	}
	for i := range first {
		if first[i] != again[i] {
			t.Fatal("same seed made different choices")
		}
	}
	same := len(first) == len(other)
	for i := 0; same && i < len(first); i++ {
		same = first[i] == other[i]
	}
	if same {
		t.Fatal("different seeds made identical choices")
	}
}

func TestDuplicationDelayAndReordering(t *testing.T) {
	upstream := newSink(t, false)
	proxy := newProxy(t, upstream.addr(), Config{Requests: Faults{Duplicate: 1}})
	conn := dial(t, proxy)
	for i := 0; i < 5; i++ {
		conn.Write([]byte(strconv.Itoa(i)))
	}
	settle(t, "ten deliveries", func() bool { return upstream.count() == 10 })
	if s := proxy.Stats().Requests; s.Duplicated != 5 || s.Delivered != 10 {
		t.Fatalf("stats %+v", s)
	}

	delayed := newSink(t, false)
	slow := newProxy(t, delayed.addr(), Config{Requests: Faults{Delay: 60 * time.Millisecond, Jitter: 20 * time.Millisecond}})
	started := time.Now()
	dial(t, slow).Write([]byte("slow"))
	settle(t, "delayed delivery", func() bool { return delayed.count() == 1 })
	delayed.mu.Lock()
	at := delayed.got[0].at
	delayed.mu.Unlock()
	if elapsed := at.Sub(started); elapsed < 55*time.Millisecond || elapsed > 400*time.Millisecond {
		t.Fatalf("delivered after %s, want 60..80ms", elapsed)
	}

	late := newSink(t, false)
	reordering := newProxy(t, late.addr(), Config{Requests: Faults{Reorder: 1, ReorderDelay: 80 * time.Millisecond}})
	conn = dial(t, reordering)
	conn.Write([]byte("first"))
	settle(t, "the first datagram to be seen", func() bool { return reordering.Stats().Requests.Seen == 1 })
	if err := reordering.SetFaults(Faults{}, Faults{}); err != nil {
		t.Fatal(err)
	}
	conn.Write([]byte("second"))
	settle(t, "both datagrams", func() bool { return late.count() == 2 })
	if got := late.payloads(); got[0] != "second" || got[1] != "first" {
		t.Fatalf("arrival order %v, want the held-back datagram last", got)
	}
	if reordering.Stats().Requests.Reordered != 1 {
		t.Fatalf("stats %+v", reordering.Stats())
	}
}

func TestBlackholesAndScriptedLoss(t *testing.T) {
	upstream := newSink(t, true)
	proxy := newProxy(t, upstream.addr(), Config{})
	conn := dial(t, proxy)
	buffer := make([]byte, 64)
	exchange := func(payload string) bool {
		conn.Write([]byte(payload))
		_ = conn.SetReadDeadline(time.Now().Add(80 * time.Millisecond))
		_, err := conn.Read(buffer)
		return err == nil
	}
	proxy.BlackholeRequests(true)
	if exchange("a") || upstream.count() != 0 {
		t.Fatal("a blackholed request got through")
	}
	proxy.BlackholeRequests(false)
	proxy.DropRequests(2)
	if exchange("b") || exchange("c") || !exchange("d") || upstream.count() != 1 {
		t.Fatalf("scripted request loss: upstream saw %v", upstream.payloads())
	}
	proxy.BlackholeReplies(true)
	if exchange("e") || upstream.count() != 2 {
		t.Fatal("a blackholed reply got through, or the request did not reach the server")
	}
	proxy.BlackholeReplies(false)
	proxy.DropReplies(1)
	if exchange("f") || !exchange("g") {
		t.Fatal("scripted reply loss")
	}
	s := proxy.Stats()
	if s.Requests.Dropped != 3 || s.Replies.Dropped != 2 {
		t.Fatalf("stats %+v", s)
	}
}

func TestSessionSurvivesUpstreamRestart(t *testing.T) {
	first := newSink(t, true)
	address := first.addr()
	proxy := newProxy(t, address, Config{})
	conn := dial(t, proxy)
	buffer := make([]byte, 64)
	conn.Write([]byte("one"))
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(buffer); err != nil {
		t.Fatal(err)
	}
	first.conn.Close()
	conn.Write([]byte("into the void")) // the refusal must not kill the session
	time.Sleep(30 * time.Millisecond)
	restarted, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort(address)))
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	go func() {
		raw := make([]byte, 64)
		n, from, err := restarted.ReadFromUDPAddrPort(raw)
		if err == nil {
			restarted.WriteToUDPAddrPort(append([]byte("re:"), raw[:n]...), from)
		}
	}()
	conn.Write([]byte("two"))
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := conn.Read(buffer); err != nil || string(buffer[:n]) != "re:two" {
		t.Fatalf("%q %v", buffer[:n], err)
	}
}

func TestInvalidFaultsAreRejected(t *testing.T) {
	upstream := newSink(t, false)
	for _, faults := range []Faults{{Loss: -0.1}, {Duplicate: 1.5}, {Reorder: 2}, {Delay: -1}, {Jitter: -1}, {ReorderDelay: -1}} {
		if p, err := NewProxy(upstream.addr(), Config{Requests: faults}); err == nil {
			p.Close()
			t.Errorf("%+v accepted", faults)
		}
	}
	proxy := newProxy(t, upstream.addr(), Config{})
	if err := proxy.SetFaults(Faults{}, Faults{Loss: 3}); err == nil {
		t.Error("SetFaults accepted an invalid probability")
	}
}
