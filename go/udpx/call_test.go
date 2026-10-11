package udpx_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/udpx"
)

func TestRoundTrip(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	if err := server.Handle("test.v1/Echo", echo); err != nil {
		t.Fatal(err)
	}
	reply, err := newClient(t, ring).Call(within(t, 2*time.Second), ref(server.Addr().String()), "test.v1/Echo", []byte(`{"hello":"robot"}`))
	if err != nil {
		t.Fatal(err)
	}
	if reply.Status != udpx.StatusOK || string(reply.Body) != `{"hello":"robot"}` || reply.Instance != server.Instance() || reply.InstanceID() != server.InstanceID() || reply.Sent != 1 {
		t.Fatalf("reply %+v", reply)
	}
	if stats := server.Stats(); stats.Executed != 1 || stats.Requests < 1 {
		t.Fatalf("stats %+v", stats)
	}
	// The server counts a reply after it has written the datagram, so the caller can have the
	// reply before the counter moves.
	eventually(t, "the reply to be counted", func() bool { return server.Stats().Replies >= 1 })
}

func TestErrorBodiesCarryStatusMessageAndDetails(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	server.Handle("test.v1/Fail", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		var wanted struct{ Status udpx.Status }
		_ = json.Unmarshal(request.Body, &wanted)
		_ = response.Fail(wanted.Status, "synthetic failure", map[string]any{"revision": 12})
	})
	client := newClient(t, ring)
	names := map[udpx.Status]string{1: "invalid_argument", 2: "not_found", 3: "conflict", 4: "resource_exhausted", 5: "deadline_exceeded", 6: "cancelled", 7: "unavailable", 8: "internal", 10: "permission_denied"}
	for status, name := range names {
		reply, err := client.Call(within(t, 2*time.Second), ref(server.Addr().String()), "test.v1/Fail", []byte(`{"Status":`+strconv.Itoa(int(status))+`}`))
		failure := callError(t, err)
		if failure.Code != name || failure.Disposition != xrpc.ResponseReceived || !strings.Contains(failure.Message, "synthetic failure") {
			t.Errorf("status %d: %+v", status, failure)
		}
		var body struct {
			Code    string
			Message string
			Details map[string]float64
		}
		if err := json.Unmarshal(reply.Body, &body); err != nil || reply.Status != status || body.Code != name || body.Message != "synthetic failure" || body.Details["revision"] != 12 {
			t.Errorf("status %d: reply %+v body %s err %v", status, reply, reply.Body, err)
		}
	}
	// The unauthenticated status is never sent.
	server.Handle("test.v1/Unauthenticated", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		if err := response.Fail(udpx.StatusUnauthenticated, "x", nil); err == nil {
			t.Error("unauthenticated status was sent")
		}
		_ = response.Fail(udpx.StatusInternal, "fallback", nil)
	})
	if _, err := client.Call(within(t, 2*time.Second), ref(server.Addr().String()), "test.v1/Unauthenticated", nil); callError(t, err).Code != "internal" {
		t.Fatal(err)
	}
}

func TestUnknownMethodIsNotFound(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	reply, err := newClient(t, ring).Call(within(t, 2*time.Second), ref(server.Addr().String()), "test.v1/Missing", nil)
	if failure := callError(t, err); failure.Code != "not_found" || failure.Disposition != xrpc.ResponseReceived || reply.Status != udpx.StatusNotFound {
		t.Fatalf("%+v %+v", failure, reply)
	}
	if stats := server.Stats(); stats.Executed != 0 || stats.Refused < 1 {
		t.Fatalf("stats %+v", stats)
	}
}

// Retransmissions of a running request are ignored, the handler runs once, and
// the same id sent again after completion gets the cached reply.
func TestRetransmissionExecutesAtMostOnce(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	var runs atomic.Int32
	release := make(chan struct{})
	server.Handle("test.v1/Count", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		n := runs.Add(1)
		<-release
		_ = response.Reply([]byte(strconv.Itoa(int(n))))
	})
	time.AfterFunc(150*time.Millisecond, func() { close(release) })
	client := newClient(t, ring)
	target := ref(server.Addr().String())
	reply, err := client.Call(within(t, 3*time.Second), target, "test.v1/Count", nil)
	if err != nil || string(reply.Body) != "1" {
		t.Fatalf("reply %+v err %v", reply, err)
	}
	if reply.Sent < 4 {
		t.Fatalf("a 150 ms wait should have needed retransmissions, sent %d", reply.Sent)
	}
	stats := server.Stats()
	if runs.Load() != 1 || stats.Executed != 1 || stats.Ignored < 3 {
		t.Fatalf("runs=%d stats=%+v", runs.Load(), stats)
	}
	again, err := client.Call(within(t, 2*time.Second), target, "test.v1/Count", nil, udpx.WithRequestID(reply.ID))
	if err != nil || string(again.Body) != "1" || runs.Load() != 1 {
		t.Fatalf("replayed id: reply %+v err %v runs %d", again, err, runs.Load())
	}
	if stats := server.Stats(); stats.Duplicates < 1 || stats.Executed != 1 {
		t.Fatalf("stats %+v", stats)
	}
	// A fresh id is a new request.
	if third, err := client.Call(within(t, 2*time.Second), target, "test.v1/Count", nil); err != nil || string(third.Body) != "2" {
		t.Fatalf("fresh id: %+v %v", third, err)
	}
}

func TestHandlerMayReplyAfterItReturns(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	server.Handle("test.v1/Later", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		time.AfterFunc(60*time.Millisecond, func() {
			if err := response.Reply([]byte(`"late"`)); err != nil {
				t.Errorf("late reply: %v", err)
			}
			if err := response.Reply([]byte(`"twice"`)); !errors.Is(err, udpx.ErrAlreadyReplied) {
				t.Errorf("second reply: %v", err)
			}
		})
	})
	reply, err := newClient(t, ring).Call(within(t, 2*time.Second), ref(server.Addr().String()), "test.v1/Later", nil)
	if err != nil || string(reply.Body) != `"late"` {
		t.Fatalf("reply %+v err %v", reply, err)
	}
	eventually(t, "responder to finish", func() bool { return server.Stats().InFlight == 0 })
}

func TestMissedDeadlineProducesNoReply(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring, CallBudget: 80 * time.Millisecond})
	late := make(chan error, 1)
	var runs atomic.Int32
	server.Handle("test.v1/Slow", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		runs.Add(1)
		<-ctx.Done()
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Errorf("handler context ended with %v", ctx.Err())
		}
		late <- response.Reply([]byte(`"too late"`))
	})
	started := time.Now()
	_, err := newClient(t, ring).Call(within(t, 400*time.Millisecond), ref(server.Addr().String()), "test.v1/Slow", nil)
	failure := callError(t, err)
	if failure.Code != "deadline_exceeded" || failure.Disposition != xrpc.OutcomeUnknown {
		t.Fatalf("%+v", failure)
	}
	if elapsed := time.Since(started); elapsed < 350*time.Millisecond {
		t.Fatalf("client gave up after %s", elapsed)
	}
	if err := <-late; !errors.Is(err, udpx.ErrDeadlineExceeded) {
		t.Fatalf("late reply: %v", err)
	}
	// Retransmissions after the deadline must not run the request again.
	stats := server.Stats()
	if runs.Load() != 1 || stats.Abandoned != 1 || stats.Replies != 0 || stats.Duplicates == 0 {
		t.Fatalf("runs=%d stats=%+v", runs.Load(), stats)
	}
}

func TestServerDeadlineIsReceiptPlusTimeoutCappedByBudget(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring, CallBudget: 500 * time.Millisecond})
	deadlines := make(chan time.Duration, 2)
	server.Handle("test.v1/Deadline", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		deadline, ok := ctx.Deadline()
		if !ok || !deadline.Equal(request.Deadline) {
			t.Errorf("context deadline %v does not match the request deadline %v", deadline, request.Deadline)
		}
		deadlines <- time.Until(request.Deadline)
		_ = response.Reply(nil)
	})
	client := newClient(t, ring)
	if _, err := client.Call(within(t, 300*time.Millisecond), ref(server.Addr().String()), "test.v1/Deadline", nil); err != nil {
		t.Fatal(err)
	}
	if remaining := <-deadlines; remaining < 50*time.Millisecond || remaining > 300*time.Millisecond {
		t.Fatalf("short client budget: server deadline in %s", remaining)
	}
	if _, err := client.Call(within(t, 5*time.Second), ref(server.Addr().String()), "test.v1/Deadline", nil); err != nil {
		t.Fatal(err)
	}
	if remaining := <-deadlines; remaining < 300*time.Millisecond || remaining > 500*time.Millisecond {
		t.Fatalf("long client budget must be capped by the server budget: server deadline in %s", remaining)
	}
}

func TestWrongKeyAndUnknownKeyAreSilentAndOutcomeUnknown(t *testing.T) {
	serverRing := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: serverRing})
	var runs atomic.Int32
	server.Handle("test.v1/Echo", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		runs.Add(1)
		_ = response.Reply(request.Body)
	})
	for name, ring := range map[string]*udpx.KeyRing{
		"same id, other key": ringOf(t, keyID),
		"unknown id":         ringOf(t, keyID, 9),
	} {
		target := ref(server.Addr().String())
		if name == "unknown id" {
			target.KeyID = 9
		}
		_, err := newClient(t, ring).Call(within(t, 150*time.Millisecond), target, "test.v1/Echo", []byte("{}"))
		failure := callError(t, err)
		if failure.Code != "deadline_exceeded" || failure.Disposition != xrpc.OutcomeUnknown {
			t.Errorf("%s: %+v", name, failure)
		}
	}
	if stats := server.Stats(); runs.Load() != 0 || stats.Unauthenticated < 2 || stats.Replies != 0 || stats.Requests != 0 {
		t.Fatalf("runs=%d stats=%+v", runs.Load(), stats)
	}
}

func TestKeySelection(t *testing.T) {
	k3, k4 := randomKey(t), randomKey(t)
	ring, err := udpx.NewKeyRing(map[uint32][]byte{3: k3, 4: k4})
	if err != nil {
		t.Fatal(err)
	}
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	server.Handle("test.v1/Echo", echo)
	client := newClient(t, ring)
	addr := server.Addr().String()
	for _, id := range []uint32{3, 4} {
		if _, err := client.Call(within(t, time.Second), ref(addr, func(r *xrpc.ServiceRef) { r.KeyID = id }), "test.v1/Echo", nil); err != nil {
			t.Errorf("key %d: %v", id, err)
		}
	}
	for name, target := range map[string]xrpc.ServiceRef{
		"key not in the ring":            ref(addr, func(r *xrpc.ServiceRef) { r.KeyID = 5 }),
		"no key named, two keys in ring": ref(addr, func(r *xrpc.ServiceRef) { r.KeyID = 0 }),
	} {
		_, err := client.Call(within(t, time.Second), target, "test.v1/Echo", nil)
		if failure := callError(t, err); failure.Code != "invalid_argument" || failure.Disposition != xrpc.NotSent {
			t.Errorf("%s: %+v", name, failure)
		}
	}
	// A one-key ring is used for a reference that names no key.
	single, err := udpx.NewKeyRing(map[uint32][]byte{3: k3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newClient(t, single).Call(within(t, time.Second), ref(addr, func(r *xrpc.ServiceRef) { r.KeyID = 0 }), "test.v1/Echo", nil); err != nil {
		t.Fatal(err)
	}
}

func TestPinnedInstanceAndServerRestart(t *testing.T) {
	ring := ringOf(t, keyID)
	first := startServer(t, udpx.ServerConfig{Keys: ring})
	first.Handle("test.v1/Echo", echo)
	address := first.Addr().String()
	client := newClient(t, ring)
	pinned := ref(address, func(r *xrpc.ServiceRef) { r.InstanceID = first.InstanceID() })
	if reply, err := client.Call(within(t, time.Second), pinned, "test.v1/Echo", []byte("1")); err != nil || reply.Instance != first.Instance() {
		t.Fatalf("pinned to the live instance: %+v %v", reply, err)
	}
	// A wrong pin is refused before the handler runs, and surfaces as conflict.
	wrong := ref(address, func(r *xrpc.ServiceRef) { r.InstanceID = strings.Repeat("ab", 16) })
	if _, err := client.Call(within(t, time.Second), wrong, "test.v1/Echo", []byte("1")); callError(t, err).Code != "conflict" {
		t.Fatalf("wrong pin: %v", err)
	}
	if stats := first.Stats(); stats.Executed != 1 || stats.Refused < 1 {
		t.Fatalf("stats %+v", stats)
	}

	// The robot restarts on the same port with a new instance.
	first.Close()
	second, err := udpx.Listen(address, udpx.ServerConfig{Keys: ring})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close() })
	var secondRuns atomic.Int32
	second.Handle("test.v1/Echo", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		secondRuns.Add(1)
		_ = response.Reply(request.Body)
	})
	if second.InstanceID() == first.InstanceID() {
		t.Fatal("restart kept the instance")
	}
	_, err = client.Call(within(t, time.Second), pinned, "test.v1/Echo", []byte("2"))
	failure := callError(t, err)
	if failure.Code != "conflict" || failure.Disposition != xrpc.OutcomeUnknown || secondRuns.Load() != 0 {
		t.Fatalf("stale pin after restart: %+v runs %d", failure, secondRuns.Load())
	}
	unpinned, err := client.Call(within(t, time.Second), ref(address), "test.v1/Echo", []byte("3"))
	if err != nil || unpinned.InstanceID() != second.InstanceID() {
		t.Fatalf("unpinned discovery: %+v %v", unpinned, err)
	}
	repinned := ref(address, func(r *xrpc.ServiceRef) { r.InstanceID = unpinned.InstanceID() })
	if _, err := client.Call(within(t, time.Second), repinned, "test.v1/Echo", []byte("4")); err != nil || secondRuns.Load() != 2 {
		t.Fatalf("pin learned from a reply: %v runs %d", err, secondRuns.Load())
	}
}

func TestReplyTooLargeBecomesResourceExhausted(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	results := make(chan error, 2)
	server.Handle("test.v1/Big", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		var wanted int
		_ = json.Unmarshal(request.Body, &wanted)
		results <- response.Reply(make([]byte, wanted))
	})
	client := newClient(t, ring)
	largest := udpx.MaxDatagram - 52 - 32
	reply, err := client.Call(within(t, time.Second), ref(server.Addr().String()), "test.v1/Big", []byte(strconv.Itoa(largest)))
	if err != nil || len(reply.Body) != largest || <-results != nil {
		t.Fatalf("a reply that exactly fits must be delivered: %v", err)
	}
	reply, err = client.Call(within(t, time.Second), ref(server.Addr().String()), "test.v1/Big", []byte(strconv.Itoa(largest+1)))
	failure := callError(t, err)
	if failure.Code != "resource_exhausted" || failure.Disposition != xrpc.ResponseReceived || reply.Status != udpx.StatusResourceExhausted || len(reply.Body) > 200 {
		t.Fatalf("oversized reply: %+v %+v", failure, reply)
	}
	if err := <-results; !errors.Is(err, udpx.ErrReplyTooLarge) {
		t.Fatalf("handler must learn that its reply was replaced: %v", err)
	}
}

func TestRequestTooLargeIsNotSent(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	server.Handle("test.v1/Echo", echo)
	client := newClient(t, ring)
	method := "test.v1/Echo"
	fits := make([]byte, udpx.MaxPayload-len(method))
	if reply, err := client.Call(within(t, time.Second), ref(server.Addr().String()), method, fits); err != nil || len(reply.Body) != len(fits) {
		t.Fatalf("largest request: %v", err)
	}
	_, err := client.Call(within(t, time.Second), ref(server.Addr().String()), method, make([]byte, len(fits)+1))
	if failure := callError(t, err); failure.Code != "resource_exhausted" || failure.Disposition != xrpc.NotSent {
		t.Fatalf("%+v", failure)
	}
	// An oversized datagram would be counted malformed (a retransmission of the
	// first call is legitimate on a slow machine).
	if stats := server.Stats(); stats.Malformed != 0 || stats.Executed != 1 {
		t.Fatalf("an oversized request reached the wire: %+v", stats)
	}
}

func TestCallPreconditionsAreNotSent(t *testing.T) {
	ring := ringOf(t, keyID)
	client := newClient(t, ring)
	address := "127.0.0.1:9"
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	cancelled, cancel := context.WithCancel(within(t, time.Second))
	cancel()
	cases := map[string]struct {
		ctx    context.Context
		target xrpc.ServiceRef
		method string
		code   string
	}{
		"no deadline":       {context.Background(), ref(address), "test.v1/Echo", "invalid_argument"},
		"expired deadline":  {expired, ref(address), "test.v1/Echo", "deadline_exceeded"},
		"cancelled":         {cancelled, ref(address), "test.v1/Echo", "cancelled"},
		"empty method":      {within(t, time.Second), ref(address), "", "invalid_argument"},
		"method with space": {within(t, time.Second), ref(address), "test.v1/E cho", "invalid_argument"},
		"http profile":      {within(t, time.Second), xrpc.ServiceRef{TargetID: "t", Service: "s", APIVersion: "v", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/run/x.sock"}}, "test.v1/Echo", "invalid_argument"},
		"invalid endpoint":  {within(t, time.Second), ref("no-port"), "test.v1/Echo", "invalid_argument"},
		"unresolvable host": {within(t, time.Second), ref("256.1.1.1:9"), "test.v1/Echo", "unavailable"},
	}
	for name, c := range cases {
		_, err := client.Call(c.ctx, c.target, c.method, nil)
		if failure := callError(t, err); failure.Code != c.code || failure.Disposition != xrpc.NotSent {
			t.Errorf("%s: %+v", name, failure)
		}
	}
}

// The default schedule: the same datagram at 0, then after waits of 30, 60,
// 120 and 240 ms, then every 250 ms until the deadline.
func TestRetransmissionSchedule(t *testing.T) {
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	type arrival struct {
		at   time.Time
		data []byte
	}
	var mu sync.Mutex
	var arrived []arrival
	listening := make(chan struct{})
	go func() {
		defer close(listening)
		buffer := make([]byte, 2048)
		for {
			n, _, err := sink.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			mu.Lock()
			arrived = append(arrived, arrival{time.Now(), append([]byte(nil), buffer[:n]...)})
			mu.Unlock()
		}
	}()
	client, err := udpx.NewClient(udpx.ClientConfig{Keys: ringOf(t, keyID)})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = client.Call(within(t, 1300*time.Millisecond), ref(sink.LocalAddr().String()), "test.v1/Echo", []byte(`{}`))
	if failure := callError(t, err); failure.Code != "deadline_exceeded" || failure.Disposition != xrpc.OutcomeUnknown {
		t.Fatalf("%+v", failure)
	}
	sink.Close()
	<-listening
	var offsets []time.Duration
	var first []byte
	for _, a := range arrived {
		if first == nil {
			first = a.data
		} else if string(a.data) != string(first) {
			t.Fatal("a retransmission differs from the first datagram")
		}
		offsets = append(offsets, a.at.Sub(started))
	}
	want := []time.Duration{0, 30, 90, 210, 450, 700, 950, 1200}
	if len(offsets) < len(want)-2 || len(offsets) > len(want)+1 {
		t.Fatalf("datagram offsets %v", offsets)
	}
	for i, ms := range want {
		if i >= len(offsets) {
			break
		}
		if diff := offsets[i] - ms*time.Millisecond; diff < -15*time.Millisecond || diff > 250*time.Millisecond {
			t.Errorf("datagram %d at %s, want about %dms (all: %v)", i, offsets[i], ms, offsets)
		}
	}
	// timeout_ms carries the remaining budget when the first datagram was sent.
	if timeout := int(first[44])<<24 | int(first[45])<<16 | int(first[46])<<8 | int(first[47]); timeout < 1200 || timeout > 1300 {
		t.Fatalf("timeout_ms=%d", timeout)
	}
}

func TestCancellationInterruptsTheWait(t *testing.T) {
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	time.AfterFunc(60*time.Millisecond, cancel)
	started := time.Now()
	_, err = newClient(t, ringOf(t, keyID)).Call(ctx, ref(sink.LocalAddr().String()), "test.v1/Echo", nil)
	failure := callError(t, err)
	if failure.Code != "cancelled" || failure.Disposition != xrpc.OutcomeUnknown || !errors.Is(err, context.Canceled) {
		t.Fatalf("%+v", failure)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("cancellation took %s", elapsed)
	}
}

func TestConcurrentCalls(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring, RateLimit: -1, MaxInFlight: 200})
	var runs atomic.Int32
	server.Handle("test.v1/Count", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		_ = response.Reply([]byte(strconv.Itoa(int(runs.Add(1)))))
	})
	client := newClient(t, ring)
	var wg sync.WaitGroup
	seen := make(chan string, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reply, err := client.Call(within(t, 5*time.Second), ref(server.Addr().String()), "test.v1/Count", nil)
			if err != nil {
				t.Error(err)
				return
			}
			seen <- string(reply.Body)
		}()
	}
	wg.Wait()
	close(seen)
	distinct := map[string]bool{}
	for body := range seen {
		distinct[body] = true
	}
	if len(distinct) != 100 || runs.Load() != 100 {
		t.Fatalf("%d distinct results, %d executions", len(distinct), runs.Load())
	}
}

func TestShutdownLetsPendingRequestsFinish(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	server.Handle("test.v1/Slow", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		time.AfterFunc(150*time.Millisecond, func() { _ = response.Reply([]byte(`"done"`)) })
	})
	result := make(chan error, 1)
	var reply udpx.Reply
	go func() {
		var err error
		reply, err = newClient(t, ring).Call(within(t, 3*time.Second), ref(server.Addr().String()), "test.v1/Slow", nil)
		result <- err
	}()
	eventually(t, "the request to be pending", func() bool { return server.Stats().InFlight == 1 })
	started := time.Now()
	if err := server.Shutdown(within(t, 2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 100*time.Millisecond {
		t.Fatalf("Shutdown returned after %s without waiting for the pending request", elapsed)
	}
	if err := <-result; err != nil || string(reply.Body) != `"done"` {
		t.Fatalf("pending request lost: %+v %v", reply, err)
	}
	select {
	case <-server.Drained():
	case <-time.After(time.Second):
		t.Fatal("not drained after Shutdown")
	}
	if _, err := newClient(t, ring).Call(within(t, 100*time.Millisecond), ref(server.Addr().String()), "test.v1/Slow", nil); callError(t, err).Disposition != xrpc.OutcomeUnknown {
		t.Fatalf("a closed server answered: %v", err)
	}
}

func TestShutdownBudgetAbandonsStuckRequests(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	returned := make(chan struct{})
	server.Handle("test.v1/Stuck", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		<-ctx.Done()
		close(returned)
	})
	go newClient(t, ring).Call(within(t, time.Second), ref(server.Addr().String()), "test.v1/Stuck", nil)
	eventually(t, "the request to be pending", func() bool { return server.Stats().InFlight == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("the handler context was not canceled")
	}
	select {
	case <-server.Drained():
	case <-time.After(time.Second):
		t.Fatal("pending request was not abandoned")
	}
	if stats := server.Stats(); stats.Abandoned != 1 || stats.InFlight != 0 {
		t.Fatalf("stats %+v", stats)
	}
}

func TestPanickingHandlerIsAnsweredInternal(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	server.Handle("test.v1/Panic", func(context.Context, udpx.Request, *udpx.Responder) { panic("boom") })
	server.Handle("test.v1/Echo", echo)
	client := newClient(t, ring)
	_, err := client.Call(within(t, time.Second), ref(server.Addr().String()), "test.v1/Panic", nil)
	if failure := callError(t, err); failure.Code != "internal" || failure.Disposition != xrpc.ResponseReceived {
		t.Fatalf("%+v", failure)
	}
	if _, err := client.Call(within(t, time.Second), ref(server.Addr().String()), "test.v1/Echo", nil); err != nil {
		t.Fatalf("server did not survive the panic: %v", err)
	}
	if server.Stats().Panics != 1 {
		t.Fatalf("stats %+v", server.Stats())
	}
}

func TestRegistration(t *testing.T) {
	server := startServer(t, udpx.ServerConfig{Keys: ringOf(t, keyID)})
	if err := server.Handle("test.v1/Echo", echo); err != nil {
		t.Fatal(err)
	}
	if err := server.Handle("test.v1/Echo", echo); err == nil {
		t.Error("duplicate method accepted")
	}
	for _, name := range []string{"", "has space", strings.Repeat("m", 129)} {
		if err := server.Handle(name, echo); err == nil {
			t.Errorf("method %q accepted", name)
		}
	}
	if err := server.Handle("test.v1/Nil", nil); err == nil {
		t.Error("nil handler accepted")
	}
	if _, err := udpx.Listen("127.0.0.1:0", udpx.ServerConfig{}); err == nil {
		t.Error("server without keys accepted")
	}
	for _, config := range []udpx.ServerConfig{{CallBudget: time.Microsecond}, {CallBudget: 61 * time.Second}, {CacheTTL: -1}, {CacheCapacity: -1}, {RateBurst: -1}, {MaxInFlight: -1}, {MaxInFlight: 10, CacheCapacity: 10}, {CacheCapacity: udpx.DefaultMaxInFlight}} {
		config.Keys = ringOf(t, keyID)
		if s, err := udpx.Listen("127.0.0.1:0", config); err == nil {
			s.Close()
			t.Errorf("config %+v accepted", config)
		}
	}
}

func TestIPv6Loopback(t *testing.T) {
	ring := ringOf(t, keyID)
	server, err := udpx.Listen("[::1]:0", udpx.ServerConfig{Keys: ring})
	if err != nil {
		t.Skipf("no IPv6 loopback on this host: %v", err)
	}
	defer server.Close()
	server.Handle("test.v1/Echo", echo)
	reply, err := newClient(t, ring).Call(within(t, 2*time.Second), ref(server.Addr().String()), "test.v1/Echo", []byte("v6"))
	if err != nil || string(reply.Body) != "v6" || !server.Addr().Addr().Is6() {
		t.Fatalf("%+v %v (server %s)", reply, err, server.Addr())
	}
}
