package udpx_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/udpx"
	"github.com/XGC-Team/xgc2-xrpc/go/udpx/udptest"
)

// countingServer counts executions of test.v1/Count and replies with the count
// after the execution, so a reply identifies the execution that produced it.
func countingServer(t *testing.T, ring *udpx.KeyRing, config udpx.ServerConfig) (*udpx.Server, *atomic.Int32) {
	t.Helper()
	config.Keys = ring
	server := startServer(t, config)
	var runs atomic.Int32
	if err := server.Handle("test.v1/Count", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		_ = response.Reply([]byte(strconv.Itoa(int(runs.Add(1)))))
	}); err != nil {
		t.Fatal(err)
	}
	return server, &runs
}

func proxyTo(t *testing.T, server *udpx.Server, config udptest.Config) *udptest.Proxy {
	t.Helper()
	proxy, err := udptest.NewProxy(server.Addr().String(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { proxy.Close() })
	return proxy
}

// Thirty percent loss in both directions, duplication and reordering: every
// call still succeeds and every request executes exactly once, however many
// copies of it the network delivers.
func TestLossDuplicationAndReorderingExecuteEachRequestOnce(t *testing.T) {
	ring := ringOf(t, keyID)
	server, runs := countingServer(t, ring, udpx.ServerConfig{RateLimit: -1})
	faults := udptest.Faults{Loss: 0.3, Duplicate: 0.3, Reorder: 0.3, ReorderDelay: 15 * time.Millisecond, Delay: time.Millisecond, Jitter: 4 * time.Millisecond}
	proxy := proxyTo(t, server, udptest.Config{Seed: 20261010, Requests: faults, Replies: faults})
	client := newClient(t, ring)
	target := ref(proxy.Addr())

	const workers, calls = 8, 12
	results := make(chan string, workers*calls)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < calls; i++ {
				reply, err := client.Call(within(t, 10*time.Second), target, "test.v1/Count", nil)
				if err != nil {
					t.Errorf("call failed through the lossy network: %v", err)
					return
				}
				results <- string(reply.Body)
			}
		}()
	}
	wg.Wait()
	close(results)
	distinct := map[string]bool{}
	for body := range results {
		if distinct[body] {
			t.Fatalf("execution %s answered two calls", body)
		}
		distinct[body] = true
	}
	if runs.Load() != workers*calls || len(distinct) != workers*calls {
		t.Fatalf("%d executions, %d distinct replies for %d calls", runs.Load(), len(distinct), workers*calls)
	}
	stats, injected := server.Stats(), proxy.Stats()
	if injected.Requests.Dropped == 0 || injected.Requests.Duplicated == 0 || injected.Requests.Reordered == 0 || injected.Replies.Dropped == 0 {
		t.Fatalf("the network was not hostile: %+v", injected)
	}
	if stats.Executed != workers*calls || stats.Duplicates+stats.Ignored == 0 {
		t.Fatalf("server stats %+v", stats)
	}
}

func TestLostRepliesAreServedFromTheCache(t *testing.T) {
	ring := ringOf(t, keyID)
	server, runs := countingServer(t, ring, udpx.ServerConfig{})
	proxy := proxyTo(t, server, udptest.Config{})
	proxy.DropReplies(3)
	reply, err := newClient(t, ring).Call(within(t, 3*time.Second), ref(proxy.Addr()), "test.v1/Count", nil)
	if err != nil || string(reply.Body) != "1" {
		t.Fatalf("%+v %v", reply, err)
	}
	if reply.Sent < 4 || runs.Load() != 1 {
		t.Fatalf("sent %d datagrams, handler ran %d times", reply.Sent, runs.Load())
	}
	if stats := server.Stats(); stats.Duplicates < 3 || stats.Executed != 1 {
		t.Fatalf("stats %+v", stats)
	}
}

// A request whose reply never arrives has an unknown outcome, but the effect
// happened once; retrying under the same id recovers the reply without a second
// effect.
func TestBlackholedRepliesLeaveTheOutcomeUnknownUntilTheIdIsRetried(t *testing.T) {
	ring := ringOf(t, keyID)
	server, runs := countingServer(t, ring, udpx.ServerConfig{})
	proxy := proxyTo(t, server, udptest.Config{})
	client := newClient(t, ring)
	target := ref(proxy.Addr())

	proxy.BlackholeReplies(true)
	id, err := udpx.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Call(within(t, 200*time.Millisecond), target, "test.v1/Count", nil, udpx.WithRequestID(id))
	if failure := callError(t, err); failure.Code != "deadline_exceeded" || failure.Disposition != xrpc.OutcomeUnknown {
		t.Fatalf("%+v", failure)
	}
	if runs.Load() != 1 {
		t.Fatalf("the request should have executed once, ran %d times", runs.Load())
	}
	proxy.BlackholeReplies(false)
	reply, err := client.Call(within(t, 2*time.Second), target, "test.v1/Count", nil, udpx.WithRequestID(id))
	if err != nil || string(reply.Body) != "1" || runs.Load() != 1 {
		t.Fatalf("recovery under the same id: %+v %v runs %d", reply, err, runs.Load())
	}
}

func TestBlackholedRequestsNeverReachTheHandler(t *testing.T) {
	ring := ringOf(t, keyID)
	server, runs := countingServer(t, ring, udpx.ServerConfig{})
	proxy := proxyTo(t, server, udptest.Config{})
	proxy.BlackholeRequests(true)
	_, err := newClient(t, ring).Call(within(t, 150*time.Millisecond), ref(proxy.Addr()), "test.v1/Count", nil)
	if failure := callError(t, err); failure.Code != "deadline_exceeded" || failure.Disposition != xrpc.OutcomeUnknown {
		t.Fatalf("%+v", failure)
	}
	if runs.Load() != 0 || server.Stats().Received != 0 {
		t.Fatalf("runs=%d stats=%+v", runs.Load(), server.Stats())
	}
	proxy.BlackholeRequests(false)
	if _, err := newClient(t, ring).Call(within(t, time.Second), ref(proxy.Addr()), "test.v1/Count", nil); err != nil {
		t.Fatal(err)
	}
}

// Both directions are slower than the caller's budget: the caller gives up, the
// request executes anyway, and the outcome stays unknown to the caller.
func TestDelayBeyondTheDeadline(t *testing.T) {
	ring := ringOf(t, keyID)
	server, runs := countingServer(t, ring, udpx.ServerConfig{})
	proxy := proxyTo(t, server, udptest.Config{Requests: udptest.Faults{Delay: 120 * time.Millisecond}, Replies: udptest.Faults{Delay: 120 * time.Millisecond}})
	_, err := newClient(t, ring).Call(within(t, 200*time.Millisecond), ref(proxy.Addr()), "test.v1/Count", nil)
	if failure := callError(t, err); failure.Code != "deadline_exceeded" || failure.Disposition != xrpc.OutcomeUnknown {
		t.Fatalf("%+v", failure)
	}
	eventually(t, "the late request to execute", func() bool { return runs.Load() == 1 })
}

// A pinned call can never execute on a different instance, even when the reply
// of the first execution is lost and the robot restarts before the
// retransmission arrives: the new instance refuses the stale pin.
func TestPinnedRetransmissionIsRefusedByARestartedServer(t *testing.T) {
	ring := ringOf(t, keyID)
	first, firstRuns := countingServer(t, ring, udpx.ServerConfig{})
	proxy := proxyTo(t, first, udptest.Config{})
	proxy.BlackholeReplies(true)
	address := first.Addr().String()
	pinned := ref(proxy.Addr(), func(r *xrpc.ServiceRef) { r.InstanceID = first.InstanceID() })

	result := make(chan error, 1)
	go func() {
		_, err := newClient(t, ring).Call(within(t, 3*time.Second), pinned, "test.v1/Count", nil)
		result <- err
	}()
	eventually(t, "the first instance to execute", func() bool { return firstRuns.Load() == 1 })

	first.Close()
	second, err := udpx.Listen(address, udpx.ServerConfig{Keys: ring})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close() })
	var secondRuns atomic.Int32
	second.Handle("test.v1/Count", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		secondRuns.Add(1)
		_ = response.Reply(nil)
	})
	proxy.BlackholeReplies(false)

	failure := callError(t, <-result)
	if failure.Code != "conflict" || failure.Disposition != xrpc.OutcomeUnknown {
		t.Fatalf("%+v", failure)
	}
	if firstRuns.Load() != 1 || secondRuns.Load() != 0 || second.Stats().Refused == 0 {
		t.Fatalf("first=%d second=%d stats=%+v", firstRuns.Load(), secondRuns.Load(), second.Stats())
	}
}
