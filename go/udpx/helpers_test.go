package udpx_test

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/udpx"
)

const keyID = 7

// fastClient retransmits quickly so that tests run in milliseconds while
// exercising the same code path as the default schedule.
var fastBackoff = []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}

const fastInterval = 25 * time.Millisecond

func randomKey(t testing.TB) []byte {
	t.Helper()
	key := make([]byte, udpx.KeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

func ringOf(t testing.TB, ids ...uint32) *udpx.KeyRing {
	t.Helper()
	keys := map[uint32][]byte{}
	for _, id := range ids {
		keys[id] = randomKey(t)
	}
	ring, err := udpx.NewKeyRing(keys)
	if err != nil {
		t.Fatal(err)
	}
	return ring
}

func startServer(t testing.TB, config udpx.ServerConfig) *udpx.Server {
	t.Helper()
	server, err := udpx.Listen("127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return server
}

func newClient(t testing.TB, ring *udpx.KeyRing) *udpx.Client {
	t.Helper()
	client, err := udpx.NewClient(udpx.ClientConfig{Keys: ring, Backoff: fastBackoff, Interval: fastInterval})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func ref(address string, mutate ...func(*xrpc.ServiceRef)) xrpc.ServiceRef {
	r := xrpc.ServiceRef{TargetID: "robot-1", Service: "test.v1", APIVersion: "v1", Profile: xrpc.UDP, KeyID: keyID, Endpoint: xrpc.Endpoint{Kind: "udp", Address: address}}
	for _, edit := range mutate {
		edit(&r)
	}
	return r
}

func within(t testing.TB, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func callError(t testing.TB, err error) *xrpc.CallError {
	t.Helper()
	var failure *xrpc.CallError
	if !errors.As(err, &failure) {
		t.Fatalf("expected *xrpc.CallError, got %T: %v", err, err)
	}
	return failure
}

func echo(ctx context.Context, request udpx.Request, response *udpx.Responder) {
	_ = response.Reply(request.Body)
}

// eventually polls cond for up to two seconds.
func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}
