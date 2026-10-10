package udpx_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/udpx"
)

func newProfile(t testing.TB, ring *udpx.KeyRing) *udpx.Profile {
	t.Helper()
	profile, err := udpx.NewProfile(newClient(t, ring))
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestProfileReturnsTheResultAndTheAnsweringInstance(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	server.Handle("test.v1/Echo", echo)
	server.Handle("test.v1/Empty", func(ctx context.Context, request udpx.Request, response *udpx.Responder) { _ = response.Reply(nil) })
	profile := newProfile(t, ring)
	// The reference leaves the instance empty: the answer reveals it.
	call := xrpc.Call{Service: ref(server.Addr().String()), Method: "test.v1/Echo", RequestID: "call:1", Payload: json.RawMessage(`{"robot":"scout-1"}`)}
	result, err := profile.Call(within(t, 2*time.Second), call)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != 200 || string(result.Payload) != `{"robot":"scout-1"}` || result.InstanceID != server.InstanceID() {
		t.Fatalf("result %+v", result)
	}
	call.Method, call.RequestID, call.Payload = "test.v1/Empty", "call:2", nil
	if result, err = profile.Call(within(t, 2*time.Second), call); err != nil || string(result.Payload) != "null" {
		t.Fatalf("an empty reply is the JSON null: %+v %v", result, err)
	}
	// Pinned to the instance it just learned, the call still works.
	call.Service.InstanceID = server.InstanceID()
	if _, err = profile.Call(within(t, 2*time.Second), call); err != nil {
		t.Fatal(err)
	}
}

func TestProfileCarriesErrorsInTheHTTPEnvelope(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	server.Handle("test.v1/Conflict", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		_ = response.Fail(udpx.StatusConflict, "stale revision", map[string]any{"revision": 12})
	})
	server.Handle("test.v1/Plain", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		_ = response.Respond(udpx.StatusPermissionDenied, []byte("denied"))
	})
	profile := newProfile(t, ring)
	call := xrpc.Call{Service: ref(server.Addr().String()), Method: "test.v1/Conflict", RequestID: "call:3"}
	result, err := profile.Call(within(t, 2*time.Second), call)
	failure := callError(t, err)
	if failure.Code != "conflict" || failure.Disposition != xrpc.ResponseReceived {
		t.Fatalf("failure %+v", failure)
	}
	var envelope struct {
		Error struct {
			Code    string
			Message string
			Details struct{ Revision int }
		}
	}
	if err := json.Unmarshal(result.Payload, &envelope); err != nil || envelope.Error.Code != "conflict" || envelope.Error.Message != "stale revision" || envelope.Error.Details.Revision != 12 {
		t.Fatalf("payload %s: %v", result.Payload, err)
	}
	if result.Status != 409 || result.InstanceID != server.InstanceID() {
		t.Fatalf("result %+v", result)
	}
	// A body that is not an object still produces a valid envelope.
	call.Method, call.RequestID = "test.v1/Plain", "call:4"
	result, err = profile.Call(within(t, 2*time.Second), call)
	if failure := callError(t, err); failure.Code != "permission_denied" || result.Status != 403 {
		t.Fatalf("result %+v failure %+v", result, failure)
	}
	if err := json.Unmarshal(result.Payload, &envelope); err != nil || envelope.Error.Code != "permission_denied" || envelope.Error.Message != "denied" {
		t.Fatalf("payload %s: %v", result.Payload, err)
	}
}

func TestProfileMapsRequestIdentities(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	var mu sync.Mutex
	var seen []udpx.RequestID
	var executed atomic.Int32
	server.Handle("test.v1/Count", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		mu.Lock()
		seen = append(seen, request.ID)
		mu.Unlock()
		executed.Add(1)
		_ = response.Reply([]byte("1"))
	})
	profile := newProfile(t, ring)
	call := xrpc.Call{Service: ref(server.Addr().String()), Method: "test.v1/Count"}
	// 32 lowercase hex digits are the request id itself.
	call.RequestID = "00112233445566778899aabbccddeeff"
	if _, err := profile.Call(within(t, 2*time.Second), call); err != nil {
		t.Fatal(err)
	}
	// Any other identity maps to the first 128 bits of its SHA-256, and the
	// same identity is the same request to the server's reply cache.
	call.RequestID = "operation:42"
	for i := 0; i < 2; i++ {
		if _, err := profile.Call(within(t, 2*time.Second), call); err != nil {
			t.Fatal(err)
		}
	}
	sum := sha256.Sum256([]byte("operation:42"))
	var hashed udpx.RequestID
	copy(hashed[:], sum[:])
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[0].String() != "00112233445566778899aabbccddeeff" || seen[1] != hashed {
		t.Fatalf("server saw %v", seen)
	}
	if executed.Load() != 2 {
		t.Fatalf("the repeated identity executed %d times in total, want once for each distinct identity", executed.Load())
	}
}

func TestProfileRejectsWhatIsNotJSON(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	server.Handle("test.v1/Text", func(ctx context.Context, request udpx.Request, response *udpx.Responder) {
		_ = response.Reply([]byte("plain text"))
	})
	server.Handle("test.v1/Echo", echo)
	profile := newProfile(t, ring)
	call := xrpc.Call{Service: ref(server.Addr().String()), Method: "test.v1/Echo", RequestID: "call:5", Payload: json.RawMessage(`{"open":`)}
	if _, err := profile.Call(within(t, 2*time.Second), call); callError(t, err).Disposition != xrpc.NotSent || xrpc.Code(err) != "invalid_argument" {
		t.Fatalf("a payload that is not JSON must not be sent: %v", err)
	}
	if server.Stats().Received != 0 {
		t.Fatal("the invalid payload reached the server")
	}
	call.Method, call.Payload, call.RequestID = "test.v1/Text", nil, "call:6"
	if _, err := profile.Call(within(t, 2*time.Second), call); callError(t, err).Disposition != xrpc.ResponseReceived || xrpc.Code(err) != "internal" {
		t.Fatalf("a reply that is not JSON is an internal error: %v", err)
	}
	if _, err := udpx.NewProfile(nil); err == nil {
		t.Fatal("a profile without a client was accepted")
	}
}

func TestDispatcherComposesUDPForMethodCalls(t *testing.T) {
	ring := ringOf(t, keyID)
	server := startServer(t, udpx.ServerConfig{Keys: ring})
	server.Handle("xgc2.chassis.hold/Engage", echo)
	dispatcher, err := xrpc.NewDispatcher(map[string]xrpc.Caller{xrpc.UDP: newProfile(t, ring)})
	if err != nil {
		t.Fatal(err)
	}
	// An unpinned udp.v1 reference works; the answer names the instance.
	result, err := dispatcher.CallMethod(within(t, 2*time.Second), ref(server.Addr().String(), func(r *xrpc.ServiceRef) { r.Service = "xgc2.chassis.hold" }), "xgc2.chassis.hold/Engage", json.RawMessage(`{"robot":"a"}`))
	if err != nil || string(result.Payload) != `{"robot":"a"}` || result.InstanceID != server.InstanceID() {
		t.Fatalf("result %+v err %v", result, err)
	}
	// A wrong instance pin is the common conflict, with an unknown outcome.
	pinned := ref(server.Addr().String(), func(r *xrpc.ServiceRef) { r.InstanceID = "0123456789abcdef0123456789abcdef" })
	_, err = dispatcher.CallMethod(within(t, 2*time.Second), pinned, "xgc2.chassis.hold/Engage", nil)
	if failure := callError(t, err); failure.Code != "conflict" || failure.Disposition != xrpc.OutcomeUnknown {
		t.Fatalf("failure %+v", failure)
	}
	// The dispatcher has no http.v1 caller.
	httpRef := xrpc.ServiceRef{TargetID: "t", Service: "s", APIVersion: "v1", InstanceID: "i", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/run/xgc2/x.sock"}}
	if _, err = dispatcher.CallMethod(within(t, 2*time.Second), httpRef, "xgc2.chassis.hold/Engage", nil); xrpc.Code(err) != "unavailable" || callError(t, err).Disposition != xrpc.NotSent {
		t.Fatalf("an uncomposed profile must fail before sending: %v", err)
	}
}
