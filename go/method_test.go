package xrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

func TestParseMethod(t *testing.T) {
	for name, want := range map[string][2]string{
		"xgc2.chassis.hold/Engage": {"xgc2.chassis.hold", "Engage"},
		"test.v1/Echo":             {"test.v1", "Echo"},
		"a/b":                      {"a", "b"},
		"Svc_1.Pkg/Do_It2":         {"Svc_1.Pkg", "Do_It2"},
		strings.Repeat("s", 100) + "/" + strings.Repeat("m", 27): {strings.Repeat("s", 100), strings.Repeat("m", 27)},
	} {
		service, method, err := xrpc.ParseMethod(name)
		if err != nil || service != want[0] || method != want[1] {
			t.Errorf("%q: %q %q %v", name, service, method, err)
		}
	}
	for _, bad := range []string{
		"", "/", "Engage", "svc/", "/Engage", "svc/Method/extra", "svc//Method", ".svc/Method", "svc./Method",
		"s..v/Method", "1svc/Method", "svc/1Method", "svc/Meth-od", "svc-x/Method", "sv c/Method", "svc/Method ",
		"svc/Method\n", "svc/Méthod", "svc/Method?x=1", "svc/Method#x", "svc/../Method", "svc/_Method",
		strings.Repeat("s", 100) + "/" + strings.Repeat("m", 28),
	} {
		if _, _, err := xrpc.ParseMethod(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if path, err := xrpc.MethodPath("xgc2.chassis.hold/Engage"); err != nil || path != "/v1/call/xgc2.chassis.hold/Engage" {
		t.Errorf("path %q %v", path, err)
	}
	if _, err := xrpc.MethodPath("Engage"); err == nil {
		t.Error("a path for an invalid name")
	}
}

func methodRef(profile string) xrpc.ServiceRef {
	switch profile {
	case xrpc.HTTP:
		return xrpc.ServiceRef{TargetID: "t", Service: "world", APIVersion: "v1", InstanceID: "boot-1", Profile: profile, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/run/xgc2/world.sock"}}
	case xrpc.GRPC:
		return xrpc.ServiceRef{TargetID: "t", Service: "xgc2.chassis.hold", APIVersion: "v1", InstanceID: "boot-1", Profile: profile, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/run/xgc2/type.sock"}}
	}
	return xrpc.ServiceRef{TargetID: "robot-1", Service: "xgc2.chassis.hold", APIVersion: "v1", Profile: profile, Endpoint: xrpc.Endpoint{Kind: "udp", Address: "10.0.0.7:19530"}}
}

func TestMethodCallMapsEveryProfile(t *testing.T) {
	body := json.RawMessage(`{"robot":"scout-1"}`)
	for profile, want := range map[string]xrpc.Call{
		xrpc.HTTP: {Method: "POST", Path: "/v1/call/xgc2.chassis.hold/Engage"},
		xrpc.GRPC: {Method: "/xgc2.chassis.hold/Engage"},
		xrpc.UDP:  {Method: "xgc2.chassis.hold/Engage"},
	} {
		got, err := xrpc.MethodCall(methodRef(profile), "xgc2.chassis.hold/Engage", "id:1", body)
		want.Service, want.RequestID, want.Payload = methodRef(profile), "id:1", body
		if err != nil || got.Method != want.Method || got.Path != want.Path || got.RequestID != want.RequestID || string(got.Payload) != string(body) || got.Service != want.Service {
			t.Errorf("%s: %+v %v", profile, got, err)
		}
	}
	// An empty body is allowed: a method without parameters.
	if _, err := xrpc.MethodCall(methodRef(xrpc.UDP), "xgc2.chassis.hold/Describe", "id:2", nil); err != nil {
		t.Error(err)
	}
}

func TestMethodCallRejectsInvalidInput(t *testing.T) {
	good := methodRef(xrpc.UDP)
	badRef := good
	badRef.Endpoint.Kind = "unix"
	for name, test := range map[string]struct {
		ref  xrpc.ServiceRef
		name string
		id   string
		body string
	}{
		"reference":  {badRef, "svc/Method", "id:1", `{}`},
		"name":       {good, "Method", "id:1", `{}`},
		"identity":   {good, "svc/Method", "bad id", `{}`},
		"empty id":   {good, "svc/Method", "", `{}`},
		"body":       {good, "svc/Method", "id:1", `{"a":`},
		"body ascii": {good, "svc/Method", "id:1", `not json`},
	} {
		if _, err := xrpc.MethodCall(test.ref, test.name, test.id, json.RawMessage(test.body)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

type recorder struct {
	mu    sync.Mutex
	calls []xrpc.Call
	err   error
}

func (r *recorder) Call(_ context.Context, call xrpc.Call) (xrpc.Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
	return xrpc.Result{Status: 200, Payload: json.RawMessage(`{"ok":true}`)}, r.err
}

func TestCallMethodDispatchesByProfile(t *testing.T) {
	callers := map[string]*recorder{xrpc.HTTP: {}, xrpc.GRPC: {}, xrpc.UDP: {}}
	dispatcher, err := xrpc.NewDispatcher(map[string]xrpc.Caller{xrpc.HTTP: callers[xrpc.HTTP], xrpc.GRPC: callers[xrpc.GRPC], xrpc.UDP: callers[xrpc.UDP]})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ids := map[string]bool{}
	for _, profile := range []string{xrpc.HTTP, xrpc.GRPC, xrpc.UDP} {
		result, err := dispatcher.CallMethod(ctx, methodRef(profile), "xgc2.chassis.hold/State", json.RawMessage(`{}`))
		if err != nil || string(result.Payload) != `{"ok":true}` {
			t.Fatalf("%s: %+v %v", profile, result, err)
		}
		for other, recorded := range callers {
			if want := map[bool]int{true: 1, false: 0}[other == profile]; len(recorded.calls) != want {
				t.Fatalf("%s call reached %s %d times", profile, other, len(recorded.calls))
			}
		}
		id := callers[profile].calls[0].RequestID
		if len(id) != 32 || ids[id] {
			t.Fatalf("%s: request identity %q is not fresh 128-bit hex", profile, id)
		}
		ids[id] = true
		callers[profile].calls = nil
	}
}

func TestCallMethodFailsBeforeSending(t *testing.T) {
	recorded := &recorder{}
	dispatcher, err := xrpc.NewDispatcher(map[string]xrpc.Caller{xrpc.HTTP: recorded, xrpc.UDP: recorded})
	if err != nil {
		t.Fatal(err)
	}
	live, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	expired, cancelExpired := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancelExpired()
	<-expired.Done()
	unpinnedHTTP := methodRef(xrpc.HTTP)
	unpinnedHTTP.InstanceID = ""
	for name, test := range map[string]struct {
		ctx  context.Context
		ref  xrpc.ServiceRef
		name string
		body string
		code string
	}{
		"no deadline":          {context.Background(), methodRef(xrpc.UDP), "svc/Method", `{}`, "invalid_argument"},
		"expired":              {expired, methodRef(xrpc.UDP), "svc/Method", `{}`, "deadline_exceeded"},
		"invalid name":         {live, methodRef(xrpc.UDP), "svc.Method", `{}`, "invalid_argument"},
		"invalid body":         {live, methodRef(xrpc.UDP), "svc/Method", `{`, "invalid_argument"},
		"unpinned http.v1":     {live, unpinnedHTTP, "svc/Method", `{}`, "invalid_argument"},
		"profile not composed": {live, methodRef(xrpc.GRPC), "svc/Method", `{}`, "unavailable"},
	} {
		_, err := dispatcher.CallMethod(test.ctx, test.ref, test.name, json.RawMessage(test.body))
		var failure *xrpc.CallError
		if !errors.As(err, &failure) || failure.Code != test.code || failure.Disposition != xrpc.NotSent {
			t.Errorf("%s: %v", name, err)
		}
	}
	if len(recorded.calls) != 0 {
		t.Fatalf("%d calls reached a caller", len(recorded.calls))
	}
	var nilDispatcher *xrpc.Dispatcher
	if _, err := nilDispatcher.CallMethod(live, methodRef(xrpc.UDP), "svc/Method", nil); xrpc.Code(err) != "unavailable" {
		t.Errorf("a missing dispatcher must fail cleanly: %v", err)
	}
}

func TestNewDispatcherAcceptsExactlyTheThreeProfiles(t *testing.T) {
	if _, err := xrpc.NewDispatcher(map[string]xrpc.Caller{"udp.v9": &recorder{}}); err == nil {
		t.Error("unknown profile accepted")
	}
	if _, err := xrpc.NewDispatcher(map[string]xrpc.Caller{xrpc.UDP: nil}); err == nil {
		t.Error("nil caller accepted")
	}
	if _, err := xrpc.NewDispatcher(map[string]xrpc.Caller{xrpc.UDP: &recorder{}}); err != nil {
		t.Error(err)
	}
}

func TestRequestIdentitiesAreFreshAndCanonical(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 64; i++ {
		id, err := xrpc.NewRequestID()
		if err != nil || len(id) != 32 || !xrpc.ValidID(id) || id != strings.ToLower(id) || seen[id] {
			t.Fatalf("%q %v", id, err)
		}
		seen[id] = true
	}
}

func TestStatusForCode(t *testing.T) {
	for code, want := range map[string]int{
		"ok": 200, "invalid_argument": 400, "unauthenticated": 401, "permission_denied": 403, "not_found": 404,
		"conflict": 409, "resource_exhausted": 429, "cancelled": 499, "internal": 500, "unavailable": 503,
		"deadline_exceeded": 504, "a_domain_code": 500,
	} {
		if got := xrpc.StatusForCode(code); got != want {
			t.Errorf("%s: %d, want %d", code, got, want)
		}
	}
}

func TestDispatcherGivesACallWithoutAnIdentityAFreshOne(t *testing.T) {
	recorded := &recorder{}
	dispatcher, err := xrpc.NewDispatcher(map[string]xrpc.Caller{xrpc.UDP: recorded})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	call := xrpc.Call{Service: methodRef(xrpc.UDP), Method: "svc/Method"}
	for i := 0; i < 2; i++ {
		if _, err := dispatcher.Call(ctx, call); err != nil {
			t.Fatal(err)
		}
	}
	first, second := recorded.calls[0].RequestID, recorded.calls[1].RequestID
	if len(first) != 32 || len(second) != 32 || first == second {
		t.Fatalf("identities %q %q", first, second)
	}
	// The caller's own copy is untouched, and an invalid identity is still refused.
	if call.RequestID != "" {
		t.Fatal("the dispatcher modified the caller's call")
	}
	call.RequestID = "not valid!"
	if _, err := dispatcher.Call(ctx, call); xrpc.Code(err) != "invalid_argument" || len(recorded.calls) != 2 {
		t.Fatalf("invalid identity reached a caller: %v", err)
	}
}
