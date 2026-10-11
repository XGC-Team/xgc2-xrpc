package xrpc_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

func ref(profile, kind, address string) xrpc.ServiceRef {
	return xrpc.ServiceRef{TargetID: "robot-1", Service: "xgc2.chassis.hold", APIVersion: "v1", Profile: profile, Endpoint: xrpc.Endpoint{Kind: kind, Address: address}}
}

func TestServiceRefProfileAndEndpointPairs(t *testing.T) {
	const instance = "0123456789abcdef0123456789abcdef"
	valid := map[string]xrpc.ServiceRef{
		"http unix":            ref(xrpc.HTTP, "unix", "/run/xgc2/hold.sock"),
		"http https":           ref(xrpc.HTTP, "https", "https://core.example:8443"),
		"grpc unix":            ref(xrpc.GRPC, "unix", "/run/xgc2/adapter.sock"),
		"grpc tls":             ref(xrpc.GRPC, "tls", "core.example:9092"),
		"udp ipv4":             ref(xrpc.UDP, "udp", "192.168.1.20:19520"),
		"udp ipv6":             ref(xrpc.UDP, "udp", "[fe80::1%eth0]:19520"),
		"udp ipv6 loopback":    ref(xrpc.UDP, "udp", "[::1]:9"),
		"udp host name":        ref(xrpc.UDP, "udp", "scout-01.local:19520"),
		"udp highest port":     ref(xrpc.UDP, "udp", "10.0.0.1:65535"),
		"udp pinned instance":  func() xrpc.ServiceRef { r := ref(xrpc.UDP, "udp", "10.0.0.1:1"); r.InstanceID = instance; return r }(),
		"udp key identity":     func() xrpc.ServiceRef { r := ref(xrpc.UDP, "udp", "10.0.0.1:1"); r.KeyID = 7; return r }(),
		"http pinned instance": func() xrpc.ServiceRef { r := ref(xrpc.HTTP, "unix", "/run/a.sock"); r.InstanceID = "boot:1"; return r }(),
	}
	for name, r := range valid {
		if err := r.Validate(); err != nil {
			t.Errorf("%s rejected: %v", name, err)
		}
	}
	invalid := map[string]xrpc.ServiceRef{
		"http tls":          ref(xrpc.HTTP, "tls", "core.example:9092"),
		"http udp":          ref(xrpc.HTTP, "udp", "10.0.0.1:1"),
		"grpc https":        ref(xrpc.GRPC, "https", "https://core.example"),
		"grpc udp":          ref(xrpc.GRPC, "udp", "10.0.0.1:1"),
		"udp unix":          ref(xrpc.UDP, "unix", "/run/a.sock"),
		"udp tls":           ref(xrpc.UDP, "tls", "10.0.0.1:1"),
		"unknown profile":   ref("udp.v9", "udp", "10.0.0.1:1"),
		"udp no port":       ref(xrpc.UDP, "udp", "192.168.1.20"),
		"udp no host":       ref(xrpc.UDP, "udp", ":19520"),
		"udp port zero":     ref(xrpc.UDP, "udp", "10.0.0.1:0"),
		"udp port too high": ref(xrpc.UDP, "udp", "10.0.0.1:65536"),
		"udp port padded":   ref(xrpc.UDP, "udp", "10.0.0.1:09000"),
		"udp port name":     ref(xrpc.UDP, "udp", "10.0.0.1:hold"),
		"udp bare ipv6":     ref(xrpc.UDP, "udp", "::1:9"),
		"udp empty":         ref(xrpc.UDP, "udp", ""),
		"udp control bytes": ref(xrpc.UDP, "udp", "10.0.0.1:1\n"),
		"udp short instance": func() xrpc.ServiceRef {
			r := ref(xrpc.UDP, "udp", "10.0.0.1:1")
			r.InstanceID = "0123"
			return r
		}(),
		"udp upper-case instance": func() xrpc.ServiceRef {
			r := ref(xrpc.UDP, "udp", "10.0.0.1:1")
			r.InstanceID = strings.ToUpper("0123456789abcdef0123456789abcdef")
			return r
		}(),
		"udp non-hex instance": func() xrpc.ServiceRef {
			r := ref(xrpc.UDP, "udp", "10.0.0.1:1")
			r.InstanceID = "boot:0123456789abcdef01234567890"
			return r
		}(),
		"http key identity": func() xrpc.ServiceRef { r := ref(xrpc.HTTP, "unix", "/run/a.sock"); r.KeyID = 1; return r }(),
		"grpc key identity": func() xrpc.ServiceRef { r := ref(xrpc.GRPC, "unix", "/run/a.sock"); r.KeyID = 1; return r }(),
	}
	for name, r := range invalid {
		if err := r.Validate(); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestServiceRefKeyIdentityJSON(t *testing.T) {
	udp := ref(xrpc.UDP, "udp", "192.168.1.20:19520")
	udp.KeyID = 42
	encoded, err := json.Marshal(udp)
	if err != nil || !strings.Contains(string(encoded), `"key_id":42`) || !strings.Contains(string(encoded), `"profile":"udp.v1"`) {
		t.Fatalf("encoded=%s err=%v", encoded, err)
	}
	var decoded xrpc.ServiceRef
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != udp || decoded.Validate() != nil {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	// Other languages publish key_id as a JSON number and may omit it.
	var foreign xrpc.ServiceRef
	document := `{"target_id":"robot-1","service":"xgc2.chassis.hold","api_version":"v1","instance_id":"","profile":"udp.v1","endpoint":{"kind":"udp","address":"[::1]:19520"}}`
	if err := json.Unmarshal([]byte(document), &foreign); err != nil || foreign.KeyID != 0 || foreign.Validate() != nil {
		t.Fatalf("foreign=%+v err=%v", foreign, err)
	}
	other, _ := json.Marshal(ref(xrpc.HTTP, "unix", "/run/a.sock"))
	if strings.Contains(string(other), "key_id") {
		t.Fatalf("unset key identity serialized: %s", other)
	}
}

func TestInternalReferenceStillRequiresInstance(t *testing.T) {
	if err := ref(xrpc.UDP, "udp", "10.0.0.1:1").ValidateInternal(); err == nil {
		t.Fatal("unbound internal reference accepted")
	}
	pinned := ref(xrpc.UDP, "udp", "10.0.0.1:1")
	pinned.InstanceID = "0123456789abcdef0123456789abcdef"
	if err := pinned.ValidateInternal(); err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapBindingsRejectUDP(t *testing.T) {
	binding := xrpc.BootstrapBinding{SchemaVersion: 1, TargetID: "local", Service: "fixture", APIVersion: "v1", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "udp", Address: "10.0.0.1:1"}, RuntimeGrant: "runtime", Authentication: xrpc.ServerTLS, SecretHandles: xrpc.SecretHandles{TLSIdentity: "identity", TLSTrust: "trust", Authorization: "caller"}, StorageGrants: []string{}}
	if binding.Validate() == nil {
		t.Fatal("HTTP binding over a UDP endpoint accepted")
	}
	binding.Profile = xrpc.UDP
	if binding.Validate() == nil {
		t.Fatal("udp.v1 bootstrap binding accepted: udp.v1 authenticates with a key ring, not bootstrap grants")
	}
}
