package xrpc_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
)

func TestRolePolicyIntersectsNumericBudgetsAndProvenance(t *testing.T) {
	parent, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{Environment: []string{"XGC2_XRPC_MAX_RESPONSE_BYTES=8"}, Ceilings: map[string]int64{"MAX_RESPONSE_BYTES": 10}})
	if err != nil {
		t.Fatal(err)
	}
	caps := map[string]int64{"MAX_RESPONSE_BYTES": 4}
	role, err := xrpc.DerivePolicy(parent, "browser", caps)
	if err != nil {
		t.Fatal(err)
	}
	caps["MAX_RESPONSE_BYTES"] = 99
	snapshot := role.Effective()
	field := snapshot.Fields["MAX_RESPONSE_BYTES"]
	if snapshot.Revision != 1 || snapshot.Role != "browser" || snapshot.Parent.Fields["MAX_RESPONSE_BYTES"].Value != int64(8) || field.Value != int64(4) || field.Source != "role_cap" || field.ParentValue != int64(8) || field.ParentSource != "environment" || field.ParentCeiling != 10 || field.Ceiling != 4 || field.RoleCap != 4 {
		t.Fatalf("provenance %+v %+v", snapshot, field)
	}
	snapshot.Fields["MAX_RESPONSE_BYTES"] = xrpc.EffectivePolicyField{}
	snapshot.RoleCaps["MAX_RESPONSE_BYTES"] = 1
	snapshot.Parent.Fields["MAX_RESPONSE_BYTES"] = xrpc.EffectivePolicyField{}
	if again := role.Effective(); again.Fields["MAX_RESPONSE_BYTES"].Value != int64(4) || again.Parent.Fields["MAX_RESPONSE_BYTES"].Value != int64(8) || again.RoleCaps["MAX_RESPONSE_BYTES"] != 4 {
		t.Fatal("view aliases caller data")
	}
	broad, err := xrpc.DerivePolicy(parent, "broad", map[string]int64{"MAX_RESPONSE_BYTES": 100})
	if err != nil {
		t.Fatal(err)
	}
	if value := broad.Effective().Fields["MAX_RESPONSE_BYTES"]; value.Value != int64(8) || value.Source != "environment" || value.Ceiling != 10 {
		t.Fatal("role widened or rewrote parent", value)
	}
	if _, err := role.Update(1, map[string]string{"LOG_LEVEL": "debug"}); err == nil {
		t.Fatal("view has updater")
	}
	if _, err := xrpc.NewDiagnostics(role, xrpc.DiagnosticOptions{Sink: io.Discard}); err == nil {
		t.Fatal("view creates diagnostics")
	}
	if _, err := xrpc.DerivePolicy(role, "nested", nil); err == nil {
		t.Fatal("nested view")
	}
	if _, err := xrpc.DerivePolicy(&xrpc.Policy{}, "invalid", nil); err == nil {
		t.Fatal("fabricated parent")
	}
	for _, test := range []struct {
		role string
		caps map[string]int64
	}{{"bad role", nil}, {"", nil}, {"role", map[string]int64{"LOG_LEVEL": 1}}, {"role", map[string]int64{"TLS_VERIFY": 1}}, {"role", map[string]int64{"MAX_RESPONSE_BYTES": 0}}, {"role", map[string]int64{"MAX_RESPONSE_BYTES": -1}}, {"role", map[string]int64{"MAX_RESPONSE_BYTES": 2147483648}}, {"role", map[string]int64{"CALL_TIMEOUT_MS": 86400001}}} {
		if _, err := xrpc.DerivePolicy(parent, test.role, test.caps); err == nil {
			t.Fatalf("invalid role accepted %+v", test)
		}
	}
	limited, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{Capabilities: []string{"host"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := xrpc.DerivePolicy(limited, "unknown-capability", map[string]int64{"MAX_RESPONSE_BYTES": 4}); err == nil {
		t.Fatal("unsupported parent capability")
	}
	if _, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{Environment: []string{"XGC2_XRPC_MAX_RESPONSE_BYTES=11"}, Ceilings: map[string]int64{"MAX_RESPONSE_BYTES": 10}}); err == nil {
		t.Fatal("invalid parent was clamped")
	}
}

func TestRoleFollowsActualParentDiagnosticsAndRevision(t *testing.T) {
	parent, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	role, err := xrpc.DerivePolicy(parent, "client", map[string]int64{"CLIENT_MAX_REFERENCES": 8})
	if err != nil {
		t.Fatal(err)
	}
	old := role.Effective()
	diagnostics, err := xrpc.NewDiagnostics(parent, xrpc.DiagnosticOptions{Sink: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := diagnostics.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if _, err := xrpc.NewDiagnostics(parent, xrpc.DiagnosticOptions{Sink: io.Discard}); err == nil {
		t.Fatal("second diagnostics owner")
	}
	staged, err := parent.Update(1, map[string]string{"LOG_LEVEL": "trace"})
	if err != nil {
		t.Fatal(err)
	}
	if staged.Effective().Revision != 2 || role.Effective().Revision != 1 || parent.Effective().Revision != 1 {
		t.Fatal("proposal pretended to apply")
	}
	if _, err := xrpc.DerivePolicy(staged, "staged", nil); err == nil {
		t.Fatal("unapplied proposal derives")
	}
	updated, err := diagnostics.UpdatePolicy(1, map[string]string{"LOG_LEVEL": "debug"})
	if err != nil {
		t.Fatal(err)
	}
	if current := role.Effective(); current.Revision != 2 || current.Fields["LOG_LEVEL"].Value != "debug" || current.Parent.Revision != 2 || parent.Effective().Revision != 2 || old.Revision != 1 || old.Fields["LOG_LEVEL"].Value != "info" {
		t.Fatal("view failed to follow shared owner", current)
	}
	if _, err := xrpc.DerivePolicy(staged, "still-staged", nil); err == nil {
		t.Fatal("different unpublished proposal mistaken for applied revision")
	}
	if _, err := xrpc.DerivePolicy(updated, "actual", nil); err != nil {
		t.Fatal("actual published parent rejected", err)
	}
	if _, err := (httpx.Config{Diagnostics: diagnostics}).WithPolicy(role); err != nil {
		t.Fatal(err)
	}
	other, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	otherRole, err := xrpc.DerivePolicy(other, "other", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (httpx.Config{Diagnostics: diagnostics}).WithPolicy(otherRole); err == nil {
		t.Fatal("foreign diagnostic owner")
	}
}

func TestRoleConcurrentLiveUpdateAndQueries(t *testing.T) {
	parent, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	role, err := xrpc.DerivePolicy(parent, "race", map[string]int64{"MAX_RESPONSE_BYTES": 4})
	if err != nil {
		t.Fatal(err)
	}
	diagnostics, err := xrpc.NewDiagnostics(parent, xrpc.DiagnosticOptions{Sink: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := diagnostics.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 200; j++ {
				snapshot := role.Effective()
				if snapshot.Revision != snapshot.Parent.Revision || snapshot.Fields["LOG_LEVEL"].Value != snapshot.Parent.Fields["LOG_LEVEL"].Value || snapshot.Fields["MAX_RESPONSE_BYTES"].Value != int64(4) {
					t.Error("mixed role snapshot")
				}
				if _, err := (httpx.HostOptions{Diagnostics: diagnostics}).WithPolicy(role); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	for revision := uint64(1); revision <= 100; revision++ {
		level := "debug"
		if revision%2 == 0 {
			level = "info"
		}
		if _, err := diagnostics.UpdatePolicy(revision, map[string]string{"LOG_LEVEL": level}); err != nil {
			t.Fatal(err)
		}
	}
	workers.Wait()
}

func TestRoleBudgetsApplyOnTwoActualNativeHosts(t *testing.T) {
	parent, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{Defaults: map[string]string{"MAX_RESPONSE_BYTES": "8"}})
	if err != nil {
		t.Fatal(err)
	}
	diagnostics, err := xrpc.NewDiagnostics(parent, xrpc.DiagnosticOptions{Sink: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer diagnostics.Close(ctx)
	for _, test := range []struct {
		role   string
		cap    int64
		status int
	}{{"browser", 4, 429}, {"remote", 8, 200}} {
		view, err := xrpc.DerivePolicy(parent, test.role, map[string]int64{"MAX_RESPONSE_BYTES": test.cap})
		if err != nil {
			t.Fatal(err)
		}
		limits, err := (httpx.HostOptions{Diagnostics: diagnostics}).WithPolicy(view)
		if err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		host, err := httpx.ServeEdge(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("12345")) }), limits)
		if err != nil {
			t.Fatal(err)
		}
		defer host.Shutdown(ctx)
		response, err := (&http.Client{Timeout: time.Second}).Get("http://" + listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != test.status || int64(len(body)) > test.cap {
			t.Fatalf("role %s status=%d bytes=%d err=%v", test.role, response.StatusCode, len(body), err)
		}
	}
	if value, _ := parent.Integer("MAX_RESPONSE_BYTES"); value != 8 {
		t.Fatal("role changed parent")
	}
}

func TestRoleRejectsClosedDiagnosticsOwnerAfterSharedParentAdvances(t *testing.T) {
	parent, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	role, err := xrpc.DerivePolicy(parent, "role", nil)
	if err != nil {
		t.Fatal(err)
	}
	old, err := xrpc.NewDiagnostics(parent, xrpc.DiagnosticOptions{Sink: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := old.Close(ctx); err != nil {
		t.Fatal(err)
	}
	current, err := xrpc.NewDiagnostics(parent, xrpc.DiagnosticOptions{Sink: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close(ctx)
	if _, err := current.UpdatePolicy(1, map[string]string{"LOG_LEVEL": "debug"}); err != nil {
		t.Fatal(err)
	}
	if status := old.Status(); status.Revision != 1 || status.Level != "info" || !status.Drained {
		t.Fatal("closed owner reports another writer's actual revision", status)
	}
	if status := current.Status(); status.Revision != 2 || status.Level != "debug" {
		t.Fatal("current owner revision", status)
	}
	for _, view := range []*xrpc.Policy{parent, role} {
		if _, err := (httpx.HostOptions{Diagnostics: old}).WithPolicy(view); err == nil {
			t.Fatal("closed owner accepted current policy")
		}
		if _, err := (httpx.Config{Diagnostics: old}).WithPolicy(view); err == nil {
			t.Fatal("closed owner accepted client policy")
		}
		if _, err := (httpx.HostOptions{Diagnostics: current}).WithPolicy(view); err != nil {
			t.Fatal("actual current owner rejected", err)
		}
	}
}
