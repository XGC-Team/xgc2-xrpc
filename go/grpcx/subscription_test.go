package grpcx

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/reflect/protoregistry"
)

const healthWatchMethod = "/grpc.health.v1.Health/Watch"

func subscriptionFixture(t *testing.T, implementation healthpb.HealthServer) (*Profile, xrpc.Call, *Host) {
	t.Helper()
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "rpc.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, implementation) }, HostOptions{InstanceID: "boot", MaxCallTime: 100 * time.Millisecond, MaxInFlight: 4, SubscriptionMethods: []string{healthWatchMethod}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { host.Stop(); <-host.Drained() })
	ref := xrpc.ServiceRef{TargetID: "local", Service: "grpc.health.v1.Health", APIVersion: "1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.Path()}}
	call, err := xrpc.MethodCall(ref, "grpc.health.v1.Health/Watch", "subscription-test", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	profile := NewProfile(DialOptions{LocalTargetID: "local", MaxCallTime: 150 * time.Millisecond}, protoregistry.GlobalFiles)
	t.Cleanup(func() { profile.Close(); <-profile.Drained() })
	return profile, call, host
}

func TestSubscriptionSurvivesCallBudgetAndJoinsOnCancel(t *testing.T) {
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	profile, call, host := subscriptionFixture(t, h)
	dispatcher, err := xrpc.NewDispatcher(map[string]xrpc.Caller{xrpc.GRPC: profile})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- dispatcher.Subscribe(ctx, call, func(xrpc.Result) error {
			select {
			case first <- struct{}{}:
			default:
			}
			return nil
		})
	}()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("initial subscription did not open")
	}
	select {
	case err := <-done:
		t.Fatalf("subscription ended at the finite call budget: %v", err)
	case <-time.After(400 * time.Millisecond):
	}
	if got := host.Status().Metrics.InFlight; got != 1 {
		t.Fatalf("live stream did not retain its admission: %d", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscription did not join after cancellation")
	}
	deadline := time.Now().Add(time.Second)
	for host.Status().Metrics.InFlight != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := host.Status().Metrics.InFlight; got != 0 {
		t.Fatalf("canceled subscription retained admission: %d", got)
	}
	if err := dispatcher.Subscribe(context.Background(), call, func(xrpc.Result) error { return nil }); xrpc.Code(err) != "invalid_argument" {
		t.Fatalf("uncancellable owner accepted: %v", err)
	}
}

type blockedHealth struct {
	healthpb.UnimplementedHealthServer
}

func (blockedHealth) Watch(_ *healthpb.HealthCheckRequest, stream grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}
func TestSubscriptionSetupStillHasFiniteBudget(t *testing.T) {
	profile, call, _ := subscriptionFixture(t, blockedHealth{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	err := profile.Subscribe(ctx, call, func(xrpc.Result) error { return nil })
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("blocked setup did not end within its budget: %v", err)
	}
}
