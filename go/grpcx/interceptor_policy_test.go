package grpcx

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestPrimaryServerInterceptorRejectedBeforeServe(t *testing.T) {
	options := []grpc.ServerOption{
		grpc.UnaryInterceptor(func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
			return &healthpb.HealthCheckResponse{}, nil
		}),
		grpc.StreamInterceptor(func(any, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error { return nil }),
	}
	for _, option := range options {
		listener, err := net.Listen("unix", filepath.Join(privateTempDir(t), "rpc.sock"))
		if err != nil {
			t.Fatal(err)
		}
		host, err := ServeWithOptions(listener, nil, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, HostOptions{}, option)
		if host != nil || err == nil || !strings.Contains(err.Error(), "primary interceptors belong to SDK") {
			t.Fatalf("primary option result host=%v err=%v", host, err)
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Product Chain* interceptors run inside the SDK gate: a saturated host and a
// stale instance are rejected before any product code sees the call.
func TestProductChainCannotBypassAdmissionOrInstance(t *testing.T) {
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "rpc.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	var chainCalls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, HostOptions{InstanceID: "actual", MaxInFlight: 1},
		grpc.ChainUnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if chainCalls.Add(1) == 1 {
				close(entered)
				<-release
			}
			return next(ctx, request)
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	ref := xrpc.ServiceRef{TargetID: "local", Service: "fixture", APIVersion: "v1", InstanceID: "actual", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.Path()}}
	conn, err := Dial(ref, DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		first <- err
	}()
	<-entered
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("saturated host: %v", err)
	}
	ref.InstanceID = "stale"
	stale, err := Dial(ref, DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Close()
	if _, err := healthpb.NewHealthClient(stale).Check(ctx, &healthpb.HealthCheckRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale instance: %v", err)
	}
	if chainCalls.Load() != 1 {
		t.Fatalf("rejected calls crossed the native gate: product chain ran %d times", chainCalls.Load())
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}
