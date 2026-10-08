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
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		host, err := ServeEdgeWithOptions(listener, nil, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, EdgeOptions{OwnerStreamLifetime: time.Second}, option)
		if host != nil || err == nil || !strings.Contains(err.Error(), "primary interceptors belong to SDK") {
			t.Fatalf("primary option result host=%v err=%v", host, err)
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestProductChainCannotBypassAdmissionAuthorizationOrInstance(t *testing.T) {
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "rpc.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	var chainCalls, authCalls atomic.Int32
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, HostOptions{InstanceID: "actual", Authorize: func(context.Context) bool { authCalls.Add(1); return false }},
		grpc.ChainUnaryInterceptor(func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
			chainCalls.Add(1)
			return &healthpb.HealthCheckResponse{}, nil
		}),
		grpc.ChainStreamInterceptor(func(any, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error {
			chainCalls.Add(1)
			return nil
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := healthpb.NewHealthClient(conn)
	if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	if chainCalls.Load() != 0 || authCalls.Load() != 2 {
		t.Fatalf("auth=%d productchain=%d", authCalls.Load(), chainCalls.Load())
	}
	ref.InstanceID = "stale"
	stale, err := Dial(ref, DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Close()
	if _, err := healthpb.NewHealthClient(stale).Check(ctx, &healthpb.HealthCheckRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	if chainCalls.Load() != 0 || authCalls.Load() != 2 {
		t.Fatal("stale reference crossed native gate")
	}
}
