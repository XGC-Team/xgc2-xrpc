package grpcx

import (
	"context"
	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeGRPCDeadlineAndInstanceFence(t *testing.T) {
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "rpc.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	server := health.NewServer()
	server.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	host, err := ServeWithOptions(listener, lease, func(registrar grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(registrar, server) }, HostOptions{InstanceID: "boot-1", MaxCallTime: time.Second, MaxInFlight: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Stop()
	ref := xrpc.ServiceRef{TargetID: "local", Service: "grpc.health.v1.Health", APIVersion: "1", InstanceID: "boot-1", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.Path()}}
	connection, err := Dial(ref, DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := healthpb.NewHealthClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if response, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil || response.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("%v %v", response, err)
	}
	if _, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{}); err == nil {
		t.Fatal("missing deadline accepted")
	}
	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal(err)
	}
	ref.InstanceID = "old-boot"
	stale, err := Dial(ref, DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Close()
	if _, err = healthpb.NewHealthClient(stale).Check(ctx, &healthpb.HealthCheckRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale call=%v", err)
	}
}

func privateTempDir(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestTransportConnectionLimit(t *testing.T) {
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "rpc.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, HostOptions{MaxConnections: 2, HandshakeTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	var peers []net.Conn
	defer func() {
		for _, p := range peers {
			p.Close()
		}
	}()
	for i := 0; i < 20; i++ {
		p, err := net.DialTimeout("unix", lease.Path(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		peers = append(peers, p)
	}
	deadline := time.NewTimer(100 * time.Millisecond)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline.C:
			return
		case <-tick.C:
			if n := host.listener.Count(); n > 2 {
				t.Fatalf("admitted %d transport connections", n)
			}
		}
	}
}

func TestClientStreamAdmissionIsReleasedOnCancellation(t *testing.T) {
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "rpc.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, HostOptions{InstanceID: "boot", MaxCallTime: time.Second, MaxInFlight: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	ref := xrpc.ServiceRef{TargetID: "local", Service: "grpc.health.v1.Health", APIVersion: "1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.Path()}}
	connection, err := Dial(ref, DialOptions{LocalTargetID: "local", MaxInFlight: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := healthpb.NewHealthClient(connection)
	watchCtx, cancelWatch := context.WithTimeout(context.Background(), time.Second)
	stream, err := client.Watch(watchCtx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err = client.Check(ctx, &healthpb.HealthCheckRequest{}); xrpc.Code(err) != "resource_exhausted" {
		t.Fatalf("active stream did not consume client admission: %v", err)
	}
	cancelWatch()
	if _, err = stream.Recv(); err == nil {
		t.Fatal("cancelled stream succeeded")
	}
	if _, err = client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal("cancelled stream retained admission", err)
	}
}
