package grpcx

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestDialOwnedByActualCallerBudget(t *testing.T) {
	started := make(chan time.Time, 1)
	finished := make(chan struct{}, 1)
	ref := xrpc.ServiceRef{TargetID: "remote", Service: "grpc.health.v1.Health", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/run/xrpc-test.sock"}}
	connection, err := Dial(ref, DialOptions{LocalTargetID: "local", DialContext: func(ctx context.Context, _ xrpc.ServiceRef) (net.Conn, error) {
		deadline, _ := ctx.Deadline()
		started <- deadline
		<-ctx.Done()
		finished <- struct{}{}
		return nil, ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	_, err = healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{})
	if err == nil {
		t.Fatal("blocked dial succeeded")
	}
	select {
	case actual := <-started:
		if actual.After(deadline) {
			t.Fatalf("dial deadline=%s exceeds caller=%s", actual, deadline)
		}
	default:
		t.Fatal("dial not exercised")
	}
	select {
	case <-finished:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("dial survived caller exit")
	}
}

func TestSetupSocketBudgetIncludesTLSAndHTTP2Preface(t *testing.T) {
	for _, kind := range []string{"unix", "tls"} {
		t.Run(kind, func(t *testing.T) {
			address := filepath.Join(privateTempDir(t), "stalled.sock")
			network := "unix"
			if kind == "tls" {
				network = "tcp"
				address = "127.0.0.1:0"
			}
			listener, err := net.Listen(network, address)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			closed := make(chan struct{})
			go func() {
				peer, err := listener.Accept()
				if err != nil {
					return
				}
				defer peer.Close()
				io.Copy(io.Discard, peer)
				close(closed)
			}()
			connection, err := Dial(xrpc.ServiceRef{TargetID: "local", Service: "grpc.health.v1.Health", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: kind, Address: listener.Addr().String()}}, DialOptions{LocalTargetID: "local", TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}})
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			if _, err = healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{}); err == nil {
				t.Fatal("stalled setup succeeded")
			}
			select {
			case <-closed:
			case <-time.After(250 * time.Millisecond):
				t.Fatal("setup socket survived caller")
			}
		})
	}
}

func TestSuccessfulConnectionOutlivesFirstCallerAndHostLimitsOverrideNativeOptions(t *testing.T) {
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "rpc.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	limits := HostOptions{InstanceID: "boot", MaxRequestBytes: 32, MaxResponseBytes: 32}
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, limits, grpc.MaxRecvMsgSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	var dials atomic.Int64
	connection, err := Dial(xrpc.ServiceRef{TargetID: "local", Service: "grpc.health.v1.Health", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.Path()}}, DialOptions{LocalTargetID: "local", DialContext: func(ctx context.Context, ref xrpc.ServiceRef) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, "unix", ref.Endpoint.Address)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := healthpb.NewHealthClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	if _, err = client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	cancel()
	// Exceed the first caller deadline; a pooled established connection must
	// have cleared its setup deadline and remain independent of that caller.
	time.Sleep(60 * time.Millisecond)
	second, cancelSecond := context.WithTimeout(context.Background(), time.Second)
	defer cancelSecond()
	if _, err = client.Check(second, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal("pooled connection expired with first caller", err)
	}
	if dials.Load() != 1 {
		t.Fatalf("successful pooled connection was redialed %d times", dials.Load())
	}
	_, err = client.Check(second, &healthpb.HealthCheckRequest{Service: "payload-that-exceeds-thirty-two-bytes-by-a-long-way"}, grpc.MaxCallSendMsgSize(1<<20))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("native host option bypassed request cap: %v", err)
	}
}

func TestKnownNotSentSurvivesProfileMapping(t *testing.T) {
	before := xrpc.Failure("resource_exhausted", xrpc.NotSent, status.Error(codes.ResourceExhausted, "client full"))
	after := callError(before)
	var failure *xrpc.CallError
	if !errors.As(after, &failure) || failure.Disposition != xrpc.NotSent || failure != before {
		t.Fatalf("local admission disposition changed: %v", after)
	}
}

func TestMetadataCannotDuplicateWireIdentity(t *testing.T) {
	ref := xrpc.ServiceRef{InstanceID: "boot"}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(RequestIDMetadata, "a", RequestIDMetadata, "b"))
	if _, err := clientMetadata(ctx, ref, nil); xrpc.Code(err) != "invalid_argument" {
		t.Fatal("duplicate request identity", err)
	}
	ctx = metadata.NewOutgoingContext(context.Background(), metadata.Pairs(InstanceIDMetadata, "other"))
	if _, err := clientMetadata(ctx, ref, nil); xrpc.Code(err) != "invalid_argument" {
		t.Fatal("instance override", err)
	}
	if err := validateAuthMetadata(metadata.Pairs(RequestIDMetadata, "owned")); err == nil {
		t.Fatal("auth metadata overrode wire")
	}
}
