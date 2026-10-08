package grpcx

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type heldHealth struct {
	grpc_health_v1.UnimplementedHealthServer
	entered chan time.Time
	expired chan struct{}
	release chan struct{}
}

func (s *heldHealth) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	deadline, _ := ctx.Deadline()
	s.entered <- deadline
	<-ctx.Done()
	close(s.expired)
	<-s.release
	return &grpc_health_v1.HealthCheckResponse{}, nil
}

func TestLongUnaryEffectiveBudgetRetainsActualWork(t *testing.T) {
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "held.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	service := &heldHealth{entered: make(chan time.Time, 1), expired: make(chan struct{}), release: make(chan struct{})}
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) { grpc_health_v1.RegisterHealthServer(r, service) }, HostOptions{InstanceID: "boot", MaxCallTime: 20 * time.Millisecond, MaxInFlight: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	defer close(service.release)
	connection, err := Dial(xrpc.ServiceRef{TargetID: "local", Service: "grpc.health.v1.Health", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.Path()}}, DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	callerDeadline, _ := ctx.Deadline()
	result := make(chan error, 1)
	go func() {
		_, err := grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		result <- err
	}()
	select {
	case effective := <-service.entered:
		if !effective.Before(callerDeadline) || time.Until(effective) > 20*time.Millisecond {
			t.Fatal("native unary handler did not receive shorter effective deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("long finite unary was rejected")
	}
	select {
	case <-service.expired:
	case <-time.After(time.Second):
		t.Fatal("effective handler context did not expire")
	}
	other, cancelOther := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelOther()
	if _, err := grpc_health_v1.NewHealthClient(connection).Check(other, &grpc_health_v1.HealthCheckRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("expired real work released admission", err)
	}
	select {
	case err := <-result:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller exceeded its finite native deadline")
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShutdown()
	if err := host.Shutdown(shutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("noncooperative work falsely drained", err)
	}
	select {
	case <-host.Drained():
		t.Fatal("host released actual callback lifetime")
	default:
	}
	if replacement, err := unixlease.Reserve(context.Background(), lease.Path(), unixlease.Options{}); err == nil {
		replacement.Close()
		t.Fatal("lease released before callback returned")
	}
}
