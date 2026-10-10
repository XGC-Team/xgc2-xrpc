package grpcx

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type acceptCounter struct {
	net.Listener
	calls atomic.Int32
}

func (l *acceptCounter) Accept() (net.Conn, error) { l.calls.Add(1); return l.Listener.Accept() }

// Stop and Shutdown may arrive before the accept loop has started. The host
// must still drain, release its lease and never accept afterwards.
func TestStopRacingTheAcceptLoopStillDrains(t *testing.T) {
	for i := 0; i < 30; i++ {
		lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "race.sock"), unixlease.Options{})
		if err != nil {
			t.Fatal(err)
		}
		listener, err := lease.Listen()
		if err != nil {
			t.Fatal(err)
		}
		tracked := &acceptCounter{Listener: listener}
		host, err := ServeWithOptions(tracked, lease, func(grpc.ServiceRegistrar) {}, HostOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var group sync.WaitGroup
		group.Add(2)
		go func() { defer group.Done(); host.Stop() }()
		go func() {
			defer group.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = host.Shutdown(ctx)
		}()
		group.Wait()
		select {
		case <-host.Drained():
		case <-time.After(time.Second):
			t.Fatal("competing lifetime did not drain")
		}
		select {
		case <-host.Done():
		default:
			t.Fatal("accept loop incomplete after Drained")
		}
		if _, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatal("listener still open", err)
		}
		replacement, err := unixlease.Reserve(context.Background(), lease.Path(), unixlease.Options{})
		if err != nil {
			t.Fatal("the lease was retained after the actual drain", err)
		}
		replacement.Close()
	}
}

func TestStreamHeadersPrecedeFirstDomainReceive(t *testing.T) {
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "stream.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) {
		r.RegisterService(&grpc.ServiceDesc{ServiceName: "fixture.Stream", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{{StreamName: "Open", ServerStreams: true, ClientStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
			if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
				return err
			}
			return stream.SendMsg(&emptypb.Empty{})
		}}}}, struct{}{})
	}, HostOptions{InstanceID: "boot", MaxCallTime: time.Second, MaxInFlight: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	connection, err := Dial(xrpc.ServiceRef{TargetID: "local", Service: "fixture.Stream", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.Path()}}, DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx, err = WithRequestID(ctx, "header:first")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := connection.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, "/fixture.Stream/Open")
	if err != nil {
		t.Fatal(err)
	}
	// No message is sent until initial metadata arrives, matching the native
	// C++ Header-first consumer. The domain is already waiting in RecvMsg.
	header, err := stream.Header()
	if err != nil {
		t.Fatal("initial metadata blocked behind first message", err)
	}
	if header.Get(RequestIDMetadata)[0] != "header:first" || header.Get(InstanceIDMetadata)[0] != "boot" {
		t.Fatal("identity metadata was not flushed", header)
	}
	if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
}
