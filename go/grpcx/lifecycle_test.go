package grpcx

import (
	"context"
	"crypto/tls"
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

func TestPrepareAndStopBeforeServe(t *testing.T) {
	for _, graceful := range []bool{false, true} {
		t.Run(map[bool]string{false: "stop", true: "shutdown"}[graceful], func(t *testing.T) {
			lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "edge.sock"), unixlease.Options{})
			if err != nil {
				t.Fatal(err)
			}
			listener, err := lease.Listen()
			if err != nil {
				t.Fatal(err)
			}
			tracked := &acceptCounter{Listener: listener}
			host, err := PrepareEdgeTLS(tracked, lease, func(grpc.ServiceRegistrar) error { return nil }, &tls.Config{Certificates: []tls.Certificate{{}}}, EdgeOptions{OwnerStreamLifetime: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if tracked.calls.Load() != 0 {
				t.Fatal("Prepare accepted a connection")
			}
			select {
			case <-host.Done():
				t.Fatal("unstarted accept loop reported done")
			default:
			}
			if graceful {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := host.Shutdown(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				host.Stop()
			}
			select {
			case <-host.Drained():
			case <-time.After(time.Second):
				t.Fatal("prepared host did not drain")
			}
			select {
			case <-host.Done():
			default:
				t.Fatal("prepared accept loop incomplete")
			}
			if _, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
				t.Fatal("prepared listener still open", err)
			}
			if err := host.Serve(); !errors.Is(err, grpc.ErrServerStopped) {
				t.Fatal("stopped host restarted", err)
			}
			replacement, err := unixlease.Reserve(context.Background(), lease.Path(), unixlease.Options{})
			if err != nil {
				t.Fatal("prepared lease was retained after actual drain", err)
			}
			replacement.Close()
		})
	}
}

func TestPrepareRegistrationFailureDoesNotStart(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	tracked := &acceptCounter{Listener: listener}
	wanted := errors.New("registration failed")
	host, err := PrepareEdgeTLS(tracked, nil, func(grpc.ServiceRegistrar) error { return wanted }, &tls.Config{Certificates: []tls.Certificate{{}}}, EdgeOptions{OwnerStreamLifetime: time.Second})
	if host != nil || !errors.Is(err, wanted) || tracked.calls.Load() != 0 {
		t.Fatalf("partial startup host=%v err=%v accepts=%d", host, err, tracked.calls.Load())
	}
}

func TestPreparedServeStopCompetition(t *testing.T) {
	for i := 0; i < 30; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		host, err := PrepareEdgeTLS(listener, nil, func(grpc.ServiceRegistrar) error { return nil }, &tls.Config{Certificates: []tls.Certificate{{}}}, EdgeOptions{OwnerStreamLifetime: time.Second})
		if err != nil {
			listener.Close()
			t.Fatal(err)
		}
		start := make(chan struct{})
		var group sync.WaitGroup
		group.Add(3)
		go func() { defer group.Done(); <-start; _ = host.Serve() }()
		go func() { defer group.Done(); <-start; host.Stop() }()
		go func() {
			defer group.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = host.Shutdown(ctx)
		}()
		close(start)
		group.Wait()
		select {
		case <-host.Drained():
		case <-time.After(time.Second):
			t.Fatal("competing lifetime did not drain")
		}
		if err := host.Serve(); !errors.Is(err, grpc.ErrServerStopped) {
			t.Fatal(err)
		}
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
