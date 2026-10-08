package grpcx

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func edgeStreamService(entered chan<- struct{}, returned chan<- error, hold <-chan struct{}) func(grpc.ServiceRegistrar) {
	return func(r grpc.ServiceRegistrar) {
		r.RegisterService(&grpc.ServiceDesc{ServiceName: "edge.Session", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{{StreamName: "Connect", ServerStreams: true, ClientStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
			if metadata.ValueFromIncomingContext(stream.Context(), "authorization")[0] != "Bearer domain" {
				return errors.New("missing domain auth")
			}
			entered <- struct{}{}
			err := stream.RecvMsg(&emptypb.Empty{})
			returned <- err
			if hold != nil {
				<-hold
			}
			return err
		}}}}, struct{}{})
	}
}

func TestEdgeOwnerLifetimeBoundsNativeStreamIO(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered, returned := make(chan struct{}, 1), make(chan error, 1)
	lifetime := 80 * time.Millisecond
	host, err := ServeEdgeWithOptions(listener, nil, edgeStreamService(entered, returned, nil), EdgeOptions{Limits: HostOptions{MaxCallTime: 10 * time.Millisecond, MaxInFlight: 1}, OwnerStreamLifetime: lifetime, ConnectionGrace: 10 * time.Millisecond}, grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: time.Hour, MaxConnectionAgeGrace: time.Hour}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	connection, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	// A native edge domain session has no short internal RPC deadline/metadata.
	ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer domain")))
	defer cancel()
	stream, err := connection.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, "/edge.Session/Connect")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- stream.RecvMsg(&emptypb.Empty{}) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("edge rejected native domain session")
	}
	select {
	case err := <-returned:
		if err == nil {
			t.Fatal("stalled stream succeeded")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("owner lifetime did not interrupt native RecvMsg")
	}
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond || elapsed > 250*time.Millisecond {
		t.Fatalf("edge lifetime=%s", elapsed)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("owner stream succeeded after disconnect")
		}
	case <-time.After(time.Second):
		t.Fatal("native client did not terminate")
	}
	status := host.Status()
	if status.Metrics.Admitted != 1 || status.Metrics.PeakInFlight != 1 {
		t.Fatalf("edge not accounted %+v", status)
	}
}

func TestEdgeShutdownRetainsLeaseForDomainCallback(t *testing.T) {
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "edge.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	entered, returned, release := make(chan struct{}, 1), make(chan error, 1), make(chan struct{})
	host, err := ServeEdgeWithOptions(listener, lease, edgeStreamService(entered, returned, release), EdgeOptions{OwnerStreamLifetime: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.NewClient("passthrough:///"+lease.Path(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", lease.Path())
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer domain")))
	defer cancel()
	stream, err := connection.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, "/edge.Session/Connect")
	if err != nil {
		t.Fatal(err)
	}
	go stream.RecvMsg(&emptypb.Empty{})
	<-entered
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShutdown()
	if err = host.Shutdown(shutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("forced edge close did not interrupt stream")
	}
	if replacement, err := unixlease.Reserve(context.Background(), lease.Path(), unixlease.Options{}); err == nil {
		replacement.Close()
		close(release)
		t.Fatal("lease released before callback finished")
	}
	select {
	case <-host.Drained():
		t.Fatal("edge falsely drained")
	default:
	}
	close(release)
	select {
	case <-host.Drained():
	case <-time.After(time.Second):
		t.Fatal("edge callback did not drain")
	}
}
