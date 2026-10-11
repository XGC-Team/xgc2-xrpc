package audit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	leaseapi "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestHTTPForcedCloseRetainsLeaseUntilHandlerReturns(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "http.sock")
	lease, err := leaseapi.Reserve(context.Background(), path, leaseapi.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	host, err := httpx.Serve(listener, lease, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release }), httpx.HostOptions{})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_, err = connection.Write([]byte("GET / HTTP/1.1\r\nHost: unix\r\nX-Request-ID: fault\r\nX-Xrpc-Timeout-Ms: 1000\r\n\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = host.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("unbounded forced shutdown: %v", err)
	}
	second, err := leaseapi.Reserve(context.Background(), path, leaseapi.Options{ExistingPath: leaseapi.ReclaimUnreachable})
	if err == nil {
		second.Close()
		close(release)
		t.Fatal("forced close released lease while domain handler was live")
	}
	select {
	case <-host.Drained():
		t.Fatal("reported domain quiescence prematurely")
	default:
	}
	close(release)
	select {
	case <-host.Drained():
	case <-time.After(time.Second):
		t.Fatal("did not release ownership after handler exit")
	}
	second, err = leaseapi.Reserve(context.Background(), path, leaseapi.Options{})
	if err != nil {
		t.Fatal(err)
	}
	second.Close()
}

func TestGRPCForcedCloseRetainsLeaseUntilHandlerReturns(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "grpc.sock")
	lease, err := leaseapi.Reserve(context.Background(), path, leaseapi.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	host, err := grpcx.ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) {
		r.RegisterService(&grpc.ServiceDesc{ServiceName: "fault.Service", HandlerType: (*interface{})(nil), Methods: []grpc.MethodDesc{{MethodName: "Block", Handler: func(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			var request emptypb.Empty
			if err := decode(&request); err != nil {
				return nil, err
			}
			next := func(context.Context, any) (any, error) { close(entered); <-release; return &emptypb.Empty{}, nil }
			return interceptor(ctx, &request, &grpc.UnaryServerInfo{FullMethod: "/fault.Service/Block"}, next)
		}}}}, struct{}{})
	}, grpcx.HostOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ref := xrpc.ServiceRef{TargetID: "local", Service: "fault.Service", APIVersion: "1", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: path}}
	client, err := grpcx.Dial(ref, grpcx.DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	call, cancelCall := context.WithTimeout(context.Background(), time.Second)
	defer cancelCall()
	go client.Invoke(call, "/fault.Service/Block", &emptypb.Empty{}, &emptypb.Empty{})
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = host.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("unbounded forced gRPC shutdown: %v", err)
	}
	second, err := leaseapi.Reserve(context.Background(), path, leaseapi.Options{ExistingPath: leaseapi.ReclaimUnreachable})
	if err == nil {
		second.Close()
		close(release)
		t.Fatal("gRPC lease released while handler live")
	}
	close(release)
	select {
	case <-host.Drained():
	case <-time.After(time.Second):
		t.Fatal("gRPC ownership did not drain")
	}
	second, err = leaseapi.Reserve(context.Background(), path, leaseapi.Options{})
	if err != nil {
		t.Fatal(err)
	}
	second.Close()
}

// The child is a real independent process. Kill bypasses all deferred cleanup;
// the next generation must explicitly reclaim its unreachable socket safely.
func TestKilledOwnerRestart(t *testing.T) {
	if path := os.Getenv("XRPC_AUDIT_CHILD_SOCKET"); path != "" {
		lease, err := leaseapi.Reserve(context.Background(), path, leaseapi.Options{})
		if err != nil {
			panic(err)
		}
		listener, err := lease.Listen()
		if err != nil {
			panic(err)
		}
		fmt.Println("READY")
		for {
			connection, err := listener.Accept()
			if err != nil {
				panic(err)
			}
			connection.Close()
		}
	}
	path := filepath.Join(privateTempDir(t), "kill.sock")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(executable, "-test.run=^TestKilledOwnerRestart$")
	child.Env = append(os.Environ(), "XRPC_AUDIT_CHILD_SOCKET="+path)
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.Stderr = os.Stderr
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	ready := make(chan bool, 1)
	go func() { scanner := bufio.NewScanner(output); ready <- scanner.Scan() && scanner.Text() == "READY" }()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("child failed before bind")
		}
	case <-time.After(time.Second):
		t.Fatal("child bind stalled")
	}
	if other, e := leaseapi.Reserve(context.Background(), path, leaseapi.Options{ExistingPath: leaseapi.ReclaimUnreachable}); e == nil {
		other.Close()
		t.Fatal("reclaimed live child")
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := child.Wait()
	var exit *exec.ExitError
	if !errors.As(waitErr, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("child was not forcibly killed: %v", waitErr)
	}
	if other, e := leaseapi.Reserve(context.Background(), path, leaseapi.Options{}); e == nil {
		other.Close()
		t.Fatal("default unexpectedly reclaimed killed child")
	}
	lease, err := leaseapi.Reserve(context.Background(), path, leaseapi.Options{ExistingPath: leaseapi.ReclaimUnreachable})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lease.Listen(); err != nil {
		t.Fatal(err)
	}
	if err = lease.Close(); err != nil {
		t.Fatal(err)
	}
}
