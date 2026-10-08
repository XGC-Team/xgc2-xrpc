package audit

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTLSRequestIdentity(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	seen := make(chan bool, 1)
	host, err := httpx.ServeTLS(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen <- r.TLS != nil; w.Write([]byte("ok")) }), &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}, httpx.HostOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		host.Shutdown(ctx)
	}()
	cert, _ := x509.ParseCertificate(der)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}, Timeout: time.Second}
	request, _ := http.NewRequest("GET", "https://"+listener.Addr().String()+"/", nil)
	request.Header.Set("X-Xrpc-Timeout-Ms", "500")
	request.Header.Set("X-Request-ID", "audit")
	reply, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	reply.Body.Close()
	if !<-seen {
		t.Fatal("authenticated TLS connection reached handler with Request.TLS=nil")
	}
}
func TestStreamServerMaximum(t *testing.T) {
	socket := filepath.Join(privateTempDir(t), "grpc.sock")
	listener, _ := net.Listen("unix", socket)
	entered := make(chan struct{}, 1)
	host, err := grpcx.Serve(listener, nil, func(registrar grpc.ServiceRegistrar) {
		registrar.RegisterService(&grpc.ServiceDesc{ServiceName: "audit.Service", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{{StreamName: "Wait", ClientStreams: true, ServerStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
			entered <- struct{}{}
			return stream.RecvMsg(&emptypb.Empty{})
		}}}}, struct{}{})
	}, grpcx.BoundService("boot", 30*time.Millisecond, 1)...)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Stop()
	connection, err := grpcx.Dial(xrpc.ServiceRef{TargetID: "local", Service: "audit.Service", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: socket}}, grpcx.DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	stream, err := connection.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/audit.Service/Wait")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = stream.RecvMsg(&emptypb.Empty{})
	elapsed := time.Since(start)
	if status.Code(err) != codes.InvalidArgument || elapsed > 150*time.Millisecond {
		t.Fatalf("oversized native budget must be rejected before handler: %s: %v", elapsed, err)
	}
	select {
	case <-entered:
		t.Fatal("over-budget request reached domain handler")
	default:
	}
	short, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	accepted, err := connection.NewStream(short, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/audit.Service/Wait")
	if err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	err = accepted.RecvMsg(&emptypb.Empty{})
	elapsed = time.Since(start)
	if status.Code(err) != codes.DeadlineExceeded || elapsed > 150*time.Millisecond {
		t.Fatalf("native accepted stream failed finite budget: %s: %v", elapsed, err)
	}
}
func TestLeaseRetainedUntilDrained(t *testing.T) {
	socket := filepath.Join(privateTempDir(t), "rpc.sock")
	lease, err := unixlease.Reserve(context.Background(), socket, unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	host, err := httpx.Serve(listener, lease, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.Write([]byte("ok")) }), httpx.HostOptions{})
	if err != nil {
		t.Fatal(err)
	}
	connection, _ := net.Dial("unix", socket)
	connection.Write([]byte("GET / HTTP/1.1\r\nHost: unix\r\nX-Request-ID: audit\r\nX-Xrpc-Timeout-Ms: 1000\r\n\r\n"))
	defer connection.Close()
	<-entered
	done := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		host.Shutdown(ctx)
		close(done)
	}()
	<-host.Done()
	second, err := unixlease.Reserve(context.Background(), socket, unixlease.Options{})
	close(release)
	<-done
	if err == nil {
		second.Close()
		t.Fatal("owner lease released while admitted domain handler still running")
	}
}
func TestEmptyStreamStillChecksInstance(t *testing.T) {
	socket := filepath.Join(privateTempDir(t), "grpc.sock")
	listener, _ := net.Listen("unix", socket)
	host, err := grpcx.Serve(listener, nil, func(registrar grpc.ServiceRegistrar) {
		registrar.RegisterService(&grpc.ServiceDesc{ServiceName: "audit.Empty", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{{StreamName: "Watch", ServerStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
			return stream.SendHeader(metadata.Pairs("x-xrpc-instance-id", "wrong-epoch"))
		}}}}, struct{}{})
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Stop()
	connection, err := grpcx.Dial(xrpc.ServiceRef{TargetID: "local", Service: "audit.Empty", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: socket}}, grpcx.DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := connection.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/audit.Empty/Watch")
	if err != nil {
		t.Fatal(err)
	}
	stream.SendMsg(&emptypb.Empty{})
	stream.CloseSend()
	if err = stream.RecvMsg(&emptypb.Empty{}); err == io.EOF {
		t.Fatal("wrong server instance accepted as successful empty stream")
	}
}
func TestSymlinkParentRejected(t *testing.T) {
	dir := privateTempDir(t)
	actual := filepath.Join(dir, "actual")
	os.Mkdir(actual, 0700)
	alias := filepath.Join(dir, "alias")
	os.Symlink(actual, alias)
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(alias, "rpc.sock"), unixlease.Options{})
	if err == nil {
		lease.Close()
		t.Fatal("symlink parent accepted by endpoint owner")
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
