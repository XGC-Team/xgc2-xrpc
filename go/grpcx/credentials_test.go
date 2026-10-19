package grpcx

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TestTLSCredentialsFollowEachServiceRef(t *testing.T) {
	alphaID, betaID := newIdentity(t, "alpha"), newIdentity(t, "beta")
	alphaSeen, betaSeen := &callLog{}, &callLog{}
	alphaAddr := serveTLSHealth(t, alphaID, "boot-a", alphaSeen)
	betaAddr := serveTLSHealth(t, betaID, "boot-b", betaSeen)
	alpha := healthRef("vehicle-a", "boot-a", alphaAddr)
	beta := healthRef("vehicle-b", "boot-b", betaAddr)
	tokens := map[string]string{alphaAddr: "Bearer alpha", betaAddr: "Bearer beta"}
	pins := map[string][32]byte{alphaAddr: alphaID.pin, betaAddr: betaID.pin}
	var calls atomic.Int32
	var returnedMu sync.Mutex
	returned := map[string]metadata.MD{}
	var refsMu sync.Mutex
	var refs []xrpc.ServiceRef
	static := metadata.Pairs("x-caller", "core")
	files := healthFiles(t)
	profile := NewProfile(DialOptions{
		LocalTargetID: "core",
		Metadata:      static,
		// Present and unsafe. The callback path must not consult it.
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
		CredentialsFor: func(ref xrpc.ServiceRef) (credentials.TransportCredentials, metadata.MD, error) {
			calls.Add(1)
			refsMu.Lock()
			refs = append(refs, ref)
			refsMu.Unlock()
			md := metadata.Pairs("authorization", tokens[ref.Endpoint.Address])
			returnedMu.Lock()
			returned[ref.Endpoint.Address] = md
			returnedMu.Unlock()
			return credentials.NewTLS(pinned(pins[ref.Endpoint.Address], nil)), md, nil
		},
	}, files)
	defer func() { profile.Close(); <-profile.Drained() }()

	if err := callHealth(t, profile, alpha, "req-a-1"); err != nil {
		t.Fatal(err)
	}
	returnedMu.Lock()
	returned[alphaAddr].Set("authorization", "Bearer crossed")
	returnedMu.Unlock()
	if calls.Load() != 1 {
		t.Fatalf("resolver calls after first dial = %d", calls.Load())
	}
	if err := callHealth(t, profile, alpha, "req-a-2"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("cached ref resolved again: %d", calls.Load())
	}
	if err := callHealth(t, profile, beta, "req-b-1"); err != nil {
		t.Fatal(err)
	}
	var secondA, secondB error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); secondA = callHealth(t, profile, alpha, "req-a-3") }()
	go func() { defer wg.Done(); secondB = callHealth(t, profile, beta, "req-b-2") }()
	wg.Wait()
	if secondA != nil || secondB != nil {
		t.Fatalf("repeat calls: %v %v", secondA, secondB)
	}
	if calls.Load() != 2 {
		t.Fatalf("resolver calls = %d, want one per ref", calls.Load())
	}
	refsMu.Lock()
	gotRefs := append([]xrpc.ServiceRef(nil), refs...)
	refsMu.Unlock()
	if len(gotRefs) != 2 || gotRefs[0] != alpha || gotRefs[1] != beta {
		t.Fatalf("resolver refs = %+v", gotRefs)
	}
	if got := alphaSeen.snapshot(); !sameCalls(got, "Bearer alpha", "core", 3) {
		t.Fatalf("alpha saw %+v", got)
	}
	if got := betaSeen.snapshot(); !sameCalls(got, "Bearer beta", "core", 2) {
		t.Fatalf("beta saw %+v", got)
	}
	if len(static.Get("authorization")) != 0 {
		t.Fatal("dial mutated the caller's metadata")
	}
}

func TestCredentialResolverFailureDoesNotSend(t *testing.T) {
	address, accepted := discardListener(t)
	ref := healthRef("vehicle", "boot", address)
	refused := errors.New("resolver refused")
	var calls atomic.Int32
	options := DialOptions{
		LocalTargetID: "core",
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true, VerifyConnection: func(tls.ConnectionState) error {
			return nil
		}},
		CredentialsFor: func(xrpc.ServiceRef) (credentials.TransportCredentials, metadata.MD, error) {
			calls.Add(1)
			return nil, nil, refused
		},
	}
	if _, err := Dial(ref, options); !errors.Is(err, refused) {
		t.Fatalf("dial error = %v", err)
	}
	files := healthFiles(t)
	profile := NewProfile(options, files)
	defer func() { profile.Close(); <-profile.Drained() }()
	err := callHealth(t, profile, ref, "req-fail")
	var failure *xrpc.CallError
	if !errors.As(err, &failure) || failure.Disposition != xrpc.NotSent || !errors.Is(err, refused) {
		t.Fatalf("profile error = %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("resolver calls = %d, want one per dial and no retry", calls.Load())
	}
	if accepted.Load() != 0 {
		t.Fatalf("failed resolver sent %d connections", accepted.Load())
	}

	reservedAddr, reservedAccepted := discardListener(t)
	_, err = Dial(healthRef("vehicle", "boot", reservedAddr), DialOptions{LocalTargetID: "core", CredentialsFor: func(xrpc.ServiceRef) (credentials.TransportCredentials, metadata.MD, error) {
		return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}), metadata.Pairs(RequestIDMetadata, "owned"), nil
	}})
	if err == nil || reservedAccepted.Load() != 0 {
		t.Fatalf("reserved metadata error=%v accepts=%d", err, reservedAccepted.Load())
	}
	duplicateAddr, duplicateAccepted := discardListener(t)
	_, err = Dial(healthRef("vehicle", "boot", duplicateAddr), DialOptions{
		LocalTargetID: "core",
		Metadata:      metadata.Pairs("authorization", "Bearer static"),
		CredentialsFor: func(xrpc.ServiceRef) (credentials.TransportCredentials, metadata.MD, error) {
			return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}), metadata.Pairs("authorization", "Bearer other"), nil
		},
	})
	if err == nil || duplicateAccepted.Load() != 0 {
		t.Fatalf("duplicate metadata error=%v accepts=%d", err, duplicateAccepted.Load())
	}
}

func TestCredentialResolverRejectsNonTLSAndWrongTrust(t *testing.T) {
	address, accepted := discardListener(t)
	ref := healthRef("vehicle", "boot", address)
	for _, creds := range []credentials.TransportCredentials{nil, insecure.NewCredentials(), fixedProtocol{protocol: "insecure"}, fixedProtocol{}} {
		_, err := Dial(ref, DialOptions{LocalTargetID: "core", CredentialsFor: func(xrpc.ServiceRef) (credentials.TransportCredentials, metadata.MD, error) {
			return creds, nil, nil
		}})
		if err == nil || accepted.Load() != 0 {
			t.Fatalf("non-tls credentials error=%v accepts=%d", err, accepted.Load())
		}
	}

	serverID, otherID := newIdentity(t, "server"), newIdentity(t, "other")
	seen := &callLog{}
	serverAddr := serveTLSHealth(t, serverID, "boot", seen)
	pool := x509.NewCertPool()
	pool.AddCert(serverID.leaf)
	wrong := healthRef("vehicle", "boot", serverAddr)
	connection, err := Dial(wrong, DialOptions{
		LocalTargetID: "core",
		TLSConfig:     &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: "127.0.0.1"},
		CredentialsFor: func(xrpc.ServiceRef) (credentials.TransportCredentials, metadata.MD, error) {
			return credentials.NewTLS(pinned(otherID.pin, nil)), metadata.Pairs("authorization", "Bearer wrong"), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err = healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{}); err == nil || len(seen.snapshot()) != 0 {
		t.Fatalf("wrong trust was accepted: %v seen=%+v", err, seen.snapshot())
	}
}

func TestLocalUnixDialDoesNotResolveCredentials(t *testing.T) {
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
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, server) }, HostOptions{InstanceID: "boot", MaxCallTime: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	var calls atomic.Int32
	ref := xrpc.ServiceRef{TargetID: "local", Service: "grpc.health.v1.Health", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.Path()}}
	connection, err := Dial(ref, DialOptions{LocalTargetID: "local", CredentialsFor: func(xrpc.ServiceRef) (credentials.TransportCredentials, metadata.MD, error) {
		calls.Add(1)
		return nil, nil, errors.New("resolver must not run")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err = healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("unix dial called resolver %d times", calls.Load())
	}
}

func TestNilCredentialsForPreservesTLSConfig(t *testing.T) {
	ref := healthRef("local", "boot", "127.0.0.1:1")
	_, err := Dial(ref, DialOptions{LocalTargetID: "local", TLSConfig: &tls.Config{InsecureSkipVerify: true, VerifyConnection: func(tls.ConnectionState) error { return nil }}})
	if err == nil || err.Error() != "xrpc: authenticated gRPC cannot skip TLS verification" {
		t.Fatalf("skip verification error = %v", err)
	}

	id := newIdentity(t, "server")
	seen := &callLog{}
	address := serveTLSHealth(t, id, "boot", seen)
	pool := x509.NewCertPool()
	pool.AddCert(id.leaf)
	connection, err := Dial(healthRef("local", "boot", address), DialOptions{LocalTargetID: "local", TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: "127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("nil callback tls dial: %v %v", response, err)
	}
}

type recordedCall struct {
	authorization string
	caller        string
}

type callLog struct {
	mu    sync.Mutex
	calls []recordedCall
}

func (l *callLog) add(authorization, caller string) {
	l.mu.Lock()
	l.calls = append(l.calls, recordedCall{authorization: authorization, caller: caller})
	l.mu.Unlock()
}

func (l *callLog) snapshot() []recordedCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]recordedCall(nil), l.calls...)
}

func sameCalls(calls []recordedCall, authorization, caller string, count int) bool {
	if len(calls) != count {
		return false
	}
	for _, call := range calls {
		if call.authorization != authorization || call.caller != caller {
			return false
		}
	}
	return true
}

func healthRef(target, instance, address string) xrpc.ServiceRef {
	return xrpc.ServiceRef{TargetID: target, Service: "grpc.health.v1.Health", APIVersion: "v1", InstanceID: instance, Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "tls", Address: address}}
}

func healthFiles(t *testing.T) *protoregistry.Files {
	t.Helper()
	files := &protoregistry.Files{}
	if err := files.RegisterFile(healthpb.File_grpc_health_v1_health_proto); err != nil {
		t.Fatal(err)
	}
	return files
}

func callHealth(t *testing.T, profile *Profile, ref xrpc.ServiceRef, requestID string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := profile.Call(ctx, xrpc.Call{Service: ref, Method: "/grpc.health.v1.Health/Check", RequestID: requestID, Payload: []byte(`{}`)})
	return err
}

func serveTLSHealth(t *testing.T, id identity, instance string, seen *callLog) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{id.certificate}})),
		grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			authorization, caller := "", ""
			if values := md.Get("authorization"); len(values) == 1 {
				authorization = values[0]
			}
			if values := md.Get("x-caller"); len(values) == 1 {
				caller = values[0]
			}
			seen.add(authorization, caller)
			header := metadata.Pairs(InstanceIDMetadata, instance)
			if values := md.Get(RequestIDMetadata); len(values) == 1 {
				header.Set(RequestIDMetadata, values[0])
			}
			if err := grpc.SetHeader(ctx, header); err != nil {
				return nil, err
			}
			return handler(ctx, request)
		}),
	)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func discardListener(t *testing.T) (string, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int32
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			_ = connection.Close()
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String(), &accepted
}

type fixedProtocol struct{ protocol string }

func (p fixedProtocol) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("handshake must not run")
}
func (p fixedProtocol) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("unused")
}
func (p fixedProtocol) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: p.protocol}
}
func (p fixedProtocol) Clone() credentials.TransportCredentials { return p }
func (p fixedProtocol) OverrideServerName(string) error         { return nil }
