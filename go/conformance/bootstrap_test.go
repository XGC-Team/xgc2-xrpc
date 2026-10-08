package audit

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func credentialInput(t *testing.T, binding xrpc.BootstrapBinding, role xrpc.BootstrapRole) *xrpc.BootstrapInput {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(42), Subject: pkix.Name{CommonName: "bootstrap-test"}, DNSNames: []string{"fixture.example"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, data []byte) string {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	certPath := write("cert", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPath := write("key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}))
	tokenPath := write("token", []byte("credential-token"))
	binding.SecretHandles = xrpc.SecretHandles{TLSIdentity: "identity", TLSTrust: "trust", Authorization: "caller"}
	document := map[string]any{"schema_version": 1, "binding": binding, "grants": map[string]any{"identity": map[string]string{"kind": "tls_identity", "cert_file": certPath, "key_file": keyPath}, "trust": map[string]string{"kind": "tls_trust", "ca_file": certPath}, "caller": map[string]string{"kind": "bearer", "token_file": tokenPath}}, "application": map[string]string{"domain": "unchanged"}}
	inputJSON, _ := json.Marshal(document)
	input, err := xrpc.LoadBootstrapInput(write("input", inputJSON), role)
	if err != nil {
		t.Fatal(err)
	}
	return input
}
func remoteBinding(profile string, endpoint xrpc.Endpoint) xrpc.BootstrapBinding {

	service := "fixture"
	if profile == xrpc.GRPC {
		service = "grpc.health.v1.Health"
	}
	return xrpc.BootstrapBinding{SchemaVersion: 1, TargetID: "remote", Service: service, APIVersion: "v1", Profile: profile, Endpoint: endpoint, RuntimeGrant: "owner:runtime", Authentication: xrpc.MutualTLS, StorageGrants: []string{}}
}

func TestBoundHTTPUsesActualNativeMTLSGrantAndDNS(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	input := credentialInput(t, remoteBinding(xrpc.HTTP, xrpc.Endpoint{Kind: "https", Address: "https://fixture.example:" + port}), xrpc.BootstrapServer)
	var domains atomic.Int32
	host, err := httpx.ServeBound(listener, nil, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			t.Error("native mTLS absent")
		}
		domains.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}), input.Credentials(), "actual-instance", httpx.HostOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := host.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	clientCredentials, err := input.Binding().ResolveCredentials(input.ResolveGrant, xrpc.BootstrapClient)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := input.Binding().ServiceRef("actual-instance")
	if err != nil {
		t.Fatal(err)
	}
	dial := func(ctx context.Context, _ xrpc.ServiceRef) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}
	client, err := httpx.NewBound(clientCredentials, ref, httpx.Config{LocalTargetID: "local", DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if body, code, _, err := client.Do(ctx, "POST", "/invoke", "bound-http", "application/json", []byte(`{}`)); err != nil || code != 200 || string(body) != `{"ok":true}` {
		t.Fatalf("response=%s code=%d err=%v", body, code, err)
	}
	if _, _, _, err := client.DoWithHeaders(ctx, "GET", "/invoke", "override", "", nil, map[string]string{"authorization": "Bearer other"}); err == nil {
		t.Fatal("bound credential override accepted")
	}
	if domains.Load() != 1 {
		t.Fatal("credential override dispatched")
	}
	// Native callers with valid TLS but missing/duplicate caller authorization
	// are rejected before domain work, even when transport authentication passes.
	raw := &http.Client{Transport: &http.Transport{TLSClientConfig: clientCredentials.TLSConfig(), DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return dial(ctx, ref) }}, Timeout: time.Second}
	defer raw.CloseIdleConnections()
	for _, auth := range [][]string{nil, {"Bearer credential-token", "Bearer credential-token"}, {"Bearer wrong"}} {
		request, _ := http.NewRequest(http.MethodGet, ref.Endpoint.Address+"/invoke", nil)
		request.Header.Set(httpx.TimeoutHeader, "500")
		request.Header.Set(httpx.RequestIDHeader, "native-auth")
		request.Header.Set(httpx.InstanceIDHeader, ref.InstanceID)
		for _, value := range auth {
			request.Header.Add("Authorization", value)
		}
		response, err := raw.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != 403 {
			t.Fatalf("authorization status=%d", response.StatusCode)
		}
	}
	if domains.Load() != 1 {
		t.Fatal("unauthorized domain dispatch")
	}
	unsigned, err := httpx.New(httpx.Config{LocalTargetID: "local", Service: ref, TLSConfig: clientCredentials.TLSConfig(), DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	defer unsigned.Close()
	if _, err := unsigned.Call(ctx, xrpc.Call{Service: ref, Method: "GET", Path: "/invoke", RequestID: "unsigned-sdk"}); xrpc.Code(err) != "permission_denied" {
		t.Fatal("transport authorization category", err)
	}
	noCertificate := clientCredentials.TLSConfig()
	noCertificate.Certificates = nil
	unauthenticated, err := httpx.New(httpx.Config{LocalTargetID: "local", Service: ref, TLSConfig: noCertificate, Headers: clientCredentials.Headers(), DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	defer unauthenticated.Close()
	if _, _, _, err := unauthenticated.Do(ctx, "GET", "/invoke", "no-cert", "", nil); err == nil {
		t.Fatal("missing client certificate accepted")
	}
	wrongName := clientCredentials.TLSConfig()
	wrongName.ServerName = "wrong.example"
	badDNS, err := httpx.New(httpx.Config{LocalTargetID: "local", Service: ref, TLSConfig: wrongName, Headers: clientCredentials.Headers(), DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	defer badDNS.Close()
	if _, _, _, err := badDNS.Do(ctx, "GET", "/invoke", "wrong-san", "", nil); err == nil {
		t.Fatal("wrong SAN accepted")
	}
	if domains.Load() != 1 {
		t.Fatal("TLS rejection dispatched")
	}
	stale := ref
	stale.Service = "another"
	if _, err := httpx.NewBound(clientCredentials, stale, httpx.Config{}); err == nil {
		t.Fatal("different binding accepted")
	}
}
func TestBoundGRPCUsesNativeCredentialsAndGrant(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	input := credentialInput(t, remoteBinding(xrpc.GRPC, xrpc.Endpoint{Kind: "tls", Address: "fixture.example:" + port}), xrpc.BootstrapServer)
	var productCalls atomic.Int32
	host, err := grpcx.ServeBound(listener, nil, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, input.Credentials(), "actual-instance", grpcx.HostOptions{}, grpc.ChainUnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		productCalls.Add(1)
		return next(ctx, request)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	clientCredentials, err := input.Binding().ResolveCredentials(input.ResolveGrant, xrpc.BootstrapClient)
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := input.Binding().ServiceRef("actual-instance")
	dial := func(ctx context.Context, _ xrpc.ServiceRef) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}
	conn, err := grpcx.DialBound(clientCredentials, ref, grpcx.DialOptions{LocalTargetID: "local", DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	override := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer other"))
	if _, err := healthpb.NewHealthClient(conn).Check(override, &healthpb.HealthCheckRequest{}); err == nil {
		t.Fatal("bound authorization replaced")
	}
	if productCalls.Load() != 1 {
		t.Fatal("override dispatched")
	}
	native, err := grpcx.Dial(ref, grpcx.DialOptions{LocalTargetID: "local", TLSConfig: clientCredentials.TLSConfig(), DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()

	profile := grpcx.NewProfile(grpcx.DialOptions{LocalTargetID: "local", TLSConfig: clientCredentials.TLSConfig(), DialContext: dial}, protoregistry.GlobalFiles)
	defer profile.Close()
	if _, err := profile.Call(ctx, xrpc.Call{Service: ref, Method: "/grpc.health.v1.Health/Check", RequestID: "unsigned-profile", Payload: json.RawMessage(`{}`)}); xrpc.Code(err) != "permission_denied" {
		t.Fatal("profile authorization category", err)
	}
	if _, err := healthpb.NewHealthClient(native).Check(ctx, &healthpb.HealthCheckRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	duplicates := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer credential-token", "authorization", "Bearer credential-token"))
	if _, err := healthpb.NewHealthClient(native).Check(duplicates, &healthpb.HealthCheckRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	stream, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	if productCalls.Load() != 1 {
		t.Fatal("unauthorized caller reached product")
	}
	noCertificate := clientCredentials.TLSConfig()
	noCertificate.Certificates = nil
	rejected, err := grpcx.Dial(ref, grpcx.DialOptions{LocalTargetID: "local", TLSConfig: noCertificate, DialContext: dial, Metadata: metadata.Pairs("authorization", "Bearer credential-token")})
	if err != nil {
		t.Fatal(err)
	}
	defer rejected.Close()
	deniedCtx, denyCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer denyCancel()
	if _, err := healthpb.NewHealthClient(rejected).Check(deniedCtx, &healthpb.HealthCheckRequest{}); err == nil {
		t.Fatal("gRPC missing certificate accepted")
	}
}

func TestBoundLocalHostRetainsLeaseThroughExpiredAuthorization(t *testing.T) {
	directory := t.TempDir()
	_ = os.Chmod(directory, 0700)
	path := filepath.Join(directory, "rpc.sock")
	lease, err := unixlease.Reserve(context.Background(), path, unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	binding := remoteBinding(xrpc.HTTP, xrpc.Endpoint{Kind: "unix", Address: path})
	binding.TargetID = "local"
	binding.Authentication = xrpc.LocalPrivate
	binding.SecretHandles = xrpc.SecretHandles{Authorization: "caller"}
	entered, release := make(chan struct{}), make(chan struct{})
	grant, err := xrpc.NewAuthorizationGrant(func(context.Context, []string) bool { close(entered); <-release; return true }, nil)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := binding.ResolveCredentials(func(string) (xrpc.CredentialGrant, error) { return grant, nil }, xrpc.BootstrapServer)
	if err != nil {
		t.Fatal(err)
	}
	var domainCalls atomic.Int32
	host, err := httpx.ServeBound(listener, lease, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { domainCalls.Add(1) }), bound, "actual", httpx.HostOptions{MaxCallTime: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := binding.ServiceRef("actual")
	client, err := httpx.New(httpx.Config{Service: ref, LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	result := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, _, _, err := client.Do(ctx, "GET", "/call", "delayed-auth", "", nil)
		result <- err
	}()
	<-entered
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := host.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("callback falsely drained %v", err)
	}
	if replacement, err := unixlease.Reserve(context.Background(), path, unixlease.Options{}); err == nil {
		replacement.Close()
		t.Fatal("lease released while authorization alive")
	}
	close(release)
	select {
	case <-host.Drained():
	case <-time.After(time.Second):
		t.Fatal("authorization drain")
	}
	if domainCalls.Load() != 0 {
		t.Fatal("expired authorization dispatched")
	}
	if err := <-result; err == nil {
		t.Fatal("expired exchange succeeded")
	}
}

func TestBoundLocalListenerMustBelongToItsLease(t *testing.T) {
	directory := t.TempDir()
	_ = os.Chmod(directory, 0700)
	leaseA, err := unixlease.Reserve(context.Background(), filepath.Join(directory, "a.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listenerA, err := leaseA.Listen()
	if err != nil {
		t.Fatal(err)
	}
	defer leaseA.Close()
	defer listenerA.Close()
	leaseB, err := unixlease.Reserve(context.Background(), filepath.Join(directory, "b.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listenerB, err := leaseB.Listen()
	if err != nil {
		t.Fatal(err)
	}
	defer leaseB.Close()
	defer listenerB.Close()
	binding := remoteBinding(xrpc.HTTP, xrpc.Endpoint{Kind: "unix", Address: leaseA.Path()})
	binding.Authentication = xrpc.LocalPrivate
	bound, err := binding.ResolveCredentials(nil, xrpc.BootstrapServer)
	if err != nil {
		t.Fatal(err)
	}
	if host, err := httpx.ServeBound(listenerB, leaseA, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), bound, "actual", httpx.HostOptions{}); err == nil {
		host.Shutdown(context.Background())
		t.Fatal("HTTP mismatched listener accepted")
	}
	binding.Profile = xrpc.GRPC
	bound, err = binding.ResolveCredentials(nil, xrpc.BootstrapServer)
	if err != nil {
		t.Fatal(err)
	}
	if host, err := grpcx.ServeBound(listenerB, leaseA, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, bound, "actual", grpcx.HostOptions{}); err == nil {
		host.Stop()
		t.Fatal("gRPC mismatched listener accepted")
	}
	if err := leaseA.ValidateListener(listenerA); err != nil {
		t.Fatal("owned listener rejected", err)
	}
}

func TestBoundGRPCLateAuthorizationCannotDispatchAndRetainsLease(t *testing.T) {
	directory := t.TempDir()
	_ = os.Chmod(directory, 0700)
	path := filepath.Join(directory, "rpc.sock")
	lease, err := unixlease.Reserve(context.Background(), path, unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	binding := remoteBinding(xrpc.GRPC, xrpc.Endpoint{Kind: "unix", Address: path})
	binding.TargetID = "local"
	binding.Authentication = xrpc.LocalPrivate
	binding.SecretHandles = xrpc.SecretHandles{Authorization: "caller"}
	entered, release := make(chan struct{}), make(chan struct{})
	grant, err := xrpc.NewAuthorizationGrant(func(context.Context, []string) bool { close(entered); <-release; return true }, nil)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := binding.ResolveCredentials(func(string) (xrpc.CredentialGrant, error) { return grant, nil }, xrpc.BootstrapServer)
	if err != nil {
		t.Fatal(err)
	}
	var productCalls atomic.Int32
	host, err := grpcx.ServeBound(listener, lease, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, bound, "actual", grpcx.HostOptions{}, grpc.ChainUnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		productCalls.Add(1)
		return next(ctx, request)
	}))
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := binding.ServiceRef("actual")
	conn, err := grpcx.Dial(ref, grpcx.DialOptions{LocalTargetID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	result := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		result <- err
	}()
	<-entered
	if err := <-result; status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expired result %v", err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := host.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("callback falsely drained %v", err)
	}
	if replacement, err := unixlease.Reserve(context.Background(), path, unixlease.Options{}); err == nil {
		replacement.Close()
		t.Fatal("lease released while authorization alive")
	}
	close(release)
	select {
	case <-host.Drained():
	case <-time.After(time.Second):
		t.Fatal("gRPC authorization drain")
	}
	if productCalls.Load() != 0 {
		t.Fatal("expired authorization reached product")
	}
}

func TestBoundServerTLSAllowsNoClientCertificateButRequiresCallerGrant(t *testing.T) {
	for _, profile := range []string{xrpc.HTTP, xrpc.GRPC} {
		t.Run(profile, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			_, port, _ := net.SplitHostPort(listener.Addr().String())
			endpoint := xrpc.Endpoint{Kind: "https", Address: "https://fixture.example:" + port}
			if profile == xrpc.GRPC {
				endpoint = xrpc.Endpoint{Kind: "tls", Address: "fixture.example:" + port}
			}
			binding := remoteBinding(profile, endpoint)
			binding.Authentication = xrpc.ServerTLS
			input := credentialInput(t, binding, xrpc.BootstrapServer)
			clientCredentials, err := input.Binding().ResolveCredentials(input.ResolveGrant, xrpc.BootstrapClient)
			if err != nil {
				t.Fatal(err)
			}
			noCertificate := clientCredentials.TLSConfig()
			noCertificate.Certificates = nil
			ref, _ := input.Binding().ServiceRef("actual")
			dial := func(ctx context.Context, _ xrpc.ServiceRef) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if profile == xrpc.HTTP {
				host, err := httpx.ServeBound(listener, nil, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.TLS == nil || len(r.TLS.PeerCertificates) != 0 {
						t.Error("unexpected native TLS mode")
					}
					_, _ = w.Write([]byte(`{}`))
				}), input.Credentials(), "actual", httpx.HostOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer host.Shutdown(ctx)
				client, err := httpx.New(httpx.Config{LocalTargetID: "local", Service: ref, TLSConfig: noCertificate, Headers: clientCredentials.Headers(), DialContext: dial})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				if _, code, _, err := client.Do(ctx, "GET", "/", "server-tls", "", nil); err != nil || code != 200 {
					t.Fatalf("status=%d err=%v", code, err)
				}
			} else {
				host, err := grpcx.ServeBound(listener, nil, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, input.Credentials(), "actual", grpcx.HostOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { host.Stop(); <-host.Drained() }()
				conn, err := grpcx.Dial(ref, grpcx.DialOptions{LocalTargetID: "local", TLSConfig: noCertificate, DialContext: dial, Metadata: metadata.Pairs("authorization", "Bearer credential-token")})
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestNativeGRPCClientDoesNotOfferLegacyTLS(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	input := credentialInput(t, remoteBinding(xrpc.GRPC, xrpc.Endpoint{Kind: "tls", Address: "fixture.example:" + port}), xrpc.BootstrapServer)
	versions := make(chan []uint16, 1)
	serverTLS := input.Credentials().TLSConfig()
	serverTLS.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		versions <- append([]uint16(nil), hello.SupportedVersions...)
		return nil, nil
	}
	host, err := grpcx.ServeTLS(listener, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, health.NewServer()) }, serverTLS, grpcx.BoundService("actual", time.Second, 4)...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	clientCredentials, err := input.Binding().ResolveCredentials(input.ResolveGrant, xrpc.BootstrapClient)
	if err != nil {
		t.Fatal(err)
	}
	legacy := clientCredentials.TLSConfig()
	legacy.MinVersion = tls.VersionTLS10
	ref, _ := input.Binding().ServiceRef("actual")
	conn, err := grpcx.Dial(ref, grpcx.DialOptions{LocalTargetID: "local", TLSConfig: legacy, DialContext: func(ctx context.Context, _ xrpc.ServiceRef) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	for _, version := range <-versions {
		if version < tls.VersionTLS12 {
			t.Fatalf("native ClientHello offered legacy TLS %x", version)
		}
	}
}
