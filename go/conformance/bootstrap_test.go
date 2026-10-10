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
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

// A loaded server binding and a loaded client binding must produce TLS
// configurations that complete a real mutual-TLS exchange through the HTTPS
// client, with the endpoint DNS name verified while dialing a numeric address.
func TestBootstrapCredentialsDriveNativeMTLS(t *testing.T) {
	serverInput := credentialInput(t, remoteBinding(xrpc.HTTP, xrpc.Endpoint{Kind: "https", Address: "https://fixture.example:8443"}), xrpc.BootstrapServer)
	clientInput := credentialInput(t, remoteBinding(xrpc.HTTP, xrpc.Endpoint{Kind: "https", Address: "https://fixture.example:8443"}), xrpc.BootstrapClient)
	// Each fixture generates its own CA; trust the server's certificate on the client.
	var domains atomic.Int32
	server := httptest.NewUnstartedServer(httpx.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || r.Header.Get("Authorization") != "Bearer credential-token" {
			t.Errorf("native mTLS or bearer header absent: tls=%v", r.TLS != nil)
		}
		domains.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}), httpx.HostOptions{InstanceID: "actual-instance"}))
	serverTLS := serverInput.Credentials().TLSConfig()
	serverTLS.ClientCAs = clientInput.Credentials().TLSConfig().RootCAs
	server.TLS = serverTLS
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	clientTLS := clientInput.Credentials().TLSConfig()
	clientTLS.RootCAs = serverInput.Credentials().TLSConfig().ClientCAs
	ref, err := clientInput.Binding().ServiceRef("actual-instance")
	if err != nil {
		t.Fatal(err)
	}
	dial := func(ctx context.Context, _ xrpc.ServiceRef) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	client, err := httpx.New(httpx.Config{LocalTargetID: "local", Service: ref, TLSConfig: clientTLS, Headers: clientInput.Credentials().Headers(), DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if body, code, _, err := client.Do(ctx, "POST", "/invoke", "bound-http", "application/json", []byte(`{}`)); err != nil || code != 200 || string(body) != `{"ok":true}` {
		t.Fatalf("response=%s code=%d err=%v", body, code, err)
	}
	noCertificate := clientInput.Credentials().TLSConfig()
	noCertificate.RootCAs = clientTLS.RootCAs
	noCertificate.Certificates = nil
	unauthenticated, err := httpx.New(httpx.Config{LocalTargetID: "local", Service: ref, TLSConfig: noCertificate, Headers: clientInput.Credentials().Headers(), DialContext: dial})
	if err != nil {
		t.Fatal(err)
	}
	defer unauthenticated.Close()
	if _, _, _, err := unauthenticated.Do(ctx, "GET", "/invoke", "no-cert", "", nil); err == nil {
		t.Fatal("missing client certificate accepted")
	}
	wrongName := clientInput.Credentials().TLSConfig()
	wrongName.RootCAs = clientTLS.RootCAs
	wrongName.ServerName = "wrong.example"
	badDNS, err := httpx.New(httpx.Config{LocalTargetID: "local", Service: ref, TLSConfig: wrongName, Headers: clientInput.Credentials().Headers(), DialContext: dial})
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
	insecure := clientInput.Credentials().TLSConfig()
	insecure.InsecureSkipVerify = true
	if _, err := httpx.New(httpx.Config{LocalTargetID: "local", Service: ref, TLSConfig: insecure}); err == nil {
		t.Fatal("certificate verification skipped")
	}
	stale := ref
	stale.Service = "another"
	if err := clientInput.Credentials().CheckReference(stale); err == nil {
		t.Fatal("different binding accepted")
	}
}

// The native gRPC client must not offer legacy TLS even when the caller's
// configuration allows it. The peer only needs to see the ClientHello.
func TestNativeGRPCClientDoesNotOfferLegacyTLS(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	input := credentialInput(t, remoteBinding(xrpc.GRPC, xrpc.Endpoint{Kind: "tls", Address: "fixture.example:" + port}), xrpc.BootstrapServer)
	versions := make(chan []uint16, 1)
	serverTLS := input.Credentials().TLSConfig()
	serverTLS.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		select {
		case versions <- append([]uint16(nil), hello.SupportedVersions...):
		default:
		}
		return nil, nil
	}
	secured := tls.NewListener(listener, serverTLS)
	go func() {
		for {
			connection, err := secured.Accept()
			if err != nil {
				return
			}
			_ = connection.(*tls.Conn).Handshake()
			connection.Close()
		}
	}()
	clientInput := credentialInput(t, remoteBinding(xrpc.GRPC, xrpc.Endpoint{Kind: "tls", Address: "fixture.example:" + port}), xrpc.BootstrapClient)
	legacy := clientInput.Credentials().TLSConfig()
	legacy.MinVersion = tls.VersionTLS10
	ref, _ := clientInput.Binding().ServiceRef("actual")
	conn, err := grpcx.Dial(ref, grpcx.DialOptions{LocalTargetID: "local", TLSConfig: legacy, DialContext: func(ctx context.Context, _ xrpc.ServiceRef) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// The handshake is rejected (different CAs); only the offered versions matter.
	_, _ = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	select {
	case offered := <-versions:
		for _, version := range offered {
			if version < tls.VersionTLS12 {
				t.Fatalf("native ClientHello offered legacy TLS %x", version)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("no ClientHello observed")
	}
}
