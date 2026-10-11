package httpx

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type certificate struct {
	pair tls.Certificate
	pool *x509.CertPool
}

func newCertificate(t *testing.T, name string) certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return certificate{pair: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool: pool}
}

func tlsEdge(t *testing.T, config *tls.Config, handler http.Handler, options HostOptions) (*Host, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	options.TLSConfig = config
	host, err := ServeEdge(listener, handler, options)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		host.Shutdown(ctx)
	})
	return host, listener.Addr().String()
}

// An edge serves TLS itself, so net/http sees the *tls.Conn and fills
// Request.TLS; a TLS listener wrapped around the host's listener would hide it
// behind the connection limiter.
func TestEdgeServesTLSAndPopulatesRequestTLS(t *testing.T) {
	server := newCertificate(t, "edge")
	seen := make(chan *tls.ConnectionState, 1)
	_, address := tlsEdge(t, &tls.Config{Certificates: []tls.Certificate{server.pair}}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.TLS
		w.Write([]byte("secure"))
	}), HostOptions{})
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: server.pool}}, Timeout: 2 * time.Second}
	response, err := client.Get("https://" + address + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	state := <-seen
	if string(body) != "secure" || state == nil || state.Version < tls.VersionTLS12 || state.NegotiatedProtocol == "h2" {
		t.Fatalf("body %q state %+v", body, state)
	}
	// Plaintext on the TLS port, and a client that does not trust the server.
	if response, err := (&http.Client{Timeout: time.Second}).Get("http://" + address + "/"); err == nil {
		response.Body.Close()
		if response.StatusCode == http.StatusOK {
			t.Fatal("plaintext request served on a TLS edge")
		}
	}
	if _, err := (&http.Client{Timeout: time.Second}).Get("https://" + address + "/"); err == nil {
		t.Fatal("a client without the server's CA connected")
	}
}

func TestEdgeMutualTLSExposesThePeerCertificate(t *testing.T) {
	server, agent := newCertificate(t, "edge"), newCertificate(t, "agent")
	peers := make(chan string, 2)
	_, address := tlsEdge(t, &tls.Config{Certificates: []tls.Certificate{server.pair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: agent.pool}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peers <- r.TLS.PeerCertificates[0].Subject.CommonName
		w.Write([]byte("ok"))
	}), HostOptions{})
	authenticated := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: server.pool, Certificates: []tls.Certificate{agent.pair}}}, Timeout: 2 * time.Second}
	response, err := authenticated.Get("https://" + address + "/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if peer := <-peers; peer != "agent" {
		t.Fatalf("peer %q", peer)
	}
	anonymous := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: server.pool}}, Timeout: 2 * time.Second}
	if response, err := anonymous.Get("https://" + address + "/"); err == nil {
		response.Body.Close()
		t.Fatal("a client without a certificate was served")
	}
	select {
	case peer := <-peers:
		t.Fatalf("the handler ran for %q", peer)
	default:
	}
}

func TestEdgeKeepsItsConnectionLimitBelowTLS(t *testing.T) {
	server := newCertificate(t, "edge")
	host, address := tlsEdge(t, &tls.Config{Certificates: []tls.Certificate{server.pair}}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), HostOptions{MaxConnections: 2})
	var held []net.Conn
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	for i := 0; i < 6; i++ {
		c, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := host.Status().Connections; n > 2 {
			t.Fatalf("admitted %d connections", n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTLSConfigurationRules(t *testing.T) {
	server := newCertificate(t, "edge")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	if host, err := ServeEdge(listener, handler, HostOptions{TLSConfig: &tls.Config{}}); err == nil {
		host.Shutdown(context.Background())
		t.Fatal("TLS without an identity accepted")
	}
	unixListener, err := net.Listen("unix", filepath.Join(t.TempDir(), "edge.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer unixListener.Close()
	if host, err := Serve(unixListener, nil, handler, HostOptions{TLSConfig: &tls.Config{Certificates: []tls.Certificate{server.pair}}}); err == nil {
		host.Shutdown(context.Background())
		t.Fatal("an internal Unix host accepted a TLS configuration")
	}
}

// The event stream cell over a TLS edge: server-auth TLS from ServeEdge, a
// caller-supplied client that trusts it, and a mutual-TLS variant.
func TestSubscribeEventsOverATLSEdge(t *testing.T) {
	server, agent := newCertificate(t, "edge"), newCertificate(t, "agent")
	j := newJournal()
	j.append("tick", `{"tls":true}`)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "client certificate required", http.StatusUnauthorized)
			return
		}
		_ = ServeEvents(w, r, j, EventsOptions{})
	})
	_, address := tlsEdge(t, &tls.Config{Certificates: []tls.Certificate{server.pair}, ClientAuth: tls.RequestClientCert}, handler, HostOptions{})
	ref := httpRef("https", "https://"+address)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	trusting := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: server.pool}}}
	if _, err := subscribeEvents(ctx, trusting, ref, "/events", "", fastJitter, EventIdleTimeout); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("a client without a certificate was served: %v", err)
	}
	authenticated := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: server.pool, Certificates: []tls.Certificate{agent.pair}}}}
	events, err := subscribeEvents(ctx, authenticated, ref, "/events", "", fastJitter, EventIdleTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if event := next(t, events); event.ID != "1" || string(event.Data) != `{"tls":true}` {
		t.Fatalf("%+v", event)
	}
}
