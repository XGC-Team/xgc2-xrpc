package httpx

import (
	"bufio"
	"context"
	"errors"
	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func fixture(t *testing.T, handler http.Handler, maxBody int64) (*Client, *atomic.Int64) {
	t.Helper()
	path := filepath.Join(privateTempDir(t), "rpc.sock")
	lease, err := unixlease.Reserve(context.Background(), path, unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	calls := &atomic.Int64{}
	host, err := Serve(listener, lease, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); handler.ServeHTTP(w, r) }), HostOptions{InstanceID: "boot-1", MaxBodyBytes: maxBody})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = host.Shutdown(ctx)
	})
	client, err := New(Config{LocalTargetID: "local", Service: xrpc.ServiceRef{TargetID: "local", Service: "test", APIVersion: "1", InstanceID: "boot-1", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: path}}, MaxResponseBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, calls
}
func TestFiniteCallAndGenerationFence(t *testing.T) {
	client, calls := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Context().Deadline(); !ok {
			t.Error("unbounded handler")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}), 1024)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	output, status, _, err := client.Do(ctx, "POST", "/call", "r1", "application/json", []byte(`{}`))
	if err != nil || status != 200 || string(output) != `{"ok":true}` {
		t.Fatalf("%s %d %v", output, status, err)
	}
	if _, _, _, err = client.Do(context.Background(), "POST", "/call", "r2", "application/json", nil); err == nil {
		t.Fatal("missing deadline accepted")
	}
	client.config.Service.InstanceID = "old-boot"
	_, _, _, err = client.Do(ctx, "POST", "/call", "r3", "application/json", []byte(`{}`))
	var failure *xrpc.CallError
	if !errors.As(err, &failure) || failure.Code != "conflict" || calls.Load() != 1 {
		t.Fatalf("fence: %v calls=%d", err, calls.Load())
	}
}
func TestMutationCancellationIsUnknownAndNotReplayed(t *testing.T) {
	client, calls := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }), 1024)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, _, _, err := client.Do(ctx, "POST", "/mutate", "r1", "application/json", []byte(`{}`))
	var failure *xrpc.CallError
	if !errors.As(err, &failure) || failure.Disposition != xrpc.OutcomeUnknown || calls.Load() != 1 {
		t.Fatalf("%v calls=%d", err, calls.Load())
	}
}
func TestRemoteUnixNeedsRouter(t *testing.T) {
	_, err := New(Config{LocalTargetID: "local", Service: xrpc.ServiceRef{TargetID: "remote", Service: "test", APIVersion: "1", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/run/remote.sock"}}})
	if err == nil {
		t.Fatal("remote path routed locally")
	}
}

func TestShutdownClosesHijackedEdgeConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host, err := ServeEdge(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
		_ = rw.Flush()
	}), HostOptions{})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_, _ = connection.Write([]byte("GET /stream HTTP/1.1\r\nHost: local\r\n\r\n"))
	reader := bufio.NewReader(connection)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = host.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = reader.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("hijacked connection survived shutdown: %v", err)
	}
}

func TestRemoteDialRetainsFiniteCallerBudget(t *testing.T) {
	var remaining time.Duration
	client, err := New(Config{LocalTargetID: "local", Service: xrpc.ServiceRef{TargetID: "remote", Service: "test", APIVersion: "1", InstanceID: "boot", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/run/remote.sock"}}, DialContext: func(ctx context.Context, _ xrpc.ServiceRef) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return nil, errors.New("lost budget")
		}
		remaining = time.Until(deadline)
		return nil, errors.New("offline")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, _, _, _ = client.Do(ctx, "POST", "/mutate", "request", "application/json", nil)
	if remaining <= 0 || remaining > 80*time.Millisecond {
		t.Fatalf("dial budget=%v", remaining)
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
func TestClientCloseIsTerminalAndInterruptsActiveResponse(t *testing.T) {
	entered := make(chan struct{})
	client, _ := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(entered)
		<-r.Context().Done()
	}), 1024)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := client.DoStream(ctx, "GET", "/wait", "close-owner", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	client.Close()
	_, err = io.ReadAll(response.Body)
	response.Body.Close()
	if err == nil {
		t.Fatal("active response survived owner close")
	}
	if _, err = client.DoStream(ctx, "GET", "/wait", "closed-owner", "", nil); err == nil {
		t.Fatal("terminal owner accepted new call")
	}
}
func TestClientRejectsOverloadBeforeDial(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	client, _ := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.Write([]byte("null")) }), 1024)
	client.slots = make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() { _, _, _, err := client.Do(ctx, "POST", "/wait", "first", "", nil); first <- err }()
	<-entered
	_, _, _, err := client.Do(ctx, "POST", "/wait", "second", "", nil)
	var failure *xrpc.CallError
	if !errors.As(err, &failure) || failure.Code != "resource_exhausted" || failure.Disposition != xrpc.NotSent {
		t.Fatalf("unexpected overload result: %v", err)
	}
	close(release)
	if err = <-first; err != nil {
		t.Fatal(err)
	}
}

func TestClientGeneratesARequestIdentityWhenNoneIsGiven(t *testing.T) {
	seen := make(chan string, 4)
	client, _ := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get(RequestIDHeader)
		w.Write([]byte("null"))
	}), 1024)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ids := map[string]bool{}
	for i := 0; i < 3; i++ {
		if _, _, _, err := client.Do(ctx, "POST", "/generated", "", "", nil); err != nil {
			t.Fatal(err)
		}
		id := <-seen
		if len(id) != 32 || !xrpc.ValidID(id) || ids[id] {
			t.Fatalf("generated identity %q is not fresh 128-bit hex", id)
		}
		ids[id] = true
	}
	// A supplied identity is still carried unchanged; an invalid one is refused.
	if _, _, _, err := client.Do(ctx, "POST", "/given", "caller:1", "", nil); err != nil || <-seen != "caller:1" {
		t.Fatalf("supplied identity: %v", err)
	}
	var failure *xrpc.CallError
	if _, _, _, err := client.Do(ctx, "POST", "/bad", "not valid!", "", nil); !errors.As(err, &failure) || failure.Disposition != xrpc.NotSent || failure.Code != "invalid_argument" {
		t.Fatalf("invalid identity: %v", err)
	}
}

func TestDiscoveryRouteTakesAQueryWithoutAnInstance(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "rpc.sock")
	lease, err := unixlease.Reserve(context.Background(), path, unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan string, 8)
	host, err := Serve(listener, lease, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.URL.RequestURI()
		w.Write([]byte(`{"ok":true}`))
	}), HostOptions{InstanceID: "boot-1", DiscoveryPaths: []string{"/v1/describe"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = host.Shutdown(ctx)
	})
	ref := func(instance string) xrpc.ServiceRef {
		return xrpc.ServiceRef{TargetID: "local", Service: "test", APIVersion: "1", InstanceID: instance, Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: path}}
	}
	// Core does not know the instance before the first describe.
	unpinned, err := New(Config{LocalTargetID: "local", Service: ref("")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unpinned.Close)
	pinned, err := New(Config{LocalTargetID: "local", Service: ref("boot-1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pinned.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, status, header, err := unpinned.Do(ctx, "GET", "/v1/describe?wait_ready_ms=250", "describe:1", "", nil); err != nil || status != 200 || header.Get(InstanceIDHeader) != "boot-1" {
		t.Fatalf("status=%d instance=%q err=%v", status, header.Get(InstanceIDHeader), err)
	}
	if got := <-seen; got != "/v1/describe?wait_ready_ms=250" {
		t.Fatalf("the handler saw %q", got)
	}
	if _, status, _, err := pinned.Do(ctx, "GET", "/v1/describe?wait_ready_ms=250", "describe:2", "", nil); err != nil || status != 200 {
		t.Fatalf("pinned: status=%d err=%v", status, err)
	}
	<-seen
	// The query is no part of the match: it makes no other route and no longer path discovery.
	for _, target := range []string{"/v1/echo?wait_ready_ms=250", "/v1/describe/more?wait_ready_ms=250", "/v1/other?/v1/describe"} {
		if _, status, _, err := unpinned.Do(ctx, "GET", target, "describe:3", "", nil); err != nil || status != 409 {
			t.Fatalf("%s: status=%d err=%v", target, status, err)
		}
	}
	select {
	case got := <-seen:
		t.Fatalf("a call that had to fail reached the handler: %s", got)
	default:
	}
}
