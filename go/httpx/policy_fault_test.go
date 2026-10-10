package httpx

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

func TestTLSHandshakeDoesNotOutliveCaller(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	closed := make(chan struct{})
	go func() {
		peer, err := listener.Accept()
		if err != nil {
			return
		}
		defer peer.Close()
		io.Copy(io.Discard, peer)
		close(closed)
	}()
	client, err := New(Config{LocalTargetID: "local", Service: xrpc.ServiceRef{TargetID: "remote", Service: "test", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "https", Address: "https://" + listener.Addr().String()}}, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, _, _, err = client.Do(ctx, "POST", "/", "tls-fault", "", nil); err == nil {
		t.Fatal("silent TLS peer succeeded")
	}
	select {
	case <-closed:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("TLS handshake socket survived the caller")
	}
}

func TestAuthHeadersImmutableAndWireProtected(t *testing.T) {
	client, _ := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer scoped" {
			t.Error("missing caller credential")
		}
		w.Write([]byte("null"))
	}), 1024)
	headers := map[string]string{"Authorization": "Bearer scoped"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, code, _, err := client.DoWithHeaders(ctx, "POST", "/", "auth", "", nil, headers); err != nil || code != 200 {
		t.Fatalf("auth call %d %v", code, err)
	}
	if _, _, _, err := client.DoWithHeaders(ctx, "POST", "/", "auth", "", nil, map[string]string{"x-xrpc-instance-id": "other"}); xrpc.Code(err) != "invalid_argument" {
		t.Fatal("wire override", err)
	}
	config := client.config
	config.Headers = headers
	second, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	headers["Authorization"] = "Bearer changed"
	if _, code, _, err := second.Do(ctx, "POST", "/", "auth-static", "", nil); err != nil || code != 200 {
		t.Fatalf("immutable auth call %d %v", code, err)
	}
}

func TestCallWithHeadersRejectsDifferentCompleteReferenceBeforeEffect(t *testing.T) {
	client, calls := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("null")) }), 1024)
	call := xrpc.Call{Service: client.Reference(), Method: "POST", Path: "/", RequestID: "binding", Payload: []byte("{}")}
	call.Service.TargetID = "other-target"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.CallWithHeaders(ctx, call, map[string]string{"Authorization": "Bearer scoped"})
	var failure *xrpc.CallError
	if !errors.As(err, &failure) || failure.Disposition != xrpc.NotSent || failure.Code != "invalid_argument" || calls.Load() != 0 {
		t.Fatalf("misbound effect err=%v calls=%d", err, calls.Load())
	}
	if reference := client.Reference(); reference.TargetID != "local" {
		t.Fatal("immutable reference changed")
	}
}

func TestClientRequestAdmissionAndSeparateResponseBound(t *testing.T) {
	client, calls := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("0123456789")) }), 4)
	bounded, err := New(Config{LocalTargetID: "local", Service: client.config.Service, MaxRequestBytes: 4, MaxResponseBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer bounded.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, _, err = bounded.Do(ctx, "POST", "/", "too-large", "", []byte("12345")); xrpc.Code(err) != "resource_exhausted" || calls.Load() != 0 {
		t.Fatalf("request admission err=%v calls=%d", err, calls.Load())
	}
	_, _, _, err = bounded.Do(ctx, "POST", "/", "response-bound", "", nil)
	var failure *xrpc.CallError
	if !errors.As(err, &failure) || failure.Code != "resource_exhausted" {
		t.Fatal("response not bounded", err)
	}
}

func TestTLSForServiceResolvedOnceAndCloned(t *testing.T) {
	var calls atomic.Int64
	provided := &tls.Config{ServerName: "authenticated.example", MinVersion: tls.VersionTLS12}
	client, err := New(Config{LocalTargetID: "local", Service: xrpc.ServiceRef{TargetID: "remote", Service: "test", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "https", Address: "https://authenticated.example:443"}}, TLSForService: func(ref xrpc.ServiceRef) (*tls.Config, error) {
		calls.Add(1)
		if ref.Service != "test" {
			t.Error("wrong immutable reference")
		}
		return provided, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	provided.ServerName = "changed"
	if calls.Load() != 1 || client.transport.TLSClientConfig.ServerName != "authenticated.example" {
		t.Fatal("TLS policy was mutable/repeated")
	}
}

func TestTLSResolverCannotHoldCallerOrReferenceTablePastDeadline(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	profile := NewProfile(Config{LocalTargetID: "local", TLSForService: func(xrpc.ServiceRef) (*tls.Config, error) {
		close(started)
		<-release
		return &tls.Config{MinVersion: tls.VersionTLS12}, nil
	}})
	call := xrpc.Call{Service: xrpc.ServiceRef{TargetID: "remote", Service: "test", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "https", Address: "https://authenticated.example"}}, Method: "POST", Path: "/", RequestID: "slow-resolver", Payload: []byte("{}")}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := profile.Call(ctx, call); done <- err }()
	<-started
	select {
	case err := <-done:
		var failure *xrpc.CallError
		if !errors.As(err, &failure) || failure.Code != "deadline_exceeded" || failure.Disposition != xrpc.NotSent {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("TLS resolver held caller")
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShutdown()
	if err := profile.Shutdown(shutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("noncooperative resolver ownership lost", err)
	}
	close(release)
	select {
	case <-profile.Drained():
	case <-time.After(time.Second):
		t.Fatal("late resolver did not drain")
	}
}

func TestHeaderResponseAndTrailerBoundsBeforeNativeFlush(t *testing.T) {
	for _, trailer := range []bool{false, true} {
		t.Run(strconv.FormatBool(trailer), func(t *testing.T) {
			handler := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if trailer {
					w.Header().Set("Trailer", "X-Large")
					w.Write([]byte("null"))
					w.Header().Set("X-Large", strings.Repeat("x", 10000))
				} else {
					w.Header().Set("X-Large", strings.Repeat("x", 10000))
					w.Write([]byte("null"))
				}
			}), HostOptions{MaxHeaderBytes: 512})
			request := httptest.NewRequest("GET", "/", nil)
			request.Header.Set(TimeoutHeader, "1000")
			request.Header.Set(RequestIDHeader, "headers")
			response := httptest.NewRecorder()
			var aborted bool
			func() {
				defer func() {
					if value := recover(); value != nil {
						if value != http.ErrAbortHandler {
							panic(value)
						}
						aborted = true
					}
				}()
				handler.ServeHTTP(response, request)
			}()
			if trailer {
				if !aborted {
					t.Fatal("oversized trailers would pass native finishRequest")
				}
			} else {
				if response.Code != 429 || len(response.Header().Get("X-Large")) > 512 {
					t.Fatalf("oversized response headers leaked %d", response.Code)
				}
			}
		})
	}
}
