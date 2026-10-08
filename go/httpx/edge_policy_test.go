package httpx

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

func TestEdgeResolvedResponseBudgetOnNativeWrites(t *testing.T) {
	policy, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{Environment: []string{"XGC2_XRPC_MAX_RESPONSE_BYTES=4"}})
	if err != nil {
		t.Fatal(err)
	}
	limits, err := (HostOptions{}).WithPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	if limits.MaxResponseBytes != 4 {
		t.Fatal("response policy not applied")
	}
	writeErrors := make(chan error, 2)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host, err := ServeEdge(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/single":
			_, err := w.Write([]byte("12345"))
			writeErrors <- err
		case "/chunks":
			_, _ = w.Write([]byte("1234"))
			w.(http.Flusher).Flush()
			_, err := w.Write([]byte("5"))
			writeErrors <- err
		case "/head":
			w.Header().Set("Content-Length", "5000")
			_, _ = w.Write([]byte(strings.Repeat("x", 5000)))
		}
	}), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := host.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	client := &http.Client{Timeout: time.Second}
	base := "http://" + listener.Addr().String()
	response, err := client.Get(base + "/single")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 429 || len(body) > 4 {
		t.Fatalf("single status=%d body=%q err=%v", response.StatusCode, body, err)
	}
	if err := <-writeErrors; !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("single write: %v", err)
	}
	response, err = client.Get(base + "/chunks")
	if err != nil {
		t.Fatal(err)
	}
	body, err = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || string(body) != "1234" || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("chunks status=%d body=%q err=%v", response.StatusCode, body, err)
	}
	if err := <-writeErrors; !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("chunk write: %v", err)
	}
	request, _ := http.NewRequest(http.MethodHead, base+"/head", nil)
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err = io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || len(body) != 0 || response.Header.Get("Content-Length") != strconv.Itoa(5000) {
		t.Fatalf("HEAD status=%d length=%s body=%d err=%v", response.StatusCode, response.Header.Get("Content-Length"), len(body), err)
	}
}

func TestEdgeWithoutRPCPolicyPreservesDomainStreamingBudget(t *testing.T) {
	policy, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{Environment: []string{}, Capabilities: []string{"host", "http", "transport"}})
	if err != nil {
		t.Fatal(err)
	}
	limits, err := (HostOptions{}).WithPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	if limits.MaxResponseBytes != 0 || limits.MaxCallTime != 0 {
		t.Fatal("non-RPC edge acquired an internal body/lifetime budget")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host, err := ServeEdge(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", int(xrpc.DefaultPolicyInteger("MAX_RESPONSE_BYTES"))+1)))
	}), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := host.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	response, err := (&http.Client{Timeout: time.Second}).Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || int64(len(body)) != xrpc.DefaultPolicyInteger("MAX_RESPONSE_BYTES")+1 {
		t.Fatalf("stream bytes=%d err=%v", len(body), err)
	}
}
