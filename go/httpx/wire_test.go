package httpx

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCommonWireCorpus(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/fixtures/wire.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		SchemaVersion int    `json:"schema_version"`
		InstanceID    string `json:"instance_id"`
		Cases         []struct {
			Name     string      `json:"name"`
			Method   string      `json:"method"`
			Path     string      `json:"path"`
			Headers  [][2]string `json:"headers"`
			Status   int         `json:"status"`
			Dispatch bool        `json:"dispatch"`
		} `json:"cases"`
	}
	if err = json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.SchemaVersion != 1 || len(corpus.Cases) == 0 {
		t.Fatal("unsupported/empty wire corpus")
	}
	for _, test := range corpus.Cases {
		t.Run(test.Name, func(t *testing.T) {
			dispatched := false
			handler := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dispatched = true; w.WriteHeader(200) }), HostOptions{InstanceID: corpus.InstanceID, DiscoveryPaths: []string{"/v1/describe"}})
			request := httptest.NewRequest(test.Method, test.Path, nil)
			for _, header := range test.Headers {
				request.Header.Add(header[0], header[1])
			}
			reply := httptest.NewRecorder()
			handler.ServeHTTP(reply, request)
			if reply.Code != test.Status || dispatched != test.Dispatch {
				t.Fatalf("status=%d dispatched=%t expected %d/%t", reply.Code, dispatched, test.Status, test.Dispatch)
			}
		})
	}
}

func TestRejectedBodyCannotBecomeNextRequest(t *testing.T) {
	client, calls := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("null")) }), 16)
	connection, err := net.Dial("unix", client.config.Service.Endpoint.Address)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(time.Second))
	// The rejected request includes bytes which look like another request.
	// Native net/http must drain its declared frame or terminate the socket.
	body := "GET /evil HTTP/1.1\r\nHost: unix\r\n\r\n"
	first := "POST /bad HTTP/1.1\r\nHost: unix\r\nX-Xrpc-Timeout-Ms: +1\r\nX-Request-ID: first\r\nX-Xrpc-Instance-ID: boot-1\r\nContent-Length: "
	first += strconv.Itoa(len(body)) + "\r\n\r\n" + body
	second := "GET /good HTTP/1.1\r\nHost: unix\r\nX-Xrpc-Timeout-Ms: 500\r\nX-Request-ID: second\r\nX-Xrpc-Instance-ID: boot-1\r\n\r\n"
	if _, err = io.WriteString(connection, first+second); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(connection)
	reply, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, reply.Body)
	reply.Body.Close()
	if reply.StatusCode != 413 {
		t.Fatalf("oversize body status=%d", reply.StatusCode)
	}
	next, err := http.ReadResponse(reader, nil)
	if err == nil {
		io.Copy(io.Discard, next.Body)
		next.Body.Close()
		if next.StatusCode != 200 {
			t.Fatalf("next status=%d", next.StatusCode)
		}
	}
	if calls.Load() > 1 {
		t.Fatalf("rejected body dispatched calls=%d", calls.Load())
	}
}

func TestShutdownBudgetWithHijackedHandlerStillRunning(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host, err := ServeEdge(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		close(entered)
		<-release
	}), HostOptions{})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	io.WriteString(peer, "GET / HTTP/1.1\r\nHost: local\r\n\r\n")
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- host.Shutdown(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Shutdown outlived caller budget")
	}
	select {
	case <-host.Drained():
		t.Fatal("live hijacked handler reported drained")
	default:
	}
	close(release)
	select {
	case <-host.Drained():
	case <-time.After(time.Second):
		t.Fatal("handler did not drain")
	}
}

func TestHeadHasNoBodyBudget(t *testing.T) {
	client, _ := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.Write([]byte(strings.Repeat("x", 4096)))
	}), 1024)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	body, code, headers, err := client.Do(ctx, http.MethodHead, "/", "head", "", nil)
	if err != nil || code != 200 || len(body) != 0 || headers.Get("Content-Length") != "4096" {
		t.Fatalf("HEAD %d body=%d headers=%v err=%v", code, len(body), headers, err)
	}
}
