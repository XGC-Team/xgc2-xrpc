package udpx_test

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/udpx"
	"github.com/XGC-Team/xgc2-xrpc/go/udpx/udptest"
)

// Go client <-> test server in another process (the C++ udp.v1 library in CI),
// through the fault proxy. The server binary is named by XGC2_XRPC_UDP_INTEROP_SERVER and
// follows this command line and contract; udptest/cmd/udpx-interop-server is
// the Go reference of it:
//
//	server --bind 127.0.0.1 --port 0 --key-file FILE --key-id ID
//
// FILE is a key ring text file ("<key_id> <base64 key>" lines) that holds the
// key ID. The server prints "READY <port> <instance-hex>" on stdout once it
// listens, and serves these methods (bodies are JSON):
//
//	test.v1/Echo   replies with the request body
//	test.v1/Count  counts executions (proves at-most-once); the reply is a JSON
//	               number or an object with one numeric field
//	test.v1/Sleep  {"ms":N} replies after N ms without blocking its I/O thread
//	test.v1/Fail   {"status":N} replies with status N and an error body
//	test.v1/Big    replies with more than fits in one datagram: resource_exhausted
//
// The server serves only the key --key-id of FILE, answers requests of
// unknown methods not_found or invalid_argument, and exits on SIGTERM.
const interopServerEnv = "XGC2_XRPC_UDP_INTEROP_SERVER"

type interopServer struct {
	t        *testing.T
	cmd      *exec.Cmd
	port     int
	instance string
	stderr   *syncBuffer
	exited   chan struct{}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func interopBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv(interopServerEnv)
	if binary == "" {
		t.Skipf("%s is not set: build the udp.v1 interop test server (C++ in CI, or go build ./udpx/udptest/cmd/udpx-interop-server) and set it to the binary path to run the cross-process interop tests", interopServerEnv)
	}
	return binary
}

// writeKeyFile stores one fresh key under keyID and returns the file and key.
func writeKeyFile(t *testing.T) (string, []byte) {
	t.Helper()
	key := make([]byte, udpx.KeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "interop.keys")
	content := "# udp.v1 interop key\n" + strconv.Itoa(keyID) + " " + base64.StdEncoding.EncodeToString(key) + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, key
}

// startInterop launches the server and waits for its READY line.
func startInterop(t *testing.T, binary, keyFile string, port int) *interopServer {
	t.Helper()
	s := &interopServer{t: t, stderr: &syncBuffer{}, exited: make(chan struct{})}
	s.cmd = exec.Command(binary, "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--key-file", keyFile, "--key-id", strconv.Itoa(keyID))
	s.cmd.Stderr = s.stderr
	stdout, err := s.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", binary, err)
	}
	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) < 3 || fields[0] != "READY" {
				continue
			}
			p, err := strconv.Atoi(fields[1])
			if err != nil || p < 1 || p > 65535 || len(fields[2]) != 32 || strings.Trim(fields[2], "0123456789abcdef") != "" {
				ready <- fmt.Errorf("malformed READY line %q", scanner.Text())
				return
			}
			s.port, s.instance = p, fields[2]
			ready <- nil
			_, _ = io.Copy(io.Discard, stdout) // keep the child from blocking on a full pipe
			return
		}
		ready <- fmt.Errorf("server exited before READY: %w", scanner.Err())
	}()
	go func() { _ = s.cmd.Wait(); close(s.exited) }()
	t.Cleanup(s.stop)
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("%v\nserver stderr:\n%s", err, s.stderr.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("no READY line within 15s\nserver stderr:\n%s", s.stderr.String())
	}
	return s
}

func (s *interopServer) address() string { return "127.0.0.1:" + strconv.Itoa(s.port) }

func (s *interopServer) stop() {
	select {
	case <-s.exited:
		return
	default:
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.exited:
	case <-time.After(3 * time.Second):
		_ = s.cmd.Process.Kill()
		<-s.exited
	}
}

// numericReply accepts a JSON number or an object holding one number.
func numericReply(t *testing.T, body []byte) int64 {
	t.Helper()
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("reply %q is not JSON: %v", body, err)
	}
	switch value := decoded.(type) {
	case float64:
		return int64(value)
	case map[string]any:
		if len(value) == 1 {
			for _, field := range value {
				if number, ok := field.(float64); ok {
					return int64(number)
				}
			}
		}
	}
	t.Fatalf("reply %q is neither a number nor an object with one numeric field", body)
	return 0
}

func interopClient(t *testing.T, key []byte, config udpx.ClientConfig) *udpx.Client {
	t.Helper()
	ring, err := udpx.NewKeyRing(map[uint32][]byte{keyID: key})
	if err != nil {
		t.Fatal(err)
	}
	config.Keys = ring
	client, err := udpx.NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestInterop(t *testing.T) {
	binary := interopBinary(t)
	keyFile, key := writeKeyFile(t)
	server := startInterop(t, binary, keyFile, 0)
	direct := ref(server.address())
	client := interopClient(t, key, udpx.ClientConfig{})

	t.Run("echo and instance discovery", func(t *testing.T) {
		reply, err := client.Call(within(t, 3*time.Second), direct, "test.v1/Echo", []byte(`{"hello":"cpp"}`))
		if err != nil {
			t.Fatal(err)
		}
		if string(reply.Body) != `{"hello":"cpp"}` || reply.InstanceID() != server.instance {
			t.Fatalf("reply %q from instance %s, server announced %s", reply.Body, reply.InstanceID(), server.instance)
		}
		pinned := ref(server.address(), func(r *xrpc.ServiceRef) { r.InstanceID = server.instance })
		if _, err := client.Call(within(t, 3*time.Second), pinned, "test.v1/Echo", []byte(`{}`)); err != nil {
			t.Fatalf("correct pin: %v", err)
		}
		wrong := ref(server.address(), func(r *xrpc.ServiceRef) { r.InstanceID = strings.Repeat("ab", 16) })
		_, err = client.Call(within(t, 3*time.Second), wrong, "test.v1/Echo", []byte(`{}`))
		if failure := callError(t, err); failure.Code != "conflict" {
			t.Fatalf("wrong pin: %+v", failure)
		}
	})

	t.Run("statuses and error bodies", func(t *testing.T) {
		names := map[int]string{1: "invalid_argument", 2: "not_found", 3: "conflict", 4: "resource_exhausted", 5: "deadline_exceeded", 6: "cancelled", 7: "unavailable", 8: "internal", 10: "permission_denied"}
		for status, name := range names {
			reply, err := client.Call(within(t, 3*time.Second), direct, "test.v1/Fail", []byte(`{"status":`+strconv.Itoa(status)+`}`))
			failure := callError(t, err)
			var body struct {
				Code    string
				Message string
				Details json.RawMessage
			}
			if failure.Code != name || failure.Disposition != xrpc.ResponseReceived || int(reply.Status) != status {
				t.Errorf("status %d: %+v", status, failure)
			}
			if err := json.Unmarshal(reply.Body, &body); err != nil || body.Code != name || len(body.Details) == 0 {
				t.Errorf("status %d: error body %q (%v)", status, reply.Body, err)
			}
		}
	})

	t.Run("oversized reply becomes resource_exhausted", func(t *testing.T) {
		reply, err := client.Call(within(t, 3*time.Second), direct, "test.v1/Big", []byte(`{}`))
		if failure := callError(t, err); failure.Code != "resource_exhausted" || failure.Disposition != xrpc.ResponseReceived || len(reply.Body) > 300 {
			t.Fatalf("%+v body %q", failure, reply.Body)
		}
	})

	t.Run("unknown method is refused", func(t *testing.T) {
		_, err := client.Call(within(t, 3*time.Second), direct, "test.v1/DoesNotExist", nil)
		if failure := callError(t, err); failure.Disposition != xrpc.ResponseReceived || failure.Code != "not_found" && failure.Code != "invalid_argument" {
			t.Fatalf("%+v", failure)
		}
	})

	t.Run("wrong key is silent", func(t *testing.T) {
		stranger := make([]byte, udpx.KeyLen)
		stranger[0] = 1
		_, err := interopClient(t, stranger, udpx.ClientConfig{}).Call(within(t, 400*time.Millisecond), direct, "test.v1/Echo", []byte(`{}`))
		if failure := callError(t, err); failure.Code != "deadline_exceeded" || failure.Disposition != xrpc.OutcomeUnknown {
			t.Fatalf("%+v", failure)
		}
		// The same key bytes under a key_id the server does not know.
		other, err := udpx.NewKeyRing(map[uint32][]byte{keyID + 1: key})
		if err != nil {
			t.Fatal(err)
		}
		strange, err := udpx.NewClient(udpx.ClientConfig{Keys: other})
		if err != nil {
			t.Fatal(err)
		}
		_, err = strange.Call(within(t, 400*time.Millisecond), ref(server.address(), func(r *xrpc.ServiceRef) { r.KeyID = keyID + 1 }), "test.v1/Echo", []byte(`{}`))
		if failure := callError(t, err); failure.Disposition != xrpc.OutcomeUnknown {
			t.Fatalf("unknown key_id: %+v", failure)
		}
	})

	t.Run("slow handler replies without blocking and survives retransmission", func(t *testing.T) {
		started := time.Now()
		reply, err := client.Call(within(t, 3*time.Second), direct, "test.v1/Sleep", []byte(`{"ms":300}`))
		if err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed < 250*time.Millisecond {
			t.Fatalf("replied after %s", elapsed)
		}
		if reply.Sent < 3 {
			t.Fatalf("expected retransmissions during the 300 ms wait, sent %d", reply.Sent)
		}
		// While one request sleeps the server still answers others.
		sleeper := make(chan error, 1)
		go func() {
			_, err := client.Call(within(t, 3*time.Second), direct, "test.v1/Sleep", []byte(`{"ms":400}`))
			sleeper <- err
		}()
		time.Sleep(50 * time.Millisecond)
		quick := time.Now()
		if _, err := client.Call(within(t, 3*time.Second), direct, "test.v1/Echo", []byte(`{}`)); err != nil || time.Since(quick) > 300*time.Millisecond {
			t.Fatalf("echo during sleep: %v after %s", err, time.Since(quick))
		}
		if err := <-sleeper; err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a handler past the server budget gives no reply", func(t *testing.T) {
		started := time.Now()
		_, err := client.Call(within(t, 2600*time.Millisecond), direct, "test.v1/Sleep", []byte(`{"ms":4000}`))
		if failure := callError(t, err); failure.Code != "deadline_exceeded" || failure.Disposition != xrpc.OutcomeUnknown {
			t.Fatalf("%+v", failure)
		}
		if time.Since(started) < 2500*time.Millisecond {
			t.Fatal("the client gave up early")
		}
	})

	t.Run("exactly once through loss, duplication and reordering", func(t *testing.T) {
		count := func() int64 {
			reply, err := client.Call(within(t, 3*time.Second), direct, "test.v1/Count", nil)
			if err != nil {
				t.Fatal(err)
			}
			return numericReply(t, reply.Body)
		}
		base := count()
		faults := udptest.Faults{Loss: 0.3, Duplicate: 0.3, Reorder: 0.3, ReorderDelay: 40 * time.Millisecond, Delay: time.Millisecond, Jitter: 8 * time.Millisecond}
		proxy, err := udptest.NewProxy(server.address(), udptest.Config{Seed: 20261010, Requests: faults, Replies: faults})
		if err != nil {
			t.Fatal(err)
		}
		defer proxy.Close()
		through := ref(proxy.Addr())
		const calls = 20
		for i := int64(1); i <= calls; i++ {
			reply, err := client.Call(within(t, 15*time.Second), through, "test.v1/Count", nil)
			if err != nil {
				t.Fatalf("call %d through the lossy network: %v", i, err)
			}
			if got := numericReply(t, reply.Body); got != base+i {
				t.Fatalf("call %d returned count %d, want %d: a request was executed twice or lost", i, got, base+i)
			}
			time.Sleep(60 * time.Millisecond) // stay below the server's per-source rate limit
		}
		if final := count(); final != base+calls+1 {
			t.Fatalf("server counted %d executions, want %d", final-base-1, calls)
		}
		if s := proxy.Stats(); s.Requests.Dropped == 0 || s.Requests.Duplicated == 0 || s.Replies.Dropped == 0 {
			t.Fatalf("the network was not hostile: %+v", s)
		}
	})

	t.Run("lost replies are answered from the cache", func(t *testing.T) {
		proxy, err := udptest.NewProxy(server.address(), udptest.Config{})
		if err != nil {
			t.Fatal(err)
		}
		defer proxy.Close()
		before, err := client.Call(within(t, 3*time.Second), direct, "test.v1/Count", nil)
		if err != nil {
			t.Fatal(err)
		}
		proxy.DropReplies(3)
		reply, err := client.Call(within(t, 5*time.Second), ref(proxy.Addr()), "test.v1/Count", nil)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := numericReply(t, reply.Body), numericReply(t, before.Body)+1; got != want {
			t.Fatalf("count %d, want %d: the lost reply caused a second execution", got, want)
		}
		if reply.Sent < 4 {
			t.Fatalf("sent %d datagrams for 3 lost replies", reply.Sent)
		}
	})
}

func TestInteropServerRestartChangesInstance(t *testing.T) {
	binary := interopBinary(t)
	keyFile, key := writeKeyFile(t)
	first := startInterop(t, binary, keyFile, 0)
	client := interopClient(t, key, udpx.ClientConfig{})
	pinned := ref(first.address(), func(r *xrpc.ServiceRef) { r.InstanceID = first.instance })
	if _, err := client.Call(within(t, 3*time.Second), pinned, "test.v1/Echo", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	first.stop()
	second := startInterop(t, binary, keyFile, first.port)
	if second.instance == first.instance {
		t.Fatalf("restart kept instance %s", first.instance)
	}
	_, err := client.Call(within(t, 3*time.Second), pinned, "test.v1/Echo", []byte(`{}`))
	if failure := callError(t, err); failure.Code != "conflict" || failure.Disposition != xrpc.OutcomeUnknown {
		t.Fatalf("stale pin: %+v", failure)
	}
	reply, err := client.Call(within(t, 3*time.Second), ref(second.address()), "test.v1/Echo", []byte(`{}`))
	if err != nil || reply.InstanceID() != second.instance {
		t.Fatalf("discovery after restart: %+v %v", reply, err)
	}
}
