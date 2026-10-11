package audit

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	"github.com/XGC-Team/xgc2-xrpc/go/udpx"
)

// Go client <-> the C++ xgc2-xrpc-method-interop-server: one MethodRouter served
// on an http.v1 Unix socket and on udp.v1 under the same method names, called
// through one Dispatcher. XGC2_XRPC_METHOD_INTEROP_SERVER names the binary (built
// from cpp/tests/method_interop_server.cpp); without it the tests skip. The
// command line and the methods are documented in that source.
const methodInteropEnv = "XGC2_XRPC_METHOD_INTEROP_SERVER"

const interopKeyID = 11

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

type methodInterop struct {
	cmd        *exec.Cmd
	stderr     *syncBuffer
	exited     chan struct{} // closed once the process has been waited for
	exitErr    error         // valid after exited is closed
	dispatcher *xrpc.Dispatcher
	httpRef    xrpc.ServiceRef
	udpRef     xrpc.ServiceRef
	instance   string
}

func startMethodInterop(t *testing.T) *methodInterop {
	t.Helper()
	binary := os.Getenv(methodInteropEnv)
	if binary == "" {
		t.Skipf("%s is not set: build cpp/tests/method_interop_server.cpp (target xgc2-xrpc-method-interop-server) and set it to the binary path", methodInteropEnv)
	}
	directory := privateTempDir(t)
	socket := filepath.Join(directory, "service.sock")
	key := make([]byte, udpx.KeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(directory, "interop.keys")
	if err := os.WriteFile(keyFile, []byte("# method interop key\n"+strconv.Itoa(interopKeyID)+" "+base64.StdEncoding.EncodeToString(key)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m := &methodInterop{stderr: &syncBuffer{}, exited: make(chan struct{})}
	m.cmd = exec.Command(binary, "--unix", socket, "--bind", "127.0.0.1", "--port", "0", "--key-file", keyFile, "--key-id", strconv.Itoa(interopKeyID))
	m.cmd.Stderr = m.stderr
	stdout, err := m.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := m.cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", binary, err)
	}
	t.Cleanup(func() {
		_ = m.cmd.Process.Signal(syscall.SIGTERM) // an error means it has gone already
		select {
		case <-m.exited:
		case <-time.After(5 * time.Second):
			_ = m.cmd.Process.Kill()
			<-m.exited
		}
	})
	ready := make(chan [2]string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if fields := strings.Fields(scanner.Text()); len(fields) == 3 && fields[0] == "READY" {
				ready <- [2]string{fields[1], fields[2]}
				break
			}
		}
		for scanner.Scan() {
		}
		m.exitErr = m.cmd.Wait()
		close(m.exited)
	}()
	var fields [2]string
	select {
	case fields = <-ready:
	case <-m.exited:
		t.Fatalf("the interop server exited before READY: %v\nstderr: %s", m.exitErr, m.stderr.String())
	case <-time.After(15 * time.Second):
		t.Fatalf("the interop server printed no READY line\nstderr: %s", m.stderr.String())
	}
	m.instance = fields[1]

	ring, err := udpx.NewKeyRing(map[uint32][]byte{interopKeyID: key})
	if err != nil {
		t.Fatal(err)
	}
	udpClient, err := udpx.NewClient(udpx.ClientConfig{Keys: ring})
	if err != nil {
		t.Fatal(err)
	}
	udpProfile, err := udpx.NewProfile(udpClient)
	if err != nil {
		t.Fatal(err)
	}
	httpProfile := httpx.NewProfile(httpx.Config{LocalTargetID: "local"})
	t.Cleanup(func() { httpProfile.Close(); <-httpProfile.Drained() })
	m.dispatcher, err = xrpc.NewDispatcher(map[string]xrpc.Caller{xrpc.HTTP: httpProfile, xrpc.UDP: udpProfile})
	if err != nil {
		t.Fatal(err)
	}
	m.httpRef = xrpc.ServiceRef{TargetID: "local", Service: "xgc2-fixture-host", APIVersion: "v1", InstanceID: m.instance, Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: socket}}
	// Unpinned, as a robot's configured service is: the first answer reveals the instance.
	m.udpRef = xrpc.ServiceRef{TargetID: "robot-1", Service: "xgc2.fixture", APIVersion: "v1", KeyID: interopKeyID, Profile: xrpc.UDP, Endpoint: xrpc.Endpoint{Kind: "udp", Address: "127.0.0.1:" + fields[0]}}
	return m
}

func (m *methodInterop) call(t *testing.T, ref xrpc.ServiceRef, name, body string) (xrpc.Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.dispatcher.CallMethod(ctx, ref, name, json.RawMessage(body))
}

func TestMethodInteropServesTheSameNamesOverBothProfiles(t *testing.T) {
	m := startMethodInterop(t)
	const body = `{"robot":"fixture-1","text":"café"}`
	for _, ref := range []xrpc.ServiceRef{m.httpRef, m.udpRef} {
		result, err := m.call(t, ref, "xgc2.fixture/Echo", body)
		if err != nil || string(result.Payload) != body || result.InstanceID != m.instance {
			t.Fatalf("%s: %+v %v", ref.Profile, result, err)
		}
	}
	// One counter behind both transports; udp.v1 deduplicates retransmissions.
	for i, ref := range []xrpc.ServiceRef{m.udpRef, m.httpRef, m.udpRef} {
		result, err := m.call(t, ref, "xgc2.fixture/Count", `{}`)
		if err != nil || string(result.Payload) != strconv.Itoa(i+1) {
			t.Fatalf("%s: count %s want %d: %v", ref.Profile, result.Payload, i+1, err)
		}
	}
	// A reply completed from another thread of the server.
	for _, ref := range []xrpc.ServiceRef{m.httpRef, m.udpRef} {
		if result, err := m.call(t, ref, "xgc2.fixture/Sleep", `{"ms":60}`); err != nil || string(result.Payload) != `{"ms":60}` {
			t.Fatalf("%s: %s %v", ref.Profile, result.Payload, err)
		}
	}
}

func TestMethodInteropErrorsKeepTheirCodes(t *testing.T) {
	m := startMethodInterop(t)
	codes := []string{"", "invalid_argument", "not_found", "conflict", "resource_exhausted", "deadline_exceeded", "cancelled", "unavailable", "internal", "unauthenticated", "permission_denied"}
	for code := 1; code <= 10; code++ {
		for _, ref := range []xrpc.ServiceRef{m.httpRef, m.udpRef} {
			result, err := m.call(t, ref, "xgc2.fixture/Fail", `{"code":`+strconv.Itoa(code)+`}`)
			want := codes[code]
			if code == 9 && ref.Profile == xrpc.UDP {
				want = "permission_denied" // unauthenticated has no udp.v1 form
			}
			var failure *xrpc.CallError
			if !errors.As(err, &failure) || failure.Code != want || failure.Disposition != xrpc.ResponseReceived {
				t.Fatalf("%s code %d: %v", ref.Profile, code, err)
			}
			var envelope struct {
				Error struct {
					Code    string
					Details struct{ Requested int }
				}
			}
			if err := json.Unmarshal(result.Payload, &envelope); err != nil || envelope.Error.Code != want || envelope.Error.Details.Requested != code {
				t.Fatalf("%s code %d: payload %s: %v", ref.Profile, code, result.Payload, err)
			}
			if result.Status != xrpc.StatusForCode(want) {
				t.Errorf("%s code %d: status %d, want %d", ref.Profile, code, result.Status, xrpc.StatusForCode(want))
			}
		}
	}
	for _, ref := range []xrpc.ServiceRef{m.httpRef, m.udpRef} {
		for _, name := range []string{"xgc2.fixture/Absent", "xgc2.other/Echo"} {
			if _, err := m.call(t, ref, name, `{}`); xrpc.Code(err) != "not_found" {
				t.Errorf("%s %s: %v", ref.Profile, name, err)
			}
		}
	}
}

func TestMethodInteropDescribeListsTheCapabilityPerEntity(t *testing.T) {
	m := startMethodInterop(t)
	// The same envelope arrives as the udp.v1 method and as GET /v1/describe.
	over, err := m.call(t, m.udpRef, "xgc2.fixture/Describe", ``)
	if err != nil {
		t.Fatal(err)
	}
	client, err := httpx.New(httpx.Config{LocalTargetID: "local", Service: m.httpRef})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, status, _, err := client.Do(ctx, "GET", "/v1/describe", "describe:1", "", nil)
	if err != nil || status != 200 {
		t.Fatalf("%d %v", status, err)
	}
	for name, document := range map[string][]byte{"udp.v1": over.Payload, "http.v1": raw} {
		describe, err := xrpc.ParseDescribe(document)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if describe.Service != "xgc2-fixture-host" || describe.APIVersion != "v1" || describe.InstanceID != m.instance || !describe.Ready {
			t.Fatalf("%s: %+v", name, describe)
		}
		for entity, want := range map[string]bool{"fixture-1": true, "fixture-2": true, "fixture-3": false} {
			if got, err := describe.Serves("xgc2.fixture", entity); err != nil || got != want {
				t.Errorf("%s: serves xgc2.fixture for %s: %v %v", name, entity, got, err)
			}
		}
		if got, _ := describe.Serves("xgc2.other", "fixture-1"); got {
			t.Errorf("%s: serves a capability it does not list", name)
		}
	}
}

func TestMethodInteropShutsDownOnSIGTERM(t *testing.T) {
	m := startMethodInterop(t)
	if _, err := m.call(t, m.udpRef, "xgc2.fixture/Echo", `{}`); err != nil {
		t.Fatal(err)
	}
	if err := m.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.exited:
		if m.exitErr != nil {
			t.Fatalf("the server did not exit cleanly: %v\nstderr: %s", m.exitErr, m.stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server ignored SIGTERM")
	}
}
