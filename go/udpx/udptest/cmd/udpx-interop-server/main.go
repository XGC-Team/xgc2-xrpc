// udpx-interop-server is the Go reference of the udp.v1 interop test server.
// The C++ test server implements the same command line and methods; the
// interop test in go/udpx drives either one through the fault proxy.
//
// Command line: --bind ADDR --port N (0 picks a free port) --key-file PATH
// (key ring text file) --key-id ID (a key the file must contain). After binding
// it prints "READY <port> <instance-hex>" on stdout and serves until SIGINT or
// SIGTERM.
//
// Methods (bodies are JSON):
//
//	test.v1/Echo   replies with the request body
//	test.v1/Count  counts executions; replies {"count":N}
//	test.v1/Sleep  {"ms":N}: replies {"slept_ms":N} after N ms, from another goroutine
//	test.v1/Fail   {"status":N}: replies with status N and a standard error body
//	test.v1/Big    replies with a body too large for one datagram (resource_exhausted)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go/udpx"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "udpx-interop-server:", err)
		os.Exit(1)
	}
}

func run() error {
	bind := flag.String("bind", "127.0.0.1", "address to bind")
	port := flag.Int("port", 0, "UDP port (0 picks a free one)")
	keyFile := flag.String("key-file", "", "key ring file: \"<key_id> <base64 key>\" lines")
	keyID := flag.Uint("key-id", 0, "key identity the key file must contain")
	flag.Parse()
	ring, err := udpx.LoadKeyRing(*keyFile)
	if err != nil {
		return err
	}
	if !slices.Contains(ring.IDs(), uint32(*keyID)) {
		return fmt.Errorf("key file has no key %d", *keyID)
	}
	server, err := udpx.Listen(net.JoinHostPort(*bind, strconv.Itoa(*port)), udpx.ServerConfig{Keys: ring})
	if err != nil {
		return err
	}
	var count atomic.Int64
	handlers := map[string]udpx.Handler{
		"test.v1/Echo": func(_ context.Context, request udpx.Request, response *udpx.Responder) {
			_ = response.Reply(request.Body)
		},
		"test.v1/Count": func(_ context.Context, _ udpx.Request, response *udpx.Responder) {
			_ = response.Reply([]byte(`{"count":` + strconv.FormatInt(count.Add(1), 10) + `}`))
		},
		"test.v1/Sleep": func(_ context.Context, request udpx.Request, response *udpx.Responder) {
			var wanted struct{ MS int64 }
			if json.Unmarshal(request.Body, &wanted) != nil || wanted.MS < 0 {
				_ = response.Fail(udpx.StatusInvalidArgument, `body must be {"ms":N}`, nil)
				return
			}
			time.AfterFunc(time.Duration(wanted.MS)*time.Millisecond, func() {
				_ = response.Reply([]byte(`{"slept_ms":` + strconv.FormatInt(wanted.MS, 10) + `}`))
			})
		},
		"test.v1/Fail": func(_ context.Context, request udpx.Request, response *udpx.Responder) {
			var wanted struct{ Status udpx.Status }
			if json.Unmarshal(request.Body, &wanted) != nil || wanted.Status == udpx.StatusOK || wanted.Status > udpx.StatusPermissionDenied || wanted.Status == udpx.StatusUnauthenticated {
				_ = response.Fail(udpx.StatusInvalidArgument, `body must be {"status":N} with N in 1..8 or 10`, nil)
				return
			}
			_ = response.Fail(wanted.Status, "requested failure", map[string]any{"requested": uint32(wanted.Status)})
		},
		"test.v1/Big": func(_ context.Context, _ udpx.Request, response *udpx.Responder) {
			_ = response.Reply(make([]byte, 2*udpx.MaxDatagram))
		},
	}
	for name, handler := range handlers {
		if err := server.Handle(name, handler); err != nil {
			return err
		}
	}
	fmt.Printf("READY %d %s\n", server.Addr().Port(), server.InstanceID())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return server.Shutdown(ctx)
}
