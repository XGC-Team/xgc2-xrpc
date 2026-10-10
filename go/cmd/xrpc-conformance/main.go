// xrpc-conformance is a disposable real-process SDK fixture. It is not a
// product service or registry and never starts external providers.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	socket := flag.String("socket", "", "existing private directory's absolute socket path")
	instance := flag.String("instance", "", "bound instance identity (random when omitted)")
	profile := flag.String("profile", "http", "http or grpc")
	target := flag.String("target", "fixture", "logical local target identity")
	duration := flag.Duration("duration", time.Minute, "finite fixture process lifetime")
	reclaim := flag.Bool("reclaim", false, "explicitly reclaim an unreachable killed fixture endpoint")
	flag.Parse()
	if *instance == "" {
		generated, err := randomInstance()
		if err != nil {
			return err
		}
		*instance = generated
	}
	if *socket == "" || !xrpc.ValidID(*instance) || *duration <= 0 || *duration > 24*time.Hour {
		return errors.New("xrpc fixture: explicit socket, canonical instance and finite duration required")
	}
	diagnostics, err := xrpc.NewDiagnostics(xrpc.DiagnosticOptions{Sink: os.Stderr})
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		diagnostics.Close(ctx)
	}()
	options := unixlease.Options{}
	if *reclaim {
		options.ExistingPath = unixlease.ReclaimUnreachable
	}
	lease, err := unixlease.Reserve(context.Background(), *socket, options)
	if err != nil {
		return err
	}
	listener, err := lease.Listen()
	if err != nil {
		lease.Close()
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	ref := xrpc.ServiceRef{TargetID: *target, Service: "xrpc.fixture", APIVersion: "v1", InstanceID: *instance, Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: *socket}}
	if *profile == "grpc" {
		ref.Profile = xrpc.GRPC
		ref.Service = "grpc.health.v1.Health"
		limits := grpcx.HostOptions{InstanceID: *instance, Service: ref.Service, Diagnostics: diagnostics}
		server := health.NewServer()
		server.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		host, err := grpcx.ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, server) }, limits)
		if err != nil {
			listener.Close()
			lease.Close()
			return err
		}
		if err = json.NewEncoder(os.Stdout).Encode(map[string]any{"ready": true, "reference": ref}); err != nil {
			host.Stop()
			<-host.Drained()
			return err
		}
		select {
		case <-ctx.Done():
		case <-host.Done():
		}
		shutdown, finish := context.WithTimeout(context.Background(), xrpc.DefaultShutdownTimeout)
		defer finish()
		return host.Shutdown(shutdown)
	}
	if *profile != "http" {
		listener.Close()
		lease.Close()
		return errors.New("xrpc fixture: profile must be http or grpc")
	}
	limits := httpx.HostOptions{InstanceID: *instance, Service: ref.Service, Diagnostics: diagnostics, DiscoveryPaths: []string{"/v1/describe"}}
	mux := http.NewServeMux()
	var host *httpx.Host
	var published atomic.Pointer[httpx.Host]
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		current := published.Load()
		if current == nil {
			http.Error(w, "fixture initializing", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(current.Status())
	})
	mux.HandleFunc("GET /v1/describe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ref)
	})
	mux.HandleFunc("/v1/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bounded body read failed", 413)
			return
		}
		if len(body) == 0 {
			body = []byte(`{"ok":true}`)
		} else if !json.Valid(body) {
			http.Error(w, "JSON fixture body required", 400)
			return
		}
		w.Write(body)
	})
	host, err = httpx.Serve(listener, lease, mux, limits)
	if err != nil {
		listener.Close()
		lease.Close()
		return err
	}
	published.Store(host)
	if err = json.NewEncoder(os.Stdout).Encode(map[string]any{"ready": true, "reference": ref}); err != nil {
		shutdown, finish := context.WithTimeout(context.Background(), time.Second)
		defer finish()
		host.Shutdown(shutdown)
		return err
	}
	select {
	case <-ctx.Done():
	case <-host.Done():
	}
	shutdown, finish := context.WithTimeout(context.Background(), xrpc.DefaultShutdownTimeout)
	defer finish()
	return host.Shutdown(shutdown)
}

// The optional source generator is kept local to the fixture, ensuring IDs
// would stay unique if a supervisor chooses random instance identities.
func randomInstance() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return "fixture:" + hex.EncodeToString(id[:]), nil
}
