package grpcx

import (
	"crypto/tls"
	"errors"
	"net"

	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// PrepareEdgeTLS registers a native service without accepting connections.
// On success the Host owns the listener and lease; call Serve from the existing
// application lifecycle. A registration failure stops the partial native server
// and leaves the listener and lease with the caller.
func PrepareEdgeTLS(listener net.Listener, lease *unixlease.Lease, register func(grpc.ServiceRegistrar) error, config *tls.Config, edge EdgeOptions, options ...grpc.ServerOption) (*Host, error) {
	if config == nil || len(config.Certificates) == 0 && config.GetCertificate == nil {
		return nil, errors.New("xrpc: server TLS identity required")
	}
	if err := edge.validate(); err != nil {
		return nil, err
	}
	cloned := config.Clone()
	cloned.MinVersion = max(cloned.MinVersion, tls.VersionTLS12)
	return prepareOwned(listener, lease, register, edge.Limits, &edge, append(options, grpc.Creds(credentials.NewTLS(cloned)))...)
}

// Serve runs the native accept loop once. Stop or Shutdown before Serve closes
// the prepared listener and completes its drain; a stopped Host cannot restart.
func (h *Host) Serve() error {
	if h == nil {
		return errors.New("xrpc: prepared host required")
	}
	h.mu.Lock()
	if h.stopping {
		h.mu.Unlock()
		return grpc.ErrServerStopped
	}
	if h.started {
		h.mu.Unlock()
		return errors.New("xrpc: host Serve may be called only once")
	}
	h.started = true
	h.mu.Unlock()
	err := h.server.Serve(h.serving)
	h.mu.Lock()
	if !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
		h.err = err
	}
	result := h.err
	stopping := h.stopping
	h.doneOnce.Do(func() { close(h.done) })
	h.mu.Unlock()
	if !stopping {
		h.Stop()
	}
	return result
}
