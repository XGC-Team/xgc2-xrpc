package httpx

import (
	"context"
	"net"
	"net/http"
)

// RunEdge owns a public gateway's listener until cancellation or a serving
// failure. Domain handlers retain authentication and streaming semantics.
func RunEdge(ctx context.Context, address string, handler http.Handler, options HostOptions) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return err
	}
	host, err := ServeEdge(listener, handler, options)
	if err != nil {
		_ = listener.Close()
		return err
	}
	select {
	case <-ctx.Done():
	case <-host.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), options.defaults().ShutdownTimeout)
	defer cancel()
	return host.Shutdown(shutdown)
}
