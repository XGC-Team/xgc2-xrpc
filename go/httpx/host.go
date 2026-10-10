package httpx

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/internal/netlimit"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
)

// HostOptions are plain limits. A zero field selects the default named in its
// comment; nothing is read from the process environment.
type HostOptions struct {
	// Authorize runs after finite admission and before domain dispatch. It must
	// return true while the request context remains live.
	Authorize func(*http.Request) bool
	// DiscoveryPaths names GET-only public description routes. All other routes stay instance-bound.
	DiscoveryPaths []string
	InstanceID     string
	// MaxBodyBytes bounds one request body (default xrpc.DefaultMaxMessageBytes).
	MaxBodyBytes int64
	// MaxResponseBytes bounds one response body. Serve defaults it to
	// xrpc.DefaultMaxMessageBytes; ServeEdge and RunEdge leave a zero value
	// unbounded so a domain stream keeps its own contract.
	MaxResponseBytes int64
	// MaxHeaderBytes bounds decoded header fields (default xrpc.DefaultMaxHeaderBytes).
	MaxHeaderBytes int
	// MaxConnections bounds accepted connections (default xrpc.DefaultMaxConnections).
	MaxConnections int
	// MaxInFlight bounds admitted calls (default xrpc.DefaultMaxInFlight).
	MaxInFlight int
	// MaxCallTime caps the caller's budget on Serve (default xrpc.DefaultCallTimeout).
	// ServeEdge applies it only when set, so zero preserves streaming.
	MaxCallTime time.Duration
	// HeaderTimeout bounds reading request headers (default xrpc.DefaultHeaderTimeout).
	HeaderTimeout time.Duration
	// IdleTimeout closes idle keep-alive connections (default xrpc.DefaultIdleTimeout).
	IdleTimeout time.Duration
	// WriteTimeout is net/http's absolute response deadline; zero disables it.
	// ServeEvents extends it on every frame, so streams outlive it.
	WriteTimeout time.Duration
	// ShutdownTimeout is the drain budget of RunEdge (default xrpc.DefaultShutdownTimeout).
	ShutdownTimeout time.Duration
	Diagnostics     *xrpc.Diagnostics
	Metrics         *xrpc.Metrics
	Service         string
}

// defaults applies the documented xrpc limits to zero fields. MaxResponseBytes
// is not defaulted here: internal hosts default it in Handler, while edges keep
// streaming responses unbounded.
func (o HostOptions) defaults() HostOptions {
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = xrpc.DefaultMaxMessageBytes
	}
	if o.MaxHeaderBytes <= 0 {
		o.MaxHeaderBytes = xrpc.DefaultMaxHeaderBytes
	}
	if o.MaxConnections <= 0 {
		o.MaxConnections = xrpc.DefaultMaxConnections
	}
	if o.MaxInFlight <= 0 {
		o.MaxInFlight = xrpc.DefaultMaxInFlight
	}
	if o.MaxCallTime <= 0 {
		o.MaxCallTime = xrpc.DefaultCallTimeout
	}
	if o.HeaderTimeout <= 0 {
		o.HeaderTimeout = xrpc.DefaultHeaderTimeout
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = xrpc.DefaultIdleTimeout
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = xrpc.DefaultShutdownTimeout
	}
	return o
}

// Handler applies bounded admission and the caller's finite budget to domain
// routes. net/http owns parsing, framing and connection reuse.
func Handler(next http.Handler, options HostOptions) http.Handler {
	options = options.defaults()
	if options.Metrics == nil {
		options.Metrics = &xrpc.Metrics{}
	}
	if options.MaxResponseBytes <= 0 {
		options.MaxResponseBytes = xrpc.DefaultMaxMessageBytes
	}
	slots := make(chan struct{}, options.MaxInFlight)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if headerBytes(r.Header)+int64(len(r.Host)+6) > int64(options.MaxHeaderBytes) {
			writeError(w, 431, "resource_exhausted", "request headers exceed host limit")
			return
		}
		if len(r.Header.Values(TimeoutHeader)) != 1 || len(r.Header.Values(RequestIDHeader)) != 1 || !validID(r.Header.Get(RequestIDHeader)) {
			writeError(w, 400, "invalid_argument", "one timeout and request identity required")
			return
		}
		if options.InstanceID != "" {
			w.Header().Set(InstanceIDHeader, options.InstanceID)
			instances := r.Header.Values(InstanceIDHeader)
			if len(instances) > 1 {
				writeError(w, 400, "invalid_argument", "at most one instance identity required")
				return
			}
			discovery := false
			if r.Method == http.MethodGet && len(instances) == 0 {
				for _, path := range options.DiscoveryPaths {
					if path == r.URL.Path {
						discovery = true
						break
					}
				}
			}
			if !discovery && (len(r.Header.Values(InstanceIDHeader)) != 1 || r.Header.Get(InstanceIDHeader) != options.InstanceID) {
				writeError(w, 409, "conflict", "server instance does not match service reference")
				return
			}
		}
		if r.ContentLength > options.MaxBodyBytes {
			writeError(w, http.StatusRequestEntityTooLarge, "resource_exhausted", "request body exceeds host limit")
			return
		}
		budget, err := xrpc.ParseTimeoutMS(r.Header.Get(TimeoutHeader))
		if err != nil {
			writeError(w, 400, "invalid_argument", "finite timeout and request ID required")
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			options.Metrics.Reject()
			options.Diagnostics.Emit(xrpc.Diagnostic{Level: "warn", Event: "admission_rejected", Service: options.Service, InstanceID: options.InstanceID, RequestID: r.Header.Get(RequestIDHeader), Category: "resource_exhausted"})
			writeError(w, 429, "resource_exhausted", "host concurrency exhausted")
			return
		}
		finishMetric := options.Metrics.Admit()
		defer finishMetric()
		started := time.Now()
		duration := min(time.Duration(budget)*time.Millisecond, options.MaxCallTime)
		ctx, cancel := context.WithTimeout(r.Context(), duration)
		defer cancel()
		defer func() {
			options.Metrics.Outcome(ctx.Err())
			options.Diagnostics.Emit(xrpc.Diagnostic{Level: "debug", Event: "call_finished", Service: options.Service, InstanceID: options.InstanceID, RequestID: r.Header.Get(RequestIDHeader), Operation: r.URL.Path, ElapsedMS: time.Since(started).Milliseconds()})
		}()
		// Socket deadlines bound body reads and writes as well as handler context.
		controller := http.NewResponseController(w)
		deadline := time.Now().Add(duration)
		_ = controller.SetReadDeadline(deadline)
		_ = controller.SetWriteDeadline(deadline)
		// net/http clears write deadlines after finishRequest flushes its buffers.
		// Clearing here would leave that final flush outside the caller budget.
		r.Body = http.MaxBytesReader(w, r.Body, options.MaxBodyBytes)
		w.Header().Set(RequestIDHeader, r.Header.Get(RequestIDHeader))
		bounded := &responseLimitWriter{ResponseWriter: w, remaining: options.MaxResponseBytes, head: r.Method == http.MethodHead, maxHeaderBytes: int64(options.MaxHeaderBytes)}
		request := r.WithContext(ctx)
		if options.Authorize != nil {
			allowed := options.Authorize(request)
			if ctx.Err() != nil {
				panic(http.ErrAbortHandler)
			}
			if !allowed {
				writeError(bounded, 403, "permission_denied", "caller authorization rejected")
				return
			}
		}
		next.ServeHTTP(bounded, request)
		bounded.checkHeaders()
		if bounded.abort {
			panic(http.ErrAbortHandler)
		}
		if ctx.Err() != nil {
			// Never let net/http synthesize an empty 200 after a timed-out
			// mutation. Aborting the exchange preserves outcome-unknown.
			panic(http.ErrAbortHandler)
		}
	})
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": message}})
}

type Host struct {
	server   *http.Server
	listener *netlimit.Listener
	lease    *unixlease.Lease
	once     sync.Once
	done     chan struct{}
	err      error
	mu       sync.Mutex
	stopping bool
	handlers sync.WaitGroup
	stopped  chan struct{}
	drained  chan struct{}
	stopErr  error
	options  HostOptions
}

// Serve consumes an already reserved listener. A TCP listener must be wrapped
// in authenticated TLS by the owning product; ServeTLS provides that wrapper.
func Serve(listener net.Listener, lease *unixlease.Lease, handler http.Handler, options HostOptions) (*Host, error) {
	if listener == nil || handler == nil {
		return nil, errors.New("xrpc: listener and handler required")
	}
	if options.InstanceID != "" && !xrpc.ValidID(options.InstanceID) {
		return nil, errors.New("xrpc: invalid host instance identity")
	}
	if listener.Addr().Network() != "unix" {
		if _, ok := listener.(*tlsListener); !ok {
			return nil, errors.New("xrpc: remote HTTP listener requires TLS")
		}
	}
	if options.Metrics == nil {
		options.Metrics = &xrpc.Metrics{}
	}
	return serve(listener, lease, Handler(handler, options), options)
}

// ServeEdge hosts an explicit public gateway. Its product retains HTTP auth,
// CORS, routes, SSE and WebSocket semantics. No RPC caller headers are required.
// MaxCallTime and WriteTimeout are opt-in for edges; zero preserves streaming.
func ServeEdge(listener net.Listener, handler http.Handler, options HostOptions) (*Host, error) {
	if listener == nil || handler == nil {
		return nil, errors.New("xrpc: listener and handler required")
	}
	timeout := options.MaxCallTime
	normalized := options.defaults()
	if normalized.Metrics == nil {
		normalized.Metrics = &xrpc.Metrics{}
	}
	slots := make(chan struct{}, normalized.MaxInFlight)
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if headerBytes(r.Header)+int64(len(r.Host)+6) > int64(normalized.MaxHeaderBytes) {
			writeError(w, 431, "resource_exhausted", "request headers exceed host limit")
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			normalized.Metrics.Reject()
			writeError(w, 429, "resource_exhausted", "host concurrency exhausted")
			return
		}
		finishMetric := normalized.Metrics.Admit()
		defer finishMetric()
		requestID := r.Header.Get(RequestIDHeader)
		if requestID == "" {
			var id [16]byte
			if _, err := rand.Read(id[:]); err != nil {
				writeError(w, 500, "internal", "request identity unavailable")
				return
			}
			requestID = hex.EncodeToString(id[:])
		}
		w.Header().Set(RequestIDHeader, requestID)
		r.Body = http.MaxBytesReader(w, r.Body, normalized.MaxBodyBytes)
		if timeout > 0 {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			r = r.WithContext(ctx)
		}
		bounded := &responseLimitWriter{ResponseWriter: w, remaining: normalized.MaxResponseBytes, head: r.Method == http.MethodHead, maxHeaderBytes: int64(normalized.MaxHeaderBytes), unlimitedBody: normalized.MaxResponseBytes == 0}
		handler.ServeHTTP(bounded, r)
		bounded.checkHeaders()
		if bounded.abort {
			panic(http.ErrAbortHandler)
		}
	})
	return serve(listener, nil, wrapped, normalized)
}

func serve(listener net.Listener, lease *unixlease.Lease, handler http.Handler, options HostOptions) (*Host, error) {
	options = options.defaults()
	var tlsConfig *tls.Config
	if secured, ok := listener.(*tlsListener); ok {
		listener, tlsConfig = secured.Listener, secured.config
	}
	limited := netlimit.New(listener, options.MaxConnections)
	var serving net.Listener = limited
	if tlsConfig != nil {
		// net/http must receive *tls.Conn to own the handshake and populate Request.TLS.
		serving = tls.NewListener(limited, tlsConfig)
	}
	host := &Host{listener: limited, lease: lease, done: make(chan struct{}), stopped: make(chan struct{}), drained: make(chan struct{}), options: options}
	tracked := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host.mu.Lock()
		if host.stopping {
			host.mu.Unlock()
			writeError(w, 503, "unavailable", "host stopping")
			return
		}
		host.handlers.Add(1)
		host.mu.Unlock()
		defer host.handlers.Done()
		handler.ServeHTTP(w, r)
	})
	host.server = &http.Server{Handler: tracked, ReadHeaderTimeout: options.HeaderTimeout, IdleTimeout: options.IdleTimeout, WriteTimeout: options.WriteTimeout, MaxHeaderBytes: options.MaxHeaderBytes, TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){}}
	go func() {
		err := host.server.Serve(serving)
		if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			host.err = err
		}
		close(host.done)
		host.mu.Lock()
		stopping := host.stopping
		host.mu.Unlock()
		if !stopping {
			ctx, cancel := context.WithTimeout(context.Background(), options.ShutdownTimeout)
			defer cancel()
			_ = host.Shutdown(ctx)
		}
	}()
	return host, nil
}
func (h *Host) Done() <-chan struct{} { return h.done }

// Wait returns the serving error after the listener exits.
func (h *Host) Wait() error { <-h.done; return h.err }

type tlsListener struct {
	net.Listener
	config *tls.Config
}

func ServeTLS(listener net.Listener, handler http.Handler, config *tls.Config, options HostOptions) (*Host, error) {
	if config == nil || len(config.Certificates) == 0 && config.GetCertificate == nil {
		return nil, errors.New("xrpc: server TLS identity required")
	}
	cloned := config.Clone()
	cloned.MinVersion = max(cloned.MinVersion, tls.VersionTLS12)
	cloned.NextProtos = []string{"http/1.1"}
	return Serve(&tlsListener{Listener: listener, config: cloned}, nil, handler, options)
}
func (h *Host) Shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if _, err := xrpc.Remaining(ctx); err != nil {
		return err
	}
	h.once.Do(func() {
		h.mu.Lock()
		h.stopping = true
		h.mu.Unlock()
		go func() {
			h.stopErr = h.server.Shutdown(ctx)
			if h.stopErr != nil {
				_ = h.server.Close()
			}
			h.listener.CloseConnections()
			<-h.done
			close(h.stopped)
			// A deadline closes I/O, not arbitrary domain code. Keep the lifetime
			// lease until every admitted handler has actually returned.
			h.handlers.Wait()
			if h.lease != nil {
				_ = h.lease.Close()
			}
			close(h.drained)
		}()
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.stopped:
		if h.stopErr == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-h.drained:
			}
		}
		return errors.Join(h.stopErr, h.err)
	}
}

// Drained closes only after all admitted handlers returned and ownership was released.
// Done reports listener termination and does not imply domain work has stopped.
func (h *Host) Drained() <-chan struct{} { return h.drained }

func (h *Host) Status() xrpc.TransportStatus {
	h.mu.Lock()
	stopping := h.stopping
	h.mu.Unlock()
	status := xrpc.TransportStatus{Time: time.Now().UTC(), Metrics: h.options.Metrics.Snapshot(), Connections: h.listener.Count(), ConnectionsAvailable: true, ConnectionLimit: h.options.MaxConnections, InFlightLimit: h.options.MaxInFlight, Stopping: stopping}
	select {
	case <-h.drained:
		status.Drained = true
	default:
	}
	if h.options.Diagnostics != nil {
		d := h.options.Diagnostics.Status()
		status.Diagnostics = &d
	}
	return status
}
