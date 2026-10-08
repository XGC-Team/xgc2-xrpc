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

type HostOptions struct {
	// DiscoveryPaths names GET-only public description routes. All other routes stay instance-bound.
	DiscoveryPaths   []string
	InstanceID       string
	MaxBodyBytes     int64
	MaxResponseBytes int64
	MaxHeaderBytes   int
	MaxConnections   int
	MaxInFlight      int
	MaxCallTime      time.Duration
	HeaderTimeout    time.Duration
	IdleTimeout      time.Duration
	WriteTimeout     time.Duration
	ShutdownTimeout  time.Duration
	Diagnostics      *xrpc.Diagnostics
	Metrics          *xrpc.Metrics
	Service          string
}

func (o HostOptions) defaults() HostOptions {
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = xrpc.DefaultPolicyInteger("MAX_REQUEST_BYTES")
	}
	if o.MaxHeaderBytes <= 0 {
		o.MaxHeaderBytes = int(xrpc.DefaultPolicyInteger("MAX_HEADER_BYTES"))
	}
	if o.MaxConnections <= 0 {
		o.MaxConnections = int(xrpc.DefaultPolicyInteger("HOST_MAX_CONNECTIONS"))
	}
	if o.MaxInFlight <= 0 {
		o.MaxInFlight = int(xrpc.DefaultPolicyInteger("HOST_MAX_IN_FLIGHT"))
	}
	if o.MaxCallTime <= 0 {
		o.MaxCallTime = time.Duration(xrpc.DefaultPolicyInteger("CALL_TIMEOUT_MS")) * time.Millisecond
	}
	if o.HeaderTimeout <= 0 {
		o.HeaderTimeout = time.Duration(xrpc.DefaultPolicyInteger("HEADER_TIMEOUT_MS")) * time.Millisecond
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = time.Duration(xrpc.DefaultPolicyInteger("IDLE_TIMEOUT_MS")) * time.Millisecond
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = time.Duration(xrpc.DefaultPolicyInteger("SHUTDOWN_TIMEOUT_MS")) * time.Millisecond
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
		options.MaxResponseBytes = xrpc.DefaultPolicyInteger("MAX_RESPONSE_BYTES")
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
		next.ServeHTTP(bounded, r.WithContext(ctx))
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
