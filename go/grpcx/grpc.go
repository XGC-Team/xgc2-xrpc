// Package grpcx is the optional native gRPC profile. HTTP consumers do not
// import or link it. Domain services remain generated typed gRPC services.
package grpcx

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/internal/netlimit"
	"github.com/XGC-Team/xgc2-xrpc/go/internal/refpool"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type DialOptions struct {
	boundAuthorization   bool
	LocalTargetID        string
	DialContext          xrpc.DialContext
	TLSConfig            *tls.Config
	MaxMessageBytes      int
	MaxRequestBytes      int
	MaxResponseBytes     int
	MaxHeaderBytes       uint32
	MaxCallTime          time.Duration
	IdleTimeout          time.Duration
	MaxInFlight          int
	MaxReferences        int
	ReferenceIdleTimeout time.Duration
	Metadata             metadata.MD
	Diagnostics          *xrpc.Diagnostics
	Metrics              *xrpc.Metrics
}

func (o DialOptions) defaults() DialOptions {
	if o.MaxHeaderBytes == 0 {
		o.MaxHeaderBytes = uint32(xrpc.DefaultPolicyInteger("MAX_HEADER_BYTES"))
	}
	if o.Metrics == nil {
		o.Metrics = &xrpc.Metrics{}
	}
	if o.MaxRequestBytes <= 0 {
		o.MaxRequestBytes = o.MaxMessageBytes
		if o.MaxRequestBytes <= 0 {
			o.MaxRequestBytes = int(xrpc.DefaultPolicyInteger("MAX_REQUEST_BYTES"))
		}
	}
	if o.MaxResponseBytes <= 0 {
		o.MaxResponseBytes = o.MaxMessageBytes
		if o.MaxResponseBytes <= 0 {
			o.MaxResponseBytes = int(xrpc.DefaultPolicyInteger("MAX_RESPONSE_BYTES"))
		}
	}
	if o.MaxCallTime <= 0 {
		o.MaxCallTime = time.Duration(xrpc.DefaultPolicyInteger("CALL_TIMEOUT_MS")) * time.Millisecond
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = time.Duration(xrpc.DefaultPolicyInteger("IDLE_TIMEOUT_MS")) * time.Millisecond
	}
	return o
}

func Dial(ref xrpc.ServiceRef, options DialOptions) (*grpc.ClientConn, error) {
	if err := validateAuthMetadata(options.Metadata); err != nil {
		return nil, err
	}
	options.Metadata = options.Metadata.Copy()
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if ref.Profile != xrpc.GRPC || options.LocalTargetID == "" {
		return nil, errors.New("xrpc: gRPC profile and local target identity required")
	}
	options = options.defaults()
	if options.MaxInFlight <= 0 {
		options.MaxInFlight = int(xrpc.DefaultPolicyInteger("HOST_MAX_IN_FLIGHT"))
	}
	slots := make(chan struct{}, options.MaxInFlight)
	budget := newDialBudget()
	args := []grpc.DialOption{grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithMaxHeaderListSize(options.MaxHeaderBytes), grpc.WithIdleTimeout(options.IdleTimeout), grpc.WithStatsHandler(budget), grpc.WithChainUnaryInterceptor(clientUnary(ref, slots, options, budget)), grpc.WithChainStreamInterceptor(clientStream(ref, slots, options, budget)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(options.MaxResponseBytes), grpc.MaxCallSendMsgSize(options.MaxRequestBytes))}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		ctx, finish, err := budget.context(ctx)
		if err != nil {
			return nil, err
		}
		defer finish()
		var connection net.Conn
		if options.DialContext != nil {
			connection, err = options.DialContext(ctx, ref)
		} else {
			connection, err = (&net.Dialer{}).DialContext(ctx, network, address)
		}
		if err != nil {
			return nil, err
		}
		return budget.track(ctx, connection)
	}
	target := "passthrough:///" + ref.Endpoint.Address
	switch ref.Endpoint.Kind {
	case "unix":
		if ref.TargetID != options.LocalTargetID && options.DialContext == nil {
			return nil, errors.New("xrpc: remote Unix service requires injected routing")
		}
		args = append(args, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return dial(ctx, "unix", ref.Endpoint.Address)
		}))
	case "tls":
		config := options.TLSConfig
		if config == nil {
			config = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		if config.InsecureSkipVerify {
			return nil, errors.New("xrpc: authenticated gRPC cannot skip TLS verification")
		}
		cloned := config.Clone()
		cloned.MinVersion = max(cloned.MinVersion, tls.VersionTLS12)
		args = append(args, grpc.WithTransportCredentials(credentials.NewTLS(cloned)))
		args = append(args, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return dial(ctx, "tcp", ref.Endpoint.Address) }))
	default:
		return nil, errors.New("xrpc: unsupported gRPC endpoint")
	}
	return grpc.NewClient(target, args...)
}

type HostOptions struct {
	// Authorize establishes the injected transport caller grant. Product
	// method/scope checks remain in its native interceptors.
	Authorize                                  func(context.Context) bool
	MaxConnections                             int
	MaxConcurrentStreams                       uint32
	MaxMessageBytes                            int
	MaxRequestBytes, MaxResponseBytes          int
	MaxHeaderBytes                             uint32
	MaxInFlight                                int
	MaxCallTime, IdleTimeout, HandshakeTimeout time.Duration
	ShutdownTimeout                            time.Duration
	InstanceID                                 string
	Diagnostics                                *xrpc.Diagnostics
	Metrics                                    *xrpc.Metrics
	Service                                    string
}

func (o HostOptions) defaults() HostOptions {
	if o.Metrics == nil {
		o.Metrics = &xrpc.Metrics{}
	}
	if o.MaxConnections <= 0 {
		o.MaxConnections = int(xrpc.DefaultPolicyInteger("HOST_MAX_CONNECTIONS"))
	}
	if o.MaxConcurrentStreams == 0 {
		o.MaxConcurrentStreams = uint32(xrpc.DefaultPolicyInteger("GRPC_MAX_STREAMS_PER_CONNECTION"))
	}
	if o.MaxRequestBytes <= 0 {
		o.MaxRequestBytes = o.MaxMessageBytes
		if o.MaxRequestBytes <= 0 {
			o.MaxRequestBytes = int(xrpc.DefaultPolicyInteger("MAX_REQUEST_BYTES"))
		}
	}
	if o.MaxResponseBytes <= 0 {
		o.MaxResponseBytes = o.MaxMessageBytes
		if o.MaxResponseBytes <= 0 {
			o.MaxResponseBytes = int(xrpc.DefaultPolicyInteger("MAX_RESPONSE_BYTES"))
		}
	}
	if o.MaxHeaderBytes == 0 {
		o.MaxHeaderBytes = uint32(xrpc.DefaultPolicyInteger("MAX_HEADER_BYTES"))
	}
	if o.MaxInFlight <= 0 {
		o.MaxInFlight = int(xrpc.DefaultPolicyInteger("HOST_MAX_IN_FLIGHT"))
	}
	if o.MaxCallTime <= 0 {
		o.MaxCallTime = time.Duration(xrpc.DefaultPolicyInteger("CALL_TIMEOUT_MS")) * time.Millisecond
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = time.Duration(xrpc.DefaultPolicyInteger("IDLE_TIMEOUT_MS")) * time.Millisecond
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = time.Duration(xrpc.DefaultPolicyInteger("HEADER_TIMEOUT_MS")) * time.Millisecond
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = time.Duration(xrpc.DefaultPolicyInteger("SHUTDOWN_TIMEOUT_MS")) * time.Millisecond
	}
	return o
}

type Host struct {
	server          *grpc.Server
	lease           *unixlease.Lease
	listener        *netlimit.Listener
	once, graceful  sync.Once
	done, drained   chan struct{}
	err             error
	mu              sync.Mutex
	stopping        bool
	handlers        sync.WaitGroup
	shutdownTimeout time.Duration
	options         HostOptions
}

// Serve uses finite native transport defaults; ServeWithOptions lets the owner
// configure those limits. Product registration remains generated native gRPC.
func Serve(listener net.Listener, lease *unixlease.Lease, register func(grpc.ServiceRegistrar), options ...grpc.ServerOption) (*Host, error) {
	return ServeWithOptions(listener, lease, register, HostOptions{}, options...)
}
func ServeWithOptions(listener net.Listener, lease *unixlease.Lease, register func(grpc.ServiceRegistrar), limits HostOptions, options ...grpc.ServerOption) (*Host, error) {
	return serveOwned(listener, lease, register, limits, nil, options...)
}

// EdgeOptions selects a public/domain-owned native gRPC boundary. Streams are
// bound by an explicit connection-owner lifetime, independent of short internal
// RPC budgets. Authentication, authorization and peer epochs stay with products.
type EdgeOptions struct {
	Limits              HostOptions
	OwnerStreamLifetime time.Duration
	ConnectionGrace     time.Duration
}

// ServeEdgeWithOptions shares native admission, IO and real-work drain with
// ServeWithOptions while retaining an edge's existing stream/auth contract.
// OwnerStreamLifetime must be explicit. Native connection aging (including its
// jitter and grace) is configured to hard-close IO within that upper bound.
func ServeEdgeWithOptions(listener net.Listener, lease *unixlease.Lease, register func(grpc.ServiceRegistrar), edge EdgeOptions, options ...grpc.ServerOption) (*Host, error) {
	if edge.OwnerStreamLifetime <= 0 || edge.OwnerStreamLifetime > 24*time.Hour {
		return nil, errors.New("xrpc: finite edge owner stream lifetime 1ns..24h is required")
	}
	if edge.ConnectionGrace == 0 {
		edge.ConnectionGrace = min(time.Second, edge.OwnerStreamLifetime/10)
	}
	if edge.ConnectionGrace <= 0 || edge.ConnectionGrace >= edge.OwnerStreamLifetime {
		return nil, errors.New("xrpc: edge connection grace must be positive and below owner lifetime")
	}
	return serveOwned(listener, lease, register, edge.Limits, &edge, options...)
}

func serveOwned(listener net.Listener, lease *unixlease.Lease, register func(grpc.ServiceRegistrar), limits HostOptions, edge *EdgeOptions, options ...grpc.ServerOption) (*Host, error) {
	if listener == nil || register == nil {
		return nil, errors.New("xrpc: listener and registration required")
	}
	if limits.InstanceID != "" && !xrpc.ValidID(limits.InstanceID) {
		return nil, errors.New("xrpc: invalid host instance identity")
	}
	if edge == nil && listener.Addr().Network() != "unix" {
		if _, ok := listener.(*tlsListener); !ok {
			return nil, errors.New("xrpc: remote gRPC listener requires TLS")
		}
	}
	limits = limits.defaults()
	limited := netlimit.New(listener, limits.MaxConnections)
	host := &Host{listener: limited, lease: lease, done: make(chan struct{}), drained: make(chan struct{}), shutdownTimeout: limits.ShutdownTimeout, options: limits}
	slots := make(chan struct{}, limits.MaxInFlight)
	admit := func(ctx context.Context) (func(), error) {
		if edge == nil {
			if err := validateRequestMetadata(ctx, limits.InstanceID); err != nil {
				return nil, err
			}
			_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDMetadata, metadata.ValueFromIncomingContext(ctx, RequestIDMetadata)[0]))
			if limits.InstanceID != "" {
				_ = grpc.SetHeader(ctx, metadata.Pairs(InstanceIDMetadata, limits.InstanceID))
			}
			remaining, err := xrpc.Remaining(ctx)
			if err != nil || remaining > limits.MaxCallTime {
				return nil, status.Error(codes.InvalidArgument, "finite native caller deadline within host maximum required")
			}
		}
		host.mu.Lock()
		defer host.mu.Unlock()
		if host.stopping {
			return nil, status.Error(codes.Unavailable, "host stopping")
		}
		select {
		case slots <- struct{}{}:
		default:
			limits.Metrics.Reject()
			limits.Diagnostics.Emit(xrpc.Diagnostic{Level: "warn", Event: "admission_rejected", Service: limits.Service, InstanceID: limits.InstanceID, Category: "resource_exhausted"})
			return nil, status.Error(codes.ResourceExhausted, "host concurrency exhausted")
		}
		host.handlers.Add(1)
		finishMetric := limits.Metrics.Admit()
		started := time.Now()
		return func() {
			limits.Metrics.Outcome(ctx.Err())
			limits.Diagnostics.Emit(xrpc.Diagnostic{Level: "debug", Event: "call_finished", Service: limits.Service, InstanceID: limits.InstanceID, ElapsedMS: time.Since(started).Milliseconds()})
			finishMetric()
			<-slots
			host.handlers.Done()
		}, nil
	}
	connectionPolicy := keepalive.ServerParameters{MaxConnectionIdle: limits.IdleTimeout}
	if edge != nil {
		// grpc-go adds +/-10% age jitter. Divide before multiplying to
		// avoid overflow and reserve its hard-close grace explicitly.
		connectionPolicy.MaxConnectionAge = (edge.OwnerStreamLifetime - edge.ConnectionGrace) / 11 * 10
		connectionPolicy.MaxConnectionAgeGrace = edge.ConnectionGrace
	}
	defaults := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(limits.MaxConcurrentStreams), grpc.MaxRecvMsgSize(limits.MaxRequestBytes), grpc.MaxSendMsgSize(limits.MaxResponseBytes), grpc.MaxHeaderListSize(limits.MaxHeaderBytes),
		grpc.ConnectionTimeout(limits.HandshakeTimeout), grpc.KeepaliveParams(connectionPolicy),
		grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if edge != nil {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, limits.MaxCallTime)
				defer cancel()
			}
			release, err := admit(ctx)
			if err != nil {
				return nil, err
			}
			defer release()
			if limits.Authorize != nil {
				allowed := limits.Authorize(ctx)
				if ctx.Err() != nil {
					return nil, status.FromContextError(ctx.Err()).Err()
				}
				if !allowed {
					return nil, status.Error(codes.PermissionDenied, "caller authorization rejected")
				}
			}
			result, err := next(ctx, request)
			if err == nil && ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return result, err
		}),
		grpc.StreamInterceptor(func(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
			release, err := admit(stream.Context())
			if err != nil {
				return err
			}
			defer release()
			if limits.Authorize != nil {
				allowed := limits.Authorize(stream.Context())
				if stream.Context().Err() != nil {
					return status.FromContextError(stream.Context().Err()).Err()
				}
				if !allowed {
					return status.Error(codes.PermissionDenied, "caller authorization rejected")
				}
			}
			err = next(server, stream)
			if err == nil && stream.Context().Err() != nil {
				return status.FromContextError(stream.Context().Err()).Err()
			}
			return err
		}),
	}
	// Native limits are placed last so optional product gRPC options cannot
	// silently disable the SDK's transport allocation/admission caps.
	args := append(defaults[6:], options...)
	args = append(args, defaults[:6]...)
	var err error
	host.server, err = newOwnedServer(args...)
	if err != nil {
		return nil, err
	}
	register(host.server)
	go func() {
		var serving net.Listener = limited
		if edge != nil {
			serving = &edgeDeadlineListener{Listener: limited, lifetime: edge.OwnerStreamLifetime}
		}
		err := host.server.Serve(serving)
		if !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
			host.err = err
		}
		close(host.done)
		host.mu.Lock()
		stopping := host.stopping
		host.mu.Unlock()
		if !stopping {
			host.Stop()
		}
	}()
	return host, nil
}

// Primary interceptors are reserved by the SDK. grpc-go prepends them to
// Chain* hooks; permitting a product primary could bypass admission and auth.
// Native duplicate-primary options panic before NewServer allocates resources.
func newOwnedServer(options ...grpc.ServerOption) (server *grpc.Server, err error) {
	defer func() {
		if recover() != nil {
			server = nil
			err = errors.New("xrpc: invalid gRPC server options; primary interceptors belong to SDK, use ChainUnaryInterceptor/ChainStreamInterceptor")
		}
	}()
	return grpc.NewServer(options...), nil
}

type tlsListener struct{ net.Listener }

func ServeTLS(listener net.Listener, register func(grpc.ServiceRegistrar), config *tls.Config, options ...grpc.ServerOption) (*Host, error) {
	if config == nil || len(config.Certificates) == 0 && config.GetCertificate == nil {
		return nil, errors.New("xrpc: server TLS identity required")
	}
	config = config.Clone()
	config.MinVersion = max(config.MinVersion, tls.VersionTLS12)
	return Serve(&tlsListener{listener}, nil, register, append(options, grpc.Creds(credentials.NewTLS(config)))...)
}

func ServeEdgeTLS(listener net.Listener, lease *unixlease.Lease, register func(grpc.ServiceRegistrar), config *tls.Config, edge EdgeOptions, options ...grpc.ServerOption) (*Host, error) {
	if config == nil || len(config.Certificates) == 0 && config.GetCertificate == nil {
		return nil, errors.New("xrpc: server TLS identity required")
	}
	cloned := config.Clone()
	cloned.MinVersion = max(cloned.MinVersion, tls.VersionTLS12)
	return ServeEdgeWithOptions(listener, lease, register, edge, append(options, grpc.Creds(credentials.NewTLS(cloned)))...)
}
func (h *Host) stopAdmission() { h.mu.Lock(); h.stopping = true; h.mu.Unlock() }
func (h *Host) finish() {
	<-h.done
	h.handlers.Wait()
	if h.lease != nil {
		_ = h.lease.Close()
	}
	close(h.drained)
}

// Stop closes owned I/O immediately, then retains the lease while native gRPC
// and admitted domain handlers finish. Arbitrary domain code is not preempted.
func (h *Host) Stop() {
	if h == nil {
		return
	}
	h.stopAdmission()
	_ = h.listener.Close()
	h.listener.CloseConnections()
	h.once.Do(func() { go func() { h.server.Stop(); h.finish() }() })
}
func (h *Host) Shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if _, err := xrpc.Remaining(ctx); err != nil {
		return err
	}
	h.stopAdmission()
	h.graceful.Do(func() { go func() { h.server.GracefulStop(); h.once.Do(func() { go h.finish() }) }() })
	select {
	case <-ctx.Done():
		h.Stop()
		return ctx.Err()
	case <-h.drained:
		return h.err
	}
}

// Done reports accept-loop completion; Drained also waits for domain handlers and lease release.
func (h *Host) Done() <-chan struct{}    { return h.done }
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

// FiniteUnary rejects missing budgets and bounds admitted handlers. It does
// not synthesize success when cancellation wins after a domain side effect.
func FiniteUnary(maximum time.Duration, inFlight int) grpc.UnaryServerInterceptor {
	if maximum <= 0 {
		maximum = 30 * time.Second
	}
	if inFlight <= 0 {
		inFlight = 64
	}
	slots := make(chan struct{}, inFlight)
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		remaining, err := xrpc.Remaining(ctx)
		if err != nil || remaining > maximum {
			return nil, status.Error(codes.InvalidArgument, "finite caller deadline required")
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			return nil, status.Error(codes.ResourceExhausted, "host concurrency exhausted")
		}
		result, err := handler(ctx, request)
		if err == nil && ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return result, err
	}
}

// Profile consumes a product-supplied closed protobuf descriptor set. This is
// a typed method client, not a universal Execute(string,JSON) server endpoint.
type Profile struct {
	options     DialOptions
	files       *protoregistry.Files
	connections *refpool.Pool[xrpc.ServiceRef, *grpc.ClientConn]
}

func NewProfile(options DialOptions, files *protoregistry.Files) *Profile {
	options = options.defaults()
	return &Profile{options: options, files: files, connections: refpool.New[xrpc.ServiceRef, *grpc.ClientConn](options.MaxReferences, options.ReferenceIdleTimeout, func(c *grpc.ClientConn) { _ = c.Close() })}
}
func (p *Profile) prepare(ctx context.Context, call xrpc.Call) (context.Context, *grpc.ClientConn, protoreflect.MethodDescriptor, *dynamicpb.Message, func(), error) {
	if _, err := xrpc.Remaining(ctx); err != nil {
		return ctx, nil, nil, nil, nil, xrpc.Failure(xrpc.Code(err), xrpc.NotSent, err)
	}
	if err := call.Service.ValidateInternal(); err != nil {
		return ctx, nil, nil, nil, nil, xrpc.Failure("invalid_argument", xrpc.NotSent, err)
	}
	if !xrpc.ValidID(call.RequestID) {
		return ctx, nil, nil, nil, nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: canonical request identity required"))
	}
	if len(call.Payload) > p.options.MaxRequestBytes {
		return ctx, nil, nil, nil, nil, xrpc.Failure("resource_exhausted", xrpc.NotSent, errors.New("xrpc: protobuf JSON input exceeds byte budget"))
	}
	parts := strings.Split(strings.TrimPrefix(call.Method, "/"), "/")
	if len(parts) != 2 || parts[0] != call.Service.Service || p.files == nil {
		return ctx, nil, nil, nil, nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: typed service/method required"))
	}
	descriptor, err := p.files.FindDescriptorByName(protoreflect.FullName(parts[0] + "." + parts[1]))
	if err != nil {
		return ctx, nil, nil, nil, nil, xrpc.Failure("not_found", xrpc.NotSent, err)
	}
	method, ok := descriptor.(protoreflect.MethodDescriptor)
	if !ok || method.IsStreamingClient() {
		return ctx, nil, nil, nil, nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: unary or server-streaming method required"))
	}
	input := dynamicpb.NewMessage(method.Input())
	if err = protojson.Unmarshal(call.Payload, input); err != nil {
		return ctx, nil, nil, nil, nil, xrpc.Failure("invalid_argument", xrpc.NotSent, err)
	}
	connection, release, err := p.connections.AcquireContext(ctx, call.Service, func() (*grpc.ClientConn, error) { return Dial(call.Service, p.options) })
	if err != nil {
		code := "invalid_argument"
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			code = xrpc.Code(err)
		}
		if errors.Is(err, refpool.ErrFull) {
			code = "resource_exhausted"
		}
		if errors.Is(err, refpool.ErrClosed) {
			code = "unavailable"
		}
		return ctx, nil, nil, nil, nil, xrpc.Failure(code, xrpc.NotSent, err)
	}
	return metadata.AppendToOutgoingContext(ctx, "x-request-id", call.RequestID), connection, method, input, release, nil
}
func callError(err error) error {
	var failure *xrpc.CallError
	if errors.As(err, &failure) {
		return err
	}
	code := "internal"
	switch status.Code(err) {
	case codes.InvalidArgument:
		code = "invalid_argument"
	case codes.Unauthenticated:
		code = "unauthenticated"
	case codes.PermissionDenied:
		code = "permission_denied"
	case codes.NotFound:
		code = "not_found"
	case codes.AlreadyExists, codes.Aborted, codes.FailedPrecondition:
		code = "conflict"
	case codes.ResourceExhausted:
		code = "resource_exhausted"
	case codes.DeadlineExceeded:
		code = "deadline_exceeded"
	case codes.Canceled:
		code = "cancelled"
	case codes.Unavailable:
		code = "unavailable"
	}
	return xrpc.Failure(code, xrpc.OutcomeUnknown, err)
}
func (p *Profile) Call(ctx context.Context, call xrpc.Call) (xrpc.Result, error) {
	ctx, connection, method, input, release, err := p.prepare(ctx, call)
	if err != nil {
		return xrpc.Result{}, err
	}
	defer release()
	if method.IsStreamingServer() {
		return xrpc.Result{}, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: streaming method requires Observe"))
	}
	output := dynamicpb.NewMessage(method.Output())
	if err = connection.Invoke(ctx, call.Method, input, output); err != nil {
		return xrpc.Result{}, callError(err)
	}
	raw, err := protojson.Marshal(output)
	return xrpc.Result{Payload: raw}, err
}
func (p *Profile) Observe(ctx context.Context, call xrpc.Call, emit func(xrpc.Result) error) error {
	ctx, connection, method, input, release, err := p.prepare(ctx, call)
	if err != nil {
		return err
	}
	defer release()
	if !method.IsStreamingServer() || emit == nil {
		return xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: server-streaming method and receiver required"))
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := connection.NewStream(streamCtx, &grpc.StreamDesc{ServerStreams: true}, call.Method)
	if err != nil {
		return callError(err)
	}
	if err = stream.SendMsg(input); err != nil {
		return callError(err)
	}
	if err = stream.CloseSend(); err != nil {
		return callError(err)
	}
	for {
		output := dynamicpb.NewMessage(method.Output())
		err = stream.RecvMsg(output)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return callError(err)
		}
		raw, err := protojson.Marshal(output)
		if err != nil {
			return err
		}
		if err = emit(xrpc.Result{Payload: raw}); err != nil {
			return err
		}
	}
}
func (p *Profile) Close() error             { p.connections.Close(); return nil }
func (p *Profile) Drained() <-chan struct{} { return p.connections.Drained() }
func (p *Profile) Shutdown(ctx context.Context) error {
	if _, err := xrpc.Remaining(ctx); err != nil {
		return err
	}
	p.Close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.Drained():
		return nil
	}
}
func (p *Profile) Status() xrpc.TransportStatus {
	refs, active, capacity := p.connections.Snapshot()
	return xrpc.TransportStatus{Time: time.Now().UTC(), Metrics: p.options.Metrics.Snapshot(), References: refs, ActiveReferences: active, ReferenceCapacity: capacity}
}
