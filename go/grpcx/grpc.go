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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// DialOptions are plain limits. A zero field selects the default named in its
// comment; nothing is read from the process environment.
type DialOptions struct {
	LocalTargetID string
	DialContext   xrpc.DialContext
	TLSConfig     *tls.Config
	// MaxMessageBytes sets both message limits when MaxRequestBytes or
	// MaxResponseBytes is zero (default xrpc.DefaultMaxMessageBytes).
	MaxMessageBytes  int
	MaxRequestBytes  int
	MaxResponseBytes int
	// JSON representation limits are independent of native protobuf wire limits.
	// Zero selects twice the corresponding finite wire budget.
	MaxRequestJSONBytes  int
	MaxResponseJSONBytes int
	// MaxHeaderBytes bounds received header lists (default xrpc.DefaultMaxHeaderBytes).
	MaxHeaderBytes uint32
	// MaxCallTime caps every call's budget (default xrpc.DefaultCallTimeout).
	MaxCallTime time.Duration
	// IdleTimeout closes an idle channel (default xrpc.DefaultIdleTimeout).
	IdleTimeout time.Duration
	// MaxInFlight bounds admitted calls (default xrpc.DefaultMaxInFlight).
	MaxInFlight int
	// MaxReferences bounds cached references of a Profile (default xrpc.DefaultMaxReferences).
	MaxReferences int
	// ReferenceIdleTimeout retires idle cached references (default xrpc.DefaultReferenceIdleTimeout).
	ReferenceIdleTimeout time.Duration
	Metadata             metadata.MD
	Diagnostics          *xrpc.Diagnostics
	Metrics              *xrpc.Metrics
}

func (o DialOptions) defaults() DialOptions {
	if o.MaxHeaderBytes == 0 {
		o.MaxHeaderBytes = xrpc.DefaultMaxHeaderBytes
	}
	if o.Metrics == nil {
		o.Metrics = &xrpc.Metrics{}
	}
	if o.MaxRequestBytes <= 0 {
		o.MaxRequestBytes = o.MaxMessageBytes
		if o.MaxRequestBytes <= 0 {
			o.MaxRequestBytes = xrpc.DefaultMaxMessageBytes
		}
	}
	if o.MaxResponseBytes <= 0 {
		o.MaxResponseBytes = o.MaxMessageBytes
		if o.MaxResponseBytes <= 0 {
			o.MaxResponseBytes = xrpc.DefaultMaxMessageBytes
		}
	}
	if o.MaxCallTime <= 0 {
		o.MaxCallTime = xrpc.DefaultCallTimeout
	}
	if o.MaxRequestJSONBytes <= 0 {
		o.MaxRequestJSONBytes = representationBudget(o.MaxRequestBytes)
	}
	if o.MaxResponseJSONBytes <= 0 {
		o.MaxResponseJSONBytes = representationBudget(o.MaxResponseBytes)
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = xrpc.DefaultIdleTimeout
	}
	if o.MaxReferences <= 0 {
		o.MaxReferences = xrpc.DefaultMaxReferences
	}
	if o.ReferenceIdleTimeout <= 0 {
		o.ReferenceIdleTimeout = xrpc.DefaultReferenceIdleTimeout
	}
	return o
}

func representationBudget(wire int) int {
	maximum := int(^uint(0) >> 1)
	if wire > maximum/2 {
		return maximum
	}
	return wire * 2
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
		options.MaxInFlight = xrpc.DefaultMaxInFlight
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

// HostOptions are plain limits. A zero field selects the default named in its
// comment; nothing is read from the process environment.
type HostOptions struct {
	// MaxConnections bounds accepted connections (default xrpc.DefaultMaxConnections).
	MaxConnections int
	// MaxConcurrentStreams bounds streams per connection (default xrpc.DefaultStreamsPerConnection).
	MaxConcurrentStreams uint32
	// MaxMessageBytes sets both message limits when MaxRequestBytes or
	// MaxResponseBytes is zero (default xrpc.DefaultMaxMessageBytes).
	MaxMessageBytes  int
	MaxRequestBytes  int
	MaxResponseBytes int
	// MaxHeaderBytes bounds received header lists (default xrpc.DefaultMaxHeaderBytes).
	MaxHeaderBytes uint32
	// MaxInFlight bounds admitted calls and streams (default xrpc.DefaultMaxInFlight).
	MaxInFlight int
	// MaxCallTime caps the caller's budget (default xrpc.DefaultCallTimeout).
	MaxCallTime time.Duration
	// IdleTimeout closes idle connections (default xrpc.DefaultIdleTimeout).
	IdleTimeout time.Duration
	// HandshakeTimeout bounds connection setup (default xrpc.DefaultHeaderTimeout).
	HandshakeTimeout time.Duration
	// ShutdownTimeout is the drain budget of owners that call Shutdown with it
	// (default xrpc.DefaultShutdownTimeout).
	ShutdownTimeout time.Duration
	InstanceID      string
	// DiscoveryMethods names exact unary description methods. Only an absent
	// instance is unbound; supplied empty or mismatched instances stay rejected.
	DiscoveryMethods []string
	Diagnostics      *xrpc.Diagnostics
	Metrics          *xrpc.Metrics
	Service          string
}

func (o HostOptions) defaults() HostOptions {
	if o.Metrics == nil {
		o.Metrics = &xrpc.Metrics{}
	}
	if o.MaxConnections <= 0 {
		o.MaxConnections = xrpc.DefaultMaxConnections
	}
	if o.MaxConcurrentStreams == 0 {
		o.MaxConcurrentStreams = xrpc.DefaultStreamsPerConnection
	}
	if o.MaxRequestBytes <= 0 {
		o.MaxRequestBytes = o.MaxMessageBytes
		if o.MaxRequestBytes <= 0 {
			o.MaxRequestBytes = xrpc.DefaultMaxMessageBytes
		}
	}
	if o.MaxResponseBytes <= 0 {
		o.MaxResponseBytes = o.MaxMessageBytes
		if o.MaxResponseBytes <= 0 {
			o.MaxResponseBytes = xrpc.DefaultMaxMessageBytes
		}
	}
	if o.MaxHeaderBytes == 0 {
		o.MaxHeaderBytes = xrpc.DefaultMaxHeaderBytes
	}
	if o.MaxInFlight <= 0 {
		o.MaxInFlight = xrpc.DefaultMaxInFlight
	}
	if o.MaxCallTime <= 0 {
		o.MaxCallTime = xrpc.DefaultCallTimeout
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = xrpc.DefaultIdleTimeout
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = xrpc.DefaultHeaderTimeout
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = xrpc.DefaultShutdownTimeout
	}
	return o
}

// Host owns one native gRPC server: its listener, optional endpoint lease and
// the admitted calls and streams. Done reports accept-loop completion; Drained
// also waits for domain handlers and lease release.
type Host struct {
	server         *grpc.Server
	lease          *unixlease.Lease
	listener       *netlimit.Listener
	once, graceful sync.Once
	doneOnce       sync.Once
	done, drained  chan struct{}
	err            error
	mu             sync.Mutex
	stopping       bool
	started        bool
	handlers       sync.WaitGroup
	options        HostOptions
}

func newHost(listener net.Listener, lease *unixlease.Lease, limits HostOptions) *Host {
	return &Host{listener: netlimit.New(listener, limits.MaxConnections), lease: lease, done: make(chan struct{}), drained: make(chan struct{}), options: limits}
}

// admit takes one of the host's in-flight slots for a call or stream and
// accounts for it until the returned release runs. A stopping host and a full
// host refuse.
func (h *Host) admit(ctx context.Context, slots chan struct{}) (func(), error) {
	limits := h.options
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopping {
		return nil, status.Error(codes.Unavailable, "host stopping")
	}
	select {
	case slots <- struct{}{}:
	default:
		limits.Metrics.Reject()
		limits.Diagnostics.Emit(xrpc.Diagnostic{Level: "warn", Event: "admission_rejected", Service: limits.Service, InstanceID: limits.InstanceID, Category: "resource_exhausted"})
		return nil, status.Error(codes.ResourceExhausted, "host concurrency exhausted")
	}
	h.handlers.Add(1)
	finishMetric := limits.Metrics.Admit()
	started := time.Now()
	return func() {
		limits.Metrics.Outcome(ctx.Err())
		limits.Diagnostics.Emit(xrpc.Diagnostic{Level: "debug", Event: "call_finished", Service: limits.Service, InstanceID: limits.InstanceID, ElapsedMS: time.Since(started).Milliseconds()})
		finishMetric()
		<-slots
		h.handlers.Done()
	}, nil
}

// ServeWithOptions hosts product-registered native gRPC services on a Unix
// listener with finite admission, instance fencing and call budgets. Product
// registration remains generated native gRPC; remote or long-lived sessions
// use ServeSession. The host reserves the connection idle timeout, so
// product keepalive parameters do not apply here.
func ServeWithOptions(listener net.Listener, lease *unixlease.Lease, register func(grpc.ServiceRegistrar), limits HostOptions, options ...grpc.ServerOption) (*Host, error) {
	if listener == nil || register == nil {
		return nil, errors.New("xrpc: listener and registration required")
	}
	if limits.InstanceID != "" && !xrpc.ValidID(limits.InstanceID) {
		return nil, errors.New("xrpc: invalid host instance identity")
	}
	if listener.Addr().Network() != "unix" {
		return nil, errors.New("xrpc: internal gRPC hosts serve Unix listeners; use ServeSession for remote listeners")
	}
	limits = limits.defaults()
	limits.DiscoveryMethods = append([]string(nil), limits.DiscoveryMethods...)
	discovery := make(map[string]bool, len(limits.DiscoveryMethods))
	for _, method := range limits.DiscoveryMethods {
		parts := strings.Split(method, "/")
		if len(parts) != 3 || parts[0] != "" || parts[1] == "" || parts[2] == "" || discovery[method] {
			return nil, errors.New("xrpc: unique exact unary discovery method paths required")
		}
		discovery[method] = true
	}
	host := newHost(listener, lease, limits)
	slots := make(chan struct{}, limits.MaxInFlight)
	admit := func(ctx context.Context, method string, stream bool) (func(), error) {
		if stream && discovery[method] {
			return nil, status.Error(codes.InvalidArgument, "discovery must be unary")
		}
		if err := validateRequestMetadataFor(ctx, limits.InstanceID, discovery[method]); err != nil {
			return nil, err
		}
		remaining, err := xrpc.Remaining(ctx)
		if err != nil || stream && remaining > limits.MaxCallTime {
			return nil, status.Error(codes.InvalidArgument, "finite native caller deadline within host maximum required")
		}
		return host.admit(ctx, slots)
	}
	defaults := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(limits.MaxConcurrentStreams), grpc.MaxRecvMsgSize(limits.MaxRequestBytes), grpc.MaxSendMsgSize(limits.MaxResponseBytes), grpc.MaxHeaderListSize(limits.MaxHeaderBytes),
		grpc.ConnectionTimeout(limits.HandshakeTimeout), grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: limits.IdleTimeout}),
		grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if _, err := xrpc.Remaining(ctx); err != nil {
				return nil, status.Error(codes.InvalidArgument, "finite native caller deadline required")
			}
			ctx, cancel := context.WithTimeout(ctx, limits.MaxCallTime)
			defer cancel()
			release, err := admit(ctx, info.FullMethod, false)
			if err != nil {
				return nil, err
			}
			defer release()
			result, err := next(admittedContext(ctx, limits.InstanceID), request)
			if err == nil && ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return result, err
		}),
		grpc.StreamInterceptor(func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
			release, err := admit(stream.Context(), info.FullMethod, true)
			if err != nil {
				return err
			}
			defer release()
			stream = &boundedStream{ServerStream: stream, ctx: admittedContext(stream.Context(), limits.InstanceID)}
			err = next(server, &readyStream{ServerStream: stream})
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
	if host.server, err = newOwnedServer(args...); err != nil {
		return nil, err
	}
	register(host.server)
	go host.serve()
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

// serve runs the accept loop once. A Stop or Shutdown that wins the race
// against the goroutine start leaves a stopped host that never accepts.
func (h *Host) serve() {
	h.mu.Lock()
	if h.stopping {
		h.mu.Unlock()
		return
	}
	h.started = true
	h.mu.Unlock()
	err := h.server.Serve(h.listener)
	h.mu.Lock()
	if !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
		h.err = err
	}
	stopping := h.stopping
	h.doneOnce.Do(func() { close(h.done) })
	h.mu.Unlock()
	if !stopping {
		h.Stop()
	}
}

func (h *Host) stopAdmission() {
	h.mu.Lock()
	h.stopping = true
	if !h.started {
		h.doneOnce.Do(func() { close(h.done) })
	}
	h.mu.Unlock()
}
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
	_ = h.listener.Close()
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
	if len(call.Payload) > p.options.MaxRequestJSONBytes {
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
	if proto.Size(input) > p.options.MaxRequestBytes {
		return ctx, nil, nil, nil, nil, xrpc.Failure("resource_exhausted", xrpc.NotSent, errors.New("xrpc: protobuf input exceeds native wire byte budget"))
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
	if err == nil && len(raw) > p.options.MaxResponseJSONBytes {
		return xrpc.Result{}, xrpc.Failure("resource_exhausted", xrpc.ResponseReceived, errors.New("xrpc: protobuf JSON output exceeds representation byte budget"))
	}
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
		if len(raw) > p.options.MaxResponseJSONBytes {
			return xrpc.Failure("resource_exhausted", xrpc.ResponseReceived, errors.New("xrpc: protobuf JSON output exceeds representation byte budget"))
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
