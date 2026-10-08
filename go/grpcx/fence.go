package grpcx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const InstanceIDMetadata = "x-xrpc-instance-id"
const RequestIDMetadata = "x-request-id"

// WithRequestID lets native generated stubs preserve a caller's domain request
// identity. Dial generates a random identity only when the caller omits one.
func WithRequestID(ctx context.Context, requestID string) (context.Context, error) {
	if !xrpc.ValidID(requestID) {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: invalid request identity"))
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set(RequestIDMetadata, requestID)
	return metadata.NewOutgoingContext(ctx, md), nil
}

func validateRequestMetadata(ctx context.Context, instanceID string) error {
	ids := metadata.ValueFromIncomingContext(ctx, RequestIDMetadata)
	if len(ids) != 1 || !xrpc.ValidID(ids[0]) {
		return status.Error(codes.InvalidArgument, "one canonical request identity required")
	}
	if instanceID != "" {
		instances := metadata.ValueFromIncomingContext(ctx, InstanceIDMetadata)
		if len(instances) > 1 {
			return status.Error(codes.InvalidArgument, "one instance identity required")
		}
		if len(instances) != 1 || instances[0] != instanceID {
			return status.Error(codes.FailedPrecondition, "server instance does not match service reference")
		}
	}
	return nil
}

func validateAuthMetadata(md metadata.MD) error {
	for name := range md {
		lower := strings.ToLower(name)
		if lower == RequestIDMetadata || lower == InstanceIDMetadata || lower == "grpc-timeout" || lower == "content-type" || len(name) == 0 || name[0] == ':' {
			return errors.New("xrpc: client metadata key is owned by transport: " + name)
		}
		if lower != name {
			return errors.New("xrpc: metadata names must be canonical lowercase")
		}
	}
	return nil
}

func clientMetadata(ctx context.Context, ref xrpc.ServiceRef, auth metadata.MD, boundAuthorization bool) (context.Context, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	if boundAuthorization && len(md.Get("authorization")) != 0 {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: caller authorization belongs to bootstrap owner"))
	}
	for name := range md {
		if name != strings.ToLower(name) {
			return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: metadata names must be canonical lowercase"))
		}
		if name == "grpc-timeout" || name == "content-type" {
			return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: caller metadata overrides transport framing"))
		}
	}
	for key, values := range auth {
		if len(md.Get(key)) == 0 {
			md[key] = append([]string(nil), values...)
		}
	}
	ids := md.Get(RequestIDMetadata)
	if len(ids) == 0 {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return nil, xrpc.Failure("internal", xrpc.NotSent, errors.New("xrpc: request identity unavailable"))
		}
		md.Set(RequestIDMetadata, hex.EncodeToString(id[:]))
	} else if len(ids) != 1 || !xrpc.ValidID(ids[0]) {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: one canonical request identity required"))
	}
	instances := md.Get(InstanceIDMetadata)
	if len(instances) > 1 || len(instances) == 1 && instances[0] != ref.InstanceID {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: instance metadata conflicts with service reference"))
	}
	if ref.InstanceID != "" {
		md.Set(InstanceIDMetadata, ref.InstanceID)
	}
	return metadata.NewOutgoingContext(ctx, md), nil
}

// BoundService applies instance fencing and finite admission to every unary
// and stream handler. Public gateways can explicitly omit instance binding.
func BoundService(instanceID string, maximum time.Duration, inFlight int) []grpc.ServerOption {
	if maximum <= 0 {
		maximum = 30 * time.Second
	}
	if inFlight <= 0 {
		inFlight = 64
	}
	slots := make(chan struct{}, inFlight)
	admit := func(ctx context.Context) (context.Context, func(), error) {
		if err := validateRequestMetadata(ctx, instanceID); err != nil {
			return nil, nil, err
		}
		if instanceID != "" {
			_ = grpc.SetHeader(ctx, metadata.Pairs(InstanceIDMetadata, instanceID))
		}
		remaining, err := xrpc.Remaining(ctx)
		if err != nil {
			return nil, nil, status.Error(codes.InvalidArgument, "finite caller deadline required")
		}
		// grpc-go owns the stream's transport context. A derived Context cannot
		// interrupt its RecvMsg/SendMsg; only accept native budgets we can honor.
		if remaining > maximum {
			return nil, nil, status.Error(codes.InvalidArgument, "caller deadline exceeds host maximum")
		}
		select {
		case slots <- struct{}{}:
		default:
			return nil, nil, status.Error(codes.ResourceExhausted, "host concurrency exhausted")
		}
		return ctx, func() { <-slots }, nil
	}
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			ctx, release, err := admit(ctx)
			if err != nil {
				return nil, err
			}
			defer release()
			result, err := next(ctx, request)
			if err == nil && ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return result, err
		}),
		grpc.ChainStreamInterceptor(func(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
			ctx, release, err := admit(stream.Context())
			if err != nil {
				return err
			}
			defer release()
			err = next(server, &boundedStream{ServerStream: stream, ctx: ctx})
			if err == nil && ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			return err
		}),
	}
}

type boundedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *boundedStream) Context() context.Context { return s.ctx }

func clientUnary(ref xrpc.ServiceRef, slots chan struct{}, limits DialOptions, budget *dialBudget) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, next grpc.UnaryInvoker, options ...grpc.CallOption) error {
		if _, err := xrpc.Remaining(ctx); err != nil {
			return xrpc.Failure("invalid_argument", xrpc.NotSent, err)
		}
		ctx, cancel := context.WithTimeout(ctx, limits.MaxCallTime)
		defer cancel()
		var err error
		ctx, err = clientMetadata(ctx, ref, limits.Metadata, limits.boundAuthorization)
		if err != nil {
			return err
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			limits.Metrics.Reject()
			return xrpc.Failure("resource_exhausted", xrpc.NotSent, status.Error(codes.ResourceExhausted, "client concurrency exhausted"))
		}
		finishMetric := limits.Metrics.Admit()
		defer finishMetric()
		started := time.Now()
		defer func() {
			limits.Metrics.Outcome(ctx.Err())
			limits.Diagnostics.Emit(xrpc.Diagnostic{Level: "debug", Event: "call_finished", Service: ref.Service, InstanceID: ref.InstanceID, Operation: method, ElapsedMS: time.Since(started).Milliseconds()})
		}()
		releaseDial := budget.admit(ctx)
		defer releaseDial()
		var header metadata.MD
		err = next(ctx, method, request, reply, connection, append(options, grpc.MaxCallRecvMsgSize(limits.MaxResponseBytes), grpc.MaxCallSendMsgSize(limits.MaxRequestBytes), grpc.Header(&header))...)
		if err != nil {
			return err
		}
		return checkInstance(ref, header)
	}
}
func clientStream(ref xrpc.ServiceRef, slots chan struct{}, limits DialOptions, budget *dialBudget) grpc.StreamClientInterceptor {
	return func(ctx context.Context, descriptor *grpc.StreamDesc, connection *grpc.ClientConn, method string, next grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
		if _, err := xrpc.Remaining(ctx); err != nil {
			return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, err)
		}
		ctx, budgetCancel := context.WithTimeout(ctx, limits.MaxCallTime)
		var err error
		ctx, err = clientMetadata(ctx, ref, limits.Metadata, limits.boundAuthorization)
		if err != nil {
			budgetCancel()
			return nil, err
		}
		select {
		case slots <- struct{}{}:
		default:
			budgetCancel()
			limits.Metrics.Reject()
			return nil, xrpc.Failure("resource_exhausted", xrpc.NotSent, status.Error(codes.ResourceExhausted, "client concurrency exhausted"))
		}
		ctx, cancel := context.WithCancel(ctx)
		finishMetric := limits.Metrics.Admit()
		started := time.Now()
		releaseDial := budget.admit(ctx)
		var released sync.Once
		release := func() {
			released.Do(func() {
				limits.Metrics.Outcome(ctx.Err())
				limits.Diagnostics.Emit(xrpc.Diagnostic{Level: "debug", Event: "call_finished", Service: ref.Service, InstanceID: ref.InstanceID, Operation: method, ElapsedMS: time.Since(started).Milliseconds()})
				finishMetric()
				releaseDial()
				budgetCancel()
				<-slots
			})
		}
		stop := context.AfterFunc(ctx, release)
		finish := func() { release(); stop(); cancel() }
		stream, err := next(ctx, descriptor, connection, method, append(options, grpc.MaxCallRecvMsgSize(limits.MaxResponseBytes), grpc.MaxCallSendMsgSize(limits.MaxRequestBytes))...)
		if err != nil {
			finish()
			return nil, err
		}
		return &fencedStream{ClientStream: stream, ref: ref, cancel: finish}, nil
	}
}
func checkInstance(ref xrpc.ServiceRef, header metadata.MD) error {
	if ref.InstanceID == "" {
		return nil
	}
	values := header.Get(InstanceIDMetadata)
	if len(values) != 1 || values[0] != ref.InstanceID {
		return xrpc.Failure("conflict", xrpc.OutcomeUnknown, status.Error(codes.FailedPrecondition, "server instance changed or identity missing"))
	}
	return nil
}

type fencedStream struct {
	grpc.ClientStream
	ref    xrpc.ServiceRef
	cancel context.CancelFunc
}

func (s *fencedStream) Header() (metadata.MD, error) {
	header, err := s.ClientStream.Header()
	if err != nil {
		s.cancel()
		return nil, err
	}
	err = checkInstance(s.ref, header)
	if err != nil {
		s.cancel()
	}
	return header, err
}

func (s *fencedStream) SendMsg(value any) error {
	err := s.ClientStream.SendMsg(value)
	if err != nil {
		s.cancel()
	}
	return err
}
func (s *fencedStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	if err != nil {
		s.cancel()
	}
	return err
}
func (s *fencedStream) RecvMsg(value any) error {
	header, err := s.ClientStream.Header()
	if err != nil {
		s.cancel()
		return err
	}
	// Check available metadata before allowing grpc-go to mutate the output.
	// Empty metadata may mean an error before response headers; preserve that
	// native error, but never accept a successful empty stream without fencing.
	if len(header) > 0 {
		if err = checkInstance(s.ref, header); err != nil {
			s.cancel()
			return err
		}
	}
	err = s.ClientStream.RecvMsg(value)
	if err == nil || errors.Is(err, io.EOF) {
		if fenceErr := checkInstance(s.ref, header); fenceErr != nil {
			s.cancel()
			return fenceErr
		}
	}
	if err != nil {
		s.cancel()
	}
	return err
}
