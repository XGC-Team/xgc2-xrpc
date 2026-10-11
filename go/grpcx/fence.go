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
	"google.golang.org/protobuf/proto"
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

func validateRequestMetadataFor(ctx context.Context, instanceID string, discovery bool) error {
	ids := metadata.ValueFromIncomingContext(ctx, RequestIDMetadata)
	if len(ids) != 1 || !xrpc.ValidID(ids[0]) {
		return status.Error(codes.InvalidArgument, "one canonical request identity required")
	}
	if instanceID != "" {
		instances := metadata.ValueFromIncomingContext(ctx, InstanceIDMetadata)
		if len(instances) > 1 {
			return status.Error(codes.InvalidArgument, "one instance identity required")
		}
		if !(discovery && len(instances) == 0) && (len(instances) != 1 || instances[0] != instanceID) {
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

func clientMetadata(ctx context.Context, ref xrpc.ServiceRef, auth metadata.MD) (context.Context, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
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

type boundedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *boundedStream) Context() context.Context { return s.ctx }

// Flush pending native identity metadata before a domain's first receive. A
// peer may wait for initial metadata before sending its first message. Keeping
// the flush at the IO boundary also lets product Chain* hooks add their native
// authorization/instance headers before the initial HEADERS are written.
type readyStream struct {
	grpc.ServerStream
	mu   sync.Mutex
	sent bool
}

func (s *readyStream) SetHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ServerStream.SetHeader(md)
}

func (s *readyStream) SendHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.ServerStream.SendHeader(md)
	if err == nil {
		s.sent = true
	}
	return err
}

func (s *readyStream) ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sent {
		return nil
	}
	if err := s.ServerStream.SendHeader(nil); err != nil {
		return err
	}
	s.sent = true
	return nil
}

func (s *readyStream) RecvMsg(message any) error {
	if err := s.ready(); err != nil {
		return err
	}
	return s.ServerStream.RecvMsg(message)
}

func (s *readyStream) SendMsg(message any) error {
	if err := s.ready(); err != nil {
		return err
	}
	return s.ServerStream.SendMsg(message)
}

func clientUnary(ref xrpc.ServiceRef, slots chan struct{}, limits DialOptions, budget *dialBudget) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, next grpc.UnaryInvoker, options ...grpc.CallOption) error {
		if _, err := xrpc.Remaining(ctx); err != nil {
			return xrpc.Failure("invalid_argument", xrpc.NotSent, err)
		}
		ctx, cancel := context.WithTimeout(ctx, limits.MaxCallTime)
		defer cancel()
		var err error
		ctx, err = clientMetadata(ctx, ref, limits.Metadata)
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
			if ctx.Err() != nil {
				return err
			}
			return responseError(err, ref, outgoingRequestID(ctx), header)
		}
		return checkResponse(ref, outgoingRequestID(ctx), header)
	}
}
func clientStream(ref xrpc.ServiceRef, slots chan struct{}, limits DialOptions, budget *dialBudget) grpc.StreamClientInterceptor {
	return func(ctx context.Context, descriptor *grpc.StreamDesc, connection *grpc.ClientConn, method string, next grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
		if _, err := xrpc.Remaining(ctx); err != nil {
			return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, err)
		}
		ctx, budgetCancel := context.WithTimeout(ctx, limits.MaxCallTime)
		var err error
		ctx, err = clientMetadata(ctx, ref, limits.Metadata)
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
		return &fencedStream{ClientStream: stream, ref: ref, requestID: outgoingRequestID(ctx), ctx: ctx, cancel: finish}, nil
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

func outgoingRequestID(ctx context.Context) string {
	md, _ := metadata.FromOutgoingContext(ctx)
	ids := md.Get(RequestIDMetadata)
	if len(ids) == 1 {
		return ids[0]
	}
	return ""
}

func checkResponse(ref xrpc.ServiceRef, requestID string, header metadata.MD) error {
	ids := header.Get(RequestIDMetadata)
	if len(ids) != 1 || ids[0] != requestID || !xrpc.ValidID(requestID) {
		return xrpc.Failure("conflict", xrpc.OutcomeUnknown, status.Error(codes.FailedPrecondition, "server request identity changed or identity missing"))
	}
	return checkInstance(ref, header)
}

type fencedStream struct {
	grpc.ClientStream
	ref       xrpc.ServiceRef
	requestID string
	ctx       context.Context
	cancel    context.CancelFunc
}

func (s *fencedStream) Header() (metadata.MD, error) {
	header, err := s.ClientStream.Header()
	if err != nil {
		s.cancel()
		return nil, err
	}
	err = checkResponse(s.ref, s.requestID, header)
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
		if err = checkResponse(s.ref, s.requestID, header); err != nil {
			// A partial identity header can precede a native admission error.
			// Preserve that original status without letting a successful message
			// mutate the caller's output before the response fence is satisfied.
			if message, ok := value.(proto.Message); ok {
				if nativeErr := s.ClientStream.RecvMsg(message.ProtoReflect().New().Interface()); nativeErr != nil && !errors.Is(nativeErr, io.EOF) {
					s.cancel()
					return nativeErr
				}
			}
			s.cancel()
			return err
		}
	}
	err = s.ClientStream.RecvMsg(value)
	if err == nil || errors.Is(err, io.EOF) {
		if fenceErr := checkResponse(s.ref, s.requestID, header); fenceErr != nil {
			s.cancel()
			return fenceErr
		}
	}
	if err != nil {
		if s.ctx.Err() == nil {
			err = responseError(err, s.ref, s.requestID, header)
		}
		s.cancel()
		return err
	}
	return err
}
