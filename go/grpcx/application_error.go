package grpcx

import (
	"context"
	"errors"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const applicationErrorDomain = "xgc2.xrpc"
const applicationErrorReason = "APPLICATION_ERROR"

type admittedIdentity struct{ requestID, instanceID string }
type admissionKey struct{}

// Install identity once across the native managed gate and any later stage.
// Each later stage can tighten an unbound instance without duplicating headers.
func admittedContext(ctx context.Context, instanceID string) context.Context {
	previous, installed := ctx.Value(admissionKey{}).(admittedIdentity)
	ids := metadata.ValueFromIncomingContext(ctx, RequestIDMetadata)
	if len(ids) != 1 || !xrpc.ValidID(ids[0]) {
		return ctx
	}
	if !installed {
		_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDMetadata, ids[0]))
	}
	if instanceID != "" && previous.instanceID == "" {
		_ = grpc.SetHeader(ctx, metadata.Pairs(InstanceIDMetadata, instanceID))
	}
	if instanceID == "" {
		instanceID = previous.instanceID
	}
	return context.WithValue(ctx, admissionKey{}, admittedIdentity{requestID: ids[0], instanceID: instanceID})
}

// ApplicationError explicitly marks a known application failure from an
// admitted, instance-bound native handler. It preserves the original status and
// other details. The marker says a response was received; domain rollback and
// replayability remain the application's responsibility. Unadmitted contexts
// and successful nil errors do not receive a marker.
func ApplicationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	identity, ok := ctx.Value(admissionKey{}).(admittedIdentity)
	if !ok || !xrpc.ValidID(identity.requestID) || !xrpc.ValidID(identity.instanceID) {
		return err
	}
	current := status.Convert(err)
	marked, detailErr := current.WithDetails(&errdetails.ErrorInfo{Domain: applicationErrorDomain, Reason: applicationErrorReason, Metadata: map[string]string{"request_id": identity.requestID, "instance_id": identity.instanceID}})
	if detailErr != nil {
		return err
	}
	return marked.Err()
}

func validApplicationError(err error, requestID, instanceID string) bool {
	if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
		return false
	}
	if !xrpc.ValidID(requestID) || !xrpc.ValidID(instanceID) {
		return false
	}
	count := 0
	for _, detail := range status.Convert(err).Details() {
		if _, malformed := detail.(error); malformed {
			return false
		}
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.Domain != applicationErrorDomain && info.Reason != applicationErrorReason {
			continue
		}
		if info.Domain != applicationErrorDomain || info.Reason != applicationErrorReason || len(info.Metadata) != 2 || info.Metadata["request_id"] != requestID || info.Metadata["instance_id"] != instanceID {
			return false
		}
		count++
	}
	return count == 1
}

// Preserve grpc-go's original code/message for native generated callers while
// exposing the common delivery fact to managed profiles and applications.
type applicationCallError struct {
	*xrpc.CallError
	native *status.Status
}

func (e *applicationCallError) GRPCStatus() *status.Status { return e.native }
func (e *applicationCallError) Unwrap() error              { return e.CallError }
func (e *applicationCallError) Error() string              { return e.native.Err().Error() }

func responseError(err error, ref xrpc.ServiceRef, requestID string, header metadata.MD) error {
	if !validApplicationError(err, requestID, ref.InstanceID) || checkResponse(ref, requestID, header) != nil {
		return err
	}
	var failure *xrpc.CallError
	if !errors.As(callError(err), &failure) {
		return err
	}
	copy := *failure
	copy.Disposition = xrpc.ResponseReceived
	return &applicationCallError{CallError: &copy, native: status.Convert(err)}
}
