package grpcx

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestApplicationErrorVersusCommittedResponseReceiveLimit(t *testing.T) {
	files, message := bytesDescriptor(t)
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "application.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	var rolledBack, committed atomic.Int32
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) {
		r.RegisterService(&grpc.ServiceDesc{ServiceName: "fixture.Representation", HandlerType: (*interface{})(nil), Methods: []grpc.MethodDesc{{MethodName: "Echo", Handler: func(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			input := dynamicpb.NewMessage(message)
			if err := decode(input); err != nil {
				return nil, err
			}
			handler := func(ctx context.Context, _ any) (any, error) {
				if string(input.Get(message.Fields().Get(0)).Bytes()) == "quota" {
					rolledBack.Add(1)
					return nil, ApplicationError(ctx, status.Error(codes.ResourceExhausted, "quota denied"))
				}
				committed.Add(1)
				output := dynamicpb.NewMessage(message)
				output.Set(message.Fields().Get(0), protoreflect.ValueOfBytes(make([]byte, 256)))
				return output, nil
			}
			return interceptor(ctx, input, &grpc.UnaryServerInfo{Server: server, FullMethod: "/fixture.Representation/Echo"}, handler)
		}}}}, struct{}{})
	}, HostOptions{InstanceID: "boot", MaxResponseBytes: 1024, MaxCallTime: time.Second, MaxInFlight: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	profile := NewProfile(DialOptions{LocalTargetID: "local", MaxResponseBytes: 128}, files)
	defer func() { profile.Close(); <-profile.Drained() }()
	call := xrpc.Call{Service: xrpc.ServiceRef{TargetID: "local", Service: "fixture.Representation", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.Path()}}, Method: "/fixture.Representation/Echo", RequestID: "paired:1"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	input := dynamicpb.NewMessage(message)
	input.Set(message.Fields().Get(0), protoreflect.ValueOfBytes([]byte("quota")))
	call.Payload, err = protojson.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	_, err = profile.Call(ctx, call)
	var failure *xrpc.CallError
	if !errors.As(err, &failure) || failure.Code != "resource_exhausted" || failure.Disposition != xrpc.ResponseReceived {
		t.Fatalf("known application response was lost: %v", err)
	}
	if status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != "quota denied" {
		t.Fatal("native code/message changed", err)
	}
	detail := status.Convert(err).Details()[0].(*errdetails.ErrorInfo)
	if detail.Domain != "xgc2.xrpc" || detail.Reason != "APPLICATION_ERROR" || len(detail.Metadata) != 2 || detail.Metadata["request_id"] != call.RequestID || detail.Metadata["instance_id"] != "boot" {
		t.Fatal("wrong wire marker", detail)
	}
	input.Set(message.Fields().Get(0), protoreflect.ValueOfBytes([]byte("commit")))
	call.RequestID = "paired:2"
	call.Payload, err = protojson.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	_, err = profile.Call(ctx, call)
	if !errors.As(err, &failure) || failure.Code != "resource_exhausted" || failure.Disposition != xrpc.OutcomeUnknown {
		t.Fatalf("local receive cap falsely implied known domain failure: %v", err)
	}
	if rolledBack.Load() != 1 || committed.Load() != 1 {
		t.Fatalf("domain outcomes rollback=%d commit=%d", rolledBack.Load(), committed.Load())
	}
}

func TestApplicationMarkerFailsClosed(t *testing.T) {
	ref := xrpc.ServiceRef{InstanceID: "boot"}
	header := metadata.Pairs(RequestIDMetadata, "request:1", InstanceIDMetadata, "boot")
	base := status.New(codes.ResourceExhausted, "unchanged")
	valid := func() *errdetails.ErrorInfo {
		return &errdetails.ErrorInfo{Domain: applicationErrorDomain, Reason: applicationErrorReason, Metadata: map[string]string{"request_id": "request:1", "instance_id": "boot"}}
	}
	marked, err := base.WithDetails(valid())
	if err != nil {
		t.Fatal(err)
	}
	var known *xrpc.CallError
	if !errors.As(responseError(marked.Err(), ref, "request:1", header), &known) || known.Disposition != xrpc.ResponseReceived {
		t.Fatal("valid marker not accepted")
	}
	wrong := valid()
	wrong.Metadata["instance_id"] = "other"
	extra := valid()
	extra.Metadata["extra"] = "untrusted"
	for _, infos := range [][]*errdetails.ErrorInfo{{}, {wrong}, {extra}, {valid(), valid()}} {
		candidate := base
		for _, info := range infos {
			candidate, err = candidate.WithDetails(info)
			if err != nil {
				t.Fatal(err)
			}
		}
		actual := responseError(candidate.Err(), ref, "request:1", header)
		var failure *xrpc.CallError
		if errors.As(actual, &failure) && failure.Disposition == xrpc.ResponseReceived {
			t.Fatal("malformed/duplicate marker was accepted", infos)
		}
		if status.Convert(actual).Message() != "unchanged" {
			t.Fatal("error message changed")
		}
	}
	if validApplicationError(marked.Err(), "other", "boot") {
		t.Fatal("request marker mismatch accepted")
	}
	var mismatch *xrpc.CallError
	if errors.As(responseError(marked.Err(), ref, "request:1", metadata.Pairs(RequestIDMetadata, "request:1")), &mismatch) {
		t.Fatal("application marker bypassed response instance fence")
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(RequestIDMetadata, "request:1", InstanceIDMetadata, "boot"))
	unadmitted := base.Err()
	if ApplicationError(ctx, unadmitted) != unadmitted {
		t.Fatal("metadata alone forged admitted ownership")
	}
}
