package grpcx

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func bytesDescriptor(t *testing.T) (*protoregistry.Files, protoreflect.MessageDescriptor) {
	t.Helper()
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("representation.proto"), Package: proto.String("fixture"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Bytes"), Field: []*descriptorpb.FieldDescriptorProto{{
			Name: proto.String("data"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(),
		}}}},
		Service: []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("Representation"), Method: []*descriptorpb.MethodDescriptorProto{{
			Name: proto.String("Echo"), InputType: proto.String(".fixture.Bytes"), OutputType: proto.String(".fixture.Bytes"),
		}}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	files := &protoregistry.Files{}
	if err := files.RegisterFile(file); err != nil {
		t.Fatal(err)
	}
	return files, file.Messages().Get(0)
}

func TestProfileSeparatesRepresentationAndWireBudgets(t *testing.T) {
	for _, test := range []struct {
		name        string
		bytes, wire int
	}{
		{"three-MiB-at-four-MiB", 3 << 20, 4 << 20},
		{"13.47-MiB-at-sixteen-MiB", (13 << 20) + (480 << 10), 16 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			files, message := bytesDescriptor(t)
			lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "rpc.sock"), unixlease.Options{})
			if err != nil {
				t.Fatal(err)
			}
			listener, err := lease.Listen()
			if err != nil {
				t.Fatal(err)
			}
			var dispatch atomic.Int32
			host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) {
				r.RegisterService(&grpc.ServiceDesc{ServiceName: "fixture.Representation", HandlerType: (*interface{})(nil), Methods: []grpc.MethodDesc{{MethodName: "Echo", Handler: func(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					input := dynamicpb.NewMessage(message)
					if err := decode(input); err != nil {
						return nil, err
					}
					handler := func(context.Context, any) (any, error) { dispatch.Add(1); return input, nil }
					if interceptor == nil {
						return handler(ctx, input)
					}
					return interceptor(ctx, input, &grpc.UnaryServerInfo{Server: server, FullMethod: "/fixture.Representation/Echo"}, handler)
				}}}}, struct{}{})
			}, HostOptions{InstanceID: "boot", MaxRequestBytes: test.wire, MaxResponseBytes: test.wire})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { host.Stop(); <-host.Drained() }()
			profile := NewProfile(DialOptions{LocalTargetID: "local", MaxRequestBytes: test.wire, MaxResponseBytes: test.wire}, files)
			defer func() { profile.Close(); <-profile.Drained() }()
			input := dynamicpb.NewMessage(message)
			input.Set(message.Fields().Get(0), protoreflect.ValueOfBytes(bytes.Repeat([]byte{42}, test.bytes)))
			raw, err := protojson.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			if len(raw) <= test.wire || proto.Size(input) > test.wire {
				t.Fatal("test did not cross representation-only budget")
			}
			call := xrpc.Call{Service: xrpc.ServiceRef{TargetID: "local", Service: "fixture.Representation", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.Path()}}, Method: "/fixture.Representation/Echo", RequestID: "bytes:1", Payload: raw}
			// Two multi-megabyte JSON round trips share this budget; under the race
			// detector on a busy machine they need far more than the 5 s they were
			// given. The test is about budgets, not speed.
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			result, err := profile.Call(ctx, call)
			if err != nil {
				t.Fatal("legal binary group was rejected by its base64 JSON size", err)
			}
			output := dynamicpb.NewMessage(message)
			if err := protojson.Unmarshal(result.Payload, output); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(input, output) || dispatch.Load() != 1 {
				t.Fatal("native round trip differs")
			}
			input.Set(message.Fields().Get(0), protoreflect.ValueOfBytes(make([]byte, test.wire)))
			call.Payload, err = protojson.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			_, err = profile.Call(ctx, call)
			var failure *xrpc.CallError
			if !errors.As(err, &failure) || failure.Disposition != xrpc.NotSent || failure.Code != "resource_exhausted" || dispatch.Load() != 1 {
				t.Fatalf("binary overflow reached native domain: %v dispatch=%d", err, dispatch.Load())
			}
		})
	}
}

func TestRepresentationBudgetRemainsExplicitAndBounded(t *testing.T) {
	files, _ := bytesDescriptor(t)
	profile := NewProfile(DialOptions{MaxRequestBytes: 1024, MaxRequestJSONBytes: 32}, files)
	defer func() { profile.Close(); <-profile.Drained() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	call := xrpc.Call{Service: xrpc.ServiceRef{TargetID: "local", Service: "fixture.Representation", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/run/does-not-exist.sock"}}, Method: "/fixture.Representation/Echo", RequestID: "budget:1", Payload: bytes.Repeat([]byte{' '}, 33)}
	_, err := profile.Call(ctx, call)
	if xrpc.Code(err) != "resource_exhausted" || profile.Status().References != 0 {
		t.Fatalf("representation overflow allocated transport: %v", err)
	}
}
