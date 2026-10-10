package audit

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	"github.com/XGC-Team/xgc2-xrpc/go/udpx"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// methodFixture serves the method "xgc2.fixture/Echo" and "xgc2.fixture/Refuse"
// over all three profiles from three real servers, and composes one Dispatcher
// that calls them.
type methodFixture struct {
	dispatcher      *xrpc.Dispatcher
	httpRef, udpRef xrpc.ServiceRef
	grpcRef         xrpc.ServiceRef
	httpInstance    string
	udpInstance     string
	echoed          chan string
}

func methodDescriptors(t *testing.T) (*protoregistry.Files, protoreflect.MessageDescriptor) {
	t.Helper()
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("method.proto"), Package: proto.String("xgc2"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Text"), Field: []*descriptorpb.FieldDescriptorProto{{
			Name: proto.String("text"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), JsonName: proto.String("text"),
		}}}},
		Service: []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("fixture"), Method: []*descriptorpb.MethodDescriptorProto{
			{Name: proto.String("Echo"), InputType: proto.String(".xgc2.Text"), OutputType: proto.String(".xgc2.Text")},
			{Name: proto.String("Refuse"), InputType: proto.String(".xgc2.Text"), OutputType: proto.String(".xgc2.Text")},
		}}},
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

func newMethodFixture(t *testing.T) *methodFixture {
	t.Helper()
	f := &methodFixture{echoed: make(chan string, 16)}

	// http.v1 over a private Unix socket: the route is /v1/call/<service>/<Method>.
	httpSocket := filepath.Join(privateTempDir(t), "world.sock")
	lease, err := unixlease.Reserve(context.Background(), httpSocket, unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/call/xgc2.fixture/Echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.echoed <- "http"
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	mux.HandleFunc("POST /v1/call/xgc2.fixture/Refuse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":"conflict","message":"stale revision","details":{"revision":12}}}`))
	})
	f.httpInstance = "world-boot-1"
	httpHost, err := httpx.Serve(listener, lease, mux, httpx.HostOptions{InstanceID: f.httpInstance})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpHost.Shutdown(ctx)
	})
	f.httpRef = xrpc.ServiceRef{TargetID: "local", Service: "world", APIVersion: "v1", InstanceID: f.httpInstance, Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: httpSocket}}

	// udp.v1: the datagram's method field is the name.
	key := make([]byte, udpx.KeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	ring, err := udpx.NewKeyRing(map[uint32][]byte{3: key})
	if err != nil {
		t.Fatal(err)
	}
	udpServer, err := udpx.Listen("127.0.0.1:0", udpx.ServerConfig{Keys: ring})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { udpServer.Close() })
	udpServer.Handle("xgc2.fixture/Echo", func(_ context.Context, request udpx.Request, response *udpx.Responder) {
		f.echoed <- "udp"
		_ = response.Reply(request.Body)
	})
	udpServer.Handle("xgc2.fixture/Refuse", func(_ context.Context, _ udpx.Request, response *udpx.Responder) {
		_ = response.Fail(udpx.StatusConflict, "stale revision", map[string]any{"revision": 12})
	})
	f.udpInstance = udpServer.InstanceID()
	f.udpRef = xrpc.ServiceRef{TargetID: "robot-1", Service: "xgc2.fixture", APIVersion: "v1", KeyID: 3, Profile: xrpc.UDP, Endpoint: xrpc.Endpoint{Kind: "udp", Address: udpServer.Addr().String()}}

	// grpc.v1: the native full method; the caller links the descriptors.
	files, message := methodDescriptors(t)
	grpcSocket := filepath.Join(privateTempDir(t), "type.sock")
	grpcLease, err := unixlease.Reserve(context.Background(), grpcSocket, unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	grpcListener, err := grpcLease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	unary := func(name string, serve func(ctx context.Context, in *dynamicpb.Message) (any, error)) grpc.MethodDesc {
		return grpc.MethodDesc{MethodName: name, Handler: func(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			input := dynamicpb.NewMessage(message)
			if err := decode(input); err != nil {
				return nil, err
			}
			handler := func(ctx context.Context, _ any) (any, error) { return serve(ctx, input) }
			if interceptor == nil {
				return handler(ctx, input)
			}
			return interceptor(ctx, input, &grpc.UnaryServerInfo{Server: server, FullMethod: "/xgc2.fixture/" + name}, handler)
		}}
	}
	grpcHost, err := grpcx.ServeWithOptions(grpcListener, grpcLease, func(r grpc.ServiceRegistrar) {
		r.RegisterService(&grpc.ServiceDesc{ServiceName: "xgc2.fixture", HandlerType: (*interface{})(nil), Methods: []grpc.MethodDesc{
			unary("Echo", func(_ context.Context, in *dynamicpb.Message) (any, error) { f.echoed <- "grpc"; return in, nil }),
			unary("Refuse", func(ctx context.Context, _ *dynamicpb.Message) (any, error) {
				return nil, grpcx.ApplicationError(ctx, status.Error(codes.FailedPrecondition, "stale revision"))
			}),
		}}, struct{}{})
	}, grpcx.HostOptions{InstanceID: "type-boot-1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { grpcHost.Stop(); <-grpcHost.Drained() })
	f.grpcRef = xrpc.ServiceRef{TargetID: "local", Service: "xgc2.fixture", APIVersion: "v1", InstanceID: "type-boot-1", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: grpcSocket}}

	httpProfile := httpx.NewProfile(httpx.Config{LocalTargetID: "local"})
	t.Cleanup(func() { httpProfile.Close(); <-httpProfile.Drained() })
	grpcProfile := grpcx.NewProfile(grpcx.DialOptions{LocalTargetID: "local"}, files)
	t.Cleanup(func() { grpcProfile.Close(); <-grpcProfile.Drained() })
	udpClient, err := udpx.NewClient(udpx.ClientConfig{Keys: ring})
	if err != nil {
		t.Fatal(err)
	}
	udpProfile, err := udpx.NewProfile(udpClient)
	if err != nil {
		t.Fatal(err)
	}
	f.dispatcher, err = xrpc.NewDispatcher(map[string]xrpc.Caller{xrpc.HTTP: httpProfile, xrpc.GRPC: grpcProfile, xrpc.UDP: udpProfile})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestOneMethodNameReachesEveryProfile(t *testing.T) {
	f := newMethodFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, test := range []struct {
		ref      xrpc.ServiceRef
		profile  string
		instance string
	}{
		{f.httpRef, "http", f.httpInstance},
		{f.udpRef, "udp", f.udpInstance},
		{f.grpcRef, "grpc", "type-boot-1"},
	} {
		result, err := f.dispatcher.CallMethod(ctx, test.ref, "xgc2.fixture/Echo", json.RawMessage(`{"text":"hello"}`))
		if err != nil {
			t.Fatalf("%s: %v", test.profile, err)
		}
		var echoed struct{ Text string }
		if err := json.Unmarshal(result.Payload, &echoed); err != nil || echoed.Text != "hello" {
			t.Fatalf("%s: payload %s: %v", test.profile, result.Payload, err)
		}
		if result.InstanceID != test.instance {
			t.Errorf("%s: instance %q, want %q", test.profile, result.InstanceID, test.instance)
		}
		select {
		case served := <-f.echoed:
			if served != test.profile {
				t.Fatalf("%s call was served by %s", test.profile, served)
			}
		default:
			t.Fatalf("%s: handler did not run", test.profile)
		}
	}
}

func TestAnsweredErrorsKeepTheirCodeAndDisposition(t *testing.T) {
	f := newMethodFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, test := range []struct {
		ref     xrpc.ServiceRef
		profile string
		status  int
		details bool
	}{
		{f.httpRef, "http", 409, true},
		{f.udpRef, "udp", 409, true},
		{f.grpcRef, "grpc", 0, false},
	} {
		result, err := f.dispatcher.CallMethod(ctx, test.ref, "xgc2.fixture/Refuse", json.RawMessage(`{"text":"x"}`))
		var failure *xrpc.CallError
		if !errors.As(err, &failure) || failure.Code != "conflict" || failure.Disposition != xrpc.ResponseReceived {
			t.Fatalf("%s: %v", test.profile, err)
		}
		if result.Status != test.status {
			t.Errorf("%s: status %d, want %d", test.profile, result.Status, test.status)
		}
		if test.details {
			// http.v1 and udp.v1 hand back the same envelope, details included.
			var envelope struct {
				Error struct {
					Code    string
					Details struct{ Revision int }
				}
			}
			if err := json.Unmarshal(result.Payload, &envelope); err != nil || envelope.Error.Code != "conflict" || envelope.Error.Details.Revision != 12 {
				t.Errorf("%s: payload %s: %v", test.profile, result.Payload, err)
			}
		}
	}
}

func TestMethodNamesReachNoHandlerOfAnotherName(t *testing.T) {
	f := newMethodFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, ref := range []xrpc.ServiceRef{f.httpRef, f.udpRef} {
		_, err := f.dispatcher.CallMethod(ctx, ref, "xgc2.fixture/Absent", json.RawMessage(`{}`))
		var failure *xrpc.CallError
		if !errors.As(err, &failure) || failure.Code != "not_found" || failure.Disposition != xrpc.ResponseReceived {
			t.Errorf("%s: %v", ref.Profile, err)
		}
	}
	// A reference pinned to another instance fails the same way on every profile.
	for _, ref := range []xrpc.ServiceRef{f.httpRef, f.grpcRef} {
		ref.InstanceID = "someone-else"
		_, err := f.dispatcher.CallMethod(ctx, ref, "xgc2.fixture/Echo", json.RawMessage(`{"text":"x"}`))
		if xrpc.Code(err) != "conflict" {
			t.Errorf("%s: pinned to another instance: %v", ref.Profile, err)
		}
	}
	select {
	case served := <-f.echoed:
		t.Fatalf("a call that had to fail reached the %s handler", served)
	default:
	}
}
