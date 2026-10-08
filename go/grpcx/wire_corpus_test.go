package grpcx

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSharedNativeGRPCWireCorpus(t *testing.T) {
	var corpus struct {
		SchemaVersion int    `json:"schema_version"`
		Profile       string `json:"profile"`
		InstanceID    string `json:"instance_id"`
		HostTimeout   int    `json:"host_call_timeout_ms"`
		Cases         []struct {
			Name      string      `json:"name"`
			Method    string      `json:"method"`
			Discovery bool        `json:"discovery"`
			Deadline  *int        `json:"deadline_ms"`
			Metadata  [][2]string `json:"metadata"`
			Status    string      `json:"status"`
			Dispatch  int         `json:"dispatch"`
		} `json:"cases"`
	}
	raw, err := os.ReadFile("../../contracts/fixtures/grpc-wire.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.SchemaVersion != 1 || corpus.Profile != "grpc.v1" || len(corpus.Cases) != 23 {
		t.Fatal("unexpected shared corpus")
	}
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "corpus.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	var dispatch atomic.Int32
	var effectiveNS atomic.Int64
	unary := func(method string) grpc.MethodDesc {
		return grpc.MethodDesc{MethodName: method, Handler: func(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			input := &emptypb.Empty{}
			if err := decode(input); err != nil {
				return nil, err
			}
			handler := func(ctx context.Context, _ any) (any, error) {
				dispatch.Add(1)
				deadline, _ := ctx.Deadline()
				effectiveNS.Store(time.Until(deadline).Nanoseconds())
				return &emptypb.Empty{}, nil
			}
			return interceptor(ctx, input, &grpc.UnaryServerInfo{Server: server, FullMethod: "/fixture.Wire/" + method}, handler)
		}}
	}
	stream := func(method string) grpc.StreamDesc {
		return grpc.StreamDesc{StreamName: method, ServerStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
			dispatch.Add(1)
			return stream.RecvMsg(&emptypb.Empty{})
		}}
	}
	host, err := ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) {
		r.RegisterService(&grpc.ServiceDesc{ServiceName: "fixture.Wire", HandlerType: (*interface{})(nil), Methods: []grpc.MethodDesc{unary("Unary"), unary("Describe")}, Streams: []grpc.StreamDesc{stream("Stream"), stream("DescribeStream")}}, struct{}{})
	}, HostOptions{InstanceID: corpus.InstanceID, MaxCallTime: time.Duration(corpus.HostTimeout) * time.Millisecond, DiscoveryMethods: []string{"/fixture.Wire/Describe", "/fixture.Wire/DescribeStream"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { host.Stop(); <-host.Drained() }()
	transport := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", lease.Path())
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	for _, test := range corpus.Cases {
		t.Run(test.Name, func(t *testing.T) {
			path := "/fixture.Wire/Unary"
			if test.Method == "stream" {
				path = "/fixture.Wire/Stream"
			}
			if test.Discovery {
				path = "/fixture.Wire/Describe"
				if test.Method == "stream" {
					path += "Stream"
				}
			}
			// Native HTTP/2 sends literal fixture metadata, avoiding the SDK
			// client's intentional identity generation and preflight validation.
			request, err := http.NewRequest(http.MethodPost, "http://fixture"+path, bytes.NewReader([]byte{0, 0, 0, 0, 0}))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/grpc")
			request.Header.Set("Te", "trailers")
			if test.Deadline != nil {
				request.Header.Set("Grpc-Timeout", fmt.Sprintf("%dm", *test.Deadline))
			}
			for _, pair := range test.Metadata {
				request.Header.Add(pair[0], pair[1])
			}
			before := dispatch.Load()
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(io.LimitReader(response.Body, 1024))
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if len(body) == 1024 {
				t.Fatal("unexpected corpus response size")
			}
			wireStatus := response.Trailer.Get("Grpc-Status")
			if wireStatus == "" {
				wireStatus = response.Header.Get("Grpc-Status")
			}
			number, err := strconv.Atoi(wireStatus)
			if err != nil {
				t.Fatalf("native status missing: header=%v trailer=%v", response.Header, response.Trailer)
			}
			actual := strings.ToUpper(codes.Code(number).String())
			expected := strings.ReplaceAll(test.Status, "_", "")
			if strings.ReplaceAll(actual, "_", "") != expected || int(dispatch.Load()-before) != test.Dispatch {
				t.Fatalf("status=%s dispatch=%d expected=%s/%d message=%s", actual, dispatch.Load()-before, test.Status, test.Dispatch, response.Trailer.Get("Grpc-Message"))
			}
			if test.Name == "unary-longer-caller-budget" && time.Duration(effectiveNS.Load()) > time.Duration(corpus.HostTimeout)*time.Millisecond {
				t.Fatal("unary effective deadline widened")
			}
		})
	}
}
