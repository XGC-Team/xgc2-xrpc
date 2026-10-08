package grpcx

import (
	"context"
	"errors"
	"net"

	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// ServeBound adds resolved native credentials and caller authorization to the
// existing finite host. The application owns listener, readiness and drain.
func ServeBound(listener net.Listener, lease *unixlease.Lease, register func(grpc.ServiceRegistrar), bound *xrpc.BootstrapCredentials, instanceID string, limits HostOptions, options ...grpc.ServerOption) (*Host, error) {
	if bound == nil || bound.Role() != xrpc.BootstrapServer || listener == nil {
		return nil, errors.New("xrpc: resolved server bootstrap required")
	}
	binding := bound.Binding()
	if binding.Profile != xrpc.GRPC {
		return nil, errors.New("xrpc: gRPC bootstrap profile required")
	}
	if _, err := binding.ServiceRef(instanceID); err != nil {
		return nil, err
	}
	if limits.InstanceID != "" && limits.InstanceID != instanceID || limits.Service != "" && limits.Service != binding.Service || limits.Authorize != nil {
		return nil, errors.New("xrpc: host options conflict with bootstrap owner")
	}
	limits.InstanceID = instanceID
	limits.Service = binding.Service
	limits.Authorize = func(ctx context.Context) bool {
		return bound.Authorize(ctx, metadata.ValueFromIncomingContext(ctx, "authorization"))
	}
	if binding.Endpoint.Kind == "unix" {
		if lease == nil || lease.Path() != binding.Endpoint.Address || listener.Addr().Network() != "unix" {
			return nil, errors.New("xrpc: local bootstrap requires matching private endpoint lease")
		}
		if err := lease.ValidateListener(listener); err != nil {
			return nil, err
		}
		return ServeWithOptions(listener, lease, register, limits, options...)
	}
	if lease != nil || listener.Addr().Network() == "unix" {
		return nil, errors.New("xrpc: remote bootstrap requires native remote listener")
	}
	return ServeWithOptions(&tlsListener{listener}, nil, register, limits, append(options, grpc.Creds(credentials.NewTLS(bound.TLSConfig())))...)
}

func DialBound(bound *xrpc.BootstrapCredentials, ref xrpc.ServiceRef, options DialOptions) (*grpc.ClientConn, error) {
	if bound == nil || bound.Role() != xrpc.BootstrapClient || ref.Profile != xrpc.GRPC {
		return nil, errors.New("xrpc: resolved gRPC client bootstrap required")
	}
	if err := bound.CheckReference(ref); err != nil {
		return nil, err
	}
	if options.TLSConfig != nil {
		return nil, errors.New("xrpc: client TLS belongs to bootstrap owner")
	}
	md := options.Metadata.Copy()
	if len(md.Get("authorization")) != 0 {
		return nil, errors.New("xrpc: client authorization belongs to bootstrap owner")
	}
	if md == nil {
		md = metadata.MD{}
	}
	for key, value := range bound.Headers() {
		if key == "Authorization" {
			md.Set("authorization", value)
		}
	}
	options.boundAuthorization = len(bound.Headers()) != 0
	options.Metadata = md
	options.TLSConfig = bound.TLSConfig()
	return Dial(ref, options)
}
