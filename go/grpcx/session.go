package grpcx

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// A session is a long-lived native gRPC connection carrying streams that last
// as long as their peers do: a presence stream, a management tunnel. It is the
// opposite of the fenced internal call: no per-call deadline, no request or
// instance identity on the wire, and nothing the SDK adds that a product
// cannot see. The SDK owns TLS hygiene, message and header caps, admission
// and the accounted drain; the product owns identity, trust decisions, session
// semantics and keepalive policy.

// SessionOptions configure DialSession. A zero field selects the default named
// in its comment.
type SessionOptions struct {
	// TLSConfig is cloned; the minimum version is raised to TLS 1.2. Nil verifies
	// the server against the system roots. InsecureSkipVerify is accepted only
	// together with VerifyConnection, which then carries the whole trust
	// decision (pinning, trust on first use); VerifyPeerCertificate does not
	// count, because it is skipped when a TLS session is resumed.
	TLSConfig *tls.Config
	// Keepalive is passed to the channel unchanged. The zero value sends no
	// pings; grpc-go raises a Time below 10 s to 10 s.
	Keepalive keepalive.ClientParameters
	// MaxRequestBytes and MaxResponseBytes bound one sent and one received
	// message (default xrpc.DefaultMaxMessageBytes).
	MaxRequestBytes  int
	MaxResponseBytes int
	// MaxHeaderBytes bounds received header lists (default xrpc.DefaultMaxHeaderBytes).
	MaxHeaderBytes uint32
	// Options are native dial options of the product (interceptors, stats
	// handlers, authentication). The SDK's own options are applied after them.
	Options []grpc.DialOption
}

// DialSession opens a channel to target ("host:port") for long-lived streams.
// No deadline is imposed on calls or streams, nothing is added to the wire
// beyond TLS, and the channel never goes idle on its own; the caller closes it.
// The returned connection connects lazily and reconnects with grpc-go's
// backoff; a product that wants its own reconnect policy closes it and dials
// again.
func DialSession(target string, options SessionOptions) (*grpc.ClientConn, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil || host == "" {
		return nil, errors.New("xrpc: session target must be host:port")
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
		return nil, errors.New("xrpc: session target port must be canonical 1..65535")
	}
	config := &tls.Config{}
	if options.TLSConfig != nil {
		config = options.TLSConfig.Clone()
	}
	config.MinVersion = max(config.MinVersion, tls.VersionTLS12)
	if config.InsecureSkipVerify && config.VerifyConnection == nil {
		return nil, errors.New("xrpc: InsecureSkipVerify needs an explicit VerifyConnection hook that authenticates the server")
	}
	if options.MaxRequestBytes <= 0 {
		options.MaxRequestBytes = xrpc.DefaultMaxMessageBytes
	}
	if options.MaxResponseBytes <= 0 {
		options.MaxResponseBytes = xrpc.DefaultMaxMessageBytes
	}
	if options.MaxHeaderBytes == 0 {
		options.MaxHeaderBytes = xrpc.DefaultMaxHeaderBytes
	}
	args := append([]grpc.DialOption(nil), options.Options...)
	args = append(args,
		grpc.WithTransportCredentials(credentials.NewTLS(config)),
		grpc.WithKeepaliveParams(options.Keepalive),
		grpc.WithIdleTimeout(0),
		grpc.WithMaxHeaderListSize(options.MaxHeaderBytes),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(options.MaxResponseBytes), grpc.MaxCallSendMsgSize(options.MaxRequestBytes)),
	)
	return grpc.NewClient("passthrough:///"+target, args...)
}

// SessionServerOptions configure ServeSession. A zero field selects the default
// named in its comment.
type SessionServerOptions struct {
	// TLSConfig is the server identity and client-certificate policy (for
	// example tls.RequireAnyClientCert for self-signed peers whose key the
	// product pins). It is cloned; the minimum version is raised to TLS 1.2.
	TLSConfig *tls.Config
	// Keepalive and KeepaliveEnforcement are passed to the server unchanged.
	// The zero values mean grpc-go's defaults: no idle timeout, no maximum
	// connection age, pings only after two hours, and clients may ping no more
	// often than every five minutes. The SDK sets none of them.
	Keepalive            keepalive.ServerParameters
	KeepaliveEnforcement keepalive.EnforcementPolicy
	// MaxRequestBytes and MaxResponseBytes bound one received and one sent
	// message (default xrpc.DefaultMaxMessageBytes).
	MaxRequestBytes  int
	MaxResponseBytes int
	// MaxHeaderBytes bounds received header lists (default xrpc.DefaultMaxHeaderBytes).
	MaxHeaderBytes uint32
	// MaxConnections bounds accepted connections (default xrpc.DefaultMaxConnections).
	MaxConnections int
	// MaxConcurrentStreams bounds streams per connection (default xrpc.DefaultStreamsPerConnection).
	MaxConcurrentStreams uint32
	// MaxStreams bounds the calls and streams admitted at once across all
	// connections; one beyond it fails with ResourceExhausted. Every open stream
	// holds a slot for its whole life, so size it for streams per peer times
	// peers (default xrpc.DefaultMaxInFlight).
	MaxStreams int
	// HandshakeTimeout bounds connection setup (default xrpc.DefaultHeaderTimeout).
	HandshakeTimeout time.Duration
	// Options are native server options of the product. Primary interceptors
	// belong to the SDK's admission gate: use grpc.ChainUnaryInterceptor and
	// grpc.ChainStreamInterceptor, which run inside it. Credentials, message,
	// header and stream limits and keepalive are applied after Options and
	// cannot be replaced from there.
	Options     []grpc.ServerOption
	Diagnostics *xrpc.Diagnostics
	Metrics     *xrpc.Metrics
	Service     string
}

// ServeSession serves native gRPC services on listener over TLS for long-lived
// streams. Calls and streams carry no deadline or identity requirement; the
// product authenticates its peers (client certificate, tokens) in its own
// interceptors. Admission is bounded by MaxStreams, connections by
// MaxConnections. The returned Host drains like any other: Shutdown stops
// admission, gives running streams the context's budget and then closes.
// listener may be any net.Listener, for example a multi-address one.
func ServeSession(listener net.Listener, register func(grpc.ServiceRegistrar), options SessionServerOptions) (*Host, error) {
	if listener == nil || register == nil {
		return nil, errors.New("xrpc: listener and registration required")
	}
	if options.TLSConfig == nil || len(options.TLSConfig.Certificates) == 0 && options.TLSConfig.GetCertificate == nil && options.TLSConfig.GetConfigForClient == nil {
		return nil, errors.New("xrpc: server TLS identity required")
	}
	config := options.TLSConfig.Clone()
	config.MinVersion = max(config.MinVersion, tls.VersionTLS12)
	limits := HostOptions{MaxConnections: options.MaxConnections, MaxInFlight: options.MaxStreams, MaxConcurrentStreams: options.MaxConcurrentStreams,
		MaxRequestBytes: options.MaxRequestBytes, MaxResponseBytes: options.MaxResponseBytes, MaxHeaderBytes: options.MaxHeaderBytes, HandshakeTimeout: options.HandshakeTimeout,
		Diagnostics: options.Diagnostics, Metrics: options.Metrics, Service: options.Service}.defaults()
	host := newHost(listener, nil, limits)
	slots := make(chan struct{}, limits.MaxInFlight)
	args := []grpc.ServerOption{
		grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			release, err := host.admit(ctx, slots)
			if err != nil {
				return nil, err
			}
			defer release()
			return next(ctx, request)
		}),
		grpc.StreamInterceptor(func(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
			release, err := host.admit(stream.Context(), slots)
			if err != nil {
				return err
			}
			defer release()
			return next(server, stream)
		}),
	}
	args = append(args, options.Options...)
	args = append(args,
		grpc.Creds(credentials.NewTLS(config)),
		grpc.MaxConcurrentStreams(limits.MaxConcurrentStreams), grpc.MaxRecvMsgSize(limits.MaxRequestBytes), grpc.MaxSendMsgSize(limits.MaxResponseBytes),
		grpc.MaxHeaderListSize(limits.MaxHeaderBytes), grpc.ConnectionTimeout(limits.HandshakeTimeout),
		grpc.KeepaliveParams(options.Keepalive), grpc.KeepaliveEnforcementPolicy(options.KeepaliveEnforcement),
	)
	var err error
	if host.server, err = newOwnedServer(args...); err != nil {
		return nil, err
	}
	register(host.server)
	go host.serve()
	return host, nil
}
