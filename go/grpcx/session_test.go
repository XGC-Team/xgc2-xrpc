package grpcx

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	channelzpb "google.golang.org/grpc/channelz/grpc_channelz_v1"
	channelzservice "google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// identity is a self-signed certificate, the kind AgentLink peers present and
// pin by public key.
type identity struct {
	certificate tls.Certificate
	leaf        *x509.Certificate
	pin         [32]byte
}

func newIdentity(t testing.TB, name string) identity {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return identity{certificate: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, leaf: leaf, pin: sha256.Sum256(leaf.RawSubjectPublicKeyInfo)}
}

// pinned trusts exactly the certificate whose public key hashes to pin.
func pinned(pin [32]byte, verified *atomic.Int32) *tls.Config {
	return &tls.Config{InsecureSkipVerify: true, VerifyConnection: func(state tls.ConnectionState) error {
		if verified != nil {
			verified.Add(1)
		}
		if len(state.PeerCertificates) != 1 || sha256.Sum256(state.PeerCertificates[0].RawSubjectPublicKeyInfo) != pin {
			return errors.New("pinned key mismatch")
		}
		return nil
	}}
}

// sessionService is a bidirectional echo: every message comes back, and the
// peer certificate it was sent under is recorded.
type sessionService struct {
	opened atomic.Int32
	peers  chan *x509.Certificate
}

func (s *sessionService) register(extra ...func(grpc.ServiceRegistrar)) func(grpc.ServiceRegistrar) {
	return func(r grpc.ServiceRegistrar) {
		r.RegisterService(&grpc.ServiceDesc{ServiceName: "session.Echo", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{{StreamName: "Pipe", ClientStreams: true, ServerStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
			s.opened.Add(1)
			if info, ok := peer.FromContext(stream.Context()); ok && s.peers != nil {
				if tlsInfo, ok := info.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
					select {
					case s.peers <- tlsInfo.State.PeerCertificates[0]:
					default:
					}
				}
			}
			for {
				var message wrapperspb.StringValue
				if err := stream.RecvMsg(&message); err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
				if err := stream.SendMsg(&message); err != nil {
					return err
				}
			}
		}}}}, struct{}{})
		for _, add := range extra {
			add(r)
		}
	}
}

func pipe(ctx context.Context, conn *grpc.ClientConn) (grpc.ClientStream, error) {
	return conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/session.Echo/Pipe")
}

func roundTrip(stream grpc.ClientStream, text string) error {
	if err := stream.SendMsg(wrapperspb.String(text)); err != nil {
		if errors.Is(err, io.EOF) { // the stream ended; its status is on the receive side
			return stream.RecvMsg(&wrapperspb.StringValue{})
		}
		return err
	}
	var echoed wrapperspb.StringValue
	if err := stream.RecvMsg(&echoed); err != nil {
		return err
	}
	if echoed.Value != text {
		return errors.New("echo mismatch: " + echoed.Value)
	}
	return nil
}

func listenTCP(t testing.TB) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func startSession(t testing.TB, listener net.Listener, server identity, register func(grpc.ServiceRegistrar), mutate ...func(*SessionServerOptions)) *Host {
	t.Helper()
	options := SessionServerOptions{TLSConfig: &tls.Config{Certificates: []tls.Certificate{server.certificate}}}
	for _, edit := range mutate {
		edit(&options)
	}
	host, err := ServeSession(listener, register, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { host.Stop(); <-host.Drained() })
	return host
}

func dialSession(t testing.TB, listener net.Listener, options SessionOptions) *grpc.ClientConn {
	t.Helper()
	conn, err := DialSession(listener.Addr().String(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestSessionStreamOutlivesTheCallBudget(t *testing.T) {
	t.Parallel()
	server := newIdentity(t, "core")
	listener := listenTCP(t)
	service := &sessionService{}
	startSession(t, listener, server, service.register())
	conn := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil)})

	// No deadline: the stream lives as long as its peers do. 1.5 s is five
	// times the 300 ms budget the control host below enforces on its calls.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := pipe(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	for i := 0; time.Since(started) < 1500*time.Millisecond; i++ {
		if err := roundTrip(stream, "presence"); err != nil {
			t.Fatalf("session stream broke after %s: %v", time.Since(started), err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(&wrapperspb.StringValue{}); !errors.Is(err, io.EOF) {
		t.Fatalf("clean close: %v", err)
	}

	// Control: a fenced internal host with a 300 ms call budget cuts the same stream.
	lease := privateLease(t)
	fenced, err := ServeWithOptions(lease.listener, lease.lease, service.register(), HostOptions{InstanceID: "boot", MaxCallTime: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { fenced.Stop(); <-fenced.Drained() }()
	ref := xrpc.ServiceRef{TargetID: "local", Service: "session.Echo", APIVersion: "v1", InstanceID: "boot", Profile: xrpc.GRPC, Endpoint: xrpc.Endpoint{Kind: "unix", Address: lease.lease.Path()}}
	internal, err := Dial(ref, DialOptions{LocalTargetID: "local", MaxCallTime: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer internal.Close()
	short, cancelShort := context.WithTimeout(context.Background(), 280*time.Millisecond)
	defer cancelShort()
	cut, err := pipe(short, internal)
	if err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	var cutErr error
	for time.Since(started) < 1500*time.Millisecond && cutErr == nil {
		cutErr = roundTrip(cut, "internal")
		time.Sleep(50 * time.Millisecond)
	}
	if status.Code(cutErr) != codes.DeadlineExceeded {
		t.Fatalf("the fenced stream must hit its budget, got %v after %s", cutErr, time.Since(started))
	}
}

type leased struct {
	lease    *unixlease.Lease
	listener net.Listener
}

func privateLease(t *testing.T) leased {
	t.Helper()
	lease, err := unixlease.Reserve(context.Background(), filepath.Join(privateTempDir(t), "rpc.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	return leased{lease, listener}
}

func TestSessionAuthenticatesThroughACallerSuppliedPin(t *testing.T) {
	server, impostor := newIdentity(t, "core"), newIdentity(t, "impostor")
	listener := listenTCP(t)
	service := &sessionService{peers: make(chan *x509.Certificate, 4)}
	startSession(t, listener, server, service.register(), func(o *SessionServerOptions) { o.TLSConfig.ClientAuth = tls.RequireAnyClientCert })
	agent := newIdentity(t, "agent")

	var verified atomic.Int32
	good := pinned(server.pin, &verified)
	good.Certificates = []tls.Certificate{agent.certificate}
	conn := dialSession(t, listener, SessionOptions{TLSConfig: good})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := pipe(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(stream, "hello"); err != nil {
		t.Fatalf("pinned server rejected: %v", err)
	}
	if verified.Load() != 1 {
		t.Fatalf("the verification hook ran %d times", verified.Load())
	}
	select {
	case seen := <-service.peers:
		if seen.Subject.CommonName != "agent" {
			t.Fatalf("server saw client certificate %q", seen.Subject.CommonName)
		}
	default:
		t.Fatal("the client certificate did not reach the handler")
	}

	// A server whose key does not match the pin is rejected before any stream opens.
	wrong := pinned(impostor.pin, nil)
	wrong.Certificates = []tls.Certificate{agent.certificate}
	rejected := dialSession(t, listener, SessionOptions{TLSConfig: wrong})
	attempt, cancelAttempt := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttempt()
	recvErr := error(nil)
	if s, openErr := pipe(attempt, rejected); openErr != nil {
		recvErr = openErr
	} else {
		recvErr = s.RecvMsg(&wrapperspb.StringValue{})
	}
	if status.Code(recvErr) != codes.Unavailable || !strings.Contains(recvErr.Error(), "pinned key mismatch") {
		t.Fatalf("pin mismatch: %v", recvErr)
	}
	if service.opened.Load() != 1 {
		t.Fatalf("the rejected peer reached the handler: %d streams", service.opened.Load())
	}

	// Without a certificate the server's client-auth policy rejects the peer.
	anonymous := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil)})
	if s, openErr := pipe(attempt, anonymous); openErr == nil {
		if recvErr = s.RecvMsg(&wrapperspb.StringValue{}); status.Code(recvErr) == codes.OK {
			t.Fatal("anonymous peer accepted")
		}
	}
	if service.opened.Load() != 1 {
		t.Fatalf("an anonymous peer reached the handler: %d streams", service.opened.Load())
	}
}

func TestSessionTLSConfigurationRules(t *testing.T) {
	listener := listenTCP(t)
	defer listener.Close()
	address := listener.Addr().String()
	hook := func(tls.ConnectionState) error { return nil }
	for name, config := range map[string]*tls.Config{
		"skip verification without a hook":        {InsecureSkipVerify: true},
		"skip verification with only a peer hook": {InsecureSkipVerify: true, VerifyPeerCertificate: func([][]byte, [][]*x509.Certificate) error { return nil }},
	} {
		if conn, err := DialSession(address, SessionOptions{TLSConfig: config}); err == nil {
			conn.Close()
			t.Errorf("%s: accepted", name)
		}
	}
	for _, config := range []*tls.Config{nil, {InsecureSkipVerify: true, VerifyConnection: hook}, {VerifyConnection: hook}} {
		conn, err := DialSession(address, SessionOptions{TLSConfig: config})
		if err != nil {
			t.Errorf("valid configuration rejected: %v", err)
			continue
		}
		conn.Close()
	}
	for _, target := range []string{"", "no-port", ":9092", "host:0", "host:65536", "host:08080", "host:http", "::1:9092"} {
		if conn, err := DialSession(target, SessionOptions{}); err == nil {
			conn.Close()
			t.Errorf("target %q accepted", target)
		}
	}
	// The caller's configuration is cloned: later changes do not reach the channel.
	config := &tls.Config{InsecureSkipVerify: true, VerifyConnection: hook}
	conn, err := DialSession(address, SessionOptions{TLSConfig: config})
	if err != nil {
		t.Fatal(err)
	}
	config.VerifyConnection = nil
	conn.Close()

	server := newIdentity(t, "core")
	secure := listenTCP(t)
	startSession(t, secure, server, (&sessionService{}).register())
	// The system roots do not trust a self-signed server.
	untrusting := dialSession(t, secure, SessionOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := pipe(ctx, untrusting)
	if err == nil {
		err = s.RecvMsg(&wrapperspb.StringValue{})
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("default verification accepted a self-signed server: %v", err)
	}
	// A server needs an identity.
	if host, err := ServeSession(listenTCP(t), func(grpc.ServiceRegistrar) {}, SessionServerOptions{}); err == nil {
		host.Stop()
		t.Fatal("server without a TLS identity accepted")
	}
}

func TestSessionMessageSizeLimits(t *testing.T) {
	server := newIdentity(t, "core")
	listener := listenTCP(t)
	startSession(t, listener, server, (&sessionService{}).register(), func(o *SessionServerOptions) { o.MaxRequestBytes, o.MaxResponseBytes = 2048, 2048 })
	small := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := pipe(ctx, small)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(stream, strings.Repeat("a", 1500)); err != nil {
		t.Fatalf("a message within the limit: %v", err)
	}
	if err := roundTrip(stream, strings.Repeat("a", 3000)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("server receive limit: %v", err)
	}

	// The client enforces its own limits in both directions.
	tight := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil), MaxRequestBytes: 100, MaxResponseBytes: 100})
	stream, err = pipe(ctx, tight)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(wrapperspb.String(strings.Repeat("a", 500))); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("client send limit: %v", err)
	}
	roomy := listenTCP(t)
	startSession(t, roomy, server, (&sessionService{}).register(), func(o *SessionServerOptions) { o.MaxRequestBytes, o.MaxResponseBytes = 1<<20, 1<<20 })
	receiver := dialSession(t, roomy, SessionOptions{TLSConfig: pinned(server.pin, nil), MaxResponseBytes: 100})
	stream, err = pipe(ctx, receiver)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(stream, strings.Repeat("a", 500)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("client receive limit: %v", err)
	}
	// The default is the common 1 MiB budget, and large sessions raise it.
	big := listenTCP(t)
	startSession(t, big, server, (&sessionService{}).register(), func(o *SessionServerOptions) { o.MaxRequestBytes, o.MaxResponseBytes = 16<<20, 16<<20 })
	conn := dialSession(t, big, SessionOptions{TLSConfig: pinned(server.pin, nil), MaxRequestBytes: 16 << 20, MaxResponseBytes: 16 << 20})
	stream, err = pipe(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(stream, strings.Repeat("a", 4<<20)); err != nil {
		t.Fatalf("a 4 MiB message with 16 MiB limits: %v", err)
	}
}

func TestSessionStreamAdmission(t *testing.T) {
	server := newIdentity(t, "core")
	listener := listenTCP(t)
	service := &sessionService{}
	host := startSession(t, listener, server, service.register(), func(o *SessionServerOptions) { o.MaxStreams = 2 })
	conn := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil)})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	open := func() grpc.ClientStream {
		s, err := pipe(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	first, second := open(), open()
	for _, s := range []grpc.ClientStream{first, second} {
		if err := roundTrip(s, "x"); err != nil {
			t.Fatal(err)
		}
	}
	third := open()
	if err := roundTrip(third, "x"); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("third stream: %v", err)
	}
	if status := host.Status(); status.Metrics.Admitted != 2 || status.Metrics.Rejected != 1 || status.InFlightLimit != 2 {
		t.Fatalf("status %+v", status)
	}
	// Closing a stream returns its slot.
	first.CloseSend()
	if err := first.RecvMsg(&wrapperspb.StringValue{}); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	waitUntil(t, "the slot to be released", func() bool { return host.Status().Metrics.InFlight == 1 })
	if err := roundTrip(open(), "again"); err != nil {
		t.Fatalf("stream after a slot was freed: %v", err)
	}
	if limit := host.Status().ConnectionLimit; limit != xrpc.DefaultMaxConnections {
		t.Fatalf("connection limit %d", limit)
	}
}

func waitUntil(t testing.TB, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSessionProductInterceptorsRunInsideTheGateAndPrimariesAreReserved(t *testing.T) {
	server := newIdentity(t, "core")
	var chained atomic.Int32
	listener := listenTCP(t)
	startSession(t, listener, server, (&sessionService{}).register(), func(o *SessionServerOptions) {
		o.MaxStreams = 1
		o.Options = []grpc.ServerOption{grpc.ChainStreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
			chained.Add(1)
			return next(srv, stream)
		})}
	})
	conn := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	holder, _ := pipe(ctx, conn)
	if err := roundTrip(holder, "x"); err != nil {
		t.Fatal(err)
	}
	refused, _ := pipe(ctx, conn)
	if err := roundTrip(refused, "x"); status.Code(err) != codes.ResourceExhausted || chained.Load() != 1 {
		t.Fatalf("refused stream: %v; product chain ran %d times", err, chained.Load())
	}

	for _, option := range []grpc.ServerOption{
		grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			return next(ctx, request)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
			return next(srv, stream)
		}),
	} {
		l := listenTCP(t)
		host, err := ServeSession(l, func(grpc.ServiceRegistrar) {}, SessionServerOptions{TLSConfig: &tls.Config{Certificates: []tls.Certificate{server.certificate}}, Options: []grpc.ServerOption{option}})
		if host != nil || err == nil || !strings.Contains(err.Error(), "primary interceptors belong to SDK") {
			t.Fatalf("primary interceptor: host=%v err=%v", host, err)
		}
		l.Close()
	}
}

// The SDK sets no connection age: product keepalive parameters are passed
// through unchanged, so a product that wants an age gets exactly that.
func TestSessionKeepaliveParametersAreNotOverridden(t *testing.T) {
	t.Parallel()
	server := newIdentity(t, "core")
	run := func(t *testing.T, params keepalive.ServerParameters, lifetime time.Duration) error {
		listener := listenTCP(t)
		startSession(t, listener, server, (&sessionService{}).register(), func(o *SessionServerOptions) { o.Keepalive = params })
		conn := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil)})
		stream, err := pipe(context.Background(), conn)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		for time.Since(started) < lifetime {
			if err := roundTrip(stream, "x"); err != nil {
				return err
			}
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	}
	if err := run(t, keepalive.ServerParameters{}, 1500*time.Millisecond); err != nil {
		t.Fatalf("by default a session has no maximum age: %v", err)
	}
	err := run(t, keepalive.ServerParameters{MaxConnectionAge: 300 * time.Millisecond, MaxConnectionAgeGrace: 100 * time.Millisecond}, 3*time.Second)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("the product's connection age was not applied: %v", err)
	}
}

// socketKeepalives sums KeepAlivesSent over the sockets channelz reports for
// the server listening on port, or for the client channel dialing it.
func socketKeepalives(t testing.TB, ctx context.Context, conn *grpc.ClientConn, port string, client bool) int64 {
	t.Helper()
	service := channelzpb.NewChannelzClient(conn)
	var sockets []*channelzpb.SocketRef
	if client {
		channels, err := service.GetTopChannels(ctx, &channelzpb.GetTopChannelsRequest{})
		if err != nil {
			t.Fatal(err)
		}
		for _, channel := range channels.Channel {
			if !strings.HasSuffix(channel.Data.Target, ":"+port) {
				continue
			}
			for _, ref := range channel.SubchannelRef {
				sub, err := service.GetSubchannel(ctx, &channelzpb.GetSubchannelRequest{SubchannelId: ref.SubchannelId})
				if err != nil {
					t.Fatal(err)
				}
				sockets = append(sockets, sub.Subchannel.SocketRef...)
			}
		}
	} else {
		servers, err := service.GetServers(ctx, &channelzpb.GetServersRequest{})
		if err != nil {
			t.Fatal(err)
		}
		for _, server := range servers.Server {
			for _, listen := range server.ListenSocket {
				socket, err := service.GetSocket(ctx, &channelzpb.GetSocketRequest{SocketId: listen.SocketId})
				if err != nil {
					continue
				}
				if address := socket.Socket.Local.GetTcpipAddress(); address == nil || strconvPort(address.Port) != port {
					continue
				}
				accepted, err := service.GetServerSockets(ctx, &channelzpb.GetServerSocketsRequest{ServerId: server.Ref.ServerId})
				if err != nil {
					t.Fatal(err)
				}
				sockets = append(sockets, accepted.SocketRef...)
			}
		}
	}
	var total int64
	for _, ref := range sockets {
		socket, err := service.GetSocket(ctx, &channelzpb.GetSocketRequest{SocketId: ref.SocketId})
		if err != nil {
			continue
		}
		total += socket.Socket.Data.KeepAlivesSent
	}
	return total
}

func strconvPort(port int32) string { return strconv.Itoa(int(port)) }

func TestSessionServerKeepalivePingsAreSent(t *testing.T) {
	t.Parallel()
	server := newIdentity(t, "core")
	listener := listenTCP(t)
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	startSession(t, listener, server, (&sessionService{}).register(channelzservice.RegisterChannelzServiceToServer), func(o *SessionServerOptions) {
		// grpc-go clamps a server ping interval below one second to one second.
		o.Keepalive = keepalive.ServerParameters{Time: time.Second, Timeout: time.Second}
	})
	conn := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil)})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := pipe(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(stream, "x"); err != nil {
		t.Fatal(err)
	}
	before := socketKeepalives(t, ctx, conn, port, false)
	time.Sleep(2500 * time.Millisecond) // idle: the server pings after one second of silence
	after := socketKeepalives(t, ctx, conn, port, false)
	if after-before < 2 {
		t.Fatalf("server keepalive pings: %d before, %d after 2.5 s idle", before, after)
	}
	// The stream survives its pings: the client acknowledged them.
	if err := roundTrip(stream, "still here"); err != nil {
		t.Fatalf("stream after pings: %v", err)
	}
}

// The client pings with the product's parameters. grpc-go raises a client ping
// interval below ten seconds to ten, so this test has to wait that long.
func TestSessionClientKeepalivePingsAreSent(t *testing.T) {
	if testing.Short() {
		t.Skip("waits ten seconds for the first client ping")
	}
	t.Parallel()
	server := newIdentity(t, "core")
	listener := listenTCP(t)
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	startSession(t, listener, server, (&sessionService{}).register(channelzservice.RegisterChannelzServiceToServer), func(o *SessionServerOptions) {
		o.KeepaliveEnforcement = keepalive.EnforcementPolicy{MinTime: 5 * time.Second}
	})
	conn := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil), Keepalive: keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 2 * time.Second}})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := pipe(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(stream, "x"); err != nil {
		t.Fatal(err)
	}
	if sent := socketKeepalives(t, ctx, conn, port, true); sent != 0 {
		t.Fatalf("pings before the interval: %d", sent)
	}
	time.Sleep(11 * time.Second)
	if sent := socketKeepalives(t, ctx, conn, port, true); sent < 1 {
		t.Fatalf("the client sent %d keepalive pings in 11 s idle", sent)
	}
	// The server's enforcement policy accepts them: the stream is still up.
	if err := roundTrip(stream, "still here"); err != nil {
		t.Fatalf("stream after the client pinged: %v", err)
	}
}

// A server that pings a peer which has stopped acknowledging closes the
// connection, which ends the session's streams: dead-peer detection.
func TestSessionServerKeepaliveClosesADeadPeer(t *testing.T) {
	t.Parallel()
	server := newIdentity(t, "core")
	listener := listenTCP(t)
	gate := &freezable{Listener: listener}
	released := make(chan struct{}, 1)
	startSession(t, gate, server, func(r grpc.ServiceRegistrar) {
		r.RegisterService(&grpc.ServiceDesc{ServiceName: "session.Echo", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{{StreamName: "Pipe", ClientStreams: true, ServerStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
			<-stream.Context().Done()
			released <- struct{}{}
			return stream.Context().Err()
		}}}}, struct{}{})
	}, func(o *SessionServerOptions) {
		o.Keepalive = keepalive.ServerParameters{Time: time.Second, Timeout: time.Second}
	})
	conn := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil)})
	stream, err := pipe(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	stream.SendMsg(wrapperspb.String("hello"))
	time.Sleep(200 * time.Millisecond)
	gate.freeze() // the peer goes silent without closing
	select {
	case <-released:
	case <-time.After(6 * time.Second):
		t.Fatal("the server did not notice its dead peer")
	}
}

// freezable makes accepted connections deaf on demand: once frozen, nothing the
// peer sends reaches the server, so the peer appears to have vanished without
// closing the connection.
type freezable struct {
	net.Listener
	mu    sync.Mutex
	conns []*frozenConn
}

func (l *freezable) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	frozen := &frozenConn{Conn: conn, closed: make(chan struct{})}
	l.mu.Lock()
	l.conns = append(l.conns, frozen)
	l.mu.Unlock()
	return frozen, nil
}

func (l *freezable) freeze() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		c.frozen.Store(true)
	}
}

type frozenConn struct {
	net.Conn
	frozen atomic.Bool
	closed chan struct{}
	once   sync.Once
}

func (c *frozenConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.frozen.Load() {
		<-c.closed // whatever arrived is held back until the server gives up
		return 0, net.ErrClosed
	}
	return n, err
}

func (c *frozenConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func TestSessionShutdownGivesStreamsTheirBudgetThenCloses(t *testing.T) {
	server := newIdentity(t, "core")
	listener := listenTCP(t)
	service := &sessionService{}
	host := startSession(t, listener, server, service.register())
	conn := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil)})
	stream, err := pipe(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(stream, "x"); err != nil {
		t.Fatal(err)
	}
	// A graceful shutdown waits for the stream; the context bounds the wait.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := host.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("Shutdown with a live stream: %v after %s", err, time.Since(started))
	}
	if err := stream.RecvMsg(&wrapperspb.StringValue{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("stream after the forced close: %v", err)
	}
	select {
	case <-host.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("not drained after the forced close")
	}
	// A closed host accepts nothing.
	fresh := dialSession(t, listener, SessionOptions{TLSConfig: pinned(server.pin, nil)})
	attempt, cancelAttempt := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttempt()
	var refused error
	if s, openErr := pipe(attempt, fresh); openErr != nil {
		refused = openErr
	} else {
		refused = s.RecvMsg(&wrapperspb.StringValue{})
	}
	if status.Code(refused) != codes.Unavailable {
		t.Fatalf("stream on a closed host: %v", refused)
	}

	// With the stream gone a graceful shutdown returns at once.
	listener2 := listenTCP(t)
	host2 := startSession(t, listener2, server, service.register())
	conn2 := dialSession(t, listener2, SessionOptions{TLSConfig: pinned(server.pin, nil)})
	s2, err := pipe(context.Background(), conn2)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(s2, "x"); err != nil {
		t.Fatal(err)
	}
	s2.CloseSend()
	s2.RecvMsg(&wrapperspb.StringValue{})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := host2.Shutdown(ctx2); err != nil {
		t.Fatalf("graceful Shutdown: %v", err)
	}
}
