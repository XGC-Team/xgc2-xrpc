# grpc.v1 long-lived sessions

A session is a gRPC connection that lasts as long as its peers do: a presence stream, a
management tunnel. It is an option of `grpc.v1` (Go only; the first consumer is AgentLink
between Core and the Agent) for the case the fenced call model cannot serve: a call
model has a finite deadline for every call, echoes request id and instance on every
response, and caps every stream at the host's call budget. A session has none of these.
Registry, identity, pinning policy, presence lease and tunnel semantics stay in the
product; XRPC supplies the transport hygiene around them.

APIs: `grpcx.DialSession(target, SessionOptions)` returns a `*grpc.ClientConn`;
`grpcx.ServeSession(listener, register, SessionServerOptions)` returns the same `Host` as
`ServeWithOptions`.

## What a session does not have

No per-call deadline interceptor, no request-id or instance echo requirement (it adds
nothing to the wire), no maximum connection age and no idle timeout set by the SDK. The
zero values of keepalive settings are grpc-go's own defaults.

## What the SDK owns

- **TLS hygiene.** TLS is mandatory, version 1.2 or newer. `SessionOptions.TLSConfig` may set
  `InsecureSkipVerify` only together with `VerifyConnection`, which then carries the whole
  trust decision (certificate pinning, trust on first use). `VerifyPeerCertificate` does not
  count, because grpc-go skips it when a TLS session is resumed. The server takes its
  identity and client-certificate policy from `SessionServerOptions.TLSConfig`
  (`tls.RequireAnyClientCert` for self-signed peers); the peer certificate is visible to the
  product's interceptors and handlers through `peer.FromContext`.
- **Limits.** `MaxRequestBytes`, `MaxResponseBytes` and `MaxHeaderBytes` bound messages and
  headers (16 MiB messages are expressible); `MaxConnections`, `MaxConcurrentStreams` (per
  connection) and `MaxStreams` (calls and streams at once, each open stream holds a slot)
  bound admission, and a stream beyond the limit fails with `ResourceExhausted`.
- **Accounted drain.** `Host.Shutdown` stops admission, gives running streams the context's
  budget, then closes; `Drained` waits for the real handlers.
- **Option order.** Product `grpc.DialOption` and `grpc.ServerOption` values go in `Options`;
  primary interceptors are reserved (use `grpc.ChainUnaryInterceptor` and
  `grpc.ChainStreamInterceptor`, which run inside the SDK's gate); credentials, limits and
  keepalive are applied after `Options`, so they cannot be undone by it. `listener` may be any
  `net.Listener`, for example a multi-address one.

## Keepalive and enforcement go together

Keepalive client parameters, keepalive server parameters and the enforcement policy are
passed to grpc-go unchanged, and the two sides must agree. grpc-go raises a client ping
interval below 10 seconds to 10 and a server one below 1 second to 1. **A client that pings
more often than the server's `KeepaliveEnforcement.MinTime` (five minutes by default) is
disconnected with GOAWAY `too_many_pings` after a few pings.** A product that sets
`SessionOptions.Keepalive` therefore also sets `MinTime` (and `PermitWithoutStream` if it pings
idle connections) in the server's enforcement policy, for example client pings every 30
seconds with a 10-second timeout against `MinTime` 20 seconds. Without keepalive on either
side, a half-open connection is only noticed by the transport's own timeouts; with it, a dead
peer is closed (observed through channelz in the tests).

## Tests

All in `go/grpcx`: `go/grpcx::TestSessionStreamOutlivesTheCallBudget`,
`go/grpcx::TestSessionAuthenticatesThroughACallerSuppliedPin`,
`go/grpcx::TestSessionTLSConfigurationRules`, `go/grpcx::TestSessionMessageSizeLimits`,
`go/grpcx::TestSessionStreamAdmission`,
`go/grpcx::TestSessionProductInterceptorsRunInsideTheGateAndPrimariesAreReserved`,
`go/grpcx::TestSessionKeepaliveParametersAreNotOverridden`,
`go/grpcx::TestSessionServerKeepalivePingsAreSent`,
`go/grpcx::TestSessionClientKeepalivePingsAreSent` (ten seconds; skipped by `-short`),
`go/grpcx::TestSessionServerKeepaliveClosesADeadPeer` and
`go/grpcx::TestSessionShutdownGivesStreamsTheirBudgetThenCloses`. The
[capability matrix](../docs/capability-matrix.md) ties them to the cells.
