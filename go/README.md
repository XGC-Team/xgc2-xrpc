# XRPC Go runtime

Go 1.24+, native `net/http`, native `grpc-go`, native TLS and plain UDP;
production builds use `CGO_ENABLED=0`. There are no domain, database, workflow,
discovery, or provider-activation dependencies.

| Package | Provides |
|---|---|
| `xrpc` | `ServiceRef`, dispositions and `CallError`, `Dispatcher`, `Diagnostics`, `LoadBootstrapInput`, default limits |
| `xrpc/httpx` | `http.v1`: internal Unix hosts and clients, edge hosts, event streams |
| `xrpc/grpcx` | `grpc.v1`: internal Unix hosts and clients, typed `Profile`, long-lived sessions |
| `xrpc/udpx` | `udp.v1`: datagram client and server, key ring |
| `xrpc/udpx/udptest` | in-process UDP fault proxy for tests |
| `xrpc/unix` | private Unix endpoint lease |

## Limits

Limits are plain option structs (`httpx.HostOptions`, `httpx.Config`,
`grpcx.HostOptions`, `grpcx.DialOptions`, `grpcx.SessionOptions`,
`grpcx.SessionServerOptions`, `udpx.ServerConfig`, `udpx.ClientConfig`). A zero
field selects the default documented on the field; nothing is read from the
process environment. The shared defaults are constants of package `xrpc`:

| Constant | Value |
|---|---|
| `DefaultMaxConnections` | 32 host connections |
| `DefaultMaxInFlight` | 32 admitted calls (host or client) |
| `DefaultMaxHeaderBytes` | 16 KiB |
| `DefaultMaxMessageBytes` | 1 MiB per request or response |
| `DefaultCallTimeout` | 30 s |
| `DefaultHeaderTimeout` | 5 s |
| `DefaultIdleTimeout` | 30 s |
| `DefaultShutdownTimeout` | 5 s |
| `DefaultClientConnections` | 16 HTTP connections per reference |
| `DefaultMaxReferences`, `DefaultReferenceIdleTimeout` | 64 cached references, 30 s retirement |
| `DefaultStreamsPerConnection` | 32 native gRPC streams per connection |

Limits bound SDK/transport-owned state, not arbitrary product allocations
inside a domain handler. `net/http` has a 4096-byte request parser allowance
beyond `MaxHeaderBytes`; the SDK additionally rejects decoded header fields
above the selected budget, and outgoing decoded headers/trailers are checked
before native writes. gRPC request/response limits are per encoded message;
HTTP internal response limits count the finite response's actual body.

## Service references and common mechanics

`ServiceRef{TargetID, Service, APIVersion, InstanceID, Profile, Endpoint,
KeyID}`. Profiles are `http.v1`, `grpc.v1` and `udp.v1`; endpoint kinds are
`unix`, `https`, `tls` and `udp` (`host:port`, IPv6 in brackets). `udp.v1` and
the `udp` kind only exist together, a `udp.v1` instance pin is the 32-digit
lowercase hex form of the 128-bit instance, and `KeyID` (the `key_id` JSON
field, zero when unspecified) applies to `udp.v1` only.

Every client result carries a disposition: `not_sent`, `outcome_unknown` or
`response_received` (`xrpc.CallError.Disposition`). The error vocabulary is
`invalid_argument, not_found, conflict, resource_exhausted, deadline_exceeded,
cancelled, unavailable, internal, unauthenticated, permission_denied`. No
transport replays a mutation; `udp.v1` retransmission of the same request id
inside one deadline is deduplicated by the server and is not a replay.

## http.v1 and grpc.v1 hosts and calls

`httpx.Serve` requires canonical finite wire metadata on a Unix listener. Bound
calls require an exact instance header; only explicitly configured GET
discovery routes permit an absent instance. Empty/duplicate supplied
identities remain errors. `Client.Do/DoStream` accept caller request IDs;
`DoWithHeaders` and immutable `Config.Headers` supply authentication while
rejecting wire/framing overrides. `Client.Reference()` returns its immutable
binding; generic callers use `CallWithHeaders(ctx, call, headers)`, which
checks the complete ServiceRef before any network admission.
`TLSForService(ServiceRef)` resolves/clones a per-entry TLS policy; HTTPS origin
DNS remains the default verified server name when an injected dialer connects
through a numeric address. Verification cannot be skipped. `DialContext`
bounds connection establishment. A successful returned connection has its own
pool lifetime; it must not remain tied to the dial context's cancel. Its
native SetDeadline methods must actually interrupt its IO.

`httpx.ServeEdge/RunEdge` serve a public gateway on any listener and preserve
product auth, routes, SSE and WebSockets. Internal RPC budgets are optional on
public edges. Hijacked protocols retain domain framing and byte budgets; the
shared host tracks and closes their underlying connections. Remote TLS is the
product's listener to wrap; the SDK has no TLS server of its own for http.v1.

`grpcx.ServeWithOptions` owns native connection/message/header/stream
admission and finite internal calls on a Unix listener. `HostOptions.InstanceID`
supplies fencing directly. Typed generated stubs remain native.
`grpcx.WithRequestID` supplies a caller identity; a missing identity gets a
cryptographically random default. `DialOptions.Metadata` supplies immutable
authentication metadata and protects wire-owned keys. Accepted native streams
carry real caller deadlines within the host maximum. The SDK does not pretend
a derived context can interrupt native RecvMsg. Pending raw dial, TLS and
HTTP/2 preface IO use caller-owned setup deadlines; native outgoing-header
observation releases that setup bound so established pooled connections
survive a previous caller's deadline. The host reserves the connection idle
timeout: use `ServeSession` when a product needs keepalive control.

Unary handlers receive the smaller of the finite caller budget and host
budget; their admission and lease remain owned until actual handler return.
Exact `HostOptions.DiscoveryMethods` permits unary description calls with an
absent instance. Supplied empty/mismatched instances remain rejected. Streams
flush native identity metadata before their first blocking receive or send,
after product interceptor headers have been assembled. Native gRPC hosts
reserve `grpc.UnaryInterceptor` and `grpc.StreamInterceptor` for SDK
admission and instance/deadline checks; product interceptors must use
`grpc.ChainUnaryInterceptor` and `grpc.ChainStreamInterceptor`, which run
inside the SDK gate. Supplying a primary interceptor returns a startup error.
SDK native message, header, stream and connection limits remain last in server
option application and cannot be loosened by product options.

`grpcx.Profile` separates `MaxRequestJSONBytes`/`MaxResponseJSONBytes` from
native `MaxRequestBytes`/`MaxResponseBytes`. JSON defaults to twice its wire
budget; decoded `proto.Size` and native gRPC limits still enforce the original
wire caps.

`grpcx.ApplicationError(ctx, err)` explicitly marks a known domain failure from
an admitted, instance-bound handler with standard `google.rpc.ErrorInfo`. The
client accepts exactly one `xgc2.xrpc` / `APPLICATION_ERROR` marker only after
matching both request and instance identities to the response fence. It then
reports `response_received`; this does not promise rollback or replay.
Unmarked errors, bad/duplicate markers, cancellation and local receive-size
failures keep `outcome_unknown` and retain the original native status.

## grpc.v1 sessions

`grpcx.DialSession(target, SessionOptions)` and
`grpcx.ServeSession(listener, register, SessionServerOptions)` carry streams
that last as long as their peers do (a presence stream, a management tunnel).
A session has no per-call deadline, no request id or instance echo, and adds
nothing to the wire. The SDK owns TLS hygiene, message and header caps,
admission and the accounted drain; the product owns identity, trust, session
semantics and keepalive policy:

- Keepalive client parameters, server parameters and the enforcement policy
  are passed to grpc-go unchanged. The SDK sets no idle timeout and no maximum
  connection age; the zero values are grpc-go's own defaults. (grpc-go raises a
  client ping interval below 10 s to 10 s and a server one below 1 s to 1 s.)
- TLS is mandatory (1.2+). `SessionOptions.TLSConfig` may set
  `InsecureSkipVerify` only together with `VerifyConnection`, which then
  carries the whole trust decision: certificate pinning, trust on first use.
  `VerifyPeerCertificate` does not count, because it is skipped when a TLS
  session is resumed. The server takes its identity and client-certificate
  policy (`tls.RequireAnyClientCert` for self-signed peers) from
  `SessionServerOptions.TLSConfig`; the peer certificate is visible to the
  product's interceptors and handlers through `peer.FromContext`.
- `MaxRequestBytes`/`MaxResponseBytes`/`MaxHeaderBytes` bound messages and
  headers; `MaxConnections`, `MaxConcurrentStreams` (per connection) and
  `MaxStreams` (calls and streams at once, each open stream holds a slot) bound
  admission, and a stream beyond the limit fails with `ResourceExhausted`.
- Product `grpc.DialOption`/`grpc.ServerOption` values go in `Options`;
  primary interceptors are reserved (use `Chain*`), and credentials, limits and
  keepalive are applied after `Options`. `listener` may be any `net.Listener`,
  for example a multi-address one.
- `ServeSession` returns the same `Host` as `ServeWithOptions`: `Shutdown`
  stops admission, gives running streams the context's budget, then closes;
  `Drained` waits for the real handlers.

Registry, identity, pinning policy, presence lease and tunnel semantics stay
in the product.

## http.v1 event streams

`httpx.ServeEvents(w, r, source, EventsOptions)` serves server-sent events from
a domain `EventSource`:

```go
type EventSource interface {
	Subscribe(ctx context.Context, after string, emit func(Event) error) error
}
```

The cursor `after` is an opaque string (at most 1024 bytes, no CR/LF/NUL).
Frames are `id: <cursor>\nevent: <type>\ndata: <json>\n\n`; an idle stream
carries `: hb` comments every 15 s (`EventsOptions.Heartbeat`). The client
resumes with `Last-Event-ID` (or `?after=` where a header is impossible; the
header wins). When the source does not know the cursor it returns
`ErrCursorGone` and `ServeEvents` sends `id:` / `event: reset` and continues with
a fresh subscription (`after == ""`); the types `reset` and `closing` are
reserved for the transport. When the serving `httpx.Host` drains,
`ServeEvents` sends `event: closing` and returns, so a graceful `Shutdown`
never waits for a stream. Every frame pushes the write deadline forward with
`http.ResponseController` (`EventsOptions.WriteTimeout`, 10 s per frame), so a
stream lives as long as its client and source, beyond the server's
`WriteTimeout`. Streams belong on edge hosts (`ServeEdge`, `RunEdge`) or any
`net/http` server, not on internal `Serve` hosts, and each holds one admission
slot of its host for its lifetime.

`httpx.SubscribeEvents(ctx, client, ref, path, after)` is the Go client. It
reconnects with full-jitter backoff (uniform in `[0, min(5 s, 500 ms << n)]`),
resumes from the last event id, delivers `reset` (and forgets its cursor),
consumes `closing`, and retries transient failures (network errors, 408, 429,
5xx). A failure retrying cannot fix (other 4xx, a non-stream response, a
certificate that does not verify) is returned by the call when it happens on
the first connection and later as a final `Event` whose `Err` is set. Pass a
nil client to derive one from `ref` (Unix socket, or HTTPS with system roots);
a caller-supplied client must not set `Timeout`.

## udp.v1

`udpx` implements the profile for low-frequency, on-demand control requests to
robots on a weak LAN (first consumer: chassis HOLD). One datagram per request
and per reply, at most 1200 bytes, authenticated with HMAC-SHA256 under a
per-key secret; there is no confidentiality. Layout (network byte order):
magic `XRU1`, version 1, type (1 request, 2 reply), flags (bit 0: expected
instance present), `key_id` u32, 16-byte `request_id`, 16-byte `instance`,
`timeout_ms` (request, 1..60000) or `status` (reply), `method_len`, `body_len`,
method, JSON body, 32-byte tag over everything before it. Statuses are
0 ok, 1 invalid_argument, 2 not_found, 3 conflict, 4 resource_exhausted,
5 deadline_exceeded, 6 cancelled, 7 unavailable, 8 internal, 9 unauthenticated
(never sent), 10 permission_denied; a non-zero body is
`{"code":"<name>","message":"...","details":{...}}`.

**Key ring.** `udpx.LoadKeyRing(path)` / `ParseKeyRing` read the file format
shared with the C++ library: one `<key_id> <base64 key>` per line, a decimal
u32 and a key that decodes to exactly 32 bytes; blank lines and `#` comments
(whole-line or trailing) are ignored. The file is secret material; keep it
readable by the service account only. `NewKeyRing` builds a ring from memory.

**Client.** `udpx.NewClient(ClientConfig{Keys})` and
`Client.Call(ctx, ref, method, body, opts...) (Reply, error)`. The context must
carry a deadline. The request is one datagram with a random request id, sent
at once and retransmitted unchanged after waits of 30, 60, 120 and 240 ms and
then every 250 ms (`ClientConfig.Backoff/Interval`) until a valid reply or the
deadline; a call lasts at most 60 s whatever the deadline. A reply is valid
only if the tag verifies under the request's key, the type is reply and the
request id matches; anything else is ignored. The key is `ref.KeyID`, or the
ring's only key when `ref.KeyID` is zero. If `ref.InstanceID` is set it is sent
as the expected instance. A server of another instance answers `conflict`
without running the request, and the call fails with `conflict` and
`outcome_unknown`, not `response_received`: the pinned instance may have run the
request before it went away (reply lost, process restarted), so the caller must
not conclude that nothing happened. Other replies from another instance are
ignored. Dispositions:
`not_sent` when no datagram left (invalid reference, oversized request, no
deadline, dial failure), `outcome_unknown` when at least one was sent and no
valid reply came (including a wrong key: the server drops silently, so
authentication failure is indistinguishable from loss), `response_received`
otherwise; a non-zero status returns both the `Reply` (with its error body) and
a `*xrpc.CallError` with `response_received`. `WithRequestID` reuses a request
id: within the server's cache window the cached reply comes back without a
second execution, which is how a caller recovers the result of a call whose
reply was lost. The transport never retries on its own beyond one deadline.

**Server.** `udpx.Listen(addr, ServerConfig{Keys})` binds and serves;
`Server.Handle(method, func(ctx, Request, *Responder))` registers a method at
any time. A handler answers through the `Responder` (`Reply`, `Fail`,
`Respond`) at most once, before it returns or later from any goroutine, until
`Request.Deadline`; `ctx` ends at the deadline, once the request is answered
or when the server closes. Semantics:

- A datagram with bad magic/version/length, an unknown `key_id` or a bad tag
  is dropped silently. Only authenticated datagrams count against the
  per-source token bucket (50 requests/s, burst 100, keyed by source IP), and
  excess is dropped. An authenticated request with reserved flag bits or a
  `timeout_ms` outside 1..60000 is answered `invalid_argument`.
- At-most-once execution: a reply cache keyed by `(key_id, request_id)` with a
  120 s TTL (from completion) and 1024 entries. A retransmission of an answered
  request gets the cached reply bytes, a retransmission of a running request
  is ignored, and one of an abandoned request stays silent. Pending entries
  are never evicted. At most `MaxInFlight` (64) requests run at once; a new
  request beyond that is answered `resource_exhausted` without executing, and
  the cache capacity must exceed it.
- Server deadline = receipt + `min(timeout_ms, CallBudget)` (default 2 s). A
  handler that misses it produces no reply, the client reports
  `outcome_unknown`.
- A reply that would exceed 1200 bytes is replaced by a short
  `resource_exhausted` reply and the handler's call returns `ErrReplyTooLarge`.
- Every server process has a random 128-bit instance; every reply carries it.
  A request that pins an instance the server does not have is answered
  `conflict` without running the handler.
- `Shutdown(ctx)` stops admitting: new requests are answered `unavailable`
  (a retransmission of an answered request still gets its cached reply), and
  pending requests may answer within the context before the socket closes.
  `Close` abandons them. `Stats()` returns the counters
  (received, malformed, unauthenticated, rate limited, requests, executed,
  duplicates, ignored, replies, refused, abandoned, panics, send errors,
  in-flight, cache entries).
- The transport does not prevent replay beyond the cache window. Domains that
  expose non-idempotent methods fence them with the instance and a monotonic
  revision.

**Testing.** `udptest.NewProxy(upstream, Config{Seed, Requests, Replies})`
relays between clients and a server over loopback with loss, duplication,
reordering (`Reorder`/`ReorderDelay`), delay and jitter per direction,
`BlackholeRequests/BlackholeReplies`, scripted `DropRequests/DropReplies(n)`
and counters; the random choices depend only on the seed and the order of
datagrams. `udpx/interop_test.go` drives a separate server process through the
proxy when `XGC2_XRPC_UDP_INTEROP_SERVER` names its binary (the C++ test
server in CI); without it the test skips. The command line and methods are
documented in the test and implemented by the Go reference
`udpx/udptest/cmd/udpx-interop-server`:

```
go build -o /tmp/udpx-interop ./udpx/udptest/cmd/udpx-interop-server
XGC2_XRPC_UDP_INTEROP_SERVER=/tmp/udpx-interop go test ./udpx -run Interop -v
```

## Ownership and status

Reserve a private Unix endpoint before binding. The runtime directory is owned
mode0700 with no symlink components. The common `<socket>.xrpc.lock` is an
owned, single-link mode0600 file. `Lease.Listen` uses its pinned directory
namespace; external binders use `BindPath` and `RecordBound`. Reclaim of a
killed owner's unreachable socket is explicit. Late cleanup never deletes a
replacement inode.

`Host.Done` means the accept loop exited; `Host.Drained` additionally waits for
real handlers and lease release. Shutdown may exceed its budget and report an
error while retaining ownership. Closing IO cannot preempt arbitrary product
code.

Both profile caches have a fixed reference table, one factory/close worker and
one idle-timer owner. No foreign callback runs under the table mutex. Caller
cancellation stops reference/factory waits; a noncooperative resolver retains
its slot and ownership until it really returns. Active references are pinned;
replacement closes the previous value before creating another.
`Profile.Close` initiates terminal close; `Profile.Shutdown(ctx)` and
`Drained()` expose actual factory/call/cleanup drain. `Status()` returns
maintained counts and timestamps without probes. Host connection counts are
available; the HTTP client's exact connection count is explicitly
unavailable. Metrics have fixed cardinality and no robot/request/path labels.
Streaming HTTP callers must close returned bodies.

## Diagnostics

Create one `xrpc.NewDiagnostics(DiagnosticOptions{Sink: ..., Level: ..., Format:
...})` explicitly and pass it through host/client options. `Level` is one of
`error, warn, info, debug, trace` (default `info`), `Format` is `json` or
`text` (default `json`); `SetLevel` changes verbosity while the owner runs.
Logs have fixed bounded fields; SDK emission excludes payloads, headers,
credentials and arbitrary error text. The default queue holds 256 records of at
most 2048 bytes plus one writer's current record. Four fixed warning buckets
rate-limit repeated events; saturation and sink failures are counted. Emit does
not wait for IO. The process owner exposes admin queries/updates on its
existing authenticated API and owns sink shutdown; no diagnostic port is
opened. Diagnostics.Close can report failed drain while a sink is blocked; Done
closes only after the real writer/owned sink exits. Rotation and collection of
log files belong to the supervisor.

## Bootstrap input

For an actual service owner, `xrpc.LoadBootstrapInput(explicitPath, role)` with
`xrpc.BootstrapServer` or `xrpc.BootstrapClient` reads the common
`--bootstrap-input` document once. `input.Binding()` and `input.Application()`
return copied metadata and opaque JSON; `input.Credentials()` returns the
native credentials (`TLSConfig()`, `Headers()`, `CheckReference(ref)`). The SDK
does not interpret application domains or manage runtime/storage grants.
Bootstrap bindings are `http.v1` or `grpc.v1`; `udp.v1` authenticates with a
key ring file instead.

The loader accepts local Unix bindings with `grants: {}` and preserves the
existing private lease. Remote bindings load identity, CA and bearer files from
explicit handles: pinned canonical paths, no symlinks, owned 0700 final
parent, single-link regular 0600 files, nonblocking open and bounded overflow
reads. Input is at most 16 KiB with strict exact JSON keys and no duplicates;
application nesting is limited to 32 levels. TLS/key/CA/token material is
validated once, without environment variables, file searching or per-call
reads. Native remote TLS requires at least TLS 1.2 and verifies the endpoint
DNS/IP; `mutual_tls` also requires verified native client certificates.
`binding.ServiceRef(instanceID)` constructs the actual readiness reference
after startup and `xrpc.NewInstanceID()` creates a fresh unpredictable
identity. The application passes the credentials' TLS configuration and
headers to its own listener and `httpx.Config`/`grpcx.DialOptions`.

## Disposable conformance fixture

`CGO_ENABLED=0 go run ./cmd/xrpc-conformance --socket /private/fixture.sock
--instance fixture:boot-1 --profile http --duration 1m` emits one JSON
readiness line with a complete reference. The parent directory must already be
owned 0700. Omitting instance creates a random identity. HTTP routes are GET
/v1/describe, /v1/echo (bounded JSON), and bound GET /v1/status. `--profile
grpc` exposes native grpc.health.v1.Health with instance/request metadata and
finite budgets. SIGTERM drains; SIGKILL intentionally bypasses cleanup;
`--reclaim` is explicit. This seam lets external language clients and fault
proxies use a real Go process.

## Validation

```
cd go
CGO_ENABLED=0 go vet ./... && CGO_ENABLED=0 go test ./... -timeout 120s
CGO_ENABLED=1 go test -race ./... -timeout 300s
```

The race detector needs cgo; production does not. `go test -short` skips the
ten-second client keepalive test. The tests use real Unix sockets, loopback
TCP and UDP, TLS with disposable certificates (including a pinned self-signed
session), the shared 33 wire cases and the shared bootstrap and gRPC wire
corpora, the fault proxy, lease/work drain and 10000-reference churn. They do
not claim the entire remote loss/CPU pause/OOM matrix, worst-case RSS, durable
power-loss recovery, every product chain, or deployment ABI/install
validation; those remain joint release evidence. IPv6 is exercised only on
hosts that have an IPv6 loopback.

## Removed APIs

These had no consumer outside this repository and were deleted; there are no
compatibility shims.

| Removed | Use instead |
|---|---|
| `ResolvePolicy`, `PolicyOptions`, `Policy`, `EffectivePolicy`, `DerivePolicy`, `DefaultPolicyInteger`, `CheckDiagnosticPolicy`, `*.WithPolicy`, the `XGC2_XRPC_*` variables | plain option structs and the `xrpc.Default*` constants; the owner reads its own configuration |
| `NewDiagnostics(policy, opts)`, `Diagnostics.UpdatePolicy`, `DiagnosticStatus.Revision` | `NewDiagnostics(opts)` with `Level`/`Format`, `Diagnostics.SetLevel` |
| `NewRotatingFileSink`, `FileSinkOptions` | supervisor-owned collection and rotation of stderr |
| `NewBearerGrant`, `NewTLSIdentityGrant`, `NewTLSTrustGrant`, `NewAuthorizationGrant`, `GrantResolver`, `CredentialGrant`, `ParseBootstrapBinding`, `ReadPrivateBootstrapFile`, `BootstrapInput.ResolveGrant`, `BootstrapBinding.ResolveCredentials`, `BootstrapCredentials.Authorize/Binding/Role`, `Authorization` | `LoadBootstrapInput` |
| `httpx.NewBound`, `httpx.ServeBound`, `httpx.ServeTLS`, `HostOptions.Authorize` | `httpx.New` with the credentials' TLS config and headers; `httpx.ServeEdge` behind the product's TLS listener |
| `grpcx.DialBound`, `grpcx.ServeBound`, `grpcx.ServeTLS`, `grpcx.Serve`, `grpcx.FiniteUnary`, `grpcx.BoundService`, `HostOptions.Authorize` | `grpcx.ServeWithOptions` (set `HostOptions.InstanceID`, `MaxCallTime`, `MaxInFlight`), `grpcx.DialSession`/`ServeSession` for remote TLS |
| `grpcx.ServeEdgeTLS`, `ServeEdgeWithOptions`, `PrepareEdgeTLS`, `EdgeOptions`, `Host.Serve` | `grpcx.ServeSession` |

`httpx.Serve` and `grpcx.ServeWithOptions` now serve Unix listeners only: a
remote HTTP listener is `httpx.ServeEdge`, a remote gRPC listener is
`grpcx.ServeSession`. Consumers that pin an earlier revision keep compiling
against it; moving to this revision means replacing their policy resolution
(`ResolvePolicy`/`WithPolicy`/`NewDiagnostics(policy, ...)`) with option
structs and their uses of the removed hosts with the replacements above.
