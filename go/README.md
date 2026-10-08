# XRPC Go runtime

Go 1.24+, native `net/http`, native `grpc-go`, and native TLS; production builds
use `CGO_ENABLED=0`. There are no domain, database, workflow, discovery, or
provider-activation dependencies.

## Startup policy

The composition root calls `xrpc.ResolvePolicy(xrpc.PolicyOptions{Environment:
os.Environ(), Defaults: ..., Ceilings: ..., Capabilities: ...})` once before
opening listeners. The generated embedded registry comes from
`../contracts/runtime-policy.json`. The library never reads getenv during an
import, call, callback, or worker. Unknown reserved names, empty/noncanonical
values, unsupported selected capabilities, conflicting options and exceeded
ceilings fail explicitly. `Effective()` copies values, sources, mutability,
ceilings and revision; it excludes unrelated environment variables.

Pass the same snapshot through `httpx.Config.WithPolicy`,
`httpx.HostOptions.WithPolicy`, `grpcx.DialOptions.WithPolicy` and
`grpcx.HostOptions.WithPolicy`. Explicit option choices must be resolved as
product defaults/ceilings first. Selected fields map to native admission,
header/body/message, call/header/idle/shutdown and reference-cache limits.
gRPC uses one HTTP/2 connection per immutable reference, satisfying every
positive `CLIENT_MAX_CONNECTIONS` ceiling. Client in-flight admission has an
independent finite option; the common registry does not expose a client
in-flight setting. gRPC request/response limits are per encoded message;
HTTP internal response limits count the finite response's actual body.

Common defaults are 32 host connections/in-flight calls, 16 KiB headers,
1 MiB request/response, 30s calls/idle, 5s headers/shutdown, 16 HTTP connections
per reference, 64 cached references with 30s retirement, and 32 native gRPC
streams per connection. Limits bound SDK/transport-owned state, not arbitrary
product allocations before invoking an SDK or inside a domain handler.
`net/http` has a 4096-byte request parser allowance beyond MaxHeaderBytes;
the SDK additionally rejects decoded header fields above the selected budget.
Outgoing decoded headers/trailers are checked before native writes/final flush.
Parser buffering and native synthesized fields are not claimed to have zero
additional allocation or overhead.

## Hosts and calls

`httpx.Serve` requires canonical finite wire metadata. Bound calls require an
exact instance header; only explicitly configured GET discovery routes permit
an absent instance. Empty/duplicate supplied identities remain errors.
`Client.Do/DoStream` accept caller request IDs; `DoWithHeaders` and immutable
`Config.Headers` supply authentication while rejecting wire/framing overrides.
`Client.Reference()` returns its immutable binding; generic callers use
`CallWithHeaders(ctx, call, headers)`, which checks the complete ServiceRef before
any network admission. `TLSForService(ServiceRef)` resolves/clones a per-entry
TLS policy; HTTPS origin DNS remains the default verified server name when an
injected dialer connects through a numeric address. Verification cannot be skipped.
`DialContext` bounds connection establishment. A successful returned connection
has its own pool lifetime; it must not remain tied to the dial context's cancel.
Its native SetDeadline methods must actually interrupt its IO.

`httpx.ServeEdge/RunEdge` preserve product auth, routes, SSE and WebSockets.
Internal RPC budgets are optional on public edges; an edge policy can select
host/http/transport without rpc. Hijacked protocols retain domain framing and
byte budgets; the shared host tracks and closes their underlying connections.

`grpcx.ServeWithOptions` owns native connection/message/header/stream admission
and finite internal calls. `HostOptions.InstanceID` supplies fencing directly;
`BoundService` is available for a native product registrar. Typed generated
stubs remain native. `grpcx.WithRequestID` supplies a caller identity; a missing
identity gets a cryptographically random default. `DialOptions.Metadata` supplies
immutable authentication metadata and protects wire-owned keys. Accepted native
streams carry real caller deadlines within the host maximum. The SDK does not
pretend a derived context can interrupt native RecvMsg. Pending raw dial, TLS
and HTTP/2 preface IO use caller-owned setup deadlines; native outgoing-header
observation releases that setup bound so established pooled connections survive
a previous caller's deadline.

Persistent public/domain gRPC streams use `ServeEdgeWithOptions` with explicit
`EdgeOptions{Limits: ..., OwnerStreamLifetime: ..., ConnectionGrace: ...}`.
The lifetime must be positive and at most 24h. `ServeEdgeTLS` supplies native TLS;
product native interceptors retain authentication/authorization and peer epochs.
These streams are independent of the short internal RPC budget. Native aging
is adjusted for its +/-10% jitter; an immutable accepted-socket deadline enforces
the actual owner IO boundary even through handshake deadline resets and native
graceful-close delays. There is no per-stream polling/timer worker.

## Ownership and status

Reserve a private Unix endpoint before binding. The runtime directory is owned
mode0700 with no symlink components. The common `<socket>.xrpc.lock` is an owned,
single-link mode0600 file. `Lease.Listen` uses its pinned directory namespace;
external binders use `BindPath` and `RecordBound`. Reclaim of a killed owner's
unreachable socket is explicit. Late cleanup never deletes a replacement inode.

`Host.Done` means the accept loop exited; `Host.Drained` additionally waits for
real handlers and lease release. Shutdown may exceed its budget and report an
error while retaining ownership. Closing IO cannot preempt arbitrary product
code. Public gRPC and hijacked HTTP callbacks follow the same actual-work rule.

Both profile caches have a fixed reference table, one factory/close worker and
one idle-timer owner. No foreign callback runs under the table mutex. Caller
cancellation stops reference/factory waits; a noncooperative resolver retains its
slot and ownership until it really returns. Cancelled queued factories are
discarded. Active references are pinned; replacement closes the previous value
before creating another. `Profile.Close` initiates terminal close;
`Profile.Shutdown(ctx)` and `Drained()` expose actual factory/call/cleanup drain.
`Status()` returns maintained counts and timestamps without probes. Host
connection counts are available; HTTP client's exact connection count is
explicitly unavailable. Metrics have fixed cardinality and no robot/request/path
labels. Streaming HTTP callers must close returned bodies.

## Diagnostics

Create one `xrpc.NewDiagnostics(policy, DiagnosticOptions{Sink: ...})` explicitly
and pass it through host/client options. Explicit LOG_LEVEL/LOG_FORMAT settings
are rejected when no diagnostics owner is wired. Logs have fixed bounded fields;
SDK emission excludes payloads, headers, credentials and arbitrary error text.
The default queue holds 256 records of at most 2048 bytes plus one writer's
current record. Four fixed warning buckets rate-limit repeated events; saturation
and sink failures are counted. Emit does not wait for IO. JSON/text and filtering
are executed. `Diagnostics.UpdatePolicy(expectedRevision, {"LOG_LEVEL": ...})`
changes verbosity and applied revision atomically; LOG_FORMAT/resource changes
require restart. The process owner exposes admin queries/updates on its existing
authenticated API and owns sink shutdown; no diagnostic port is opened.

A supervisor may own bounded stderr collection/rotation. Alternatively,
`NewRotatingFileSink(FileSinkOptions{Directory, Name, MaxBytes, Files})` accepts
an existing private directory grant, pins it, rejects symlinks/hardlinks/FIFOs,
and owns one writer lock. Files includes the active file and archives; one fixed
zero-length `.xrpc-log.lock` is additional ownership metadata. Archives are
size/count checked at startup; shrinking below existing state fails explicitly.
Rotation does not follow replacement inodes. Routine logs have no durable
experiment-evidence/fsync promise. No external rotator may rename this active
file. Diagnostics.Close can report failed drain while a sink is blocked;
Done closes only after the real writer/owned sink exits.

## Disposable conformance fixture

`CGO_ENABLED=0 go run ./cmd/xrpc-conformance --socket /private/fixture.sock
--instance fixture:boot-1 --profile http --duration 1m` emits one JSON readiness
line with a complete reference. The parent directory must already be owned0700.
Omitting instance creates a random identity. HTTP routes are GET /v1/describe,
/v1/echo (bounded JSON), and bound GET /v1/status. `--profile grpc` exposes native
grpc.health.v1.Health with instance/request metadata and finite budgets.
SIGTERM drains; SIGKILL intentionally bypasses cleanup; `--reclaim` is explicit.
This seam lets external language clients and fault proxies use a real Go process.

## Validation and limits

`CGO_ENABLED=0 go test ./... -timeout 30s` and
`CGO_ENABLED=1 go test -race ./... -timeout 45s` cover the shared 33 wire cases,
22 environment cases, TLS identity/handshake stalls, native gRPC setup/stream
limits, no replay, actual lease/work drain, auth binding, HEAD, header/trailer
caps, 10000-reference churn, factory cancellation, log saturation/rotation/FIFO
and namespace replacement. Production does not require CGO; the race tool does.
The fixture's 33 wire cases also run over real Unix sockets in an independent
process. These tests do not claim the entire remote loss/CPU pause/OOM matrix,
worst-case RSS, durable power-loss recovery, every product chain, or deployment
ABI/install validation has passed. Those remain joint release evidence.

Native gRPC hosts reserve `grpc.UnaryInterceptor` and `grpc.StreamInterceptor`
for SDK admission, instance/deadline checks and caller authorization. Product
interceptors must use `grpc.ChainUnaryInterceptor` and
`grpc.ChainStreamInterceptor`; these run inside the SDK gate. Supplying a
primary interceptor returns a startup error instead of bypassing that gate or
panicking the process. SDK native message, header, stream and connection limits
remain last in server option application and cannot be loosened by product options.

For an actual service owner, `xrpc.LoadBootstrapInput(explicitPath,
xrpc.BootstrapServer)` reads the common `--bootstrap-input` document once. Use
`xrpc.BootstrapClient` for an outbound owner, or resolve additional explicit
bindings through `binding.ResolveCredentials(input.ResolveGrant, role)`.
`input.Binding()` and `input.Application()` return copied metadata and opaque
JSON; the SDK does not interpret application domains or manage runtime/storage
grants. Modules sharing a host and direct libraries need no listener/bootstrap.

The loader accepts local Unix bindings with `grants: {}` and preserves the
existing private lease. Remote bindings load identity, CA and bearer files
from explicit handles: pinned canonical paths, no symlinks, owned 0700 final
parent, single-link regular 0600 files, nonblocking open and bounded overflow
reads. Input is at most 16 KiB with strict exact JSON keys and no duplicates;
application nesting is limited to 32 levels. TLS/key/CA/token material is
validated once, without environment variables, file searching or per-call reads.
Native remote TLS requires at least TLS 1.2 and verifies the endpoint DNS/IP;
`mutual_tls` also requires verified native client certificates.

The application supplies its already bound listener to
`httpx.ServeBound(listener, lease, handler, input.Credentials(), instanceID,
options)` or `grpcx.ServeBound(listener, lease, register, input.Credentials(),
instanceID, limits, grpc.Chain*Interceptor(...))`. These reuse the existing
native host lifecycle. Unix listeners must match the live lease's native
address and retained inode. `xrpc.NewInstanceID()` creates a fresh unpredictable
identity; `binding.ServiceRef(instanceID)` constructs the actual readiness
reference after startup. These wrappers require the loaded server role and
fence every internal request. Product method/scope checks remain inside the
handler/interceptor. The declared caller grant must return `true` while the
native request context remains live before product dispatch; a blocked grant
still counts as owned work and retains the lease until it returns.

For a client owner use `httpx.NewBound(credentials, actualRef, config)` or
`grpcx.DialBound(credentials, actualRef, dialOptions)`. Full reference identity
must match the resolved binding; limits and remote numeric dialing remain
explicit owner options. Bootstrap authorization cannot be overridden by
per-call headers/outgoing metadata. `NewTLSIdentityGrant`, `NewTLSTrustGrant`,
`NewBearerGrant` and `NewAuthorizationGrant` let an existing credential owner
supply native grants through `xrpc.GrantResolver` without another manager.

Bootstrap validation includes the shared corpus and actual HTTP/gRPC mTLS and
server-TLS calls with authenticated DNS plus injected numeric dialing, missing
client certificate/wrong SAN/duplicate authorization rejection, immutable
credential snapshots, FIFO/permission/link/size failures, and expired grant
callbacks retaining actual work/lease ownership. These checks do not establish
end-to-end product acceptance or sustained CPU/cache/allocation/RSS/latency
performance; controlled heavy workloads remain separately measured.
