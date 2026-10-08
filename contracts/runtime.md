# XGC2 XRPC runtime contract

XRPC supplies transport and host libraries. It does not run a central daemon,
start a provider on an invocation, discover services by querying a workflow
database, or implement domain operations.

Runtime environment, configuration delivery, persistence, observability and
administrative ownership follow [operations.md](operations.md). The shared
environment namespace is `XGC2_XRPC_`; domain configuration is separate.

## Choose the actual service boundary

A product directory, process or small function does not by itself require an
RPC listener. Establish its actual functions, callers and useful isolation or
remote invocation before choosing one of these forms:

| Form | Ownership and invocation |
| --- | --- |
| Independent service | A justified process boundary owns its endpoint, readiness and drain; callers use XRPC for its necessary service capabilities |
| Module in an existing service host | The host shares one endpoint, execution/admission and lifecycle; modules expose domain functions through that host without their own listener, probe or ServiceRef |
| Library or pure data path | Call domain functions directly or use the native data interface; no listener or service bootstrap is required |

Simple modules may belong in an existing aggregate host. A data relay or
device-facing process may retain its necessary data transport without gaining
a separate RPC control plane. When service invocation is needed, use the shared
SDK mechanisms; do not infer that need from a protocol or directory inventory.
Record the chosen boundary and its callers/benefit in the owning product.

## Profiles

`http.v1` uses HTTP/1.1 over a private Unix stream or authenticated HTTPS.
Named domain routes use JSON for structured values. Binary resources use HTTP
content types and streaming; multipart/mixed carries typed metadata plus
binary parts when both belong to one immutable capture. HTTP parsers and
serializers come from maintained language libraries. First-party local RPC
must not introduce separate JSON-line or private binary framing.

`grpc.v1` uses typed Protobuf methods and native gRPC unary/stream semantics.
Private Unix endpoints and authenticated remote channels share the endpoint
ownership contract. HTTP-only applications do not link gRPC.

Browser APIs, MCP, provider stdio, discovery, ROS and hardware protocols are
explicit edge adapters. Their public or device wire contracts remain with
their owning product. First-party domain operations exposed through those
adapters still use the common host and invocation primitives where applicable.
Platform-owned control calls use XRPC even when an existing implementation
uses ROS services or command topics. ROS remains an algorithm/user-facing or
device integration interface (for example MAVROS flight controller services),
not the platform's internal service bus. ROS/Zenoh/DDS/RTP telemetry, real-time
data loops, C ABI and shared memory remain data/implementation boundaries.
Functions within the same host can call their domain implementation directly.

## Service references and lifecycle

A service reference identifies `target_id`, `service`, `api_version`,
`instance_id`, `profile`, and `endpoint` (`kind`, `address`). The service/process
owner returns this reference. An invocation consumes it; it never contains a
process definition or bootstrap settings. SDKs have no workflow, SQL, process
supervisor or product-domain dependencies. Remote dialing is injected by the
Agent transport owner; a remote Unix pathname is not a local dial target.

Internal service references have a nonempty instance_id. HTTP clients send
X-Xrpc-Instance-ID and verify the same response header. A host configured with
an instance rejects a missing or different instance with conflict (409),
before domain dispatch. Public edge APIs and explicit discovery may omit
instance binding; they must not be used as an internal invocation fallback.

Unix ownership is one exclusive lifetime lease. The lease rejects non-socket
paths and symlinks, retains an advisory owner lock, records the bound inode,
and only removes that inode at close. The default existing-path policy is
fail; reclaim-unreachable is explicit and requires a bounded failed connect
and an unchanged inode. Socket mode defaults to 0600 and is configurable.
Leases release on exceptions. Close/stop are idempotent. gRPC external bind
uses reserve, bind, record-bound while retaining the same lease.

All languages use the same sibling lock name: `<socket-path>.xrpc.lock` and
Linux `flock(LOCK_EX|LOCK_NB)`, acquired before any existing socket probe or
bind. The runtime directory is owned by the effective user and mode 0700.
The lock is an owned regular 0600 file with one hard link and is not unlinked
at unlock. Directory/path checks and bind must refer to the same retained
directory. A binding failure releases only resources owned by that attempt.
Stopping acceptance is not termination of admitted handlers: the endpoint
lease remains held until all owned work is quiescent. A shutdown deadline can
report failed quiescence; it cannot release the lease while old work continues.

No implicit global umask changes, generic process launches, domain membership,
workflow recovery state or registry service are part of the SDK.

## Calls and resources

Every call has a finite caller deadline; transports include connect, write,
read and bounded cancellation in that budget. HTTP passes remaining budget
in `X-Xrpc-Timeout-Ms` and correlates requests through `X-Request-ID`.
Server limits constrain header/body size, admitted connections, in-flight
calls, idle time and handler dispatch. Limits are options of the host, not
new robot or algorithm limits. Persistent connections are supported when
bounded by idle time; clients reuse their transport rather than create one
per call. A product may close a connection, but one-call-per-connection is not
a compatibility requirement.

The wire contract is shared across languages, including negative cases:

- Internal requests contain exactly one X-Xrpc-Timeout-Ms in canonical ASCII
  decimal form, 1 through 86400000 milliseconds inclusive (no sign, leading
  zero, fraction or exponent), and exactly one X-Request-ID; discovery remains subject to
  these limits. IDs are 1–128 ASCII letters, digits, `.`, `_`, `:` or `-`.
- Exactly one X-Xrpc-Instance-ID is required on bound calls and verified on
  their responses. Explicit discovery permits a missing instance header only
  on configured GET routes; a supplied empty, duplicate or mismatched instance
  is still rejected. Empty header values are not missing headers.
- Standard GET/HEAD requests may have no body or Content-Length. JSON parsing
  applies only when a route requires a JSON body. Correct HEAD responses have
  no body even when Content-Length describes the corresponding GET resource.
- HTTP framing belongs to the maintained parser. Rejection before reading a
  body either safely drains that body's framing or closes the connection;
  rejected body bytes must never become another request.
- Every client permits the caller to supply a request ID. A generated default
  must be unique across independently created clients and service restarts.
  An SDK-local counter alone is not sufficient for operation deduplication.
- Caller budget includes queue/pool/lock wait and connection setup. Idle time
  before the next request does not consume the next call's budget. Timeout
  metadata can shorten a host limit; a longer caller budget does not make an
  otherwise valid request malformed.

For `grpc.v1`, the SDK owns lowercase `x-request-id` and
`x-xrpc-instance-id` metadata, alongside native gRPC deadline/framing. Request
IDs use the same 1–128 ASCII token grammar above. Missing, duplicate, empty or
invalid request IDs return `INVALID_ARGUMENT`. Duplicate instance metadata
returns `INVALID_ARGUMENT`; a bound call with a missing, single empty or
mismatched instance returns `FAILED_PRECONDITION`. Every rejection precedes
domain dispatch. A declared unary discovery method alone may omit the instance
key; a supplied value is still bound exactly, and discovery is never a fallback
for a rejected invocation or a streaming mode.

Admitted responses echo exactly one matching request ID and the actual hosting
instance. Streaming hosts publish those initial metadata before blocking on
payload I/O, and clients verify them before using stream payloads. A successful
response with absent, duplicate or mismatched fence/correlation metadata fails
with `FAILED_PRECONDITION`. A native transport rejection with no response
metadata retains its native failure; it is not converted into a fictitious
successful response. Unbound discovery returns the same hosting incarnation in
its metadata and domain ServiceRef. There is no common business Protobuf schema;
generated methods and payloads remain with their domain owners. The metadata
corpus is [fixtures/grpc-wire.json](fixtures/grpc-wire.json); it does not claim
that each language has already passed the required native checks.

Internal gRPC streaming requires an actual caller deadline no later than the
host's maximum call budget. Reject an absent/longer deadline before dispatch;
substituting a shorter wrapper Context does not cancel the native stream's
blocked receive. Cancellation must reach the underlying transport. Unary
handlers may use a shorter effective budget, with admitted work still counted
until it really terminates.

Persistent native edge streams use an explicitly declared finite owner stream
lifetime and connection grace, independent of the short internal RPC budget.
Native authentication, peer epochs and domain stream semantics remain with
that edge. The shared host enforces the actual transport lifetime, including
blocked native I/O; a wrapper context or configurable aging grace alone cannot
extend it. This does not release ownership of domain work that has not ended.

Synchronous transport failures distinguish not-sent from outcome-unknown.
Mutation calls are not automatically replayed. A cancelled RPC does not imply
rollback. Domain responses distinguish accepted/queued/applied/completed as
appropriate; the SDK cannot declare physical completion. Long operations
have explicit domain operation references and event-based observation;
periodic receipt polling is not a generic recovery mechanism.

Infrastructure errors map to invalid_argument, not_found, conflict,
resource_exhausted, deadline_exceeded, cancelled, unavailable and internal.
HTTP uses status codes and structured error responses, gRPC uses its native
status codes. Unknown outcome is a client-side result property, not a claim
that the server rejected a request. Domain errors may extend this vocabulary.

Handlers own domain payloads and state. An async HTTP reply completes at most
once and may complete from another thread. Fixed host workers/event loops
must not multiply with robot members. Real-time loops do not accept sockets,
parse HTTP, await network results, or run transport retries; a bounded
handoff reports the actual domain acceptance point.

## Data-oriented implementation

Predictable memory use, lifetime safety and measured throughput/latency take
precedence over an object-per-member design. Hosts share fixed execution and
connection resources; adding a robot does not add a listener, worker pool or
transport client pool. Store hot homogeneous records contiguously and process
them in batches where the domain supports it. Stable handles with generations
identify records across removal/reuse; do not expose pointers into movable
storage. Separate cold configuration/diagnostics from hot control state.

Long-lived observation holds its bounded transport/observer allocation rather
than occupying an execution worker while idle. Hosts distinguish accepted
connections/streams and pending replies from runnable domain execution.
Registering an asynchronous observer may release its execution slot only after
that execution actually returns; cancellation cannot pretend blocked native
work or its memory has ended. Command overload rejects explicitly. A latest
telemetry value policy is chosen by that data path's semantics, never imposed
as a silent generic command drop policy.

Memory budgets include admitted connections, parser buffers, bodies, pending
replies, queued work, operation results and slow consumers. Queue admission
reserves its storage before reporting acceptance. Products publish limits and
overload behavior rather than allowing unbounded heap growth. Real-time paths
use preallocated records/handoffs and have no transport allocation or waiting.

Typed facades and RAII can express ownership without requiring fragmented
object graphs. Buffer sharing, pooling and zero-copy paths require explicit
ownership through cancellation and async completion; memory is not recycled
while a reader or write still owns it. Prefer a bounded copy over unsafe
aliasing. Measure allocation counts, peak resident/buffer memory, latency and
throughput at realistic concurrency, including overload and slow peers; a
class/struct choice or microbenchmark alone is not performance evidence. Report
allocation rate, RSS peak, actual native/SDK thread counts, tail latency and
saturation/recovery against the combined connection, parser, body, queued work,
execution, pending reply and slow-consumer budget. Fixed concurrency alone does
not establish completion of performance acceptance.

Choose compact AoS or SoA layouts from measured hot access patterns; neither
SoA nor ECS is a mandatory repository-wide representation. Batch compatible
work, separate hot data from cold metadata, and check shared counters for false
sharing and global pools for contention. Locate CPU cost before changing a
layout. Compare the same workload's cycles/instructions/cache misses where
hardware counters are available, together with throughput, p99, allocation and
RSS. Report unavailable counters explicitly; object/array names alone are not
evidence of cache benefit.

## Language packages

C++20: `cpp/`, public namespace `xgc2::xrpc`, CMake targets `XgcXrpc::unix`,
`XgcXrpc::http`, and optional gRPC support. Boost.Beast/Asio implements HTTP;
the Unix ownership primitive does not require Boost or ROS.

Go: `go/`, module `github.com/XGC-Team/xgc2-xrpc/go`; standard net/http and
native gRPC, without CGO. Python: `python/`, import `xgc2_xrpc`. Rust: `rust/`,
crate `xgc2-xrpc`. Node: `node/`, package `@xgc2/xrpc`, with maintained HTTP/WS
implementations. All share these semantics; products import the package
rather than copy its listener, parser, deadline or cleanup implementation.
