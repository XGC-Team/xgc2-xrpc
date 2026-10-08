# Native C++ gRPC integration

The optional `XgcXrpc::grpc` target uses Linux, C++20 and native gRPC >= 1.51.
Build with `-DXGC2_XRPC_ENABLE_GRPC=ON`; installed consumers request
`find_package(XgcXrpc REQUIRED COMPONENTS grpc)`. HTTP-only consumers retain
their ordinary Unix/HTTP targets without a gRPC link dependency. Product
Protobuf methods, generated services/stubs, payload validation and domain
completion semantics remain with the product. The test proto is not installed.

## Service integration

Create one `GrpcAdmission` per host incarnation, then give it to every generated
synchronous service implementation. Call `begin(*context)` for unary or
`begin_stream(*context, *native_stream)` for streaming before
domain effects, return its native status on rejection, and keep the returned
scope alive until that method exits. `begin_stream` publishes initial identity
metadata before the method blocks reading, so a client can fence its stream
before sending/receiving payloads. An absent caller deadline always fails; a
stream deadline beyond the host maximum also fails before domain dispatch.
This checks the **actual native context**: its deadline/cancellation wakes
blocked native `Read`/`Write`, rather than only shortening a wrapper timer.
For a client using the same host maximum, use
`grpc_stream_deadline(limits, caller_deadline)`: native timeout encoding rounds
up, so an exact `now + host_maximum` can otherwise be rejected. The helper
reserves one percent plus 3 ms for gRPC 1.51's representation rounding and keeps
the earlier caller deadline. A host budget too small for that positive margin
(including 1 ms) fails locally. This margin handles native representation;
it does not establish a bound on scheduling pauses or clock adjustments.

```cpp
grpc::Status MyService::Apply(grpc::ServerContext* context,
                              const Request* request, Reply* reply) {
  auto call = admission.begin(*context);
  if (!call) return call.status();
  // Domain owns validation, bounded work and the acceptance/completion result.
  return apply(*request, *reply, call.deadline());
}

xgc2::xrpc::GrpcAdmission admission(owner_generated_instance_id, limits);
MyService service(admission);
xgc2::xrpc::GrpcUnixServer host(unix_options, admission, {&service});
```

Each bound call requires exactly one valid `x-xrpc-instance-id` and
`x-request-id`; IDs use the shared 1–128 ASCII identifier grammar. Replies echo
both. Missing, empty or mismatched instance metadata returns
`FAILED_PRECONDITION`; duplicate instance metadata and invalid request identity
return `INVALID_ARGUMENT`.
An explicitly declared unary discovery method may call
`begin(*context, false, true)`. It accepts a completely absent instance key,
returns the actual host incarnation in initial metadata, and retains request
identity, native deadline, admission and stopping checks. Any supplied empty,
duplicate, invalid or different instance key still fails. Discovery cannot be
used for streaming; ordinary methods always retain their required fence. An
owner's discovery response must return a ServiceRef for that same incarnation.
An unbound discovery client explicitly passes `discovery=true` to
`GrpcClientCall`. An empty instance argument then omits the metadata key;
it never sends an empty instance field. A nonempty argument still binds to
that exact incarnation. Native deadline and request correlation stay required.
After successful `invoke`, compare the typed ServiceRef's instance to
`response_instance_id()` before storing/using it. The getter is empty on
failed unary calls, including malformed metadata or native rejection. Streaming
metadata helpers reject discovery and cancel before waiting for initial
metadata. That local mode rejection remains in `mark_dispatched` and `verify`,
even if a completed native call reports success after best-effort cancellation.
Do not retry a rejected internal call as discovery.

```cpp
grpc::ClientContext describe_context;
GrpcClientCall describe(describe_context, "", GrpcClock::now() + 2s,
                        {}, "owner.describe-1", true);
auto status = describe.invoke([&] {
  return stub->Describe(&describe_context, request, &reply);
});
if (status.ok()) {
  // The domain also validates the typed reference's service/profile/endpoint.
  if (reply.service_ref().instance_id() != describe.response_instance_id())
    throw std::runtime_error("discovery reference incarnation mismatch");
}
```

The scope may hand off **one** move-only `GrpcWorkPermit` to owned domain work.
Reserve the domain queue first. Admission remains occupied after native method
return until this permit is destroyed. Its `cancelled()` reports host stop,
known native cancellation or elapsed effective budget. Completing the native
RPC does not cancel durable domain operations or establish physical completion.
A unary host budget may shorten its effective work deadline; handlers must
observe that deadline. Scope must die before `ServerContext`; permits do not
keep or dereference a destroyed native context. Do not spawn detached work
without a retained permit and an owned shutdown/join path.

`configure_grpc_server(builder, limits)` supplies the same native limits to an
authenticated remote server. The remote owner supplies credentials, route,
connection admission and teardown. `GrpcAdmission` is also reusable there.
The helper covers synchronous generated services; raw async CQ handlers need
separate completion-tag ownership and are not admitted through this API.

## Client integration

Reuse `make_grpc_unix_channel(path, limits)` and generated stubs for a local
ServiceRef. This helper accepts only an absolute local Unix path. A remote
owner creates its authenticated channel using `grpc_channel_arguments(limits)`;
it must not send a remote Unix pathname to the local dialer.

`grpc_client_limits(policy)` projects a client policy with `rpc` and `transport`
capabilities without requiring `host`. It maps message sizes, finite caller
budget and native client idle time; use its call budget when creating each
`GrpcClientCall` or paired stream deadline. Optional header limits also map when
present. Client idle time below the documented native minimum of 1,000 ms
fails. The channel actually sets `grpc.client_idle_timeout_ms`.
Both native client and server reject `IDLE_TIMEOUT_MS=2147483647`: native
gRPC interprets that integer sentinel as unlimited, so accepted finite values
stop at 2147483646. These boundaries follow the
[native channel arguments](https://raw.githubusercontent.com/grpc/grpc/v1.51.1/include/grpc/impl/codegen/grpc_types.h).
Host policy fields in a composed process remain with their host owner.

`GRPC_MAX_STREAMS_PER_CONNECTION` controls incoming server streams; it does not
cap outgoing native client calls. An explicit override or ceiling therefore
fails the default client projection. A client owner that counts and bounds
**all** concurrent calls/streams may declare that field with
`grpc_client_limits(policy, {"GRPC_MAX_STREAMS_PER_CONNECTION"})`, check its fixed
population against the returned limit, and enforce the bound before opening
native calls. Merely declaring the field does not create a semaphore or native
client cap. A paired-stream owner must include overlapping registration,
replacement and other calls in this accounting. Client pools/reference
registries retain their separate owners.

For an owner's reverse Register/Control/Work connection, validate its typed
ServiceRef first, reuse one channel, and take the same reference incarnation
for every bound call's instance metadata. Register is an ordinary unary
`invoke`; Control and Work each use `mark_dispatched`, then
`receive_initial_metadata` before payload. Keep both native contexts and call
scopes alive through their reader/writer joins and `Finish`. A 30-second
ceiling is finite at the native layer: use the earlier caller/policy budget
for Register and `grpc_stream_deadline(limits, GrpcClock::now() + 30s)` for
each stream. The stream helper also reserves native timeout-rounding margin,
so the actual deadline is slightly earlier than that ceiling. Session renewal
is domain-owned and must respect the fixed population even while replacing
old streams. The SDK does not import the owner's Bootstrap proto or validate
its ServiceRef service/profile fields.

```cpp
grpc::ClientContext context;
xgc2::xrpc::GrpcClientCall call(context, service_ref.instance_id,
    std::chrono::steady_clock::now() + std::chrono::seconds(2), stop_token,
    caller_request_id);
auto status = call.invoke([&] { return stub->Apply(&context, request, &reply); });
```

For streams, call `mark_dispatched()` and check its status before opening the
native stream. Then call `receive_initial_metadata(*stream)` and check its
status before the first native `Read` or `Write`. A bad fence cancels the native
context; complete `Finish` and ignore its payload. After `Finish`, use
`verify(status)`. Unary callers also ignore output whenever returned status
fails. The call scope remains
alive through stream completion. The context must be fresh, with no duplicate
XRPC metadata supplied by the caller. An omitted request ID is generated with
the Unix library's random incarnation generator, independently of local
client counters. The finite caller deadline includes setup/native queue wait.
`std::stop_token` installs a synchronized callback calling native `TryCancel`.
Destroy the call scope before destroying its context.

`delivery()` is `NotSent` for pre-dispatch validation or cancellation. After
`mark_dispatched`/`invoke` it is conservatively `OutcomeUnknown`, including
connection failures: the native API does not expose exact transmitted bytes
or domain effects. Never infer rollback from cancellation. Native retries are
disabled on helper-created channels; the SDK does not replay mutations.
`verify` rejects malformed/mismatched successful response identity and retains
native transport failures when no response metadata arrived.

## Ownership, shutdown and resource limits

`GrpcUnixServer` binds via `UnixPathLease::bind_stream`: a random private child
of the retained directory owns the socket during bind, inode recording,
permission setup and listen. `linkat` then publishes that inode at the public
name without overwriting any existing entry. A competing public socket, file
or symlink causes publication to fail and survives cleanup. One fixed
poll/accept/shutdown thread handles all local peers and
hands accepted descriptors to native `grpc::AddInsecureChannelFromFd`. gRPC
owns connected descriptors, never the pathname. This avoids native
`AddListeningPort(unix:...)`, whose gRPC 1.51 implementation unlinks by pathname
both before bind and on teardown without checking the leased inode. Replacement
files/sockets and replaced parent directories survive SDK teardown.

The preallocated connection table admits at most `limits.connections` live
peers. Each admitted connection has one duplicate socket descriptor to observe
native/peer shutdown; a terminal event shuts down and closes the monitor before
releasing that connection slot. A duplicate retains the underlying socket until
that event, so a native close that did not shutdown the socket could conservatively
retain a slot until peer disconnect. Native idle/stalled-preface teardown was
tested on 1.51.1 and wakes the monitor. Disconnect may precede asynchronous
native transport destruction; the table does not prove an instantaneous bound
on all gRPC internal objects during connection churn.

One native server CQ and fixed poller settings are configured.
`ResourceQuota.SetMaxThreads(native_threads)` additionally bounds the native
synchronous handler pool: `MAX_POLLERS` alone limits idle pollers, not running
handlers. gRPC global timer/executor threads are outside this per-server quota;
neither `native_threads` nor the SDK's single accept thread is a process-wide
thread count. Overload can be rejected by native gRPC before application
admission. Native message-size, metadata, streams-per-connection, connection
idle limits are configured, and application call/work storage
uses fixed admission slots. gRPC parses metadata and decodes unary messages
before `begin`; these native buffers are included in the transport budget.
The remote builder also configures native handshake timeout. Accepted-fd local
channels bypass that native handshake stage; their stalled-preface cleanup uses
the native transport idle timeout, as verified in the test.
`ResourceQuota.Resize(native_memory_bytes)` is a native pressure target, **not a
hard total RSS/allocation ceiling**. Products must measure their payload/queue
and slow-consumer resource behavior; the helper does not bound domain queues.

Every live synchronous stream occupies one admission slot and one native
handler while blocked in `Read`/`Write`. With this one-poller configuration,
reserve long-stream count L and short-call concurrency U such that
`L + U <= min(limits.inflight, limits.native_threads - 1)`. The default pool of
eight threads permits at most seven simultaneous blocked handlers; an eighth
stream or unary call can fail with native `RESOURCE_EXHAUSTED` before application
admission even when a stream/inflight limit is 32. All generated services on a
shared host must use the **same** `GrpcAdmission` passed to `GrpcUnixServer`;
separate admissions would escape its lease/quiescence accounting. Multiplex
members over a fixed number of domain streams, or size a fixed pool and reserve
short-call room. Async/CQ/callback long-stream admission is not implemented by
this synchronous API. This server handler formula does not apply to a reverse
C++ client connected to another language's host; that owner must separately
bound its own reader/writer workers and queues.

`request_stop` stops application admission, wakes the owner and cancels native
contexts. `shutdown_until` performs forced shutdown and waits for native handlers and retained work until
the supplied steady deadline. False means failed quiescence and preserves the
lease; retry after work exits. True joins the owner, destroys the native server,
cleans only the recorded socket and releases the flock. `shutdown()` uses the
resolved shutdown budget. `drain_until` stops new admission/acceptance and allows
admitted native calls to complete until its deadline before cancelling the
remainder. Its outer wait also returns false at failed quiescence while keeping
the lease. Destruction waits for actual quiescence: a handler
that ignores cancellation or a never-released permit can block it. A timeout
does not make releasing a live endpoint lease safe.

`GrpcLimits` common defaults come from the generated shared registry through
`RuntimePolicy`. `grpc_limits(policy)` maps host, RPC, transport and gRPC fields;
optional selected `MAX_HEADER_BYTES` maps to native metadata limits. Default
policy resolution should select `host`, `rpc`, `transport`, `grpc`; include `http` only when
selecting that optional metadata setting. A standalone gRPC policy rejects
HTTP-only overrides during resolution. In a composed process policy, HTTP and
diagnostics fields belong to their declared owners; this adapter checks applied
fields only for `host`, `rpc`, `transport` and `grpc`. Explicit unenforced fields
in those owned capabilities fail via `check_applied`. Client-pool/registry
ownership remains separate. `native_threads` and
`native_memory_bytes` are explicit immutable bootstrap limits without new env
aliases. Keep the resolved policy for effective-source inspection.

An optional owner-supplied `Diagnostics` sink can be installed on admission with
`set_diagnostics` before serving. It must outlive the host and retained work.
Admission, actual completion, cancellation, rejected admission and drain
transitions publish bounded identity/elapsed facts using nonblocking `try_emit`.
The host creates no global sink, file or diagnostics worker and logs no domain
payloads. Native rejections before application admission remain native facts,
rather than being invented as application counters.

## Verification

`xrpc_cpp_grpc` uses real Unix native generated unary, client/server/bidi streams.
It covers request/response fencing, absent/long stream deadlines, native blocked
receive cancellation/deadline, message caps, canceled noncooperative handlers,
retained work, bounded failed shutdown and lease reacquisition, replacement
socket/parent cleanup, connection overload, idle preface teardown, native thread
quota, policy mapping, and 1,100 calls on one reused channel with stable FD count.
Tests also cover graceful admitted-result preservation, conservative matching
stream budgets, cross-thread stop during owner close, and injected redacted
lifecycle diagnostics.
The shared-host test registers multiple generated services on one listener and
channel, opens two server-first bidirectional streams, checks both initial
fences before payload, shares global admission with unary calls, and releases
slots through native cancellation/deadline. Discovery and client policy tests
cover the explicit unary exemption and rejection of unenforced client settings.
These are transport evidence, not robot-domain or deployment ABI acceptance.

## Dependency toolchain and component packaging

The validated native build is Ubuntu Noble amd64, GCC 13.3, C++20,
Boost 1.83, gRPC 1.51.1 (`1.51.1-4.1build5`) and Protobuf 3.21.12.
The isolated dependency prefix used for verification is
`/tmp/sol10-grpc-prefix/usr`; it was extracted from official distribution
packages. Builds require its CMake prefix and native library search paths;
runtime scratch consumers need its native library path as well. No host
package installation or running-container mutation was required.
The [baseline handoff](validation/2026-10-09-sol10.md) records the exact
configuration and sanitizer/deployment boundaries.

Focal's cached Clang10/libstdc++10.5 route is proven for HTTP/policy/diagnostics,
including installed consumption on the Focal runtime. Its cached gRPC1.16.1
does not meet this SDK's >=1.51 requirement. Focal gRPC needs a maintained
rootfs-matched gRPC/Protobuf distribution or an approved newer deployment
rootfs; Noble native binaries are not asserted to load on Focal.

An explicit installed `COMPONENTS grpc` imports unix/policy/diagnostics and
native gRPC dependencies, with no HTTP target/library requirement. An explicit
`COMPONENTS policy` requires only the policy component. Use those component
declarations for a reduced installation prefix. Calling `find_package` without
components imports the basic unix/policy/diagnostics/http set. HTTP consumers
do not search/link native gRPC unless they request its component.

Implementation references: [native accepted-fd API](https://raw.githubusercontent.com/grpc/grpc/v1.51.1/include/grpcpp/server_posix.h),
[native resource quota](https://raw.githubusercontent.com/grpc/grpc/v1.51.1/include/grpcpp/resource_quota.h),
[sync thread manager](https://raw.githubusercontent.com/grpc/grpc/v1.51.1/src/cpp/thread_manager/thread_manager.cc),
[timeout encoding](https://raw.githubusercontent.com/grpc/grpc/v1.51.1/src/core/lib/transport/timeout_encoding.cc),
[native Unix teardown](https://raw.githubusercontent.com/grpc/grpc/v1.51.1/src/core/lib/iomgr/tcp_server_posix.cc).
