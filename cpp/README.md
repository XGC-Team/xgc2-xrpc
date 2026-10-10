# XRPC C++ SDK

Linux C++20 libraries for reusable transport and resource ownership. Domain
payloads, completion, configuration schemas and persistence remain with the
product. This package has no ROS, workflow, process-launch or database dependency.

Targets are `XgcXrpc::unix`, `XgcXrpc::policy`, `XgcXrpc::diagnostics`, `XgcXrpc::bootstrap`,
`XgcXrpc::http`, `XgcXrpc::json_http`, and optional `XgcXrpc::grpc`. HTTP-only builds do not find or
link gRPC. Installed consumers use `find_package(XgcXrpc REQUIRED)` and link the
target they consume; JSON-HTTP consumers request `COMPONENTS json_http`; gRPC consumers request
`COMPONENTS grpc`.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
cmake --install build --prefix /explicit/package/prefix
```

Boost >= 1.70 supplies Beast/Asio and cold policy JSON parsing. GNU builds
require GCC >= 11; CMake also checks actual C++20 standard-library facilities.
The Unix primitive itself has no Boost dependency. Optional gRPC requires a
native gRPC >= 1.16 package, Protobuf and its C++ generator for tests. The test
service/generated proto is not an installed product contract.

## JSON-HTTP function integration

The supported business profiles are JSON-HTTP and generated Protobuf/gRPC.
`JsonHttpClient` accepts/returns typed `JsonHttpRequest`/`JsonHttpResponse` values;
`JsonHttpHandler` adapts a function receiving `JsonHttpRequest` and
`JsonHttpReply` into the ordinary `HttpServer::Handler` signature. It owns no
listener, thread, router or work queue. Products decide where to mount their
functions and who owns the host; there is no independent/shared server mode.
Raw `http` remains available for binary payloads and mixed-content hosts.

```cpp
#include <xgc2/xrpc/json_http.hpp>
using namespace xgc2::xrpc;
JsonHttpHandler method([](JsonHttpRequest request, JsonHttpReply reply) {
  reply.complete(Json{{"accepted", request.body.has_value()}});
}, {limits.request_bytes, 32}, limits.response_bytes);
// Pass method to HttpServer, or call method(request, reply) in the product's
// existing dispatcher. That dispatcher owns its paths and domain methods.
```

The JSON profile uses nlohmann-json >=3.7, parses once on the IO owner, rejects
invalid syntax/UTF-8, duplicate keys and excess nesting, and preserves uint64
integers. Nonempty bodies require application/json. Missing bodies and JSON
null are distinct. Serialization streams into bounded storage and stops on
overflow, without a temporary unbounded dump. The resulting buffer is moved
into HTTP. Domain objects and business schema validation remain domain-owned.
Typed replies retain the original HTTP admission and lease; no lifecycle is
shortened by the adapter. The client reuses the original HTTP connection and
never retries. Invalid responses report OutcomeUnknown; requests rejected
before transport report NotSent. These are local Unix transports.

## Policy and HTTP ownership

The native application consumes its sole explicit `--bootstrap-input` argument
with `<xgc2/xrpc/bootstrap.hpp>` and `loadBootstrapInput(path)`. This validates
the shared binding/input schemas, pins private input and credential files,
and retains an immutable application JSON view for the product's own schema.
It does not search for a default path or read credential environment variables.
`BootstrapBinding` (also named `Binding`) produces a typed `ServiceRef` from
the owner's new instance ID after startup. Runtime/storage grants remain
purpose-tagged opaque handles, separate from that public reference and policy.

The existing process/managed-file owner supplies `OwnerGrantResolver`, mapping
only its issued handles to `DirectoryGrant::from_owned_directory(fd, purpose)`.
`resolve_runtime` and `resolve_storage` resolve once. Runtime verifies the
private directory and its identity against the Unix endpoint's parent;
storage retains its directory descriptor and never removes persistent content.
Pass a runtime grant's caller-owned `duplicate_fd()` to the five-argument
`HttpServer(options, handler, limits, identity, parent_fd)` overload and close
that duplicate with the caller's normal RAII owner after construction. The
host duplicates it internally and retains its lease through actual work.
Do not reopen a pathname after validating a directory grant.

Bootstrap has a private OpenSSL Crypto >=1.1.1 build dependency for PEM/key,
trust and bearer validation. Installed public headers expose only standard
C++ types; pure policy/unix and gRPC component selection do not import the
bootstrap component. HTTP imports bootstrap automatically. Validated TLS
material is consumed only through the explicit native credential callback;
the existing native transport still applies peer checks and authorization.
This addition does not implement a remote HTTP transport.

The composition root supplies one explicit startup environment snapshot to
`resolve_runtime_policy`. Its generated registry is owned by the common
generator, with SDK defaults < deployment defaults < startup environment.
Unknown reserved keys, empty/noncanonical integers, unsupported explicitly
requested capabilities and violated ceilings fail. Effective snapshots expose
value, source, mutability, ceiling and revision without unrelated environment
contents. The resolver never reads `getenv` or watches the environment.

```cpp
xgc2::xrpc::RuntimePolicyOptions input;
input.environment = startup_environment_snapshot;
input.capabilities.push_back("diagnostics");
const auto policy = xgc2::xrpc::resolve_runtime_policy(input);
xgc2::xrpc::Diagnostics diagnostics(policy);
auto limits = xgc2::xrpc::http_limits(policy);
xgc2::xrpc::HttpServer host(unix_options, bounded_handler, limits,
    {owner_generated_instance_id, {"/v1/describe"}});
host.set_diagnostics(&diagnostics, "product.service");
```

HTTP policy maps connection/in-flight caps, header/request/response limits,
separate header/call/idle budgets and shutdown budget. `HttpLimits` defaults
also come from the generated registry. Host/client options remain explicit;
an absolute deadline passed to `HttpClient::call` includes admission, connection,
send and receive. Client calls serialize on one reusable local Unix connection;
the caller owns its calling threads. Host connections/in-flight/shutdown fields
do not create a client pool. Client-pool and reference-registry environment
capabilities are not implemented and explicit requests fail startup.

`HttpServer` has one owner for run/poll/drain/destruction. Handlers must hand
off bounded work without blocking that IO owner. Cross-thread reply completion
is at most once. **Every retained `HttpReply` copy reserves admission and the
endpoint lease until the copy is released**, including after timeout, peer
disconnect, stop or host destruction. A worker releases its reply when actual
business work ends. Cancellation is advisory and does not mean rollback.

`drain_until` stops new admission and allows admitted replies to finish within
the owner deadline. An expired drain cancels transport and returns false while
unquiescent work retains the lease. `drain()` uses resolved shutdown timeout;
`request_stop()` requests immediate transport cancellation. A new incarnation
cannot acquire the lease while old work remains. After quiescence terminal
lease cleanup removes only its recorded inode and releases flock; it cannot
bind again. Private parent/lock ownership, anchored bind/probe/cleanup and
replacement-inode checks also cover a renamed/replaced parent directory.
Listener setup binds, records and protects its socket inside a private child
of that retained directory, then publishes with no-overwrite `linkat`.
Competing public nodes cannot be adopted as the listener's recorded inode.

Internal calls bind an incarnation, canonical finite timeout and caller request
ID. Explicit discovery permits an absent instance only on configured GET
routes; empty/duplicate/mismatched fields still fail. Responses verify both
instance and request ID. Default IDs use OS randomness across client/process
instances. HTTP parsing/serialization belongs to Beast; rejected body framing
is closed before another request can be interpreted. HEAD is header-only.

`HttpClient::close` is terminal, cancels the active call and rejects waiting or
future calls. Join all calling threads before destruction. Transport failures
distinguish `NotSent` from conservative `OutcomeUnknown`; mutations are never
automatically replayed. `HttpStats` is a maintained bounded snapshot with source
time, stopping state, active/admitted/rejected counts and timeout/cancel facts.
It performs no probes. Caller-owned domain bodies/queues and parser buffers
must be included in product budgets; a finite parser limit is not a universal
RSS guarantee or a preallocation guarantee.

## Diagnostics and gRPC

Optional shared diagnostics enqueue preallocated fixed records, with no
producer allocation, wait, file write, thread or automatic administrative
endpoint. Schema excludes payloads, paths, headers and arbitrary error text.
One owner drains records to its declared sink and owns collection/rotation.
See [diagnostics API and limits](README-diagnostics.md) for saturation, rate
limits and authorized revision-checked severity changes.

Native typed unary/streaming, pre-read response fences, fixed application work
slots, native cancellation, conservative stream-budget encoding and graceful
versus forced shutdown are documented in [gRPC integration](README-grpc.md).
Remote gRPC builders/channels accept explicitly supplied native credentials;
the remote owner supplies authentication, routing, connection admission and
teardown. The HTTP implementation here is the local UDS host/client; an
authenticated remote HTTP transport is not implemented by this package.

## Verification and deployment scope

Tests use real Unix HTTP and native gRPC, the actual common wire/environment
corpora, allocation-rejected diagnostic producers, retained work after cancel,
owner destruction, pause/crash/reclaim/restart fences, header/body rejection,
slow stream cancellation, inode replacement and resource samples. gRPC native
global threads and pressure quotas are explicitly distinguished from hard
application bounds. Tests establish measured conditions; they do not prove
arbitrary domain memory or real robot behavior.

The cached Focal build rootfs with **clang++-10 plus libstdc++ 10.5** builds this
C++20 HTTP/policy/diagnostics SDK; matching Focal runtime loads and executes it.
GCC9 does not satisfy the supported GNU baseline. Noble-built binaries are not
claimed to be Focal-compatible. Focal's cached gRPC 1.16.1 is not a supported
gRPC dependency: a maintained rootfs-matched gRPC/Protobuf distribution or an
approved newer deployment rootfs is still required. No PPA or live station
change is part of these SDK checks.
