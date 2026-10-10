# XRPC C++ SDK

Linux C++17 libraries for reusable transport and resource ownership. Domain
payloads, completion, configuration schemas and persistence remain with the
product. This package has no ROS, workflow, process-launch or database dependency.

## Components

`XGC2_XRPC_COMPONENTS` selects what CMake builds, installs and exports. A
component pulls in the ones it links against, and only the third-party packages of
the selected components are searched for.

| Component | Target | Needs | Compiler |
| --- | --- | --- | --- |
| `udp` | `XgcXrpc::udp` | OpenSSL libcrypto only | GNU >= 7.5, clang >= 10 |
| `unix` | `XgcXrpc::unix` | POSIX | GNU >= 9, clang >= 10 |
| `diagnostics` | `XgcXrpc::diagnostics` | standard library | GNU >= 9, clang >= 10 |
| `bootstrap` | `XgcXrpc::bootstrap` | OpenSSL libcrypto, `unix` | GNU >= 9, clang >= 10 |
| `http` | `XgcXrpc::http` | Boost >= 1.70 (Asio, Beast), `unix`, `diagnostics`, `bootstrap` | GNU >= 9, clang >= 10 |
| `json_http` | `XgcXrpc::json_http` | nlohmann-json >= 3.7, `http` | GNU >= 9, clang >= 10 |
| `grpc` | `XgcXrpc::grpc` | gRPC++ >= 1.16, Protobuf, pkg-config, `unix`, `diagnostics` | GNU >= 9, clang >= 10 |

The default is every component except `grpc`, which needs the gRPC development
packages. `-DXGC2_XRPC_COMPONENTS=udp` builds the udp.v1 library without Boost,
nlohmann-json or gRPC being present. The Beast-based components need Boost >= 1.70,
which Ubuntu 18.04 (Bionic) does not ship, so Bionic gets `udp` only; focal, jammy and
noble get everything (see `packaging/runtime-abi.json`).

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release                            # default components
cmake -S . -B build -DXGC2_XRPC_COMPONENTS="http;json_http;grpc;udp"       # an explicit set
cmake -S . -B build -DXGC2_XRPC_COMPONENTS=udp                             # udp.v1 alone
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
cmake --install build --prefix /explicit/package/prefix
```

Headers and CMake exports are installed per component, so a reduced installation
contains only what it was built with. Installed consumers use
`find_package(XgcXrpc REQUIRED COMPONENTS <names>)` and link `XgcXrpc::<name>`.
Without `COMPONENTS` they get what the installation provides of `unix`,
`diagnostics`, `bootstrap`, `http` and `udp`; `json_http` and `grpc` are always
requested by name. A sample installed consumer lives in `packaging/probes/cpp`.

The code is C++17 throughout (`CMAKE_CXX_STANDARD 17`, no compiler extensions) and
builds warning-free with `-Wall -Wextra -Wpedantic`. The suite passes with the
system g++ 9.4 and with clang++-10, each in its own build directory, and with g++ 9.4
under AddressSanitizer and UBSan. `udp` additionally builds and passes with g++ 7.5
against the Ubuntu 18.04 sysroot, see [Bionic](#bionic-build-and-run-of-udp).

## udp.v1

`xgc2/xrpc/udp.hpp` and `libxgc2_xrpc_udp` implement the udp.v1 profile of the XRPC
design record: one JSON request and one reply per UDP datagram, authenticated with
HMAC-SHA256, for low-rate control calls to robots on a weak LAN without long-lived
connections. The library depends only on the C++17 standard library, POSIX sockets
and OpenSSL libcrypto (`HMAC`, `RAND_bytes`, `CRYPTO_memcmp`); bodies are bytes, so
there is no JSON library either. Namespace `xgc2::xrpc::udp`; the header lists the
datagram layout.

```cpp
#include <xgc2/xrpc/udp.hpp>
using namespace xgc2::xrpc::udp;

// Server: one socket, one I/O thread, handlers that may answer later.
KeyRing keys = KeyRing::load_file("/etc/xgc2/hold.keys");
ServerOptions options;
options.bind_address = "0.0.0.0";           // or "::" for IPv6 and IPv4 senders
options.port = 19520;
options.instance = domain_instance_id;      // optional: reuse the domain's 128-bit identity
Server server(options, keys);
server.add_method("xgc2.chassis.hold.v2/Engage", [&](Request request, Reply reply) {
  // request.body is the JSON text. Hand slow work to another thread and complete the
  // Reply from there before request.deadline; this thread is the I/O thread.
  tick_queue.push([reply = std::move(reply)]() mutable { reply.complete(Status::Ok, "{\"held\":true}"); });
});
server.start();
// ...
server.shutdown(std::chrono::milliseconds(500));      // stop admitting, drain, close

// Client: blocking, stateless, safe from any number of threads.
Client client(keys);
const Response response = client.call("192.168.1.20:19520", /*key_id=*/7, "xgc2.chassis.hold.v2/Engage",
    "{}", std::chrono::steady_clock::now() + std::chrono::seconds(2), expected_instance /* optional pin */);
switch (response.delivery) {
case Delivery::ResponseReceived: /* response.status, response.body, response.instance */ break;
case Delivery::OutcomeUnknown:   /* sent, no valid reply by the deadline (or a stale pin) */ break;
case Delivery::NotSent:          /* nothing left this host; safe to retry */ break;
}
```

**Authentication.** A datagram with a bad layout, an unknown `key_id` or a bad tag gets
no reply (no oracle, no reflection) and is counted as `dropped_malformed` or
`dropped_auth`. Keys are exactly 32 bytes; `key_id` is a public label. `KeyRing::parse`
and `KeyRing::load_file` read `<key_id> <base64 key>` lines (decimal `key_id` in
0..4294967295, canonical standard base64 of 32 bytes, `#` comments, blank lines
allowed) and reject anything else, a duplicate id and keys of another length; the error
names the line and never quotes the key. The file should be readable only by the
service's user. Keys are wiped when the ring is destroyed. Base64 is implemented in the
library.

**At most once.** The server keeps a reply cache keyed by `(key_id, request_id)`
(`reply_cache_ttl` 120 s, `reply_cache_capacity` 1024). A retransmitted request whose id
is cached is answered with the cached datagram and the handler does not run again
(`cache_hits`); one whose handler is still running, or that ended without a reply, is
ignored (`inflight_ignored`). Beyond the window the transport cannot tell a replay from a
new call: non-idempotent methods must fence themselves with the instance and a domain
revision. Entries are evicted oldest first and running calls are never evicted
(`max_inflight` 64 must stay below the capacity).

**Deadlines.** The server deadline is the receipt time plus the request's `timeout_ms`
(1..60000), capped by `call_budget` (2 s). `Reply::complete` after that returns false
and sends nothing; so does a `Reply` that is destroyed without completing, and a
call whose deadline passes with its `Reply` still alive is ended by the I/O thread
(`unanswered`), which frees its in-flight slot. The client then sees an unknown outcome.
A handler that throws before answering is answered `internal`.

**Client retransmission.** `Client::call` sends the same datagram at once and again
after 30, 60, 120 and 240 ms, then every 250 ms (`ClientOptions`), until a valid reply
arrives or the deadline passes; a deadline beyond 60 s is shortened to it. A reply is
valid only if its tag verifies with the call's key and it answers this request id. The
result's `delivery` is `NotSent` (no datagram could be sent: unusable arguments, a
request that does not fit 1200 bytes, no route), `OutcomeUnknown` (sent, no valid reply)
or `ResponseReceived`, with `status`, `body` and the server's `instance`. `endpoint` is a
numeric `host:port` (`[::1]:19520` for IPv6); names are not resolved because the resolver
cannot honor the deadline.

**Instance fence.** Every reply carries the server's 128-bit instance. A pinned call
(`expected_instance`) is answered `conflict` by a server of another instance without
running the request; the client returns that as `Conflict` with `OutcomeUnknown` and the
instance that answered, because the pinned instance may have run the request before it
went away (the same rule as the HTTP and gRPC clients). Other replies of a foreign
instance are ignored. `ServerOptions::instance` lets a host use its domain instance as the
transport instance; unset, one is generated per server. `Server::instance_hex()` is the
32-digit lowercase form.

**Size and rate.** A datagram is at most 1200 bytes; a reply that would exceed it becomes
`resource_exhausted` with a short error body, and a request that does not fit is
`NotSent` at the client. Authenticated requests are rate limited per source address with
a token bucket (`requests_per_second` 50, `burst` 100, at most `max_rate_sources` tracked
sources); excess datagrams are dropped (`dropped_rate`). Unauthenticated datagrams never
spend tokens.

**Addresses.** The server answers from its single socket and the kernel picks the reply's
source address by route. On a host with several addresses, bind to the address that
callers use: a client with a connected socket (the Go client's) ignores a reply that
comes from another address, whereas this client accepts any authentic reply. `"::"`
accepts IPv4 senders too; a specific IPv6 address does not. There is no `SO_REUSEADDR`:
a second server on the same port fails to bind, which keeps the endpoint exclusive.

**Statuses and errors.** `Status` is 0 ok, 1 invalid_argument, 2 not_found, 3 conflict,
4 resource_exhausted, 5 deadline_exceeded, 6 cancelled, 7 unavailable, 8 internal,
9 unauthenticated (never sent) and 10 permission_denied. Non-zero statuses carry
`{"code":"<name>","message":"...","details":{...}}`; `error_body` and `Reply::error`
build it with proper escaping. The transport itself answers authenticated requests with
`invalid_argument` (reserved flag bits, `timeout_ms` outside 1..60000), `conflict` (stale
pin), `not_found` (unknown method), `resource_exhausted` (in-flight limit) and
`unavailable` (draining).

**Lifecycle.** `Server` binds in its constructor, takes methods with `add_method` before
`start()`, and owns one I/O thread. `shutdown(budget)` stops admitting (new requests get
`unavailable`; cached replies are still resent), waits up to the budget for running
handlers to complete and send, then stops the thread and closes the socket; it returns
whether everything finished and is idempotent. The destructor shuts down at once. A
`Reply` may outlive its `Server`; completing it then returns false. `stats()` returns
`received`, `dropped_malformed`, `dropped_auth`, `dropped_rate`, `cache_hits`,
`inflight_ignored`, `unanswered`, `replies_sent` and `inflight`. Handlers run on the I/O
thread: they must not block, and must not call `shutdown` or destroy the server.

### Interop server and tests

`xgc2-xrpc-udp-interop-server` (built with the tests, from `tests/udp_interop_server.cpp`)
is the peer of the cross-language tests:

```
xgc2-xrpc-udp-interop-server --bind <address> --port <port|0> --key-file <path> --key-id <id>
```

It serves only the named key, prints `READY <port> <instance-hex>` on stdout once bound,
and runs until SIGINT or SIGTERM. `test.v1/Echo` returns the body; `test.v1/Count`
adds one to a process counter and returns it as a decimal number (a retransmitted request
never counts twice); `test.v1/Sleep` takes `{"ms":N}` and completes from another thread
after N ms, returning the body; `test.v1/Fail` takes `{"status":N}` and replies with
status N (1..8 or 10) and an error body, anything else is `invalid_argument`;
`test.v1/Big` tries to reply with 2000 bytes, which the transport turns into
`resource_exhausted`. The Go library's cross-process interop test passes against it
(loss, duplication and reordering through its fault proxy, instance pins, restarts), and
this client passes the same scenarios against the Go reference server.

| ctest | What it covers |
| --- | --- |
| `xrpc_cpp_udp_wire` | RFC 4231 HMAC vectors, RFC 4648 base64, golden datagrams computed with Python's `hmac`/`struct`, round trips, every malformed layout, per-byte tag binding, key ring parsing, error bodies |
| `xrpc_cpp_udp_server` | real sockets on 127.0.0.1: calls and statuses, duplicate datagrams and a running call, async completion from other threads, missed deadlines, dropped/throwing handlers, size limits, authentication drops, rate limits, pins, in-flight limit, cache TTL and capacity, client validation and retransmission timing, forged replies, request and reply loss through a lossy proxy, shutdown with work in flight, option rules, concurrent callers |
| `xrpc_cpp_udp_server_ipv6` | the same on `::1`; without IPv6 loopback the test builds a private one in a new user+network namespace, and is skipped (exit 77) if the system refuses that too |
| `xrpc_cpp_udp_interop` | the interop server as a separate process: command line, READY line, each method, key selection, SIGTERM |

### Bionic build and run of udp

The `udp` component is built for Ubuntu 18.04 (g++ 7.5, glibc 2.27, libstdc++ 7, OpenSSL
1.1.1). On a machine without Bionic, the repository's toolchain wrapper compiles with the
real Bionic g++ against a Bionic sysroot; OpenSSL comes from that sysroot.

```sh
T=/home/node/workspace/toolchains
cmake -S . -B build-bionic -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=$T/bin/bionic-gcc -DCMAKE_CXX_COMPILER=$T/bin/bionic-g++ \
  -DCMAKE_SYSROOT=$T/bionic-sysroot -DXGC2_XRPC_COMPONENTS=udp
cmake --build build-bionic
ctest --test-dir build-bionic --output-on-failure           # runs on the host's glibc

# The same binaries on the sysroot's own loader, glibc 2.27, libstdc++ 7 and OpenSSL:
S=$T/bionic-sysroot
RUN="$S/lib/x86_64-linux-gnu/ld-2.27.so --library-path $S/lib/x86_64-linux-gnu:$S/usr/lib/x86_64-linux-gnu:$PWD/build-bionic/cpp"
$RUN build-bionic/cpp/xrpc_cpp_udp_wire_test
$RUN build-bionic/cpp/xrpc_cpp_udp_server_test 127.0.0.1
$RUN build-bionic/cpp/xrpc_cpp_udp_server_test ::1
$RUN build-bionic/cpp/xrpc_cpp_udp_interop_test $PWD/build-bionic/cpp/xgc2-xrpc-udp-interop-server

# The binaries need no more than Bionic provides:
readelf --version-info build-bionic/cpp/libxgc2_xrpc_udp.so.0.1.0 | grep -oE '(GLIBC|GLIBCXX|CXXABI)_[0-9.]+' | sort -Vu
```

Result: all four tests pass both ways; the library requires `GLIBC_2.14`, `GLIBCXX_3.4.22`
and `CXXABI_1.3.9`, below the sysroot's 2.27, 3.4.25 and 1.3.11. A real Bionic host
(Jetson Nano, Xavier) should build the same way with its own `g++-7` and `libssl-dev` and
without the sysroot options; that was not run here.

## Language level and stop tokens

Everything is C++17. Where C++20 offered `std::stop_token` the SDK has its own
`xgc2::xrpc::StopSource`, `StopToken` and `StopCallback` (`xgc2/xrpc/stop.hpp`, header
only, installed with `unix`): same names and semantics, callbacks run once on the
requesting thread, deregistration waits for a callback running on another thread, and
`wait_until(condition, lock, token, deadline, predicate)` is the cancellable wait that
`condition_variable_any` provided. `HttpClient::call` and `GrpcClientCall` take a
`StopToken`. `std::span` and `starts_with` are not used; `BootstrapBinding::storage_grants()`
returns a vector and `BootstrapInput::authorize` takes one.

## Dispositions

Every client result says what the caller can know. `xgc2::xrpc::Delivery`
(`delivery.hpp`) is `NotSent` (nothing was sent; retrying is safe), `OutcomeUnknown` (bytes
may have reached the peer and no usable response arrived; never replayed by the SDK) or
`ResponseReceived`. The udp client has the same three values in `xgc2::xrpc::udp`. For
HTTP, `HttpClient::call` returns any received response, whatever its status; an
`HttpCallError` has `NotSent`, `OutcomeUnknown`, or `ResponseReceived` with
`resource_exhausted` when a complete header block announces a body above the client's
`response_bytes`. For gRPC, `GrpcClientCall::delivery()` is `ResponseReceived` after
`verify` for an OK response, or for a failure that carries the host's
`APPLICATION_ERROR` marker ([README-grpc.md](README-grpc.md)).

## Limits are plain structs

`HttpLimits`, `GrpcLimits` and `DiagnosticsOptions` are plain structs whose default member
values are the documented defaults; there is no environment-variable layer and no
registry. The composition root sets the fields it needs and passes the struct to
`HttpServer`, `HttpClient`, `GrpcAdmission`, `make_grpc_unix_channel` or `Diagnostics`.
`HttpLimits` defaults: 32 connections, 32 in flight, 16 KiB headers, 1 MiB request and
response bodies, 30 s request, 30 s idle, 5 s header and 5 s shutdown budgets.

## JSON-HTTP function integration

The supported business profiles are JSON-HTTP and generated Protobuf/gRPC.
`JsonHttpHandler` adapts a function receiving `JsonHttpRequest` and `JsonHttpReply` into
the ordinary `HttpServer::Handler` signature. It owns no listener, thread, router or work
queue. Products decide where to mount their functions and who owns the host; there is no
independent/shared server mode. Raw `http` remains available for binary payloads and
mixed-content hosts, and for clients (`HttpClient` with a JSON body and header).

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
shortened by the adapter. These are local Unix transports.

## Bootstrap and HTTP ownership

The native application consumes its sole explicit `--bootstrap-input` argument
with `<xgc2/xrpc/bootstrap.hpp>` and `loadBootstrapInput(path)`. This validates
the shared binding/input schemas, pins private input and credential files,
and retains an immutable application JSON view for the product's own schema.
It does not search for a default path or read credential environment variables.
`BootstrapBinding` (also named `Binding`) produces a typed `ServiceRef` from
the owner's new instance ID after startup. Runtime/storage grants remain
purpose-tagged opaque handles, separate from that public reference.

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
C++ types; `unix` and gRPC component selection do not import the bootstrap
component. HTTP imports bootstrap automatically. Validated TLS material is
consumed only through the explicit native credential callback; the existing
native transport still applies peer checks and authorization. This addition does not
implement a remote HTTP transport.

```cpp
xgc2::xrpc::DiagnosticsOptions logging;
logging.level = xgc2::xrpc::LogSeverity::Debug;
xgc2::xrpc::Diagnostics diagnostics(logging);
xgc2::xrpc::HttpLimits limits;                 // defaults, or set the fields you need
limits.inflight = 8;
xgc2::xrpc::HttpServer host(unix_options, bounded_handler, limits,
    {owner_generated_instance_id, {"/v1/describe"}});
host.set_diagnostics(&diagnostics, "product.service");
```

`HttpLimits` maps connection/in-flight caps, header/request/response limits, separate
header/call/idle budgets and the shutdown budget. An absolute deadline passed to
`HttpClient::call` includes admission, connection, send and receive. Client calls
serialize on one reusable local Unix connection; the caller owns its calling threads.
Host connections/in-flight/shutdown fields do not create a client pool.

`HttpServer` has one owner for run/poll/drain/destruction. Handlers must hand
off bounded work without blocking that IO owner. Cross-thread reply completion
is at most once. **Every retained `HttpReply` copy reserves admission and the
endpoint lease until the copy is released**, including after timeout, peer
disconnect, stop or host destruction. A worker releases its reply when actual
business work ends. Cancellation is advisory and does not mean rollback.

`drain_until` stops new admission and allows admitted replies to finish within
the owner deadline. An expired drain cancels transport and returns false while
unquiescent work retains the lease. `drain()` uses `HttpLimits::shutdown_timeout`;
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
is closed before another request can be interpreted. HEAD is header-only. The
client reads a response's header block before its body, so that the response limit is
enforced before any body byte is stored (Beast 1.71 loses its `body_limit` error when
header and body are parsed in one read).

`HttpClient::close` is terminal, cancels the active call and rejects waiting or
future calls. Join all calling threads before destruction. Mutations are never
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

Native typed unary/streaming, pre-read response fences, the application-error marker,
fixed application work slots, native cancellation, conservative stream-budget encoding
and graceful versus forced shutdown are documented in [gRPC integration](README-grpc.md).
Remote gRPC channels accept explicitly supplied native credentials; the remote owner
supplies authentication, routing, connection admission and teardown. The HTTP
implementation here is the local UDS host/client; an authenticated remote HTTP transport
is not implemented by this package.

## Removed

No consumer remained, so these are gone rather than deprecated: `JsonHttpClient` and
`JsonHttpResponse`; `configure_grpc_server`, `grpc_channel_arguments` and
`grpc_client_limits` as public functions (the first two live on as file-local helpers);
the environment-variable policy layer (`RuntimePolicy`, `RuntimePolicyOptions`,
`resolve_runtime_policy`, `http_limits(policy)`, `grpc_limits(policy)`, the generated
registry, `XgcXrpc::policy`); `std::stop_token` in signatures; the `GrpcDelivery` enum
(use `Delivery`); `XGC2_XRPC_ENABLE_GRPC` (list `grpc` in `XGC2_XRPC_COMPONENTS`);
`Diagnostics::effective_policy()` (use `settings()`); `std::span` parameters.

## Verification and scope

Tests use real Unix HTTP and native gRPC, real UDP sockets, the common wire corpus,
allocation-rejected diagnostic producers, retained work after cancel, owner destruction,
pause/crash/reclaim/restart fences, header/body rejection, slow stream cancellation,
inode replacement and resource samples. gRPC native global threads and pressure quotas
are explicitly distinguished from hard application bounds. Tests establish measured
conditions; they do not prove arbitrary domain memory or real robot behavior.

On this repository's CI image (Ubuntu 20.04) the suite builds and passes with g++ 9.4 and
clang++-10 against Boost 1.71, nlohmann-json 3.7.3, OpenSSL 1.1.1f, gRPC 1.16.1 and
Protobuf 3.6.1. ThreadSanitizer was not usable here: this machine's clang 10 runtime
reports false races on any `std::condition_variable` timed wait with libstdc++ 10. Jammy and Noble builds, arm64 and
real vehicle hardware were not exercised. Noble-built binaries are not claimed to be
compatible with older distributions: each distribution is built on its own image.
