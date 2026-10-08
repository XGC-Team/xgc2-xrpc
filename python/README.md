# Python XRPC

Python >=3.10; verified here on Python 3.12.3 with aiohttp 3.14.4,
HTTPX 0.28.1/httpcore 1.0.9, and optional grpcio 1.84.0. ROS Noetic's system
Python 3.8 cannot consume this package. A deployment owner must supply a
supported interpreter/image; changing an SDK import is not deployment proof.

## Explicit ownership and startup policy

```python
from xgc2_xrpc import Client, Host, Runtime

runtime = Runtime.from_environment()  # composition root snapshots once
host = Host(socket_path, routes, runtime=runtime, instance_id=boot_id,
            discovery_routes=("/v1/describe",)).start()
client = Client(socket_path, runtime=runtime, instance_id=boot_id)
try:
    result = client.json("/v1/command", payload, request_id="caller:operation")
finally:
    client.close()
    host.close()
    runtime.close()
```

`Runtime(policy=resolved_policy)` shares an explicitly resolved policy.
`Runtime()` uses registry defaults without reading the environment. Startup
defaults, overrides, capability selection and ceilings use `resolve_policy`;
there are no environment aliases or per-request environment reads.
`runtime.effective_policy()` reports value/source/revision/ceilings and actual
shared Runtime capacities. `policy.update(..., expected_revision=...,
authorized=True)` applies only declared mutable settings; authentication is
the existing administrative interface owner's responsibility.

Host/client options consume the generated `_runtime_policy.json`; supplied
environment/deployment values take precedence over role-specific `Limits`.
The generated asset is packaged in the wheel and owned by the common registry
generator. No copy of registry defaults is maintained in Python source.

One Runtime owns one IO thread, a fixed shared blocking pool, finite scheduled
work, admitted calls/connections, and shared HTTP/gRPC endpoint capacity.
Managed gRPC listeners reserve their full native connection cap; HTTP admission
deducts those reservations from the same Runtime capacity. Reservations are not
observed connection counts, and failed shutdown keeps them until cleanup succeeds.
Reservations precede transport construction, including injected Agent
transport factories. Retiring owners and failed cleanup still consume capacity.
Per-client/robot handles share endpoint pools; instance generations are fenced
per call without creating another pool. Synchronous factories and handlers
must cooperate with their owner; timeout does not imply rollback or termination.
Runtime owns a native asyncio selector loop. At its public readiness seam,
one-shot same-loop `Future.set_result` callbacks check whether cancellation or
an earlier event already completed that Future. This handles AnyIO's UDS
readiness/cancellation race without modifying AnyIO or suppressing unrelated
event-loop exceptions; HTTPX/httpcore still own HTTP transport and parsing.
The IO loop's native default executor is this same fixed pool. Native aiohttp
file-spool and file-response work and native DNS submissions cannot create a
second untracked default pool. Saturated submissions fail before entering an
executor queue; running work retains its call and Host lease after cancellation.

## Native aiohttp public edges

```python
from aiohttp import web
from xgc2_xrpc import AppRouter, Host, Limits, RawStreamResponse, iter_body

router = AppRouter()

async def upload(request):
    reply = RawStreamResponse(max_bytes=1048576)
    await reply.prepare(request)
    async for chunk in iter_body(request, max_bytes=1048576):
        await reply.write(chunk)
    return reply

router.add_post("/upload", upload)
host = Host.from_app(router, address=("127.0.0.1", 0), runtime=runtime,
                     limits=Limits(call_timeout=30)).start()
address = host.bound_address
```

`Host.from_app` accepts `AppRouter` or `aiohttp.web.Application`, owns native
startup/cleanup signals, middlewares, routing, keepalive, body framing and
the entire handler/response-flush lifetime. Exactly one of `address=` or private
`path=` is supplied. `Host.from_handler(async_handler, address=..., ...)` is
the native lower-level handler seam. Public handlers do not require internal
XRPC headers; products retain their authentication and public wire contracts.
`host.bound_address` is read-only: after TCP start it returns the native address
with the actual assigned port, including `address=("127.0.0.1", 0)`; it is `None`
before start and after the listener stops. Unix listeners return their native
socket address. Products do not inspect runner/site/server internals.

`Limits` declares `connections`, `in_flight`, `header_bytes`, `header_count`,
`body_bytes`, `response_bytes`, `header_timeout`, `call_timeout`, `idle_timeout`
and `shutdown_timeout`. Byte/count limits are positive integers and times are
finite positive seconds. Environment/deployment policy and declared ceilings
resolve through the Runtime. `HOST_KEY` and `DEADLINE_KEY` expose the selected
Host and absolute monotonic request deadline to a native handler.

`request.read()` uses the native body limit. Raw chunked input uses `iter_body`
to enforce a cumulative byte limit. Raw output uses `RawStreamResponse`, which
checks bytes before each native write and honors the tighter Host response
limit. The Host also checks bounded `web.Response` bodies. Arbitrary native
stream writers cannot be retroactively guaranteed to enforce this total cap.
Handlers must await streams, every write and owned work; detached product
tasks do not acquire endpoint ownership. Errors after headers close the
transport and never serialize another HTTP response.

Bounded WebSocket handlers use the native response subclass:

```python
from xgc2_xrpc import BoundedWebSocketResponse

async def websocket(request):
    ws = BoundedWebSocketResponse(
        max_msg_size=160000, max_receive_bytes=32000000,
        max_send_bytes=1000000, receive_timeout=30, timeout=2)
    await ws.prepare(request)
    async for message in ws:
        # Product authentication and message semantics stay here.
        ...
    return ws
```

Products declare a finite Host `call_timeout` for the entire long session and
body/response budgets at least as large as their intended cumulative payloads.
The helper tightens its totals to those Host budgets, counts UTF-8 text bytes,
bounds output before encoding, and disables compression. Receive idle and close
timeouts are finite and tightened by Host idle/shutdown limits. Bare native
`web.WebSocketResponse(max_msg_size=...)` only limits incoming messages; it
does not enforce output or cumulative payload limits.

SSE and MJPEG use `RawStreamResponse(max_bytes=...)`, await `prepare` and every
`write`, and keep their producer/cleanup inside the handler. They retain one
admitted request for the complete stream and actual cleanup. The lifetime and
cumulative output budget apply to the full session; a session ends or fails
when either is exhausted. The endpoint lease remains owned if real work has
not quiesced during Host shutdown.

At the absolute deadline a separate owner-loop timer aborts the transport,
even if a handler delays cancellation cleanup. The call, work and lease remain
owned until that cleanup actually finishes. This requires the IO loop to run;
CPU-blocking domain work belongs in the shared pool. Native WS framing/backpressure stays with aiohttp;
there is no stdlib `HTTPServer` bridge or `edge.py` compatibility module.

Native multipart `request.post()` bounds encoded request input and field count;
it may spool files and decode part encodings. Products own decoded-domain and
temporary-storage quotas. Streaming `iter_body` bounds raw wire payload without
introducing decoded or spool storage. A native `FileResponse` or another custom
stream writer needs its own declared output quota; `RawStreamResponse` supplies
the SDK cumulative raw output check.

Direct SDK `ssl_context=` listeners are rejected: asyncio creates TLS
handshakes before aiohttp's admission callback, bypassing connection counts
and native Host shutdown tracking. Use an explicitly bounded authenticated
ingress owner. HTTPX clients retain authenticated HTTPS and injected Agent
transport support; they reject certificate/hostname verification disablement.

## HTTPX raw response consumption

```python
async def consume(stream):
    async for chunk in stream.iter_raw(65536):
        await destination.write(chunk)

client.consume("/v1/binary", consume, method="GET", timeout=10)
# On the selected Runtime loop: await client.consume_async(...)
```

The consumer runs within the original call budget and owns a borrowed native
response only until return. Total bytes, native reads and consumer backpressure
remain under admission. Retaining the wrapper does not extend its lifetime.
`iter_raw(chunk_size=65536)` yields available native arrivals immediately;
`chunk_size` is a maximum output fragment size, not a buffering target. Small
SSE events and MJPEG fragments do not wait for a full chunk or upstream EOF.
HTTPX retries and redirects are disabled. Caller IDs, canonical timeout metadata
and instance fencing share the common wire helpers; queue, pool and connection
wait consume the same deadline. Native header-send tracing conservatively
distinguishes `not_sent` from `outcome_unknown`. No mutation is replayed.
`Client` checks response request-ID correlation and instance metadata before
consumption, including error replies. Its shared pool has no persistent cookie
authority. Explicit per-call timeouts are shortened by resolved role policy
before startup/scheduling, for both synchronous and asynchronous calls.

## Public raw outbound HTTP and WS relay

```python
from xgc2_xrpc import RawClient, WebSocketClient, relay_websocket

outbound = RawClient("https://upstream.example", runtime=runtime,
                     tls_context=verified_tls)

async def forward(request):
    async def consume(upstream):
        reply = RawStreamResponse(max_bytes=outbound.limits.response_bytes,
                                  status=upstream.status,
                                  headers={"Content-Type": upstream.content_type})
        await reply.prepare(request)
        async for chunk in upstream.iter_raw():
            await reply.write(chunk)
        return reply
    return await outbound.request_async(
        "/upload", method="POST", content=iter_body(request),
        headers={"Content-Type": request.content_type},
        consumer=consume, timeout=10)

websockets = WebSocketClient("wss://upstream.example", runtime=runtime,
                             tls_context=verified_tls)

async def relay(request):
    return await relay_websocket(request, websockets, "/session", timeout=300,
                                 max_msg_bytes=160000,
                                 total_send_bytes=32000000,
                                 total_receive_bytes=1000000)
```

`RawClient.request` is the synchronous counterpart. Raw content is immutable
bytes or an async iterable of byte buffers; chunk totals are checked before
copying and the source's real `aclose` remains owned. Raw consumers run for every
status, including non-JSON errors. Explicit origins accept HTTP/HTTPS; `uds=`
selects native Unix transport with the supplied HTTP authority. Public requests
do not add internal XRPC metadata, retry, redirect, cookies or decompression.
Products select authentication and forwarded headers, including HTTP hop-header
and content-encoding handling. Equivalent handles share HTTPX endpoint pools.

`WebSocketClient.consume[_async](path, async_consumer, timeout=..., headers=...,
protocols=..., max_msg_bytes=..., total_send_bytes=..., total_receive_bytes=...)`
borrows a `BoundedWebSocket` with `receive`, `send_bytes`, `send_str`, `ping`,
`pong`, and `close`. The message and cumulative limits tighten to role budgets;
TEXT counts UTF-8 bytes and control payloads are finite. Equivalent handles
share one native aiohttp session/connector and one Runtime reservation. `uds=`
supports Unix WS; WSS validates certificate and hostname. Public native trace
and middleware abort redirects and aiohttp's implicit persistent-GET retry.
Compression is disabled.

`relay_websocket` awaits two message-by-message pumps within the Host request;
query selection and authentication headers stay with the product. It preserves
the upstream-selected offered subprotocol, binary/text/control payloads and
valid close reason/code. It creates no payload queue or reconnect loop. Relay
and Host use the same Runtime; the shorter Host/client deadline closes the
network while delayed consumer cleanup retains its call and pool ownership.
Before the relay handler returns or raises, this relay's consumer, pumps and
native close must actually finish. Thus a product's handler-scoped lease stays
held during delayed cleanup, including repeated cancellation. Cleanup failure
retains ownership until the composition owner explicitly retries client close;
unrelated calls on the same client do not participate in this wait. Standalone
`WebSocketClient.consume_async` still returns within its caller deadline while
the SDK retains any unfinished cleanup.

Close both outbound handles before closing their Runtime. Native close failure
is distinct from quiescence. HTTPX/httpcore public closure traces catch failures
even during read-error unwinding before a Response exists. Failed HTTP cleanup
quarantines its shared endpoint and retains the native owner, session capacity
and any still-running call. Some native wrappers mark closed before awaiting
actual socket cleanup; a repeated no-op close cannot prove recovery, so those
failed HTTP owners remain charged until process exit. Ordinary cancellation
with successful actual close releases ownership. Native WS close retries are
accepted only through tracked real cleanup; failure is reported to the caller.

JSON encoding snapshots bounded containers before native encoding, retaining
checked immutable scalar values. Scratch containers/entries and result space
are proportional to the output policy, rather than a full oversized domain
encoding. Concurrent domain mutation may fail or produce a mixed domain
snapshot; the SDK does not supply domain consistency/transactions.

## Optional typed gRPC

```python
from xgc2_xrpc.grpc import GrpcHost, aio_channel, channel

grpc_host = GrpcHost(socket_path, runtime=runtime, instance_id=boot_id)
add_generated_servicer_to_server(servicer, grpc_host.server)
grpc_host.start()

async def invoke():
    async with aio_channel(service_ref, runtime=runtime,
                           local_target=local_target) as transport:
        stub = GeneratedStub(transport)
        return await stub.Command(request, timeout=2,
                                  metadata=(("x-request-id", "caller:operation"),))

result = runtime.run(invoke(), timeout=3)
```

Native aio hosting supports all four typed RPC shapes and both native async
and conventional sync generated servicers. Sync domain work uses the same
fixed Runtime pool as HTTP. Sync response streams have one buffered record.
`aio_channel` is the preferred request-stream seam: native scheduling uses the
selected Runtime loop. `channel` supports conventional generated sync stubs;
grpcio itself creates a producer thread per admitted request stream. Its call
permit and channel owner remain held until iterator execution and cleanup end.
Closed channel handles revoke already-created stubs. Channels share a bounded
cache with HTTP, including constructors and retiring owners.

gRPC stream calls require an actual finite native deadline no longer than the
Host maximum; native cancellation reaches blocked receive. Missing/duplicate
IDs, stale instances and invalid deadlines fail before domain dispatch. Calls
may supply a request ID; native errors after dispatch conservatively expose
`GrpcTransportError.disposition == "outcome_unknown"` because grpcio provides
no public header-send trace. Local validation/admission errors are `not_sent`.
Native retry and wait-for-ready queues are disabled.

Native stop is distinct from actual Python handler/iterator completion.
Failed/noncooperative cleanup retains permits, channel/Runtime owners and the
Unix lease lock; a later successful close retries failed iterator cleanup.
grpc Core can remove the socket pathname during stop while the SDK lifetime
lock remains exclusive. Path absence alone is not evidence of quiescence.

`GrpcHost(..., max_connections=None)` consumes `HOST_MAX_CONNECTIONS` and the
Runtime connection capacity. An explicit smaller `max_connections` allocates
a tighter share for multiple Hosts; allocation exceeding the remaining shared
capacity fails before binding. The default reserves the whole available role
cap, so compositions sharing a Runtime should choose their shares explicitly.
Pinned grpcio 1.84.0 enforces `grpc.max_allowed_incoming_connections` before
handshaking, including raw connections without an HTTP/2 preface. The SDK's
single UDS or numeric-address listener reserves this complete cap until successful Host close.
Closing before `start()` completes the native start/stop lifecycle with domain
admission closed; native stop alone leaves an unstarted listener FD open in
this pinned version. Bind-failure cleanup uses this same owned lifecycle.

The native quota applies **per listener**. Additional listeners created directly
with public `host.server.add_*_port` belong to the caller: each inherits the cap,
but their aggregate is outside the SDK-managed Runtime reservation. This native
registration seam must not be used to claim a stronger shared budget.
`host.status()` reports the cap, reservation and scope; native live connection
counts are unavailable. `runtime.status().connections` counts HTTP connections;
`native_reserved_connections` reports gRPC reserved capacity and
`connection_envelope` sums the two. Concurrent RPC, stream, metadata, message
and idle limits are separate bounds, not a total process/RSS/FD ceiling.
Explicit runtime settings that this profile cannot enforce are rejected.

`GrpcHost(path, ...)` owns one private UDS listener. Alternatively,
`GrpcHost(address=(numeric_ip, port), credentials=native_server_credentials, ...)`
owns one native TLS listener, with `port=0` allowed for an explicitly ephemeral
endpoint. DNS listener addresses and missing native credentials are rejected.
The composition owner supplies credentials from `grpc.ssl_server_credentials`;
the SDK cannot inspect the TLS provenance of an opaque native ServerCredentials
object. Native local credentials do not satisfy the network TLS contract.
The read-only `bound_address` returns the actual endpoint. Connection caps at
or above `INT_MAX` are rejected before native binding, including an invalid
explicit value that a resolved environment setting would otherwise replace.

## Explicit bootstrap input

`load_bootstrap_input(explicit_absolute_path, role="server" | "client")` loads
the common `bootstrap-input.schema.json` and `bootstrap.schema.json` inputs.
The owner supplies one path; the SDK performs no environment search or product
configuration parsing. Its `BootstrapInput` exposes `binding`, deeply immutable
`application`, resolved `credentials` and `resolve_grant(handle, purpose)`.
`binding.storage_grants` contains the declared opaque names; products retain
their storage configuration and scope authority. `binding.service_ref(instance_id)`
combines a fresh application-owned generation with the binding after readiness.
No incarnation is accepted in the startup binding.

Canonical private input and credential files are opened once through retained
directory descriptors, without following symlinks; owned 0700 parents and
single-link 0600 regular files are required. Input is bounded to 16 KiB and
application depth to 32. Named native TLS/bearer grants are resolved and validated
before listener or pool construction. Empty local-private grants are valid and
retain the formal Host's existing Unix lease requirements.
The maintained credential adapters here expose native HTTP SSL contexts and
HTTP authorization headers; native gRPC credentials and metadata remain an
explicit composition step for gRPC owners.

```python
startup = load_bootstrap_input(owner_input_path, role="client")
reference = startup.binding.service_ref(actual_ready_instance_id)
authorization = startup.credentials.authorization
client = Client.from_service(reference, runtime=runtime, local_target=local_target,
    tls_context=startup.credentials.tls_context,
    headers=authorization.headers if authorization is not None else None)
```

For an explicitly named outbound storage credential, use
`startup.resolve_grant(handle, "authorization").headers` as `Client(..., headers=...)`
or `Client.from_service(..., headers=...)`. These immutable per-handle headers
are attached to every request, including on shared persistent pools. They cannot
override wire identity/framing headers; no product accesses `_session` to
install credentials. The bearer grant's `authorize(native_request, context=None)`
checks exactly one raw authorization header using a fixed-size constant-time
digest comparison. Product method/scope authorization remains in the product.

## Diagnostics and limits

`runtime.status()` and `host.status()` expose maintained source-timed state;
they perform no peer probes. One bounded lazy diagnostic writer emits
redacted JSON/text to supervisor stderr. The supervisor owns rotation.
Record/queue size, fixed event counters, rate suppression, saturation/recovery
and dropped counts are bounded; arbitrary payloads/authentication/path/error
text never reach the sink or observer. Observers run outside the IO thread.
A stalled observer/sink can fail finite shutdown and retains its owner.

Request headers are checked after native parsing; response headers are checked
after all native app prepare signals and before native serialization. aiohttp's transient parser
storage includes separate header count and field-size limits. The native
pipeline queue is 32. Native transport/read buffers and parser-generated errors
exist before SDK admission/response checks; nominal body/header limits are not
an exact peak RSS claim. Header/idle timers use a conservative minimum. Native
HTTPX/httpcore/h11 parsing has its own finite buffers before response policy.
Caller-owned values, native TLS/grpc buffers and domain allocations remain
separate from SDK retained buffers.

Verification commands (dependencies installed in an isolated location):

```sh
PYTHONPATH=/tmp/xrpc-python-deps:python python3 -m unittest discover -s python/tests -q
PYTHONPATH=/tmp/xrpc-python-deps:python python3 python/benchmarks/resource_probe.py --calls 500
```

These are real Unix/TCP/TLS/native WS/gRPC tests and an isolated load/RSS probe.
They do not establish live station, physical device, remote Agent/TLS routing,
multi-language interoperability or sustained production readiness.
