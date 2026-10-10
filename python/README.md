# Python XRPC

Python 3.8 or newer (`requires-python >=3.8`; the suite runs on 3.8.10, the
interpreter of ROS Noetic). Runtime dependencies are pinned exactly:
`aiohttp==3.10.11`, `httpx==0.28.1` and `httpcore==1.0.9`. There is no gRPC,
no raw outbound HTTP client and no configuration from the process environment.
`pip install ./python` installs the package; sources must stay valid Python 3.8.

What it offers: a bounded HTTP/JSON (`http.v1`) host and client over private
Unix sockets, native aiohttp public edges (TCP) with bounded bodies, streams and
WebSockets, an HTTPS client, an outbound `WebSocketClient`, Unix endpoint
ownership, the bootstrap input loader and bounded diagnostics.

## Explicit ownership and plain limits

```python
from xgc2_xrpc import Client, Host, Limits, Runtime, new_instance_id

runtime = Runtime()  # one IO thread and one bounded blocking pool
boot_id = new_instance_id()
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

Limits are plain values with the defaults below; the environment is never read.

| `Limits` field | Default | `Runtime` parameter | Default |
|---|---|---|---|
| `connections` | 32 (a `Client` without `limits` uses 16 per reference) | `blocking_workers` | 4 |
| `in_flight` | 32 | `max_calls` | 32 |
| `header_bytes`, `header_count` | 16384, 64 | `max_connections` | 32 |
| `body_bytes`, `response_bytes` | 1048576 | `max_sessions` (client references) | 64 |
| `header_timeout` | 5.0 s | `shutdown_timeout` | 5.0 s |
| `call_timeout` (at most 86400) | 30.0 s | `log_level` | `"info"` |
| `idle_timeout`, `reference_idle_timeout` | 30.0 s | `log_format` | `"json"` |
| `shutdown_timeout` | 5.0 s | `observer` | none |

Byte and count limits are positive integers up to 2147483647; times are finite
positive seconds. `Runtime.status()["capacities"]` reports the Runtime values.

One Runtime owns one IO thread, a fixed shared blocking pool, finite scheduled
work, admitted calls and connections, and the shared HTTP endpoint capacity.
Per-client handles share endpoint pools; instance generations are fenced per
call without creating another pool. Synchronous factories and handlers must
cooperate with their owner; timeout does not imply rollback or termination.
Runtime owns a native asyncio selector loop. At its public readiness seam,
one-shot same-loop `Future.set_result` callbacks check whether cancellation or
an earlier event already completed that Future. This handles AnyIO's UDS
readiness/cancellation race without modifying AnyIO or suppressing unrelated
event-loop exceptions; HTTPX/httpcore still own HTTP transport and parsing.
The IO loop's native default executor is this same fixed pool. Native aiohttp
file-spool and file-response work and native DNS submissions cannot create a
second untracked default pool. Saturated submissions fail before entering an
executor queue; running work retains its call and Host lease after cancellation.

## Dispositions and errors

Every call ends in one of three dispositions (`NOT_SENT`, `OUTCOME_UNKNOWN`,
`RESPONSE_RECEIVED`, tuple `DISPOSITIONS`):

| Disposition | How it surfaces |
|---|---|
| `not_sent` | `TransportError`: nothing reached the peer (invalid arguments, closed client, full admission, refused connection). |
| `outcome_unknown` | `TransportError`: the request may have been processed (deadline, lost connection, answer from another instance or with another request ID). Failed mutations are never replayed. |
| `response_received` | A 2xx answer returns `Response` (`.disposition`). An error answer raises `Fault` with `.code`, `.status` and `.disposition`, taken from the standard `{"error":{"code","message"}}` envelope or implied by the HTTP status. An answer the client refuses (larger than `response_bytes`, headers over the limit) raises `TransportError(..., "response_received")`. |

The error codes are `invalid_argument`, `not_found`, `conflict`,
`resource_exhausted`, `deadline_exceeded`, `cancelled`, `unavailable`,
`internal`, `unauthenticated` (401) and `permission_denied` (403); a handler may
use domain codes of its own. HTTPX retries and redirects are disabled. Caller
IDs, canonical timeout metadata and instance fencing share the common wire
helpers; queue, pool and connection wait consume the same deadline. Native
header-send tracing conservatively distinguishes `not_sent` from
`outcome_unknown`. `Client` checks response request-ID correlation and instance
metadata before consumption, including error replies. Its shared pool has no
persistent cookie authority. Explicit per-call timeouts are shortened to
`Limits.call_timeout` before startup and scheduling, for both synchronous and
asynchronous calls.

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
`HOST_KEY` and `DEADLINE_KEY` expose the selected Host and absolute monotonic
request deadline to a native handler.

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
CPU-blocking domain work belongs in the shared pool. Native WS framing and
backpressure stay with aiohttp.

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

## Outbound WebSocket

```python
from xgc2_xrpc import WebSocketClient

sockets = WebSocketClient("wss://upstream.example", runtime=runtime,
                          tls_context=verified_tls)

async def session(socket):
    await socket.send_str("hello")
    message = await socket.receive()

sockets.consume("/session", session, timeout=300, max_msg_bytes=160000,
                total_send_bytes=32000000, total_receive_bytes=1000000)
# On the selected Runtime loop: await sockets.consume_async(...)
```

`WebSocketClient.consume[_async](path, async_consumer, timeout=..., headers=...,
protocols=..., max_msg_bytes=..., total_send_bytes=..., total_receive_bytes=...)`
lends the consumer a socket with `receive`, `send_bytes`, `send_str`, `ping`,
`pong` and `close`; it is valid only while the consumer runs. The message and
cumulative limits tighten to the `Limits` budgets; TEXT counts UTF-8 bytes and
control payloads are finite. Equivalent handles share one native aiohttp
session/connector and one Runtime reservation. `uds=` supports Unix WS; WSS
validates certificate and hostname. Public native trace and middleware abort
redirects and aiohttp's implicit persistent-GET retry. Compression is disabled.
There is no reconnect or replay.

Close outbound handles before closing their Runtime. Native close failure is
distinct from quiescence. HTTPX/httpcore public closure traces catch failures
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
retain the formal Host's existing Unix lease requirements. The maintained
credential adapters here expose native HTTP SSL contexts and HTTP authorization
headers.

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
they perform no peer probes. One bounded lazy diagnostic writer (`Diagnostics`,
configured by `Runtime(log_level=..., log_format=...)`) emits redacted JSON or
text to supervisor stderr. The supervisor owns rotation. Record/queue size,
fixed event counters, rate suppression, saturation/recovery and dropped counts
are bounded; arbitrary payloads/authentication/path/error text never reach the
sink or observer. Observers run outside the IO thread. A stalled observer/sink
can fail finite shutdown and retains its owner.

Request headers are checked after native parsing; response headers are checked
after all native app prepare signals and before native serialization. aiohttp's
transient parser storage includes separate header count and field-size limits.
The native pipeline queue is 32. Native transport/read buffers and
parser-generated errors exist before SDK admission/response checks; nominal
body/header limits are not an exact peak RSS claim. Header/idle timers use a
conservative minimum. Native HTTPX/httpcore/h11 parsing has its own finite
buffers before response policy. Caller-owned values, native TLS buffers and
domain allocations remain separate from SDK retained buffers.

Verification (install the pinned dependencies first, for example
`pip install --user aiohttp==3.10.11 httpx==0.28.1 httpcore==1.0.9`):

```sh
PYTHONPATH=python python3 -m unittest discover -s python/tests
PYTHONPATH=python python3 python/benchmarks/resource_probe.py --calls 500
```

These are real Unix, TCP, TLS and native WebSocket tests and an isolated
load/RSS probe. They do not establish live station, physical device, remote
Agent/TLS routing, multi-language interoperability or sustained production
readiness.
