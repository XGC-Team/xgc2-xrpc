# Node service edges and HTTP clients

The Node package supplements the four formal Go/C++/Python/Rust SDKs. It uses
Node's maintained HTTP/HTTPS parser, Agent and streams, and pinned `ws`. It
does not offer gRPC or a Unix endpoint lease. `createHTTPHost` and
`createFetchHost` own public edge connection/admission/drain behavior; the
product owns routes, auth, origins and the granted listening address. Do not
use their raw `server.listen(unixPath)` as a substitute for the shared exclusive
Unix lease: the host rejects Unix listen paths. Internal UDS providers use one
of the formal SDKs. Supplying explicit `tls` server credentials selects the
native HTTPS server while preserving the same host lifecycle and limits.

One explicit client owner serves all references in a process:

```js
const { HTTPClient, resolvePolicy } = require("@xgc2/xrpc");
const policy = resolvePolicy({ environment: { ...process.env } }); // startup only
const client = new HTTPClient({ policy, localTarget: "station" });
const ref = {
  target_id: "station", service: "product-storage", api_version: "v1",
  instance_id: "actual-instance-from-owner", profile: "http.v1",
  endpoint: { kind: "unix", address: "/granted/private/runtime/storage.sock" },
};
const reply = await client.call(ref, "/domain-owned-route", {
  timeoutMs: 1500, requestId: "operation:unique-id", method: "POST",
  headers: { Authorization: "Bearer <explicit granted credential>" },
  json: { bounded: "domain request" },
});
// A status is a response fact; the product interprets its domain result.
const value = JSON.parse(reply.body);
client.close();
```

`stream(ref, path, options)` returns a bounded native Readable. Consume it or
call its `close()` in `finally`. Admission stays reserved until the stream
actually closes, including buffered output held for a slow consumer. `close()`
is terminal, destroys active I/O and closes all owned Agents. Failed mutations
are never replayed. `TransportError` carries `code` and `disposition`:
`not_sent` or conservative `outcome_unknown`. Ordinary non-success HTTP status
codes are returned to the domain caller. The client verifies exactly one
response instance header, including error responses.

UDS references must match the explicit `localTarget`. Remote UDS routes are not
implemented in this supplemental package and fail before send. Remote HTTP
references use verified HTTPS; supply explicit CA/client certificates through
immutable `tls` options. TLS verification cannot be disabled. Node performs no
environment proxy lookup, redirect following or retry. One bounded Agent
cache is keyed by endpoint with a process-shared immutable TLS policy; service
instance changes do not create another pool. Endpoint acquisition fails when
all reference slots are active; idle references expire via one owner timer.
Per-reference and global admission are fail-fast and avoid an Agent retry queue.

`resolvePolicy` requires an explicit environment snapshot. Values, source,
ceiling and revision are queryable via `effective()`. Host/client options map
the supplied policy; conflicting explicit options fail. The Node package
currently implements `host`, `http`, `rpc`, `transport`, `client_pool` and
`client_registry`. Explicit diagnostics or gRPC settings fail rather than
silently being ignored. `LOG_LEVEL`/`LOG_FORMAT` are not yet an implemented
Node logging sink. No fields are currently administratively mutable.

`createRPCHost` supplies strict metadata gating for conformance and explicitly
owned native HTTP listeners. It does **not** add a Unix lease. It cannot be
advertised as a complete UDS internal provider. Its handler receives deadline,
request ID, instance and cancellation signal; arbitrary domain code that
ignores cancellation remains counted until it returns. Failed drain permits a
later `close()` to wait for actual quiescence.

The common defaults bound host admission to 32 connections/32 calls, headers to
16 KiB, request bodies to 1 MiB and clients to 16 connections per endpoint/64
references. Policy-controlled response bodies default to 1 MiB. Existing
public edge applications without a policy must explicitly set
`maxResponseBytes` where they promise a finite response; large static assets
or long streams need a declared product choice. One native parsed input chunk
may temporarily exceed the request limit before rejection. Node stream
high-water marks, native parser overhead, headers and TLS buffers contribute
additional bounded transport memory. `call({json})` uses native
`JSON.stringify` before checking the encoded size; caller-owned input and that
temporary serialization are not bounded by the wire byte limit. Use a bounded
byte payload for a hard pre-encoding memory guarantee. No zero-copy or hard
RSS guarantee is asserted.

Validation: `npm test` uses real sockets and shared `contracts/fixtures` cases.
Those fixtures are source tests; they are not shipped as a runtime dependency.
The tests include real verified HTTPS with disposable certificates, and do not
establish production Focal/ABI, mTLS, cross-language
crash, scheduler-stop, OOM or sustained overload acceptance.
