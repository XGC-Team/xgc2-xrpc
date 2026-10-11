# Node service edges and HTTP clients

`@xgc2/xrpc` supplies Node services with bounded HTTP/JSON (`http.v1`) hosts and
clients. It uses Node's maintained HTTP/HTTPS parser, Agent and streams and a
pinned `ws`; it has no gRPC. The package is CommonJS, Node 20 or newer, license
Apache-2.0. Browsers and Node programs that only call services use the ESM
[`@xgc2/xrpc-client`](../ts/README.md) instead.

| Entry point | Purpose |
|---|---|
| `createHTTPHost(handler, options)` | Bounded host with admission, drain and diagnostics: a TCP/TLS edge (`host.server.listen(port)`) or a private Unix socket (`unixPath` and `host.listen()`). The product owns routes, authentication and origins. |
| `createFetchHost(handler, options)` | The same host with a `Request` to `Response` handler. |
| `createRPCHost(handler, options)` | Strict metadata gating for internal calls: exactly one `X-Request-ID` and `X-Xrpc-Timeout-Ms`, instance fence `X-Xrpc-Instance-ID`, handler context with deadline, request ID, instance and cancellation signal. |
| `createBoundHTTPHost(handler, options)` | `createRPCHost` over TLS and bearer authorization taken from a `BootstrapBinding`. |
| `proxyWebSocket(...)` | Bounded WebSocket relay used from `host.onUpgrade`. |
| `HTTPClient` | One explicit client owner for all references in a process. |
| `Diagnostics` | One bounded, redacted stderr sink. |
| `newInstanceId()` | 128 random bits as 32 hex characters: call once per process start and pass it as `instanceId`. |

## Limits

Limits are plain options with the defaults below; nothing is read from the
process environment.

| Host option | Default | Client option | Default |
|---|---|---|---|
| `maxConnections` | 32 | `maxConnections` (per reference) | 16 |
| `maxInFlight` | 32 | `maxInFlight` | 32 |
| `maxBodyBytes` | 1048576 | `maxRequestBytes` | 1048576 |
| `maxHeaderBytes` | 16384 | `maxHeaderBytes` | 16384 |
| `maxResponseBytes` | none (RPC hosts: 1048576) | `maxResponseBytes` | 1048576 |
| `callTimeoutMs` | none (RPC hosts: 30000) | `callTimeoutMs` | 30000 |
| `headerTimeoutMs` | 5000 | `maxReferences` | 64 |
| `idleTimeoutMs` | 30000 | `referenceIdleTimeoutMs` | 30000 |
| `shutdownMs` | 5000 | | |
| `requestTimeoutMs` | the larger of `headerTimeoutMs` and `callTimeoutMs` (or 30000) | | |

Wire budgets are at most 86400000 ms. Hosts and clients also take
`diagnostics` (a `Diagnostics` owner) and accept no other configuration.
One native parsed input chunk may temporarily exceed the request limit before
rejection. Node stream high-water marks, parser overhead, headers and TLS
buffers add bounded transport memory. `call({json})` uses `JSON.stringify`
before checking the encoded size; use a byte payload for a hard pre-encoding
guarantee. No zero-copy or hard RSS guarantee is made.

## Unix-socket host

```js
const { createRPCHost, newInstanceId } = require("@xgc2/xrpc");
const host = createRPCHost(handler, {
  instanceId: newInstanceId(),
  unixPath: "/run/user/1000/lichtblick/control.sock",
  discoveryPaths: ["/v1/describe"],
});
await host.listen();   // verifies the directory, reclaims a stale socket, binds 0600
// ...
await host.close();    // drains admitted work, then removes the socket
```

`discoveryPaths` lists GET routes that may be called without the instance header. They are matched on
the path alone: `GET /v1/describe?wait_ready_ms=250` needs no instance, and the handler reads the
query from `req.url`.

The socket lives in a private runtime directory that the supervisor created: the
directory must exist, be owned by the effective user and have mode 0700, and no
path component may be a symlink. The host binds relative to that directory, sets
the socket to 0600 and unlinks only the socket it created. If the path already
exists, the host connects to it: a refused connection means a stale socket
(unlinked, then bound); an accepted connection rejects `listen()` with
`code: "EADDRINUSE"`; anything else that exists at the path (a regular file, a
symlink, a socket that does not answer within `probeTimeoutMs`) is refused
untouched. Node has no `flock`, so exclusivity is the private directory plus
this connect test: do not start two owners of one path, because the first one to
close removes the path. `server.listen()` on a Unix path is rejected. Linux only.

## Client

```js
const { HTTPClient } = require("@xgc2/xrpc");
const client = new HTTPClient({ localTarget: "station", diagnostics });
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
// reply.disposition is "response_received"; a status is a response fact the
// product interprets.
const value = JSON.parse(reply.body);
client.close();
```

Every call ends in a result with `disposition: "response_received"` or in a
`TransportError` with `code` (the shared error vocabulary) and a `disposition`:

| `disposition` | Meaning |
|---|---|
| `not_sent` | Nothing reached the peer: invalid arguments, a closed client, full admission, a call cancelled before the request went out, TLS verification failure. |
| `outcome_unknown` | The request may have been processed: deadline, lost connection, truncated body, answer from another instance. Failed mutations are never replayed. |
| `response_received` | The peer answered and the client refuses the answer, for example a body over `maxResponseBytes` (`resource_exhausted`). |

`stream(ref, path, options)` returns a bounded native Readable. Consume it or
call its `close()` in `finally`; admission stays reserved until the stream
closes. `close()` on the client is terminal, destroys active I/O and closes all
owned Agents. The client verifies exactly one response instance header,
including error responses.

UDS references must match `localTarget`; remote Unix routes fail before send.
Remote HTTP references use verified HTTPS, with explicit CA and client
certificates in immutable `tls` options; verification cannot be disabled. There
is no environment proxy lookup, redirect following or retry. One Agent per
endpoint is shared across service instance changes; reference acquisition fails
fast when all slots are active and idle references expire on one owner timer.

## Diagnostics

```js
const { Diagnostics } = require("@xgc2/xrpc");
const diagnostics = new Diagnostics({
  sink: { kind: "supervisor_stderr", rotationOwner: "supervisor" },
  level: "info", format: "json",   // the defaults
});
// Pass the same owner to every host and client, drain those first, then:
await diagnostics.close({ timeoutMs: 5000 });
```

One explicitly owned worker writes redacted JSON or text lines to stderr; the
supervisor owns collection and rotation. Admission is bounded and ACKed by the
worker, counters have fixed cardinality, and no payload, header or error text is
logged. A stalled stderr retains the worker and makes a finite `close` fail; a
later `close` can wait for the real drain.

## Startup input

`loadBootstrapInput(explicitPath, { role: "server" | "client" })` loads the
common versioned binding and the explicitly named private credential files once.
Use its `{ binding, resolveGrant, application }` with `createBoundHTTPHost`.
See the shared [startup contract](https://github.com/XGC-Team/xgc2-xrpc/blob/main/contracts/bootstrap.md)
and schemas for file ownership and byte limits. `application` is opaque product
metadata. The host verifies native TLS and authorization before dispatch. No
credential environment variables or guessed paths are introduced. Native
applications keep ownership of listen, readiness, drain and exit.

`createRPCHost` hands the handler its deadline, request ID, instance and a
cancellation signal; domain code that ignores cancellation stays counted until
it returns, and a failed drain lets a later `close()` wait for quiescence.

## Validation

`npm test` uses real sockets: Unix sockets, loopback HTTP and HTTPS with
disposable certificates, and the shared wire cases in `contracts/fixtures`.
Those fixtures are source tests and are not shipped. The tests do not establish
production ABI, cross-language crash, scheduler-stop, OOM or sustained overload
acceptance.
