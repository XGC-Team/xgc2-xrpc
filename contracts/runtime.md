# XGC2 XRPC runtime contract

XRPC owns the mechanics of communication between processes and hosts: listening and
dialing, framing, request correlation, deadlines, cancellation, the error vocabulary,
admission limits, drain, instance fencing and the authentication of the carrier.
Domains own state, the meaning of readiness, completion semantics, trust decisions and
device effects. XRPC runs no central daemon, starts no provider on an invocation,
discovers no service and implements no domain operation.

Function first: a capability is a stable in-process API first, and XRPC is the thin
adapter that exposes it to other processes or hosts. A caller in the same process calls
the function and takes no local RPC hop. A capability is built only for a cell of the
[capability matrix](../docs/capability-matrix.md) that has a real consumer; every other
cell is absent and says why.

This contract is the shared requirement of all SDKs. Companions:
[udp-v1.md](udp-v1.md) (the datagram profile), [events.md](events.md) (http.v1 event
streams), [sessions.md](sessions.md) (long-lived grpc.v1 sessions),
[operations.md](operations.md) (diagnostics and status), [bootstrap.md](bootstrap.md)
(startup input) and [fault-conformance.md](fault-conformance.md) (the fault and interop
tests that exist). A statement here is a requirement; the capability matrix names the
tests that show each SDK meets it.

## Profiles

| Profile | Encoding | Carriers | Modes | Security |
| --- | --- | --- | --- | --- |
| `http.v1` | JSON over HTTP/1.1 | private Unix socket; TLS (server authentication or mutual TLS) | unary; server event stream ([events.md](events.md)) | Unix: the permissions of a private directory. TLS: X.509 server authentication, optional client certificates |
| `grpc.v1` | Protobuf over HTTP/2 | Unix socket; TLS or mutual TLS | unary; server streaming; bidirectional; long-lived session option ([sessions.md](sessions.md)) | as http.v1; a session may verify the peer certificate with a caller-supplied hook (pinning) |
| `udp.v1` | binary header and a JSON body, one datagram per request and per reply | UDP over IPv4 or IPv6 on a LAN | unary only | HMAC-SHA256 per key: authenticity and integrity, no confidentiality ([udp-v1.md](udp-v1.md)) |

Not provided, for lack of a consumer: raw TCP RPC, client streaming as a public mode and
plaintext TCP HTTP or gRPC hosts. (The one plaintext TCP listener, Core's browser edge,
is product code, not an SDK host.)

`http.v1` uses HTTP/1.1 with JSON for structured values, parsed and serialized by the
maintained HTTP library of each language; first-party local RPC does not introduce
JSON-line or private binary framing. Binary resources use HTTP content types and
streaming; `multipart/mixed` carries typed metadata plus binary parts when both belong to
one immutable capture. `grpc.v1` uses typed Protobuf methods and native gRPC semantics;
HTTP-only applications do not link gRPC.

Browser APIs, MCP, provider stdio, ROS and hardware protocols are explicit edge adapters
whose wire contracts stay with their product. ROS remains the interface of algorithms and
devices, not the platform's internal service bus; real-time data loops, C ABIs and shared
memory are data boundaries outside XRPC.

## Service references

A service reference identifies one service endpoint:

```
{target_id, service, api_version, instance_id?, profile, endpoint{kind, address}, key_id?}
```

| Profile | Endpoint kind and address |
| --- | --- |
| `http.v1` | `unix`: an absolute canonical path of fewer than 108 bytes; `https`: an authenticated origin with no credentials, path, query or fragment |
| `grpc.v1` | `unix` as above; `tls`: `host:port` |
| `udp.v1` | `udp`: `host:port`, IPv6 literals in brackets, port 1 to 65535 in canonical form |

`udp.v1` and the `udp` kind exist only together. `key_id` names the HMAC key of a `udp.v1`
service in the caller's key ring; zero means unspecified (the ring then holds exactly one
key) and it is zero for the other profiles. A `udp.v1` instance pin is the 32-digit
lowercase hexadecimal form of the 128-bit instance. `service` names the host that serves
the call; it is not the capability (see Method addressing), except that `grpc.v1` takes
the protobuf service name from it.

The owner of a service returns its reference after startup; an invocation only consumes
it and never carries a process definition or bootstrap settings. SDKs depend on no
workflow, database, supervisor or product domain. Dialing a remote Unix path is the job of
the transport owner that injects the dial function; a remote Unix pathname is never a
local dial target.

## Instance fence

Every server draws a random 128-bit instance identity at process start. It stays fixed for
the life of the host; a service whose environment is replaced (a restarted ROS master, a
reloaded world) exits or reports `ready: false`, it does not re-issue its identity.

- Replies carry the instance: `X-Xrpc-Instance-ID` on `http.v1`, the initial metadata
  `x-xrpc-instance-id` on `grpc.v1`, the `instance` field of every `udp.v1` reply.
- A caller may pin an expected instance. A server of another instance refuses the call
  with `conflict` (HTTP 409, gRPC FAILED_PRECONDITION, `udp.v1` status conflict) before
  any domain dispatch.
- A client verifies the instance of every answer. **A pinned call that is answered by
  another instance fails with `conflict` and the disposition `outcome_unknown`**, with the
  answering instance reported where the language can. The pinned instance may have run
  the request before it went away, so the caller must not conclude that nothing happened.
  Other `udp.v1` replies from a foreign instance are ignored, because they answer nothing
  the caller asked.
- Internal `http.v1` and `grpc.v1` references pin an instance. Public edge APIs and
  explicit discovery may omit it and must not be used as an internal invocation fallback.
  A `udp.v1` reference built from configuration may omit it and learn it from the first
  reply.

## Unix endpoint ownership

A private Unix endpoint is one exclusive lifetime lease (Go, C++, Rust and Python). The
runtime directory is owned by the effective user with mode 0700 and the walk to it follows
no symlink. The lease is a sibling lock `<socket-path>.xrpc.lock`, an owned regular file
of mode 0600 with one hard link, taken with `flock(LOCK_EX|LOCK_NB)` before any probe or
bind and never unlinked at unlock. Checks and bind refer to the same retained directory.
The socket mode defaults to 0600. The lease rejects non-socket paths and symlinks, records
the bound inode and removes only that inode. An existing path fails by default; reclaiming
an unreachable socket is an explicit choice that needs a bounded failed connect and an
unchanged inode. Close and stop are idempotent and a failed bind releases only what that
attempt acquired.

Stopping admission is not the end of admitted handlers: the lease stays held until the
owned work is quiescent, and a shutdown deadline can report that it is not, but it cannot
release the lease while old work continues. Node has no `flock`; its Unix host relies on
the private directory owned by the supervisor, refuses a path that is not a socket and
binds only after a connection attempt is refused (a socket with a live owner fails with
"address in use").

## Deadlines, cancellation and replay

- Every call has a finite deadline that covers queue and pool wait, connection setup,
  write, read and cancellation. `http.v1` carries the remaining budget in
  `X-Xrpc-Timeout-Ms`, `grpc.v1` uses the native deadline, `udp.v1` carries `timeout_ms`.
  A server caps the budget by its own call budget (30 s for `http.v1` and `grpc.v1`,
  2 s for `udp.v1` by default); a caller's budget can only shorten it.
- Long-lived sessions are the only exception and are explicit
  ([sessions.md](sessions.md)). Event streams have no call deadline but heartbeats
  ([events.md](events.md)).
- Closing a stream or connection (`http.v1`, `grpc.v1`) or abandoning a call (`udp.v1`)
  never means that the business action stopped, and a cancelled call implies no rollback.
- No transport replays a mutation: not after a lost reply, not after a reconnect, not
  after a failed warm connection. `udp.v1` retransmits the same datagram with the same
  request id inside one deadline and the server answers a duplicate from its reply cache,
  so the handler runs at most once: that is deduplication, not replay. Reconciling an
  unknown outcome is a domain operation.

## Dispositions

Every client result says what the caller can know about the effect of the call, in one of
three dispositions:

| Disposition | Meaning |
| --- | --- |
| `not_sent` | Nothing reached the peer: invalid arguments, a closed client, refused admission, a refused connection. Retrying is safe. |
| `outcome_unknown` | Bytes may have reached the peer and no usable answer arrived: deadline, lost connection, a reply from another instance, a `udp.v1` call that was sent and never answered. The SDK never retries; the domain reconciles. |
| `response_received` | The peer answered. This includes error answers and answers the client refuses, such as a body larger than its response limit. It says nothing about success. |

All client SDKs implement the same three values: Go `xrpc.CallError.Disposition`,
C++ `xgc2::xrpc::Delivery` (also `udp::Delivery`), Rust `Disposition`, Python
`NOT_SENT`/`OUTCOME_UNKNOWN`/`RESPONSE_RECEIVED` on `TransportError`, `Fault` and
`Response`, and Node and TypeScript `disposition`. A failure to receive is a client-side
property, not a claim that the server rejected the request.

A `grpc.v1` application failure is `response_received` only when the host marked it. A
known domain failure of an admitted, instance-bound call may carry exactly one standard
`google.rpc.ErrorInfo` detail with `domain = "xgc2.xrpc"`, `reason = "APPLICATION_ERROR"`
and exactly two metadata entries, `request_id` and `instance_id`, taken from the admitted
call. A client reports `response_received` only when this single valid marker matches its
request and the verified response instance. Status code or instance metadata alone is
insufficient: a local receive-size failure after a committed mutation can have the same
status as an application quota rejection. A missing, duplicate or invalid marker leaves an
unsuccessful call `outcome_unknown`. The marker keeps the native code and does not imply
rollback, safe replay or physical completion. Go (`grpcx.ApplicationError`) and C++
(`GrpcCallScope::application_error`) write and read it; hosts never mark transport,
cancellation or arbitrary handler failures.

## Error vocabulary

One set in all languages. Domains may add codes of their own inside the error body.

| Code | `http.v1` | `grpc.v1` | `udp.v1` status |
| --- | --- | --- | --- |
| `invalid_argument` | 400 | INVALID_ARGUMENT | 1 |
| `not_found` | 404 | NOT_FOUND | 2 |
| `conflict` | 409 | FAILED_PRECONDITION (also ALREADY_EXISTS, ABORTED when read) | 3 |
| `resource_exhausted` | 429 (also 413, 431) | RESOURCE_EXHAUSTED | 4 |
| `deadline_exceeded` | 504 (also 408) | DEADLINE_EXCEEDED | 5 |
| `cancelled` | 499 | CANCELLED | 6 |
| `unavailable` | 503 (also 502) | UNAVAILABLE | 7 |
| `internal` | 500 | INTERNAL | 8 |
| `unauthenticated` | 401 | UNAUTHENTICATED | 9, never sent: an unauthenticated datagram gets no reply |
| `permission_denied` | 403 | PERMISSION_DENIED | 10 |

`http.v1` answers an error as `{"error":{"code":"<code>","message":"...","details":{...}}}`
(`details` optional); `udp.v1` as `{"code":"<code>","message":"...","details":{...}}`. A
client takes the code from the envelope when there is one and from the status otherwise;
a domain code is a lowercase token (`[a-z][a-z0-9_]{0,63}`) and passes through unchanged.
Messages never carry payloads, credentials or arbitrary error text from the environment.

## Request identity

Request identities match `[A-Za-z0-9._:-]{1,128}`. Every client lets the caller supply one
and generates one when the caller does not: 128 random bits as lowercase hexadecimal, unique
across independently created clients and restarts (a process-local counter alone is not).
`http.v1` carries it in `X-Request-ID` and `grpc.v1` in `x-request-id`; `udp.v1` uses a
128-bit binary id, which handlers see as its 32 lowercase hexadecimal digits (a caller
whose identity has exactly that form uses it as the id). Servers echo it in the answer.

## Shared wire rules

The wire rules of `http.v1` and `grpc.v1` are common to all languages, negative cases
included; the corpora are [fixtures/wire.json](fixtures/wire.json) (33 `http.v1` cases,
run by every host SDK) and [fixtures/grpc-wire.json](fixtures/grpc-wire.json) (23 `grpc.v1`
cases, run by Go).

- An internal `http.v1` request contains exactly one `X-Xrpc-Timeout-Ms` in canonical ASCII
  decimal form, 1 through 86400000 (no sign, leading zero, fraction or exponent), and
  exactly one `X-Request-ID`. Discovery is subject to the same limits.
- Exactly one `X-Xrpc-Instance-ID` is required on a bound call and verified on its response.
  Explicit discovery permits a missing instance only on configured GET routes; a supplied
  empty, duplicate or mismatched instance is rejected anyway, and an empty header value is
  not a missing header.
- GET and HEAD may have no body or `Content-Length`; JSON parsing applies only where a
  route requires a JSON body; a HEAD response has no body.
- HTTP framing belongs to the maintained parser. A rejection before the body is read either
  drains the body's framing safely or closes the connection; rejected body bytes never
  become another request.
- The caller's budget includes queue, pool and lock wait and connection setup. Idle time
  before the next request does not consume the next call's budget. A longer caller budget
  than the host's limit is not malformed; the host just shortens it.
- For `grpc.v1` the SDK owns the lowercase metadata `x-request-id` and `x-xrpc-instance-id`
  next to the native deadline and framing. A missing, duplicate, empty or invalid request
  id, and duplicate instance metadata, are `INVALID_ARGUMENT`; a bound call with a missing,
  single empty or mismatched instance is `FAILED_PRECONDITION`. Every rejection precedes
  domain dispatch. Only a declared unary discovery method may omit the instance, and a value
  supplied there is still bound exactly.
- An admitted `grpc.v1` response echoes exactly one matching request id and the hosting
  instance; streaming hosts publish those initial metadata before blocking on payload I/O
  and clients verify them before using stream payloads. A successful response with absent,
  duplicate or mismatched fence metadata fails with `FAILED_PRECONDITION`. A native
  rejection that carries no response metadata keeps its native failure.
- An internal `grpc.v1` stream (not a session) requires a real caller deadline no later than
  the host's call budget; an absent or longer one is rejected before dispatch. Cancellation
  reaches the native stream.
- There is no common business Protobuf schema; generated methods and payloads stay with their
  domain.

## Admission and drain

Hosts bound accepted connections, in-flight calls, header and body sizes, idle time and call
time; the bounds are options of the host, not limits of robots or algorithms. Persistent
connections are supported and bounded by idle time; clients reuse their transport. Excess
work is refused with `resource_exhausted` before payload work and never queued in a hidden
retry queue.

Shutdown stops admission, lets admitted work finish within a budget, then closes. Graceful
drain and forced close are different: a returned close never leaves owned work running
under a released lease, and a noncooperative handler is reported as failed quiescence, not
pretended away. Handlers own domain payloads and state; an asynchronous reply completes at
most once and may complete from another thread. Fixed host workers and event loops do not
multiply with robot members, and real-time loops never accept sockets, parse HTTP or await
network results.

## Describe and readiness

Process alive does not mean ready. Every service answers a describe in one envelope:

```
{"service":"<name>","api_version":"<v>","instance_id":"<hex>","ready":<bool>,"facts":{...}}
```

XRPC defines the envelope only. `facts` carries the domain dependencies that make the service
valid (for example the bound ROS master URI and its run id, a world generation, frames
flowing) and their meaning belongs to the domain. When such a dependency is replaced, the
service exits or reports `ready: false` with the reason; it never serves a stale binding.

- `http.v1`: `GET /v1/describe`. The optional query `wait_ready_ms=<0..30000>` makes the
  server hold the request (an asynchronous reply, no busy loop) until `ready` becomes true or
  the wait elapses, and then answer with the current envelope; a caller waits for readiness
  with one outstanding call instead of polling.
- `grpc.v1`: the unary `Describe(DescribeRequest{wait_ready_ms}) -> DescribeReply{service,
  api_version, instance_id, ready, facts_json}` with the same semantics.
- `udp.v1`: an ordinary authenticated method, `<service>/Describe`; every reply carries the
  instance anyway.

Go has `xrpc.Describe` and C++ `describe_json` for the envelope. The socket path or address of
a Run-owned service is passed by its owner on the command line; readiness never needs an
external probe process.

## Method addressing

A callable domain operation is named `<service>/<Method>`, for example
`xgc2.chassis.hold/Engage`, and takes and returns JSON. The service is one or more
dot-separated identifiers, the method one identifier, an identifier a letter followed by
letters, digits and underscores, the name at most 128 bytes. Every profile carries the same
name, so a caller can invoke a method without knowing its domain:

| Profile | Carrier of the name |
| --- | --- |
| `udp.v1` | the datagram `method` field, verbatim |
| `http.v1` | `POST /v1/call/<service>/<Method>` with the JSON body; the answer is the JSON result or the XRPC error envelope. Domain hosts may keep REST-style routes for other purposes. |
| `grpc.v1` | the native full method `/<service>/<Method>`; JSON and Protobuf are converted with the descriptors the caller links, or by a codec the caller supplies |

A host that serves a capability for several entities (a world host serving every chassis of
its world) lists it in its describe facts, so a caller resolves "entity X, capability C" to a
service reference generically:

```
{"capabilities":[{"name":"<service>","entities":["<id>",...]}]}
```

The name is the `<service>` part of the method names, an entity is a nonempty identity of at
most 128 bytes without control characters, and neither repeats. Go: `(*xrpc.Dispatcher).CallMethod(ctx, ref,
"<service>/<Method>", body)` dispatches on the profile of `ref` (`xrpc.MethodCall` is the
mapping, `udpx.Profile` lets a Dispatcher compose `udp.v1`; `xrpc.CapabilitiesJSON`,
`Describe.Capabilities` and `Describe.Serves` carry the facts). C++: a `MethodRouter` serves
`/v1/call/...` on an `http.v1` host (`handle_method_call`) and registers the same handlers on a
`udp.v1` server (`add_methods`); `capabilities_json` builds the facts. The same call returns
the same results across both languages and profiles (see the cross-language tests in the
[capability matrix](../docs/capability-matrix.md)).

## Limits are plain configuration

Limits are plain option structs with documented defaults in each language. Nothing is read
from the process environment, no registry resolves them and no deployment overrides them
through hidden names; the composition root of a process sets the fields it needs. They bound
SDK and transport state, not allocations inside a domain handler.

| Limit | Default |
| --- | --- |
| host connections / in-flight calls | 32 / 32 |
| request and response body or message | 1 MiB |
| header bytes | 16 KiB |
| call timeout (host) / header timeout / idle timeout / shutdown budget | 30 s / 5 s / 30 s / 5 s |
| client connections per reference / cached references / reference idle | 16 / 64 / 30 s |
| `grpc.v1` streams per connection | 32 |
| `udp.v1` datagram / call budget / reply cache / in-flight / rate | 1200 bytes / 2 s / 120 s and 1024 entries / 64 / 50 per second, burst 100 per source |

The wire budget is at most 86400000 ms on `http.v1` and 60000 ms on `udp.v1`. Each language
README lists its fields.

## Language packages

All packages carry one product version (0.2.0). Go: `go/`, module
`github.com/XGC-Team/xgc2-xrpc/go`, Go 1.24, native `net/http` and gRPC, no cgo. C++: `cpp/`,
namespace `xgc2::xrpc`, C++17, one shared library per component selected with
`XGC2_XRPC_COMPONENTS` (`unix diagnostics bootstrap http json_http grpc udp`), GNU 7.5 for
`udp` and GNU 9 for the Boost.Beast components, clang 10 and newer. Python: `python/`, import
`xgc2_xrpc`, Python 3.8. Rust: `rust/`, crate `xgc2-xrpc`, Rust 1.85. Node: `node/`, package
`@xgc2/xrpc`, Node 20. TypeScript: `ts/`, package `@xgc2/xrpc-client`, ESM for browsers and
Node 20. Products import the package; they do not copy its listener, parser, deadline or
cleanup code.
