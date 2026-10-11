# udp.v1: authenticated datagram calls

`udp.v1` carries low-frequency, on-demand control requests to robots on a weak LAN
without long-lived connections (the first consumer is chassis HOLD). One datagram is one
request and one datagram is one reply. It is for unary calls only and not for high-rate
data. It follows the common mechanics of [runtime.md](runtime.md) (service reference,
instance fence, deadlines, the error vocabulary, request identity, dispositions) and adds
what a datagram carrier needs: authentication of every datagram, deduplication of
retransmissions, and a rate limit.

Implementations: the C++ library `libxgc2_xrpc_udp` (`xgc2/xrpc/udp.hpp`, C++17,
standard library, POSIX sockets and OpenSSL libcrypto only; GNU 7.5 on Ubuntu 18.04 up)
and the Go package `udpx` (client, server, `udptest` fault proxy). A service reference
is `{profile: "udp.v1", endpoint: {kind: "udp", address: "host:port"}, key_id}`.

## Datagram layout

All numbers are in network byte order. The header is 52 bytes, then the method, the body
and a 32-byte tag. A datagram is at most 1200 bytes (the IPv6 minimum MTU, so it is never
fragmented), which leaves 1116 bytes for method and body together. Senders do not exceed
it; a request that does not fit is never sent, and a server whose reply would not fit
sends a short `resource_exhausted` reply instead.

```
off len field
  0   4  magic "XRU1"
  4   1  version = 1
  5   1  type: 1 request, 2 reply
  6   2  flags: bit 0 of a request says expected_instance is set; every other bit is 0
  8   4  key_id (u32), which selects the HMAC key
 12  16  request_id: random, chosen by the client, constant across retransmissions
 28  16  instance: request: the expected server instance, or zero; reply: the server's instance
 44   4  request: timeout_ms (1..60000), the remaining budget when sent; reply: status (see below)
 48   2  method_len: request 1..128; reply 0
 50   2  body_len
 52  ..  method (UTF-8, requests only), then body (UTF-8 JSON: parameters, result or error)
 ..  32  tag = HMAC-SHA256(key[key_id], every preceding byte)
```

A method is 1 to 128 bytes of well-formed UTF-8 without spaces and control characters.
Capability calls use the names of [runtime.md](runtime.md#method-addressing)
(`xgc2.chassis.hold/Engage`). An empty body is allowed; a result body is JSON, and an
empty one reads as the JSON `null`.

Worked examples, produced by an independent implementation (Python `hmac` and `struct`)
and held by the Go test `TestGoldenVectors` and the C++ test `xrpc_cpp_udp_wire`. The key is
the 32 bytes `00 01 ... 1f`, `key_id` 7, `request_id` the bytes `00 .. 0f`, the instance the
bytes `10 .. 1f`.

```
request  method "test.v1/Echo", body {"x":1}, timeout_ms 1500, expected_instance set (103 bytes)
585255310101000100000007000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
000005dc000c0007746573742e76312f4563686f7b2278223a317d09c76d589a3784cfc703d60ccc7e9ef946
f765b0534b882e984df6eb47e68627

reply    status 4 (resource_exhausted), body {"code":"resource_exhausted","message":"m","details":{}} (140 bytes)
585255310102000000000007000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
00000004000000387b22636f6465223a227265736f757263655f657868617573746564222c226d657373616765
223a226d222c2264657461696c73223a7b7d7dad367ae0cb0e986951e2e062f13293da29d6c591219f93a52d6
22ded4080c7e8
```

## Authentication

Keys are exactly 32 random bytes; `key_id` is a public label. The tag authenticates and
protects the integrity of every datagram, request and reply; there is no confidentiality.

- A datagram with a bad magic, version, type or length, an unknown `key_id` or a tag that
  does not verify is dropped without a reply. Nothing is reflected and nothing reveals which
  keys exist. A client that signs with a wrong key therefore sees a lost reply.
- A reply is valid only if its tag verifies with the key of the call, its type is reply and
  its `request_id` matches. Anything else is ignored.
- The transport does not prevent replay beyond the reply-cache window. A domain that exposes
  a method that is not idempotent fences it with the server instance and a monotonic revision
  carried in the parameters (HOLD does: engaging is idempotent, and a release names the
  expected instance and the robot's current revision, so a replayed release never applies
  twice).

### Key file

A key ring is loaded from a text file of lines `<key_id> <base64 key>`:

```
# udp.v1 keys of this service
7 AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=
12 AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=   # trailing comment
```

`key_id` is a decimal number from 0 to 4294967295; the key is canonical standard base64 of
exactly 32 bytes. Blank lines and `#` comments (a whole line or after the key) are ignored.
Anything else, a repeated `key_id` and a key of another length are rejected; the error names
the line and never quotes key material. The file is secret: keep it readable only by the
service's user. Keys are wiped when a ring is destroyed (C++). A key ring can also be built
programmatically in both languages. A `ServiceRef` names its key with `key_id`; zero means
unspecified, and the ring then holds exactly one key (key id 0 can sit in a file but cannot
be named from a reference).

## Server

The server binds one UDP socket and runs one I/O thread (C++) or goroutines (Go). A handler
answers through a reply that completes at most once, before it returns or later from any
thread and before the request's deadline, so a handler may wait for a device tick without
blocking the I/O thread. A reply dropped without completion sends nothing and the client sees
an unknown outcome. A handler that fails before answering is answered `internal`.

An authenticated datagram is handled by these rules, in this order:

1. **Rate limit.** Per source IP address a token bucket (50 requests per second, burst 100,
   at most 4096 sources tracked); excess datagrams are dropped. Only authenticated datagrams
   spend tokens, so an attacker cannot drain a legitimate source's budget.
2. **Structure.** Reserved flag bits, or `timeout_ms` outside 1..60000, are
   `invalid_argument`.
3. **Instance fence.** Every server draws a random 128-bit instance at start (a host whose
   domain already has an instance may supply it, so the transport and the domain agree; zero
   is not allowed). A request that pins an instance the server does not have is answered
   `conflict` without running the handler. Every reply carries the server's instance.
4. **Drain.** A draining server answers new requests `unavailable`, but still resends cached
   replies.
5. **Method.** An unknown method is `not_found`.
6. **Reply cache.** At-most-once execution is keyed by `(key_id, request_id)`. A retransmitted
   request whose reply is cached gets the cached reply bytes and its handler does not run
   again. A request whose handler is still running is ignored; the reply goes out when ready.
   A request that was abandoned (its handler never answered) stays silent, so a late duplicate
   cannot run twice. The cache keeps 1024 entries for 120 seconds from completion; pending
   entries are never evicted, and error replies generated by the transport itself are not
   cached. The capacity must exceed the in-flight limit.
7. **In-flight limit.** At most 64 requests run at once; another one is `resource_exhausted`
   without executing.
8. **Deadline.** The server deadline is the receipt time plus `timeout_ms`, capped by the
   server's call budget (2 s by default, at most 60 s). A handler that misses it produces no
   reply and the client reports `outcome_unknown`.

`Describe` is an ordinary authenticated method (`<service>/Describe`, see
[runtime.md](runtime.md#describe-and-readiness)); this is also how a caller learns the
instance. Shutdown stops admission (new requests get `unavailable`), lets running handlers
answer within a budget and then closes the socket. A reply that outlives its server is
harmless: completing it returns false.

All limits above are plain server options with these defaults.

## Client

A call needs a finite deadline, which counts for at most 60 seconds. The client sends the
datagram at once and sends the same bytes again after waits of 30, 60, 120 and 240 ms and
then every 250 ms (so at about 0, 30, 90, 210, 450, 700, 950 ms and so on) until a valid
reply arrives or the deadline passes. The schedule is a plain client option.

| Disposition | When |
| --- | --- |
| `not_sent` | no datagram could be sent: unusable arguments, a request that does not fit one datagram, no route, no deadline |
| `outcome_unknown` | at least one datagram was sent and no valid reply arrived, a wrong key included |
| `response_received` | a valid reply arrived, with any status; a non-zero status is both a reply and an error |

A call that pins an instance and is answered `conflict` by another instance fails with
`conflict` and `outcome_unknown` ([runtime.md](runtime.md#instance-fence)); other replies
from a foreign instance are ignored. Reusing a request id inside the cache window (Go
`WithRequestID`) returns the cached reply instead of executing again, which is how a caller
recovers the result of a call whose reply was lost; the transport itself never retries beyond
one deadline. The endpoint is `host:port`; the C++ client accepts numeric addresses only,
because the system resolver cannot honor a deadline, while the Go client lets the dialer
resolve a name. A server on a host with several addresses answers from the address the kernel
chooses, so bind to the address callers use.

## Status codes and error bodies

| Status | Code | Status | Code |
| --- | --- | --- | --- |
| 0 | ok | 6 | cancelled |
| 1 | invalid_argument | 7 | unavailable |
| 2 | not_found | 8 | internal |
| 3 | conflict | 9 | unauthenticated (never sent: such a datagram gets no reply) |
| 4 | resource_exhausted | 10 | permission_denied |
| 5 | deadline_exceeded | | |

A non-zero status carries `{"code":"<name>","message":"...","details":{...}}`, with the
message escaped, bytes that are not UTF-8 replaced by U+FFFD and `details` an object.

## Tests

The wire format has golden vectors in both languages, the server rules have tests for every
rule above in both, and the cross-language behavior (loss, duplication and reordering through
the fault proxy, pins, restarts) runs against the C++ server binary
`xgc2-xrpc-udp-interop-server`, whose command line and methods are documented in
`cpp/tests/udp_interop_server.cpp`. [fault-conformance.md](fault-conformance.md) lists the
tests by name; the [capability matrix](../docs/capability-matrix.md) ties them to the cells.
