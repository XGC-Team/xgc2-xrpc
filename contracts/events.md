# http.v1 event streams

An event stream delivers server-to-client events with resume: agent-runtime sessions and
Core live updates are the first consumers. It is a mode of `http.v1` (see
[runtime.md](runtime.md)), carried by server-sent events, so a browser, Node and Go all
speak it. The domain owns the journal, the cursor and the meaning of the events; XRPC owns
the framing, the heartbeat, the resume handshake and the reconnect behavior.

Implementations: Go `httpx.ServeEvents` (server) and `httpx.SubscribeEvents` (client);
TypeScript `events()` of `@xgc2/xrpc-client` (client, browsers and Node 20). There is no
C++, Rust or Python event stream for lack of a consumer.

## Wire

- Request: `GET <path>` with `Accept: text/event-stream`. The client may send
  `Last-Event-ID: <cursor>`, or `?after=<cursor>` where a header is impossible; if both are
  present the header wins, and more than one `Last-Event-ID` is `invalid_argument`. A
  server answers a verb other than GET with 405 and an `Accept` that excludes
  `text/event-stream` with 406 (both `invalid_argument`), and a cursor that fails Go
  `ValidCursor` with 400.
- Response: 200 `Content-Type: text/event-stream`, `Cache-Control: no-cache`, committed
  when the handler starts, so the request is authenticated and validated first. An error
  is therefore an ordinary XRPC error answer (status and envelope of
  [runtime.md](runtime.md#error-vocabulary)) instead of a stream.
- Frame: `id: <cursor>\nevent: <type>\ndata: <json>\n\n`. `id` and `event` are optional;
  a `data` payload spanning lines is written as several `data:` lines, and the payload is
  JSON by convention. Parsers follow the SSE rules: LF, CR or CRLF line ends (also split
  across chunks), a leading byte-order mark, comment lines, UTF-8 sequences split across
  chunks, and `retry:` fields ignored.
- Heartbeat: a comment frame `: hb\n\n` every 15 seconds on an idle stream (configurable).
  It keeps intermediaries from closing the stream, makes a dead client fail a write, and
  lets a client tell a live stream from a dead one: servers heartbeat well within the
  client's idle timeout.
- A resume cursor is opaque, and Go and TypeScript do not share one predicate.
  Go `ValidCursor` is the Go server's check on `Last-Event-ID` and `?after=`, and the
  check on an id a Go server emits. It allows at most 1024 bytes. Each byte must be tab,
  0x20–0x7E, or 0x80–0xFF, so ASCII controls other than tab, and DEL, fail it. The check
  reads UTF-8 bytes, so a code point at U+0080 or above passes. TypeScript does not
  implement `ValidCursor` and has no 1024-byte cap. Before it sends `after` or
  `Last-Event-ID` it uses `isHeaderValue`: UTF-16 code units in tab, U+0020–U+007E, or
  U+0080–U+00FF. That rejects U+0100 and above (the TypeScript client test uses U+20AC),
  which Go accepts. On that send, both refuse ASCII controls other than tab, and DEL.
  The meaning of a cursor that passes belongs to the domain.
- An SSE `id` field that contains NUL is ignored by both parsers. The previous cursor
  stays and the stream continues. That is not an invalid request cursor: a caller-supplied
  cursor containing NUL fails before a request is sent (`invalid_argument`, `not_sent`;
  the Go server answers 400). The Go client ends a live subscription with
  `invalid_argument` / `response_received` only when an id it would adopt fails
  `ValidCursor` and does not contain NUL. TypeScript still adopts that id, delivers the
  event, and fails the next send with `invalid_argument` / `not_sent`.

## Resume, reset and drain

- The client resumes from the `id` of the last complete frame it saw, including a frame
  that carries only an `id`. The server replays from its domain journal after that cursor,
  then continues live. An empty `id:` clears the cursor.
- When the server does not know the cursor (the journal was truncated or restarted) it
  sends `id:` (empty), `event: reset` and `data: {}`, and continues with a fresh
  subscription. A client that receives `reset` drops the cursor it had before that frame,
  tells its consumer that the state it derived from earlier events is stale, and keeps
  reading. An `id` on the reset frame becomes the new cursor; a missing or empty `id`
  leaves it clear. `event: closing` is not delivered, and an `id` on that frame is kept
  for the reconnect.
- On shutdown the server sends `event: closing`, then closes, so a graceful stop never waits
  for a stream. The client consumes `closing`, lets the server close, reconnects with
  backoff and resumes.
- `reset` and `closing` are reserved event types of the transport.

## Server duties

- Every frame pushes the write deadline forward (10 seconds per frame by default), so a
  stream lives as long as its client and its source do and is not cut by the host's write
  timeout. A write that stalls that long ends the stream. There is no forced time window.
- A stream holds one admission slot of its host for its lifetime; size the host's
  connection and in-flight limits for the expected subscribers. Streams belong on edge hosts
  (`ServeEdge`, `RunEdge`) or any `net/http` server, not on internal `Serve` hosts, whose
  responses are bounded by the call budget.
- A source that returns when the client leaves releases its resources; a source that fails
  or emits an invalid event ends the stream with an error.

## Client duties

- Reconnect with full-jitter backoff: a wait uniform in `[0, cap)`, the cap starting at
  500 ms and doubling per consecutive failed attempt up to 5 s, reset by an established
  stream. Reconnect after a clean close, `closing`, a network error, HTTP 408, 429 or 5xx,
  and when no byte (heartbeats included) arrived for 45 seconds, three default heartbeats.
- A failure retrying cannot fix is final: any other 4xx, a 200 that is not
  `text/event-stream`, a frame beyond the size ceiling, a cursor that cannot be sent, a
  certificate that does not verify, or an error raised by the consumer.
- Events are delivered in order, each once, because every reconnect resumes after the last
  cursor. Delivery is at least what the domain journal replays; a consumer that needs
  exactly-once effects deduplicates by cursor.
- Nothing in a stream authorizes a mutation, and nothing is replayed on the consumer's
  behalf. Browsers cannot set headers with `EventSource`, which is why the TypeScript client
  uses `fetch` with a streaming parser: it can send `Authorization` and `Last-Event-ID`.

## Tests

Go server: `go/httpx::TestServeEventsFramesAndResume`, `go/httpx::TestServeEventsResetsAnUnknownCursor`,
`go/httpx::TestServeEventsHeartbeat`, `go/httpx::TestServeEventsRejectsBadRequests`,
`go/httpx::TestStreamOutlivesTwiceTheServerWriteTimeout`,
`go/httpx::TestDrainSendsClosingAndShutdownDoesNotWaitForStreams`. Go client:
`go/httpx::TestSubscribeEventsResumesAcrossConnections`,
`go/httpx::TestSubscribeEventsResumesFromTheLastFrameID`,
`go/httpx::TestSubscribeEventsStopsWhenTheResumeIDCannotBeSent`,
`go/httpx::TestSubscribeEventsDeliversResetAndForgetsTheCursor`,
`go/httpx::TestSubscribeEventsConsumesClosingAndReconnects`,
`go/httpx::TestSubscribeEventsSurvivesAHostRestart`, `go/httpx::TestSubscribeEventsReconnectsAfterSilence`,
`go/httpx::TestReconnectBackoffIsFullJitterFrom500msTo5s`. TypeScript client: the tests of
`ts/test/events.test.mjs` and `ts/test/sse.test.mjs`, for example
`ts/test/events.test.mjs::a dropped connection resumes with Last-Event-ID and delivers each event once`
and `ts/test/sse.test.mjs::the result does not depend on how the text is split into chunks`. The
[capability matrix](../docs/capability-matrix.md) lists them by cell.
