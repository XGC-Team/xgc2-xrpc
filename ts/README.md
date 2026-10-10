# @xgc2/xrpc-client

XRPC `http.v1` client for browsers and Node 20 or newer: JSON unary calls and
resumable event streams over `fetch`. ESM, TypeScript declarations, no runtime
dependencies, no Unix sockets (use an `http(s)` URL).

```sh
npm ci
npm test          # builds dist/ with the pinned TypeScript, then runs node --test
npm run build     # dist/ only; dist/ is not committed
```

## Unary calls

```ts
import { call } from "@xgc2/xrpc-client";

const result = await call(
  { url: "https://core.example/v1/sessions", instanceId: "boot:7f3a" }, // or a plain URL string
  "POST",
  { title: "survey" },
  { deadlineMs: 5000, requestId: "op:42", signal, headers: { Authorization: `Bearer ${token}` } },
);
if (result.ok) {
  use(result.body); // parsed JSON, null for an empty body
} else {
  const { code, message, status, details } = result.error; // XrpcError
  if (result.disposition === "outcome_unknown") reconcile(); // never replayed for you
}
```

`call(target, method, body, options)` never rejects; every outcome is a
`CallResult` with a `disposition`:

| `disposition` | When |
|---|---|
| `response_received` | The server answered: `ok: true` for 2xx, otherwise `ok: false` with the parsed error. Also an answer the client refuses: larger than `maxResponseBytes` (`resource_exhausted`), not JSON (`internal`), unexpected redirect. |
| `outcome_unknown` | `fetch` was invoked and no usable answer arrived: deadline (`deadline_exceeded`), abort (`cancelled`), network failure (`unavailable`), answer from another instance (`conflict`) or with another request ID. |
| `not_sent` | Nothing left the machine: invalid arguments (`invalid_argument`), an already aborted signal, or, in Node, a refused or unresolvable connection. Browsers expose no connection-level detail, so a browser network failure is always `outcome_unknown`. |

Requests carry `X-Request-ID` (generated as 128 random bits in hex when
`requestId` is omitted), `X-Xrpc-Timeout-Ms` (the remaining budget) and, when
the target names an `instanceId`, `X-Xrpc-Instance-ID`; the answer must echo
that instance. `deadlineMs` (1 to 86400000) is required and covers the whole
call including the body. Calls are never retried and redirects are not
followed. Caller `headers` cannot override `X-Request-ID`, `X-Xrpc-*`,
`Content-Type`, `Content-Length`, `Host`, `Accept` or `Connection`.

A non-2xx answer is read as the standard envelope
`{"error":{"code","message","details"?}}`. Codes are `invalid_argument`,
`not_found`, `conflict`, `resource_exhausted`, `deadline_exceeded`,
`cancelled`, `unavailable`, `internal`, `unauthenticated` and
`permission_denied`; a server may add domain codes, which pass through. An
answer without an envelope gets the code its HTTP status implies (401
`unauthenticated`, 403 `permission_denied`, 404 `not_found`, 409 `conflict`,
413, 429 and 431 `resource_exhausted`, 408 and 504 `deadline_exceeded`, 502 and
503 `unavailable`, anything else `internal`).

Cross-origin use needs the server to allow the request headers above in
`Access-Control-Allow-Headers` and to list `X-Request-ID` and
`X-Xrpc-Instance-ID` in `Access-Control-Expose-Headers`.

TLS trust is the platform's: browsers and Node use their system trust store. For
a private CA or client certificates in Node, pass a `fetch` built on an undici
`Agent` as `options.fetch`; this package adds no dependency for that.

## Event streams

```ts
import { events } from "@xgc2/xrpc-client";

const abort = new AbortController();
await events("/v1/sessions/s1/events", {
  after: savedCursor,
  signal: abort.signal,
  onEvent: ({ event, data, id }) => { apply(JSON.parse(data)); if (id) savedCursor = id; },
  onReset: () => dropLocalState(),       // the server no longer has the cursor
  onClosing: () => showReconnecting(),   // the server is draining
  onOpen: () => setStatus("connected"),
  onError: (error, next) => setStatus(`lost: retrying in ${next.delayMs} ms`),
});
```

`events(url, options)` sends `GET` with `Accept: text/event-stream` and, when it
has a cursor, `Last-Event-ID`. It returns a promise that resolves when
`options.signal` aborts and rejects with an `XrpcError` only for final errors.

* **Frames.** `id:`, `event:` and `data:` fields (multi-line data joined with
  `\n`), comments (`: hb` heartbeats are ignored), `\n`, `\r` and `\r\n` line
  ends, a leading BOM, and chunks split anywhere, even inside a UTF-8 sequence
  or a CRLF pair. `event` defaults to `"message"`. A frame with an `event:`
  but no `data:` is still delivered (control frames); `retry:` is ignored.
* **Resume.** The cursor is the `id:` of the last frame (an empty `id:` clears
  it) and starts at `options.after`. Every reconnect sends it as
  `Last-Event-ID`; each event is delivered once, in order. Callbacks are awaited
  one at a time, and an exception in one stops the stream and rejects.
* **Reset.** `event: reset` means the server no longer has the cursor. The
  client forgets it (or takes the reset frame's `id:`), calls `onReset` and
  keeps reading.
* **Closing.** `event: closing` means the server is draining. The client calls
  `onClosing`, lets the server close, then reconnects with backoff and resumes.
  Without a dedicated callback, `reset` and `closing` are delivered to
  `onEvent` so they are never lost.
* **Reconnects.** After a clean close, `closing`, a network error, an idle
  period without any bytes (`idleTimeoutMs`, default 45 s, heartbeats count),
  or an HTTP 408, 429 or 5xx, the client waits a full-jitter delay: uniform in
  `[0, cap)` where `cap` starts at `backoff.initialMs` (500 ms) and doubles per
  consecutive attempt that never established a stream up to `backoff.maxMs`
  (5 s); an established stream resets it. `onError` reports each interruption
  with the delay.
* **Final errors.** Any other HTTP error (400, 401, 403, 404, ...), a 200 that
  is not `text/event-stream`, and a frame larger than `maxEventChars` reject
  with an `XrpcError` (the server's envelope code and status when it sent one).
* **Abort.** `signal` stops the request, the reader and any backoff wait.

Options not shown: `headers` (for example `Authorization`; `Accept` and
`Last-Event-ID` are owned by the client), `backoff.random` (jitter source),
`fetch` (replace the global `fetch`).
