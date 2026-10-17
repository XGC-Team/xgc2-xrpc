import { XrpcError, errorFromAnswer } from "./errors.js";
import { SseParser, type SseFrame } from "./sse.js";
import { DEFAULT_MAX_BYTES, checkHeaders, isHeaderValue, parseUrl, positiveInteger, sleep } from "./wire.js";

/** One event of the stream. */
export interface StreamEvent {
  /** The `event:` type; `"message"` when the frame named none. */
  event: string;
  /** The `data:` payload, `""` when the frame had none. XRPC streams carry JSON. */
  data: string;
  /** The frame's own `id:` (the resume cursor after this event), if it had one. */
  id: string | undefined;
  /** Only in onEvents batches: a frame carrying a cursor and no event/data.
   * It commits the cursor without invoking an application event callback. */
  cursorOnly?: true;
}

export interface BackoffOptions {
  /** Delay cap after a stream ended or the first failed attempt. Default 500 ms. */
  initialMs?: number;
  /** Largest delay cap. Default 5000 ms. */
  maxMs?: number;
  /** Source of the jitter in [0, 1). Default `Math.random`. */
  random?: () => number;
}

export interface ReconnectInfo {
  /** Wait before the next attempt. */
  delayMs: number;
  /** Consecutive attempts that never established a stream. */
  failures: number;
}

export interface EventsOptions {
  /** Resume cursor: the client sends it as `Last-Event-ID` on the first request. */
  after?: string;
  /** Stops the stream; {@link events} then resolves. */
  signal?: AbortSignal;
  /** Extra request headers, for example `Authorization`. */
  headers?: Record<string, string>;
  /**
   * Called for every event, in order, and awaited. An exception (or rejection)
   * stops the stream and rejects {@link events}. `reset` and `closing` events
   * go here only when their dedicated callback is missing.
   */
  onEvent: (event: StreamEvent) => void | Promise<void>;
  /** Optional ordered delivery in bounded groups (one read by default). Awaited
   * before reading more bytes. Dedicated reset/closing callbacks are barriers.
   * Includes cursor-only frames as message entries. Unlike onEvent, an empty
   * id is preserved so batch owners can commit cursor clearing after applying.
   * When absent, the original per-event delivery path is unchanged. */
  onEvents?: (events: readonly StreamEvent[]) => void | Promise<void>;
  /** Maximum events passed to onEvents. Default 128. */
  maxEventsPerBatch?: number;
  /** UTF-16 character budget for one batch, default 65536. A legal event larger
   * than this budget is delivered alone (the maxEventChars ceiling still applies). */
  maxBatchChars?: number;
  /** Opt in to cross-read batching. Flush within this many milliseconds of the
   * first pending event, or sooner at a count/character/control/EOF boundary.
   * Requires onEvents. Omitted: flush at each reader chunk as before. */
  maxBatchWaitMs?: number;
  /**
   * `event: reset`: the server no longer has the cursor, so the client's state
   * is stale. The client forgets the cursor (or takes the reset frame's `id`),
   * then calls this; the stream continues.
   */
  onReset?: (event: StreamEvent) => void | Promise<void>;
  /** `event: closing`: the server is draining; the client reconnects with backoff after it closes. */
  onClosing?: (event: StreamEvent) => void | Promise<void>;
  /** A stream was established (HTTP 200 `text/event-stream`). */
  onOpen?: () => void;
  /** The stream was lost or could not be established; a reconnect follows. */
  onError?: (error: XrpcError, next: ReconnectInfo) => void;
  /** Reconnect delays: full jitter, capped at 500 ms growing to 5 s. */
  backoff?: BackoffOptions;
  /** Reconnect when no byte (heartbeats included) arrives for this long. Default 45000 ms, three default heartbeats. 0 disables. */
  idleTimeoutMs?: number;
  /** Ceiling for one event frame in characters. Default 1048576. */
  maxEventChars?: number;
  /** `fetch` implementation. Default: the global `fetch`. */
  fetch?: typeof fetch;
}

const OWNED = ["accept", "last-event-id", "host", "connection", "content-length", "transfer-encoding"];
const DEFAULT_IDLE_MS = 45_000;

interface Ended {
  /** The server answered 200 `text/event-stream`. */
  established: boolean;
  /** Why the connection ended; `undefined` after an abort or an announced `closing`. */
  error: XrpcError | undefined;
}

/**
 * Follow an http.v1 event stream (`GET`, `Accept: text/event-stream`) until
 * `options.signal` aborts, reconnecting and resuming as needed.
 *
 * The cursor is the `id:` of the last frame. Every reconnect sends it as
 * `Last-Event-ID`; the server replays after it or sends `event: reset`.
 * After a stream ends (clean close, `event: closing`, network error, idle
 * timeout, HTTP 408/429/5xx) the client waits a full-jitter backoff and
 * reconnects. The wait is uniform in [0, cap) where the cap starts at
 * `backoff.initialMs` (500 ms) and doubles per consecutive failed attempt up
 * to `backoff.maxMs` (5 s). Other HTTP errors, a non-`text/event-stream`
 * answer and an oversized frame are final: the promise rejects with an
 * {@link XrpcError}. Resolves after an abort.
 */
export async function events(url: string | URL, options: EventsOptions): Promise<void> {
  const target = parseUrl(url, "url");
  const headers = checkHeaders(options.headers, OWNED);
  const initialMs = positiveInteger(options.backoff?.initialMs, 500, "backoff.initialMs");
  const maxMs = positiveInteger(options.backoff?.maxMs, 5000, "backoff.maxMs");
  const random = options.backoff?.random ?? Math.random;
  const idleMs = options.idleTimeoutMs === 0 ? 0 : positiveInteger(options.idleTimeoutMs, DEFAULT_IDLE_MS, "idleTimeoutMs");
  positiveInteger(options.maxEventsPerBatch, 128, "maxEventsPerBatch");
  positiveInteger(options.maxBatchChars, 65_536, "maxBatchChars");
  if (options.maxBatchWaitMs !== undefined) {
    positiveInteger(options.maxBatchWaitMs, 1, "maxBatchWaitMs");
    if (!options.onEvents) throw new XrpcError("invalid_argument", "maxBatchWaitMs requires onEvents", "not_sent");
  }
  const maxChars = positiveInteger(options.maxEventChars, DEFAULT_MAX_BYTES, "maxEventChars");
  if (options.onEvents !== undefined && typeof options.onEvents !== "function") {
    throw new XrpcError("invalid_argument", "onEvents must be a function", "not_sent");
  }
  if (typeof options.onEvent !== "function") {
    throw new XrpcError("invalid_argument", "onEvent is required", "not_sent");
  }
  const { signal } = options;
  if (options.after !== undefined && !isHeaderValue(options.after)) {
    throw new XrpcError("invalid_argument", "after must be a string usable as a Last-Event-ID header value", "not_sent");
  }
  let cursor = options.after === "" ? undefined : options.after;
  let failures = 0;

  while (!signal?.aborted) {
    const stream: Cursor = { value: cursor };
    const ended = await attempt(target, headers, options, stream, idleMs, maxChars);
    cursor = stream.value;
    if (signal?.aborted) return;
    failures = ended.established ? 0 : failures + 1;
    const delayMs = reconnectDelay(failures, initialMs, maxMs, random);
    if (ended.error !== undefined) options.onError?.(ended.error, { delayMs, failures });
    await sleep(delayMs, signal);
  }
}

interface Cursor {
  value: string | undefined;
}

/**
 * Full-jitter wait before a reconnect: uniform in [0, cap). The cap is
 * `initialMs` after an established stream or a first failure, then doubles per
 * further consecutive failure up to `maxMs`.
 */
export function reconnectDelay(
  failures: number,
  initialMs: number,
  maxMs: number,
  random: () => number,
): number {
  const cap = Math.min(maxMs, initialMs * 2 ** Math.max(0, failures - 1));
  return Math.floor(random() * cap);
}

/** One connection. Throws only for final errors; everything else is reported in `Ended`. */
async function attempt(
  url: URL,
  extra: Array<[string, string]>,
  options: EventsOptions,
  cursor: Cursor,
  idleMs: number,
  maxChars: number,
): Promise<Ended> {
  const abort = new AbortController();
  const user = options.signal;
  const onUserAbort = () => abort.abort();
  user?.addEventListener("abort", onUserAbort, { once: true });
  let established = false;
  let closing = false;
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let idle: ReturnType<typeof setTimeout> | undefined;
  let idled = false;
  const arm = () => {
    clearTimeout(idle);
    if (idleMs > 0) {
      idle = setTimeout(() => {
        idled = true;
        abort.abort();
      }, idleMs);
    }
  };
  const lost = (error: unknown): Ended => {
    const reason = idled
      ? new XrpcError("unavailable", `no data for ${idleMs} ms`, "outcome_unknown")
      : new XrpcError("unavailable", error instanceof Error ? error.message : "stream failed", "outcome_unknown", { cause: error });
    return { established, error: reason };
  };
  try {
    const headers = new Headers(extra);
    headers.set("Accept", "text/event-stream");
    if (cursor.value !== undefined) {
      // A server may send ids a header cannot carry; resuming is then impossible.
      if (!isHeaderValue(cursor.value)) {
        throw new XrpcError("invalid_argument", "the stream cursor cannot be sent as a Last-Event-ID header", "not_sent");
      }
      headers.set("Last-Event-ID", cursor.value);
    }
    arm();
    let response: Response;
    try {
      response = await (options.fetch ?? ((...args) => globalThis.fetch(...args)))(url, {
        method: "GET",
        headers,
        signal: abort.signal,
        cache: "no-store",
        redirect: "manual",
      });
    } catch (error) {
      if (user?.aborted) return { established, error: undefined };
      return lost(error);
    }
    clearTimeout(idle);

    const type = response.headers.get("Content-Type") ?? "";
    if (response.status !== 200 || !/^text\/event-stream\s*(;|$)/i.test(type)) {
      const text = await boundedText(response, 4096).catch(() => "");
      if (response.status === 408 || response.status === 429 || response.status >= 500) {
        return { established, error: errorFromAnswer(response.status, text, undefined) };
      }
      if (response.status >= 200 && response.status < 300) {
        throw new XrpcError(
          "internal",
          `expected text/event-stream, got ${type === "" ? "no content type" : type}`,
          "response_received",
          { status: response.status },
        );
      }
      throw errorFromAnswer(response.status, text, undefined);
    }
    if (response.body === null) {
      return lost(new Error("stream has no body"));
    }

    established = true;
    options.onOpen?.();
    reader = response.body.getReader();
    const decoder = new TextDecoder();
    const parser = new SseParser(maxChars);
    const timed = options.onEvents && options.maxBatchWaitMs !== undefined ? batchDelivery(cursor, options) : undefined;
    let pendingRead: Promise<ReadableStreamReadResult<Uint8Array>> | undefined;
    for (;;) {
      if (user?.aborted) return { established, error: undefined };
      if (timed?.deadline !== undefined && performance.now() >= timed.deadline) {
        await timed.flush();
        continue;
      }
      arm();
      let chunk: ReadableStreamReadResult<Uint8Array> | undefined;
      try {
        if (!timed) chunk = await reader.read();
        else {
          // A deadline may win while this read is still pending. Keep that
          // single read for after ACK instead of starting another one.
          pendingRead ??= reader.read();
          const read = await readBeforeDeadline(pendingRead, timed.deadline);
          chunk = read;
          if (read !== undefined) pendingRead = undefined;
        }
      } catch (error) {
        if (user?.aborted) return { established, error: undefined };
        clearTimeout(idle);
        if (timed) await timed.flush();
        return lost(error);
      }
      clearTimeout(idle);
      if (chunk === undefined) {
        // Application failures are final, not reader failures: never retry
        // this pending batch through the network-error recovery branch.
        await timed!.flush();
        continue;
      }
      if (chunk.done) {
        if (timed) await timed.flush();
        break;
      }
      const frames = parser.feed(decoder.decode(chunk.value, { stream: true }));
      if (options.onEvents) {
        closing = (await (timed ? timed.push(frames) : deliverBatch(frames, cursor, options))) || closing;
      } else {
        for (const frame of frames) {
          closing = (await deliver(frame, cursor, options)) || closing;
          if (user?.aborted) return { established, error: undefined };
        }
      }
      if (user?.aborted) return { established, error: undefined };
    }
    return {
      established,
      error: closing ? undefined : new XrpcError("unavailable", "stream closed by the server", "outcome_unknown"),
    };
  } finally {
    clearTimeout(idle);
    user?.removeEventListener("abort", onUserAbort);
    abort.abort();
    void reader?.cancel().catch(() => {});
  }
}

/** Apply one frame: move the cursor, route control events. Returns true for `closing`. */
async function deliver(frame: SseFrame, cursor: Cursor, options: EventsOptions): Promise<boolean> {
  const type = frame.event === undefined || frame.event === "" ? "message" : frame.event;
  if (type === "reset") cursor.value = undefined;
  if (frame.id !== undefined) cursor.value = frame.id === "" ? undefined : frame.id;
  if (frame.event === undefined && frame.data === undefined) return false;
  const event: StreamEvent = {
    event: type,
    data: frame.data ?? "",
    id: frame.id === "" ? undefined : frame.id,
  };
  if (type === "reset" && options.onReset) await options.onReset(event);
  else if (type === "closing" && options.onClosing) await options.onClosing(event);
  else await options.onEvent(event);
  return type === "closing";
}

/** Same framing/control semantics as deliver, with bounded ordinary groups. */
async function deliverBatch(frames: SseFrame[], cursor: Cursor, options: EventsOptions): Promise<boolean> {
  const batch = batchDelivery(cursor, options);
  const closing = await batch.push(frames);
  await batch.flush();
  return closing;
}

/** One attempt's bounded collector; callback completion is its cursor barrier. */
function batchDelivery(cursor: Cursor, options: EventsOptions) {
  const limit = options.maxEventsPerBatch ?? 128;
  let deadline: number | undefined;
  let pending: StreamEvent[] = [];
  let pendingChars = 0;
  const charLimit = options.maxBatchChars ?? 65_536;
  let lastCursor = cursor.value;
  const flush = async () => {
    if (options.signal?.aborted) { pending = []; pendingChars = 0; deadline = undefined; return; }
    if (pending.length === 0) return;
    await options.onEvents!(pending);
    cursor.value = lastCursor;
    pending = [];
    pendingChars = 0;
    deadline = undefined;
  };
  const push = async (frames: SseFrame[]) => {
    let closing = false;
    for (const frame of frames) {
      if (options.signal?.aborted) break;
      const type = frame.event || "message";
      if ((type === "reset" && options.onReset) || (type === "closing" && options.onClosing)) {
        await flush();
        if (options.signal?.aborted) break;
        closing = (await deliver(frame, cursor, options)) || closing;
        lastCursor = cursor.value;
        continue;
      }
      const timedControl = options.maxBatchWaitMs !== undefined && (type === "reset" || type === "closing");
      if (timedControl) {
        await flush();
        if (options.signal?.aborted) break;
      }
      const chars = (frame.data?.length ?? 0) + (frame.event?.length ?? 0) + (frame.id?.length ?? 0);
      if (pending.length && pendingChars + chars > charLimit) {
        await flush();
        if (options.signal?.aborted) break;
      }
      // Include cursor-only frames in the batch as message events. Clients using
      // onEvents can then commit their cursor only after earlier events applied.
      if (!pending.length && options.maxBatchWaitMs !== undefined) deadline = performance.now() + options.maxBatchWaitMs;
      pendingChars += chars;
      pending.push({ event: type, data: frame.data ?? "", id: frame.id,
        ...(frame.event === undefined && frame.data === undefined ? { cursorOnly: true as const } : {}) });
      if (type === "reset") lastCursor = undefined;
      if (frame.id !== undefined) lastCursor = frame.id || undefined;
      closing = type === "closing" || closing;
      if (pending.length === limit || pendingChars >= charLimit || timedControl) await flush();
    }
    return closing;
  };
  return { push, flush, get deadline() { return deadline; } };
}

/** Race one retained read with a pending batch's first-event deadline. */
async function readBeforeDeadline(read: Promise<ReadableStreamReadResult<Uint8Array>>, deadline: number | undefined) {
  if (deadline === undefined) return read;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      read,
      new Promise<undefined>(resolve => { timer = setTimeout(resolve, Math.max(0, Math.ceil(deadline - performance.now()))); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function boundedText(response: Response, limit: number): Promise<string> {
  if (response.body === null) return "";
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  let received = 0;
  try {
    while (received < limit) {
      const { done, value } = await reader.read();
      if (done) break;
      received += value.byteLength;
      text += decoder.decode(value, { stream: true });
    }
  } finally {
    void reader.cancel().catch(() => {});
  }
  return text;
}
