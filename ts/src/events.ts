import { XrpcError, errorFromAnswer } from "./errors.js";
import { SseParser, type SseFrame } from "./sse.js";
import { DEFAULT_MAX_BYTES, checkHeaders, parseUrl, positiveInteger, sleep } from "./wire.js";

/** One event of the stream. */
export interface StreamEvent {
  /** The `event:` type; `"message"` when the frame named none. */
  event: string;
  /** The `data:` payload, `""` when the frame had none. XRPC streams carry JSON. */
  data: string;
  /** The frame's own `id:` (the resume cursor after this event), if it had one. */
  id: string | undefined;
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
  const maxChars = positiveInteger(options.maxEventChars, DEFAULT_MAX_BYTES, "maxEventChars");
  if (typeof options.onEvent !== "function") {
    throw new XrpcError("invalid_argument", "onEvent is required", "not_sent");
  }
  const { signal } = options;
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
    if (cursor.value !== undefined) headers.set("Last-Event-ID", cursor.value);
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
    for (;;) {
      arm();
      let chunk: ReadableStreamReadResult<Uint8Array>;
      try {
        chunk = await reader.read();
      } catch (error) {
        if (user?.aborted) return { established, error: undefined };
        return lost(error);
      }
      clearTimeout(idle);
      if (chunk.done) break;
      for (const frame of parser.feed(decoder.decode(chunk.value, { stream: true }))) {
        closing = (await deliver(frame, cursor, options)) || closing;
        if (user?.aborted) return { established, error: undefined };
      }
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
