import { XrpcError, errorFromAnswer, isRecord, type Disposition } from "./errors.js";
import {
  DEFAULT_MAX_BYTES,
  INSTANCE_ID_HEADER,
  MAX_TIMEOUT_MS,
  REQUEST_ID_HEADER,
  TIMEOUT_HEADER,
  checkHeaders,
  isValidId,
  neverReachedPeer,
  newRequestId,
  parseUrl,
  positiveInteger,
} from "./wire.js";

/** A URL, optionally pinned to the expected server instance. */
export interface Target {
  url: string | URL;
  /** Expected `X-Xrpc-Instance-ID`; a different or missing answer fails with `conflict`. */
  instanceId?: string;
}

export interface CallOptions {
  /** Whole-call budget in milliseconds, 1..86400000: request, response head and body. */
  deadlineMs: number;
  /** Request ID; generated (128 random bits, hex) when omitted. */
  requestId?: string;
  signal?: AbortSignal;
  /** Extra request headers, for example `Authorization`. Protocol headers are rejected. */
  headers?: Record<string, string>;
  /** Response body ceiling in bytes. Default 1 MiB. */
  maxResponseBytes?: number;
  /** `fetch` implementation. Default: the global `fetch`. */
  fetch?: typeof fetch;
}

export interface CallSuccess<T = unknown> {
  ok: true;
  /** A returned answer always means the peer replied. */
  disposition: "response_received";
  status: number;
  requestId: string;
  /** `X-Xrpc-Instance-ID` of the answer, if the server sent it. */
  instanceId: string | undefined;
  headers: Headers;
  /** Parsed JSON body; `null` for an empty body. */
  body: T;
}

export interface CallFailure {
  ok: false;
  disposition: Disposition;
  error: XrpcError;
}

/** Every outcome of {@link call}; it does not reject. */
export type CallResult<T = unknown> = CallSuccess<T> | CallFailure;

const OWNED = [
  REQUEST_ID_HEADER,
  TIMEOUT_HEADER,
  INSTANCE_ID_HEADER,
  "host",
  "connection",
  "content-length",
  "content-type",
  "transfer-encoding",
  "accept",
].map((name) => name.toLowerCase());

/**
 * One http.v1 JSON unary call over `fetch`.
 *
 * The request carries `X-Request-ID`, the remaining budget in
 * `X-Xrpc-Timeout-Ms` and, when `target.instanceId` is set, `X-Xrpc-Instance-ID`.
 * A non-2xx answer is parsed as the standard `{"error":{"code","message"}}`
 * envelope. The call is never retried or replayed.
 *
 * `fetch` exposes no connection-level signal, so a failure after `fetch()` was
 * invoked is `outcome_unknown`, except connection errors that Node reports
 * (`ECONNREFUSED`, DNS failure, ...), which prove nothing was sent.
 */
export async function call<T = unknown>(
  target: string | URL | Target,
  method: string,
  body: unknown,
  options: CallOptions,
): Promise<CallResult<T>> {
  const started = performance.now();
  const requestId = options?.requestId ?? newRequestId();
  const fail = (error: XrpcError): CallFailure => ({
    ok: false,
    disposition: error.disposition,
    error,
  });
  let prepared;
  try {
    prepared = prepare(target, method, body, options, requestId);
  } catch (error) {
    return fail(error instanceof XrpcError ? error : unexpected(error, "not_sent", requestId));
  }
  const { url, instanceId, verb, payload, extra, deadlineMs, maxResponseBytes } = prepared;
  const user = options.signal;
  if (user?.aborted) {
    return fail(new XrpcError("cancelled", "call cancelled before it was sent", "not_sent", { requestId }));
  }

  const abort = new AbortController();
  let timedOut = false;
  const onUserAbort = () => abort.abort();
  user?.addEventListener("abort", onUserAbort, { once: true });
  const timer = setTimeout(() => {
    timedOut = true;
    abort.abort();
  }, deadlineMs);
  // After this point a failure may have reached the peer.
  const interrupted = (error: unknown): CallFailure => {
    if (timedOut) {
      return fail(new XrpcError("deadline_exceeded", "call deadline exceeded", "outcome_unknown", { requestId, cause: error }));
    }
    if (abort.signal.aborted) {
      return fail(new XrpcError("cancelled", "call cancelled", "outcome_unknown", { requestId, cause: error }));
    }
    return fail(unexpected(error, "outcome_unknown", requestId));
  };
  let invoked = false;
  try {
    const headers = new Headers(extra);
    headers.set("Accept", "application/json");
    headers.set(REQUEST_ID_HEADER, requestId);
    headers.set(
      TIMEOUT_HEADER,
      String(Math.max(1, Math.floor(deadlineMs - (performance.now() - started)))),
    );
    if (instanceId !== undefined) headers.set(INSTANCE_ID_HEADER, instanceId);
    if (payload !== undefined) headers.set("Content-Type", "application/json");
    const init: RequestInit = {
      method: verb,
      headers,
      signal: abort.signal,
      cache: "no-store",
      redirect: "manual",
    };
    if (payload !== undefined) init.body = payload;

    let response: Response;
    try {
      invoked = true;
      response = await (options.fetch ?? ((...args) => globalThis.fetch(...args)))(url, init);
    } catch (error) {
      if (!timedOut && !abort.signal.aborted && neverReachedPeer(error)) {
        return fail(new XrpcError("unavailable", "peer unreachable", "not_sent", { requestId, cause: error }));
      }
      return interrupted(error);
    }

    const answered = response.headers.get(INSTANCE_ID_HEADER) ?? undefined;
    if (instanceId !== undefined && answered !== instanceId) {
      void response.body?.cancel().catch(() => {});
      return fail(new XrpcError("conflict", "server instance changed or identity missing", "outcome_unknown", { requestId, status: response.status }));
    }
    const echoed = response.headers.get(REQUEST_ID_HEADER);
    if (echoed !== null && echoed !== requestId) {
      void response.body?.cancel().catch(() => {});
      return fail(new XrpcError("internal", "response request ID does not match", "outcome_unknown", { requestId, status: response.status }));
    }

    let text: string;
    try {
      text = await readText(response, maxResponseBytes, abort);
    } catch (error) {
      if (error instanceof XrpcError) return fail(withRequestId(error, requestId, response.status));
      return interrupted(error);
    }

    if (response.type === "opaqueredirect" || (response.status >= 300 && response.status < 400)) {
      return fail(new XrpcError("internal", "unexpected redirect", "response_received", { requestId, status: response.status }));
    }
    if (response.status < 200 || response.status >= 300) {
      return fail(errorFromAnswer(response.status, text, requestId));
    }
    let parsed: unknown = null;
    if (text !== "") {
      try {
        parsed = JSON.parse(text);
      } catch (cause) {
        return fail(new XrpcError("internal", "response is not JSON", "response_received", { requestId, status: response.status, cause }));
      }
    }
    return {
      ok: true,
      disposition: "response_received",
      status: response.status,
      requestId,
      instanceId: answered,
      headers: response.headers,
      body: parsed as T,
    };
  } catch (error) {
    // Nothing above should throw; if it does, report it rather than reject.
    return invoked ? interrupted(error) : fail(unexpected(error, "not_sent", requestId));
  } finally {
    clearTimeout(timer);
    user?.removeEventListener("abort", onUserAbort);
  }
}

interface Prepared {
  url: URL;
  instanceId: string | undefined;
  verb: string;
  payload: string | undefined;
  extra: Array<[string, string]>;
  deadlineMs: number;
  maxResponseBytes: number;
}

function prepare(
  target: string | URL | Target,
  method: string,
  body: unknown,
  options: CallOptions,
  requestId: string,
): Prepared {
  if (!isRecord(options)) {
    throw new XrpcError("invalid_argument", "call options with deadlineMs are required", "not_sent");
  }
  const spec: Target = typeof target === "string" || target instanceof URL ? { url: target } : target;
  const url = parseUrl(spec.url, "target");
  if (spec.instanceId !== undefined && !isValidId(spec.instanceId)) {
    throw new XrpcError("invalid_argument", "instanceId must match [A-Za-z0-9._:-]{1,128}", "not_sent");
  }
  if (!isValidId(requestId)) {
    throw new XrpcError("invalid_argument", "requestId must match [A-Za-z0-9._:-]{1,128}", "not_sent");
  }
  if (typeof method !== "string" || !/^[A-Za-z]{1,32}$/.test(method)) {
    throw new XrpcError("invalid_argument", "method must be an HTTP method token", "not_sent");
  }
  const verb = method.toUpperCase();
  const deadlineMs = options.deadlineMs;
  if (!Number.isInteger(deadlineMs) || deadlineMs < 1 || deadlineMs > MAX_TIMEOUT_MS) {
    throw new XrpcError("invalid_argument", `deadlineMs must be an integer in 1..${MAX_TIMEOUT_MS}`, "not_sent");
  }
  let payload: string | undefined;
  if (body !== undefined) {
    if (verb === "GET" || verb === "HEAD") {
      throw new XrpcError("invalid_argument", `${verb} requests cannot carry a body`, "not_sent");
    }
    try {
      payload = JSON.stringify(body);
    } catch (cause) {
      throw new XrpcError("invalid_argument", "body is not JSON serializable", "not_sent", { cause });
    }
    if (payload === undefined) {
      throw new XrpcError("invalid_argument", "body is not JSON serializable", "not_sent");
    }
  }
  return {
    url,
    instanceId: spec.instanceId,
    verb,
    payload,
    extra: checkHeaders(options.headers, OWNED),
    deadlineMs,
    maxResponseBytes: positiveInteger(options.maxResponseBytes, DEFAULT_MAX_BYTES, "maxResponseBytes"),
  };
}

/** Read the body as UTF-8 text, refusing more than `limit` bytes. */
async function readText(response: Response, limit: number, abort: AbortController): Promise<string> {
  const length = Number(response.headers.get("Content-Length"));
  const tooLarge = () => {
    abort.abort();
    return new XrpcError("resource_exhausted", `response exceeds ${limit} bytes`, "response_received");
  };
  if (Number.isFinite(length) && length > limit) throw tooLarge();
  if (response.body === null) return "";
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  let received = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    received += value.byteLength;
    if (received > limit) throw tooLarge();
    text += decoder.decode(value, { stream: true });
  }
  return text + decoder.decode();
}

function withRequestId(error: XrpcError, requestId: string, status: number): XrpcError {
  return new XrpcError(error.code, error.message, error.disposition, { requestId, status });
}

function unexpected(error: unknown, disposition: Disposition, requestId: string): XrpcError {
  return new XrpcError(
    "unavailable",
    error instanceof Error ? error.message : "request failed",
    disposition,
    { requestId, cause: error },
  );
}
