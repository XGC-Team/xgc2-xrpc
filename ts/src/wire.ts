import { XrpcError } from "./errors.js";

export const REQUEST_ID_HEADER = "X-Request-ID";
export const TIMEOUT_HEADER = "X-Xrpc-Timeout-Ms";
export const INSTANCE_ID_HEADER = "X-Xrpc-Instance-ID";

/** Longest call budget the wire carries. */
export const MAX_TIMEOUT_MS = 86_400_000;
/** Default ceiling for response bodies and single event frames: 1 MiB. */
export const DEFAULT_MAX_BYTES = 1 << 20;

const ID = /^[A-Za-z0-9._:-]{1,128}$/;
const HEADER_NAME = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
// Header values are ISO-8859-1 text without control characters other than tab.
const HEADER_VALUE = /^[\t\x20-\x7e\x80-\xff]*$/;

/** Request and instance IDs share one grammar in every SDK. */
export function isValidId(value: unknown): value is string {
  return typeof value === "string" && ID.test(value);
}

/** 128 random bits as 32 lowercase hex characters. */
export function newRequestId(): string {
  const bytes = new Uint8Array(16);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** Resolve a target URL; origin-relative URLs resolve against the page in a browser. */
export function parseUrl(value: string | URL, what: string): URL {
  let url: URL;
  try {
    const base = (globalThis as { location?: { href?: string } }).location?.href;
    url = value instanceof URL ? new URL(value.href) : new URL(value, base);
  } catch (cause) {
    throw new XrpcError("invalid_argument", `${what} is not a valid URL`, "not_sent", { cause });
  }
  if (
    (url.protocol !== "http:" && url.protocol !== "https:") ||
    url.hostname === "" ||
    url.username !== "" ||
    url.password !== ""
  ) {
    throw new XrpcError(
      "invalid_argument",
      `${what} must be an http(s) URL without credentials`,
      "not_sent",
    );
  }
  url.hash = "";
  return url;
}

/** Whether `value` can travel as an HTTP header value. */
export function isHeaderValue(value: unknown): value is string {
  return typeof value === "string" && HEADER_VALUE.test(value);
}

/**
 * Validate caller headers. Names the protocol owns are rejected so a caller
 * cannot break request correlation, the deadline or framing.
 */
export function checkHeaders(
  headers: Record<string, string> | undefined,
  owned: readonly string[],
): Array<[string, string]> {
  const result: Array<[string, string]> = [];
  const seen = new Set<string>();
  for (const [name, value] of Object.entries(headers ?? {})) {
    const lower = name.toLowerCase();
    if (!HEADER_NAME.test(name) || !isHeaderValue(value)) {
      throw new XrpcError("invalid_argument", `invalid header ${JSON.stringify(name)}`, "not_sent");
    }
    if (seen.has(lower)) {
      throw new XrpcError("invalid_argument", `duplicate header ${name}`, "not_sent");
    }
    seen.add(lower);
    if (owned.includes(lower)) {
      throw new XrpcError("invalid_argument", `header ${name} is owned by the transport`, "not_sent");
    }
    result.push([name, value]);
  }
  return result;
}

/** Positive integer option, or `fallback` when absent. */
export function positiveInteger(value: number | undefined, fallback: number, name: string): number {
  const chosen = value ?? fallback;
  if (!Number.isSafeInteger(chosen) || chosen <= 0) {
    throw new XrpcError("invalid_argument", `${name} must be a positive integer`, "not_sent");
  }
  return chosen;
}

/** Resolve after `ms`, or as soon as `signal` aborts. Never rejects. */
export function sleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener("abort", done, { once: true });
  });
}

/**
 * Connection errors that prove no request byte left the machine. Node's fetch
 * (undici) exposes them as `error.cause.code`; browsers expose nothing, so a
 * browser failure after `fetch()` is always `outcome_unknown`.
 */
const NOT_SENT_CODES = new Set([
  "ECONNREFUSED",
  "ENOTFOUND",
  "EAI_AGAIN",
  "ENETUNREACH",
  "EHOSTUNREACH",
]);

export function neverReachedPeer(error: unknown): boolean {
  const cause = (error as { cause?: { code?: unknown } } | null)?.cause;
  return typeof cause?.code === "string" && NOT_SENT_CODES.has(cause.code);
}
