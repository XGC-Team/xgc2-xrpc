/**
 * What the caller may conclude about a call, identical in every XRPC SDK.
 *
 * - `not_sent`: nothing reached the peer; retrying cannot duplicate an effect.
 * - `outcome_unknown`: the request may have been processed but no usable answer
 *   arrived (timeout, lost connection, answer that failed validation).
 * - `response_received`: the peer answered. An error answer or an answer the
 *   client refuses (too large, not JSON) still carries this disposition.
 */
export type Disposition = "not_sent" | "outcome_unknown" | "response_received";

/** The shared XRPC error vocabulary. Servers may add domain codes of their own. */
export type ErrorCode =
  | "invalid_argument"
  | "not_found"
  | "conflict"
  | "resource_exhausted"
  | "deadline_exceeded"
  | "cancelled"
  | "unavailable"
  | "internal"
  | "unauthenticated"
  | "permission_denied";

export interface XrpcErrorInit {
  status?: number | undefined;
  requestId?: string | undefined;
  details?: unknown;
  cause?: unknown;
}

/** A failed call or event stream. `code` is an {@link ErrorCode} or a domain code. */
export class XrpcError extends Error {
  readonly code: ErrorCode | (string & {});
  readonly disposition: Disposition;
  /** HTTP status of the answer, when the peer answered. */
  readonly status: number | undefined;
  readonly requestId: string | undefined;
  /** The `details` member of the server's error body, if it sent one. */
  readonly details: unknown;

  constructor(
    code: XrpcError["code"],
    message: string,
    disposition: Disposition,
    init: XrpcErrorInit = {},
  ) {
    super(message, init.cause === undefined ? undefined : { cause: init.cause });
    this.name = "XrpcError";
    this.code = code;
    this.disposition = disposition;
    this.status = init.status;
    this.requestId = init.requestId;
    this.details = init.details;
  }
}

/** The code for an HTTP error status that carries no standard error envelope. */
export function codeForStatus(status: number): ErrorCode {
  switch (status) {
    case 400:
      return "invalid_argument";
    case 401:
      return "unauthenticated";
    case 403:
      return "permission_denied";
    case 404:
      return "not_found";
    case 409:
      return "conflict";
    case 413:
    case 429:
    case 431:
      return "resource_exhausted";
    case 408:
    case 504:
      return "deadline_exceeded";
    case 499:
      return "cancelled";
    case 502:
    case 503:
      return "unavailable";
    default:
      return "internal";
  }
}

/**
 * Build the error for a non-success answer. A standard envelope
 * `{"error":{"code","message","details"?}}` wins; otherwise the code follows
 * the HTTP status and the message is a bounded excerpt of the body.
 */
export function errorFromAnswer(
  status: number,
  text: string,
  requestId: string | undefined,
): XrpcError {
  let parsed: unknown;
  try {
    parsed = text === "" ? undefined : JSON.parse(text);
  } catch {
    parsed = undefined;
  }
  const envelope =
    isRecord(parsed) && isRecord(parsed["error"]) ? parsed["error"] : undefined;
  const code = envelope && typeof envelope["code"] === "string" && envelope["code"] !== ""
    ? envelope["code"]
    : codeForStatus(status);
  const message =
    envelope && typeof envelope["message"] === "string" && envelope["message"] !== ""
      ? envelope["message"]
      : envelope
        ? `HTTP ${status}`
        : text.trim() !== ""
          ? `HTTP ${status}: ${text.trim().slice(0, 256)}`
          : `HTTP ${status}`;
  return new XrpcError(code, message, "response_received", {
    status,
    requestId,
    details: envelope?.["details"],
  });
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
