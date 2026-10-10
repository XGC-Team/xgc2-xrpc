import type { IncomingMessage, Server, ServerResponse } from "node:http";
import type { Duplex } from "node:stream";

/** Plain limits; every field has a default, listed in the README. */
export interface HostOptions {
  /** Shared diagnostics owner; hosts never create or close one. */
  diagnostics?: Diagnostics;
  tls?: import("node:https").ServerOptions;
  /** Listen on a private Unix socket instead of TCP; start with `host.listen()`. Absolute, canonical, shorter than 108 bytes, inside a directory owned by the effective user with mode 0700. */
  unixPath?: string;
  /** How long a stale-socket probe waits for the existing socket to answer. Default 250. */
  probeTimeoutMs?: number;
  /** Default 32. */
  maxConnections?: number;
  /** Default 32. */
  maxInFlight?: number;
  /** Request body ceiling in bytes. Default 1048576. */
  maxBodyBytes?: number;
  /** Default 16384. */
  maxHeaderBytes?: number;
  /** Response body ceiling in bytes. Default none (RPC hosts: 1048576). */
  maxResponseBytes?: number;
  /** Default 5000. */
  shutdownMs?: number;
  /** Default 5000. */
  headerTimeoutMs?: number;
  requestTimeoutMs?: number;
  /** Default 30000. */
  idleTimeoutMs?: number;
  /** Per-call budget. Default none (RPC hosts: 30000, at most 86400000). */
  callTimeoutMs?: number;
}
export interface Host {
  server: Server;
  /** The Unix socket path of a `unixPath` host, otherwise null. */
  readonly unixPath: string | null;
  /**
   * Bind a `unixPath` host: verify the private directory, reclaim a stale socket
   * (connect refused -> unlink; accepted -> rejects with `code: "EADDRINUSE"`),
   * bind with mode 0600. `close()` removes the socket.
   */
  listen(): Promise<void>;
  close(): Promise<void>;
  onUpgrade(handler: (request: IncomingMessage, socket: Duplex, head: Buffer) => void): void;
  stats(): { connections: number; inFlight: number };
}
export function createHTTPHost(handler: (request: IncomingMessage, response: ServerResponse) => unknown, options?: HostOptions): Host;
export interface RPCContext { requestId: string; instanceId: string; deadline: number; signal: AbortSignal }
export type RPCHandler = (request: IncomingMessage, response: ServerResponse, context: RPCContext) => unknown;
export function createRPCHost(handler: RPCHandler, options: HostOptions & { instanceId: string; discoveryPaths?: string[] }): Host;
export type GrantPurpose = "tls_identity" | "tls_trust" | "authorization";
export type GrantResolver = (handle: string, purpose: GrantPurpose) => unknown;
export interface ServerAuthorizationGrant { authorize(request: IncomingMessage, context: RPCContext): boolean | Promise<boolean> }
export interface ClientAuthorizationGrant { headers: Readonly<Record<string, string>> }
export interface StartupGrantResolver {
  (handle: string, purpose: "authorization"): ServerAuthorizationGrant & ClientAuthorizationGrant;
  (handle: string, purpose: "tls_identity"): { readonly cert: Buffer; readonly key: Buffer };
  (handle: string, purpose: "tls_trust"): { readonly ca: Buffer };
  (handle: string, purpose: GrantPurpose): unknown;
}
export type ClientTLSOptions = Pick<import("node:tls").ConnectionOptions, "ca" | "key" | "cert" | "passphrase"> & {
  minVersion?: "TLSv1.2" | "TLSv1.3"; maxVersion?: "TLSv1.2" | "TLSv1.3"; rejectUnauthorized?: true;
};
export interface BootstrapValue {
  schema_version: 1; target_id: string; service: string; api_version: string;
  profile: "http.v1" | "grpc.v1"; endpoint: { kind: "unix" | "https" | "tls"; address: string };
  runtime_grant: string; authentication: "local_private" | "server_tls" | "mutual_tls";
  secret_handles: { tls_identity?: string; tls_trust?: string; authorization?: string };
  storage_grants: string[];
}
export class BootstrapBinding {
  constructor(value: BootstrapValue);
  readonly schema_version: 1; readonly target_id: string; readonly service: string; readonly api_version: string;
  readonly runtime_grant: string; readonly authentication: BootstrapValue["authentication"];
  readonly secret_handles: Readonly<BootstrapValue["secret_handles"]>; readonly storage_grants: readonly string[];
  readonly endpoint: BootstrapValue["endpoint"];
  readonly profile: BootstrapValue["profile"];
  serviceRef(instanceId: string): ServiceRef;
  resolveCredentials(resolve: GrantResolver, role: "server"): { tls: import("node:https").ServerOptions; authorization: ServerAuthorizationGrant };
  resolveCredentials(resolve: GrantResolver, role: "client"): { tls: ClientTLSOptions; authorization: ClientAuthorizationGrant };
}
export function readBootstrapBinding(explicitGrantedPath: string): BootstrapBinding;
export interface BootstrapInputValue {
  schema_version: 1; binding: BootstrapValue;
  grants: Record<string, { kind: "tls_identity"; cert_file: string; key_file: string } | { kind: "tls_trust"; ca_file: string } | { kind: "bearer"; token_file: string }>;
  application?: unknown;
}
export function loadBootstrapInput(explicitGrantedPath: string, options: { role: "server" | "client" }): { readonly binding: BootstrapBinding; readonly resolveGrant: StartupGrantResolver; readonly application: unknown };
export function createBoundHTTPHost(handler: RPCHandler, options: HostOptions & { binding: BootstrapBinding; instanceId: string; resolveGrant: GrantResolver; discoveryPaths?: string[] }): Host;
export function createFetchHost(handler: (request: Request) => Response | Promise<Response>, options?: HostOptions): Host;
/** 128 random bits as 32 lowercase hex characters; call once per process start. */
export function newInstanceId(): string;
export function proxyWebSocket(request: IncomingMessage, socket: Duplex, head: Buffer, address: string, options?: {
  maxPayload?: number;
  maxPendingMessages?: number;
  handshakeTimeoutMs?: number;
  sendTimeoutMs?: number;
  forwardHeaders?: string[];
  onOpen?: (info: { protocol: string }) => void;
}): { close(): void };
export interface DiagnosticStatus {
  readonly state: string; readonly failed: boolean; readonly workerStarted: boolean; readonly workerAlive: boolean;
  readonly pendingRecords: number; readonly queueCapacity: number; readonly maxRecordBytes: number;
  readonly eventCounts: Readonly<Record<string, number>>; readonly admitted: number; readonly written: number;
  readonly dropped: number; readonly filtered: number; readonly level: string; readonly format: string;
}
export class Diagnostics {
  constructor(options: { sink: { kind: "supervisor_stderr"; rotationOwner: "supervisor" }; level?: "trace" | "debug" | "info" | "warn" | "error"; format?: "json" | "text"; maxPendingRecords?: number; maxRecordBytes?: number; closeTimeoutMs?: number; repeatIntervalMs?: number });
  emit(event: string, fields?: Readonly<Record<string, unknown>>): boolean;
  status(): DiagnosticStatus;
  close(options?: { timeoutMs?: number }): Promise<void>;
}
export class DiagnosticCloseError extends Error { readonly code: "deadline_exceeded" }
export class DiagnosticSinkError extends Error { readonly code: "unavailable" }
export interface ServiceRef {
  target_id: string; service: string; api_version: string; instance_id: string;
  profile: "http.v1" | "grpc.v1"; endpoint: { kind: "unix" | "https" | "tls"; address: string };
}
export type Disposition = "not_sent" | "outcome_unknown" | "response_received";
/** A call that produced no answer. `code` is one of the shared XRPC error codes. */
export class TransportError extends Error {
  code: string;
  /** `not_sent`: nothing reached the peer. `outcome_unknown`: it may have run, no usable answer. `response_received`: the peer answered and the client refused the answer (for example one over the size limit). */
  disposition: Disposition;
}
export interface CallOptions {
  timeoutMs: number; requestId?: string; method?: string; signal?: AbortSignal;
  headers?: Record<string, string>; body?: string | Uint8Array; json?: unknown;
}
/** Plain limits; defaults in parentheses. A returned answer always has `disposition: "response_received"`; HTTP error statuses are answers, not failures. */
export class HTTPClient {
  constructor(options?: {
    diagnostics?: Diagnostics; localTarget?: string; tls?: ClientTLSOptions;
    /** Connections per reference (16). */
    maxConnections?: number;
    /** Distinct references (64). */
    maxReferences?: number;
    /** Concurrent calls (32). */
    maxInFlight?: number;
    /** Bytes (1048576). */
    maxRequestBytes?: number;
    /** Bytes (1048576). */
    maxResponseBytes?: number;
    /** Bytes (16384). */
    maxHeaderBytes?: number;
    /** Longest call budget in ms, at most 86400000 (30000). */
    callTimeoutMs?: number;
    /** Idle time before an unused reference is released, ms (30000). */
    referenceIdleTimeoutMs?: number;
  });
  call(ref: ServiceRef, path: string, options: CallOptions): Promise<{ disposition: "response_received"; status: number; headers: import("node:http").IncomingHttpHeaders; body: Buffer; requestId: string }>;
  stream(ref: ServiceRef, path: string, options: CallOptions): Promise<{ disposition: "response_received"; status: number; headers: import("node:http").IncomingHttpHeaders; body: import("node:stream").Readable; requestId: string; close(): void }>;
  close(): void;
  stats(): { references: number; inFlight: number; closed: boolean };
}
