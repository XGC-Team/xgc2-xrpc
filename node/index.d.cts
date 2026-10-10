import type { IncomingMessage, Server, ServerResponse } from "node:http";
import type { Duplex } from "node:stream";

export interface HostOptions {
  policy?: PolicyView;
  tls?: import("node:https").ServerOptions;
  /** Listen on a private Unix socket instead of TCP; start with `host.listen()`. Absolute, canonical, shorter than 108 bytes, inside a directory owned by the effective user with mode 0700. */
  unixPath?: string;
  /** How long a stale-socket probe waits for the existing socket to answer. Default 250. */
  probeTimeoutMs?: number;
  maxConnections?: number;
  maxInFlight?: number;
  maxBodyBytes?: number;
  maxHeaderBytes?: number;
  maxResponseBytes?: number;
  shutdownMs?: number;
  headerTimeoutMs?: number;
  requestTimeoutMs?: number;
  idleTimeoutMs?: number;
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
export interface PolicyField {
  readonly value: number | string; readonly source: string; readonly dynamic: boolean; readonly ceiling: number | null;
  readonly parentValue?: number | string; readonly parentSource?: string; readonly parentCeiling?: number | null; readonly roleCap?: number | null;
}
export interface PolicySnapshot { readonly revision: number; readonly fields: Readonly<Record<string, PolicyField>> }
export interface PolicyView {
  readonly revision: number;
  readonly fields: PolicySnapshot["fields"];
  effective(): PolicySnapshot;
  readonly diagnostics?: Diagnostics;
}
export interface Policy extends PolicyView {
  update(changes: { LOG_LEVEL: "trace" | "debug" | "info" | "warn" | "error" }, options: { expectedRevision: number }): PolicySnapshot;
}
export interface RolePolicySnapshot extends PolicySnapshot {
  readonly role: string; readonly parent: PolicySnapshot; readonly roleCaps: Readonly<Record<string, number>>;
}
export interface DerivedPolicy extends PolicyView {
  readonly role: string;
  effective(): RolePolicySnapshot;
}
export function derivePolicy(parent: Policy, options: { role: string; ceilings?: Readonly<Record<string, number>> }): DerivedPolicy;
export interface DiagnosticStatus {
  readonly state: string; readonly failed: boolean; readonly workerStarted: boolean; readonly workerAlive: boolean;
  readonly pendingRecords: number; readonly queueCapacity: number; readonly maxRecordBytes: number;
  readonly eventCounts: Readonly<Record<string, number>>; readonly admitted: number; readonly written: number;
  readonly dropped: number; readonly filtered: number; readonly level: string; readonly format: string;
  readonly policyRevision: number | null;
}
export class Diagnostics {
  constructor(options: { sink: { kind: "supervisor_stderr"; rotationOwner: "supervisor" }; maxPendingRecords?: number; maxRecordBytes?: number; closeTimeoutMs?: number; repeatIntervalMs?: number });
  emit(event: string, fields?: Readonly<Record<string, unknown>>): boolean;
  status(): DiagnosticStatus;
  close(options?: { timeoutMs?: number }): Promise<void>;
}
export class DiagnosticCloseError extends Error { readonly code: "deadline_exceeded" }
export class DiagnosticSinkError extends Error { readonly code: "unavailable" }
export class PolicyError extends Error { field: string; }
export function resolvePolicy(options: { environment: Record<string, string | undefined>; defaults?: Record<string, string | number>; ceilings?: Record<string, number>; capabilities?: string[]; diagnostics?: Diagnostics }): Policy;
export interface ServiceRef {
  target_id: string; service: string; api_version: string; instance_id: string;
  profile: "http.v1" | "grpc.v1"; endpoint: { kind: "unix" | "https" | "tls"; address: string };
}
export class TransportError extends Error {
  code: string;
  disposition: "not_sent" | "outcome_unknown";
}
export interface CallOptions {
  timeoutMs: number; requestId?: string; method?: string; signal?: AbortSignal;
  headers?: Record<string, string>; body?: string | Uint8Array; json?: unknown;
}
export class HTTPClient {
  constructor(options?: {
    policy?: PolicyView; localTarget?: string; tls?: ClientTLSOptions;
    maxConnections?: number; maxReferences?: number; maxInFlight?: number;
    maxRequestBytes?: number; maxResponseBytes?: number; maxHeaderBytes?: number;
    callTimeoutMs?: number;
    referenceIdleTimeoutMs?: number;
  });
  call(ref: ServiceRef, path: string, options: CallOptions): Promise<{ status: number; headers: import("node:http").IncomingHttpHeaders; body: Buffer; requestId: string }>;
  stream(ref: ServiceRef, path: string, options: CallOptions): Promise<{ status: number; headers: import("node:http").IncomingHttpHeaders; body: import("node:stream").Readable; requestId: string; close(): void }>;
  close(): void;
  stats(): { references: number; inFlight: number; closed: boolean };
}
