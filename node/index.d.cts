import type { IncomingMessage, Server, ServerResponse } from "node:http";
import type { Duplex } from "node:stream";

export interface HostOptions {
  policy?: Policy;
  tls?: import("node:https").ServerOptions;
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
export function proxyWebSocket(request: IncomingMessage, socket: Duplex, head: Buffer, address: string, options?: {
  maxPayload?: number;
  maxPendingMessages?: number;
  handshakeTimeoutMs?: number;
  sendTimeoutMs?: number;
  forwardHeaders?: string[];
  onOpen?: (info: { protocol: string }) => void;
}): { close(): void };
export interface Policy {
  readonly revision: number;
  readonly fields: Readonly<Record<string, { readonly value: number | string; readonly source: string; readonly dynamic: boolean; readonly ceiling: number | null }>>;
  effective(): { revision: number; fields: Policy["fields"] };
  readonly diagnostics?: Diagnostics;
  update(changes: { LOG_LEVEL: "trace" | "debug" | "info" | "warn" | "error" }, options: { expectedRevision: number }): ReturnType<Policy["effective"]>;
}
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
    policy?: Policy; localTarget?: string; tls?: ClientTLSOptions;
    maxConnections?: number; maxReferences?: number; maxInFlight?: number;
    maxRequestBytes?: number; maxResponseBytes?: number; maxHeaderBytes?: number;
    referenceIdleTimeoutMs?: number;
  });
  call(ref: ServiceRef, path: string, options: CallOptions): Promise<{ status: number; headers: import("node:http").IncomingHttpHeaders; body: Buffer; requestId: string }>;
  stream(ref: ServiceRef, path: string, options: CallOptions): Promise<{ status: number; headers: import("node:http").IncomingHttpHeaders; body: import("node:stream").Readable; requestId: string; close(): void }>;
  close(): void;
  stats(): { references: number; inFlight: number; closed: boolean };
}
