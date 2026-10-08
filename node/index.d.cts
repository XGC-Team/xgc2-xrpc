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
export function createRPCHost(handler: (request: IncomingMessage, response: ServerResponse, context: { requestId: string; instanceId: string; deadline: number; signal: AbortSignal }) => unknown, options: HostOptions & { instanceId: string; discoveryPaths?: string[] }): Host;
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
}
export class PolicyError extends Error { field: string; }
export function resolvePolicy(options: { environment: Record<string, string | undefined>; defaults?: Record<string, string | number>; ceilings?: Record<string, number>; capabilities?: string[] }): Policy;
export interface ServiceRef {
  target_id: string; service: string; api_version: string; instance_id: string;
  profile: "http.v1"; endpoint: { kind: "unix" | "https"; address: string };
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
    policy?: Policy; localTarget?: string; tls?: import("node:tls").ConnectionOptions;
    maxConnections?: number; maxReferences?: number; maxInFlight?: number;
    maxRequestBytes?: number; maxResponseBytes?: number; maxHeaderBytes?: number;
    referenceIdleTimeoutMs?: number;
  });
  call(ref: ServiceRef, path: string, options: CallOptions): Promise<{ status: number; headers: import("node:http").IncomingHttpHeaders; body: Buffer; requestId: string }>;
  stream(ref: ServiceRef, path: string, options: CallOptions): Promise<{ status: number; headers: import("node:http").IncomingHttpHeaders; body: import("node:stream").Readable; requestId: string; close(): void }>;
  close(): void;
  stats(): { references: number; inFlight: number; closed: boolean };
}
