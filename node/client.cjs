"use strict";
const http = require("node:http");
const https = require("node:https");
const tls = require("node:tls");
const { randomUUID } = require("node:crypto");
const { performance } = require("node:perf_hooks");
const { Transform } = require("node:stream");
const { Diagnostics } = require("./diagnostics.cjs");
const id = /^[A-Za-z0-9._:-]{1,128}$/;
class TransportError extends Error {
  constructor(code, disposition, message, cause) {
    super(message, { cause }); this.name = "TransportError"; this.code = code; this.disposition = disposition;
  }
}
function positive(value, fallback, field) {
  value ??= fallback;
  if (!Number.isSafeInteger(value) || value <= 0 || value > 2147483647) throw new RangeError(`${field}: positive bounded integer required`);
  return value;
}
function validateRef(ref, localTarget) {
  for (const field of ["target_id", "service", "api_version", "instance_id"]) {
    if (typeof ref?.[field] !== "string" || !ref[field] || ref[field].trim() !== ref[field] || /[\x00\r\n]/.test(ref[field])) throw new TypeError(`canonical ${field} required`);
  }
  if (!id.test(ref.instance_id) || ref.profile !== "http.v1") throw new TypeError("bound HTTP ServiceRef required");
  const endpoint = ref.endpoint;
  if (endpoint?.kind === "unix") {
    if (ref.target_id !== localTarget) throw new TypeError("remote Unix reference requires an owner-supplied authenticated dialer");
    const path = require("node:path");
    if (typeof endpoint.address !== "string" || !path.isAbsolute(endpoint.address) || path.normalize(endpoint.address) !== endpoint.address || Buffer.byteLength(endpoint.address) >= 108 || /[\x00\r\n]/.test(endpoint.address)) throw new TypeError("canonical absolute Unix address required");
  } else if (endpoint?.kind === "https") {
    const url = new URL(endpoint.address);
    if (url.protocol !== "https:" || !url.hostname || url.username || url.password || url.search || url.hash || !["", "/"].includes(url.pathname)) throw new TypeError("authenticated HTTPS origin required");
  } else throw new TypeError("unsupported endpoint kind");
  return endpoint;
}
function single(headers, name) {
  const values = [];
  for (let i = 0; i < headers.length; i += 2) if (headers[i].toLowerCase() === name) values.push(headers[i + 1]);
  return values;
}
function secureClientTLS(input) {
  input ??= {};
  if (typeof input !== "object" || Array.isArray(input)) throw new TypeError("explicit client TLS options object required");
  const allowed = new Set(["ca", "key", "cert", "passphrase", "minVersion", "maxVersion", "rejectUnauthorized"]);
  for (const name of Reflect.ownKeys(input)) if (!allowed.has(name)) throw new TypeError(`unsupported client TLS option: ${typeof name === "string" ? name : "symbol"}`);
  if (input.rejectUnauthorized != null && input.rejectUnauthorized !== true) throw new TypeError("TLS verification cannot be disabled");
  const minVersion = input.minVersion ?? "TLSv1.2", maxVersion = input.maxVersion ?? "TLSv1.3";
  if (!["TLSv1.2", "TLSv1.3"].includes(minVersion) || !["TLSv1.2", "TLSv1.3"].includes(maxVersion) || minVersion > maxVersion) throw new TypeError("client TLS versions must be TLSv1.2 or TLSv1.3 in ascending order");
  // Resolve native crypto/trust once. Agents share the immutable native context,
  // not a mutable caller credential object. Arbitrary hostname verifiers, SNI,
  // secure contexts, connection factories and legacy-crypto overrides are not
  // accepted as substitutes for verification of the ServiceRef hostname.
  const copyMaterial = (value) => {
    if (value == null || typeof value === "string") return value;
    if (Buffer.isBuffer(value) || value instanceof Uint8Array) return Buffer.from(value);
    if (Array.isArray(value)) return Object.freeze(value.map(copyMaterial));
    if (typeof value === "object" && Reflect.ownKeys(value).every((key) => ["pem", "passphrase"].includes(key)) && Object.hasOwn(value, "pem")) {
      if (value.passphrase != null && typeof value.passphrase !== "string") throw new TypeError("TLS key passphrase must be a string");
      return Object.freeze({ pem: copyMaterial(value.pem), passphrase: value.passphrase });
    }
    throw new TypeError("TLS credentials require PEM bytes, strings or supported arrays");
  };
  if (input.passphrase != null && typeof input.passphrase !== "string") throw new TypeError("TLS passphrase must be a string");
  const material = { ca: copyMaterial(input.ca), key: copyMaterial(input.key), cert: copyMaterial(input.cert), passphrase: input.passphrase };
  const secureContext = tls.createSecureContext({ ...material, minVersion, maxVersion });
  // Native Node consumes SecureContext; compatible maintained runtimes may
  // consume explicit CA/key/cert instead. Both projections use the same private
  // startup copies and the same enforced hostname/minimum-version policy.
  return Object.freeze({ ...material, secureContext, minVersion, maxVersion, rejectUnauthorized: true, checkServerIdentity: tls.checkServerIdentity });
}

// One explicit process owner shares this client across service references.
// Node's maintained Agent/parser implement the wire; there is no replay loop.
class HTTPClient {
  #tls;
  constructor(options = {}) {
    if (options.diagnostics != null && !(options.diagnostics instanceof Diagnostics)) throw new TypeError("explicit Diagnostics owner required");
    this.#tls = secureClientTLS(options.tls);
    this.options = Object.freeze({ ...options, tls: undefined });
    this.maxConnections = positive(options.maxConnections, 16, "maxConnections");
    this.maxReferences = positive(options.maxReferences, 64, "maxReferences");
    this.maxInFlight = positive(options.maxInFlight, 32, "maxInFlight");
    this.maxRequestBytes = positive(options.maxRequestBytes, 1048576, "maxRequestBytes");
    this.maxResponseBytes = positive(options.maxResponseBytes, 1048576, "maxResponseBytes");
    this.maxHeaderBytes = positive(options.maxHeaderBytes, 16384, "maxHeaderBytes");
    this.callTimeoutMs = positive(options.callTimeoutMs, 30000, "callTimeoutMs");
    if (this.callTimeoutMs > 86400000) throw new RangeError("callTimeoutMs exceeds wire maximum");
    this.idleMs = positive(options.referenceIdleTimeoutMs, 30000, "referenceIdleTimeoutMs");
    this.pools = new Map(); this.active = new Set(); this.closed = false;
    this.sockets = new Set(); this.shutdownReported = false;
    this.timer = setInterval(() => this.evict(), Math.min(this.idleMs, 1000));
    this.timer.unref();
  }
  stats() { return { references: this.pools.size, inFlight: this.active.size, closed: this.closed }; }
  diagnostic(event, fields = {}) {
    this.options.diagnostics?.emit(event, { operation: "http_client", ...fields });
  }
  diagnosticFailure(error, fields = {}, admission = false) {
    const event = error.code === "cancelled" ? "cancelled" : error.code === "deadline_exceeded" ? "deadline_exceeded"
      : admission || ["invalid_argument", "resource_exhausted"].includes(error.code) ? "call_rejected" : "transport_failed";
    this.diagnostic(event, { ...fields, category: error.code });
  }
  trackSocket(socket) {
    if (socket.closed || this.sockets.has(socket)) return;
    this.sockets.add(socket);
    socket.once("close", () => { this.sockets.delete(socket); this.diagnosticShutdownComplete(); });
  }
  destroyPool(pool) {
    // These are documented native Agent collections. Observe existing owned
    // sockets before destruction, including a socket created during synchronous
    // request-head validation before ClientRequest emits its socket event.
    for (const collection of [pool.agent.sockets, pool.agent.freeSockets]) for (const sockets of Object.values(collection)) for (const socket of sockets) this.trackSocket(socket);
    pool.agent.destroy();
  }
  diagnosticShutdownComplete() {
    if (!this.closed || this.active.size || this.sockets.size || this.shutdownReported) return;
    this.shutdownReported = true;
    this.diagnostic("shutdown_completed", { state: "closed", in_flight: 0, connections: 0 });
  }
  evict() {
    const now = performance.now();
    for (const [key, pool] of this.pools) if (!pool.active && now - pool.lastUsed >= this.idleMs) {
      this.destroyPool(pool); this.pools.delete(key);
    }
  }
  pool(endpoint) {
    const key = `${endpoint.kind}:${endpoint.address}`;
    let pool = this.pools.get(key);
    if (pool) return pool;
    this.evict();
    if (this.pools.size >= this.maxReferences) {
      let oldest;
      for (const item of this.pools) if (!item[1].active && (!oldest || item[1].lastUsed < oldest[1].lastUsed)) oldest = item;
      if (!oldest) {
        this.diagnostic("client_pool_full", { category: "resource_exhausted", count: this.pools.size, limit: this.maxReferences });
        throw new TransportError("resource_exhausted", "not_sent", "all reference slots are active");
      }
      this.destroyPool(oldest[1]); this.pools.delete(oldest[0]);
    }
    const Agent = endpoint.kind === "https" ? https.Agent : http.Agent;
    pool = { active: 0, lastUsed: performance.now(), agent: new Agent({
      ...this.#tls, keepAlive: true, maxSockets: this.maxConnections,
      maxTotalSockets: this.maxConnections, maxFreeSockets: this.maxConnections,
      timeout: this.idleMs,
    }) };
    this.pools.set(key, pool);
    return pool;
  }
  stream(ref, path, options = {}) {
    const start = performance.now();
    let endpoint, body, pool, requestId, timeoutMs;
    let identity = {};
    try {
      if (this.closed) throw new TransportError("unavailable", "not_sent", "client is closed");
      if (this.active.size >= this.maxInFlight) {
        this.diagnostic("client_pool_full", { category: "resource_exhausted", in_flight: this.active.size, limit: this.maxInFlight });
        throw new TransportError("resource_exhausted", "not_sent", "client admission full");
      }
      endpoint = validateRef(ref, this.options.localTarget);
      identity = { service: ref.service, instance_id: ref.instance_id };
      if (typeof path !== "string" || !path.startsWith("/") || path.startsWith("//") || /[\x00-\x20\x7f]/.test(path)) throw new TypeError("canonical route path required");
      requestId = options.requestId ?? randomUUID();
      if (!id.test(requestId)) throw new TypeError("canonical request ID required");
      identity.request_id = requestId;
      timeoutMs = positive(options.timeoutMs, null, "timeoutMs");
      if (timeoutMs > 86400000) throw new RangeError("timeoutMs exceeds wire maximum");
      timeoutMs = Math.min(timeoutMs, this.callTimeoutMs);
      if (options.signal?.aborted) throw new TransportError("cancelled", "not_sent", "caller cancelled before admission");
      body = options.body;
      if (body != null && !Buffer.isBuffer(body) && !(body instanceof Uint8Array) && typeof body !== "string") throw new TypeError("body must be bytes or string");
      if (body != null && Buffer.byteLength(body) > this.maxRequestBytes) throw new TransportError("resource_exhausted", "not_sent", "request body exceeds limit");
      pool = this.pool(endpoint);
      if (pool.active >= this.maxConnections) {
        this.diagnostic("client_pool_full", { ...identity, category: "resource_exhausted", connections: pool.active, limit: this.maxConnections });
        throw new TransportError("resource_exhausted", "not_sent", "reference connection admission full");
      }
    } catch (error) {
      const failure = error instanceof TransportError ? error : new TransportError("invalid_argument", "not_sent", error.message, error);
      this.diagnosticFailure(failure, identity, true); return Promise.reject(failure);
    }
    return new Promise((resolve, reject) => {
      const abort = new AbortController();
      const state = { abort, response: null, bounded: null, request: null, sent: false, finished: false, failed: false, readableEnded: false };
      this.active.add(state); pool.active++;
      this.diagnostic("call_started", { ...identity, budget_ms: timeoutMs, in_flight: this.active.size });
      let timer, settled = false;
      const release = () => {
        if (state.finished) return;
        if (!state.failed && !(state.readableEnded && state.response?.complete)) recordFailure(failure(state.bounded ? "cancelled" : "unavailable", new Error("transport ownership ended without readable completion")));
        state.finished = true; clearTimeout(timer);
        options.signal?.removeEventListener("abort", cancelled);
        this.active.delete(state); pool.active--; pool.lastUsed = performance.now();
        if (!state.failed) this.diagnostic("call_completed", { ...identity, elapsed_ms: performance.now() - start, in_flight: this.active.size });
        this.diagnosticShutdownComplete();
      };
      const failure = (code, error) => new TransportError(code, state.sent ? "outcome_unknown" : "not_sent", error.message, error);
      const recordFailure = (error) => {
        if (state.failed || state.finished) return;
        state.failed = true; this.diagnosticFailure(error, { ...identity, elapsed_ms: performance.now() - start });
      };
      state.recordFailure = recordFailure;
      const terminate = (code) => {
        const error = failure(code, new Error(code));
        recordFailure(error);
        state.bounded?.destroy(error); state.response?.destroy(error); abort.abort(error);
        if (!settled) { settled = true; reject(error); }
      };
      const cancelled = () => terminate("cancelled");
      options.signal?.addEventListener("abort", cancelled, { once: true });
      const remaining = timeoutMs - (performance.now() - start);
      if (remaining <= 0) { const error = failure("deadline_exceeded", new Error("deadline before transport")); recordFailure(error); release(); reject(error); return; }
      timer = setTimeout(() => terminate("deadline_exceeded"), remaining);
      timer.unref();
      try {
        const headers = { ...options.headers };
        for (const name of Object.keys(headers)) {
          if (["x-request-id", "x-xrpc-timeout-ms", "x-xrpc-instance-id", "content-length", "transfer-encoding", "host", "connection"].includes(name.toLowerCase())) throw new TypeError("caller cannot override protocol headers");
          if (typeof headers[name] !== "string") throw new TypeError("caller header values must be strings");
          http.validateHeaderName(name); http.validateHeaderValue(name, headers[name]);
        }
        headers["X-Request-ID"] = requestId;
        headers["X-Xrpc-Timeout-Ms"] = String(Math.max(1, Math.floor(remaining)));
        headers["X-Xrpc-Instance-ID"] = ref.instance_id;
        if (body != null) headers["Content-Length"] = Buffer.byteLength(body);
        const transport = endpoint.kind === "https" ? https : http;
        const target = endpoint.kind === "https" ? new URL(endpoint.address) : null;
        const req = transport.request({
          ...(target ? { protocol: target.protocol, hostname: target.hostname.replace(/^\[|\]$/g, ""), port: target.port || 443 } : { socketPath: endpoint.address, host: "localhost" }),
          method: options.method ?? (body == null ? "GET" : "POST"), path, headers,
          agent: pool.agent, signal: abort.signal, maxHeaderSize: this.maxHeaderBytes,
        });
        state.request = req;
        req.on("error", () => {}); // Validation may destroy before call listeners attach.
        req.once("socket", (socket) => this.trackSocket(socket));
        // Count the complete head before native header serialization, including
        // its request target and the transport-generated Host field.
        let headerBytes = Buffer.byteLength(`${req.method} ${path} HTTP/1.1\r\nConnection: keep-alive\r\n\r\n`);
        for (const name of req.getRawHeaderNames()) headerBytes += Buffer.byteLength(name) + Buffer.byteLength(String(req.getHeader(name))) + 4;
        if (headerBytes > this.maxHeaderBytes) throw new TransportError("resource_exhausted", "not_sent", "request head exceeds limit");
        // A socket can start a queued write before its callback reports failure.
        // Mark conservatively before serializing headers; never infer rollback.
        req.once("socket", (socket) => {
          const mark = () => { state.sent = true; };
          if (endpoint.kind === "https") {
            if (socket.encrypted && !socket.secureConnecting && !socket.connecting) mark();
            else socket.prependOnceListener("secureConnect", mark);
          } else if (!socket.connecting && socket.readyState === "open") mark();
          else socket.prependOnceListener("connect", mark);
        });
        req.once("error", (error) => {
          const reason = error instanceof TransportError ? error : error.cause instanceof TransportError ? error.cause : failure("unavailable", error);
          recordFailure(reason);
          if (!settled) { settled = true; reject(reason); }
        });
        req.once("close", () => { if (!state.response) release(); });
        req.once("response", (response) => {
          state.response = response;
          const instances = single(response.rawHeaders, "x-xrpc-instance-id");
          if (instances.length !== 1 || instances[0] !== ref.instance_id) {
            const error = failure("conflict", new Error("response instance does not match"));
            recordFailure(error);
            response.once("close", release);
            response.destroy(); if (!settled) { settled = true; reject(error); } return;
          }
          let bytes = 0;
          const bounded = new Transform({ transform: (chunk, encoding, next) => {
            bytes += chunk.length;
            if (bytes > this.maxResponseBytes) next(failure("resource_exhausted", new Error("response body exceeds limit")));
            else next(null, chunk);
          } });
          state.bounded = bounded;
          bounded.once("end", () => { state.readableEnded = true; });
          bounded.once("close", release);
          bounded.once("error", (error) => { recordFailure(error); response.destroy(); });
          bounded.once("close", () => { if (!response.complete) response.destroy(); });
          response.once("error", (error) => bounded.destroy(error instanceof TransportError ? error : failure("unavailable", error)));
          response.once("aborted", () => bounded.destroy(failure("unavailable", new Error("response truncated"))));
          response.once("close", () => { if (!response.complete) bounded.destroy(failure("unavailable", new Error("response closed"))); });
          response.pipe(bounded);
          settled = true;
          resolve({ status: response.statusCode, headers: response.headers, body: bounded, requestId, close: () => bounded.destroy() });
        });
        req.end(body);
      } catch (error) {
        const reason = error instanceof TransportError ? error : failure("invalid_argument", error);
        recordFailure(reason); state.request?.destroy(); release(); settled = true; reject(reason);
      }
    });
  }
  async call(ref, path, options = {}) {
    if (this.closed) { const error = new TransportError("unavailable", "not_sent", "client is closed"); this.diagnosticFailure(error, {}, true); throw error; }
    if (this.active.size >= this.maxInFlight) {
      const error = new TransportError("resource_exhausted", "not_sent", "client admission full");
      this.diagnostic("client_pool_full", { category: error.code, in_flight: this.active.size, limit: this.maxInFlight }); this.diagnosticFailure(error, {}, true); throw error;
    }
    let body = options.body;
    if (Object.hasOwn(options, "json")) {
      try { body = JSON.stringify(options.json); }
      catch (error) { const reason = new TransportError("invalid_argument", "not_sent", "JSON serialization failed", error); this.diagnosticFailure(reason, {}, true); throw reason; }
      options = { ...options, headers: { ...options.headers, "Content-Type": "application/json" } };
    }
    const response = await this.stream(ref, path, { ...options, body });
    const chunks = [];
    try {
      for await (const chunk of response.body) chunks.push(chunk);
      return { status: response.status, headers: response.headers, body: Buffer.concat(chunks), requestId: response.requestId };
    } finally { response.close(); }
  }
  close() {
    if (this.closed) return;
    this.closed = true; clearInterval(this.timer);
    this.diagnostic("shutdown_started", { state: "closing", in_flight: this.active.size, connections: this.sockets.size });
    for (const state of this.active) {
      const error = new TransportError("cancelled", state.sent ? "outcome_unknown" : "not_sent", "client closed");
      state.recordFailure(error);
      state.bounded?.destroy(error); state.response?.destroy(error); state.abort.abort(error);
    }
    for (const pool of this.pools.values()) this.destroyPool(pool);
    this.pools.clear();
    this.diagnosticShutdownComplete();
  }
}
module.exports = { HTTPClient, TransportError };
