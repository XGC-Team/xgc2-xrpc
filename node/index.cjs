"use strict";

const http = require("node:http");
const https = require("node:https");
const { randomUUID } = require("node:crypto");
const { Readable } = require("node:stream");
const { pipeline } = require("node:stream/promises");
const { WebSocket, WebSocketServer } = require("ws");
const { resolvePolicy, PolicyError, policyOptions } = require("./policy.cjs");
const { HTTPClient, TransportError } = require("./client.cjs");
const { BootstrapBinding, readBootstrapBinding, loadBootstrapInput } = require("./bootstrap.cjs");
const { Diagnostics, DiagnosticCloseError, DiagnosticSinkError } = require("./diagnostics.cjs");

function positive(value, fallback, name) {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result <= 0 || result > 2147483647) {
    throw new RangeError(`${name} must be a positive integer no larger than 2147483647`);
  }
  return result;
}

// Public HTTP edge. Authentication, routes and allowed origins belong to the
// product. This host does not synthesize an internal ServiceRef or start it.
function createHTTPHost(handler, options = {}) {
  options = policyOptions(options, {
    HOST_MAX_CONNECTIONS: "maxConnections", HOST_MAX_IN_FLIGHT: "maxInFlight",
    MAX_HEADER_BYTES: "maxHeaderBytes", MAX_REQUEST_BYTES: "maxBodyBytes",
    MAX_RESPONSE_BYTES: "maxResponseBytes", CALL_TIMEOUT_MS: "callTimeoutMs",
    HEADER_TIMEOUT_MS: "headerTimeoutMs", IDLE_TIMEOUT_MS: "idleTimeoutMs",
    SHUTDOWN_TIMEOUT_MS: "shutdownMs",
  });
  const diagnostics = options.policy?.diagnostics;
  const emit = (event, fields) => diagnostics?.emit(event, fields);
  const maxConnections = positive(options.maxConnections, 32, "maxConnections");
  const maxInFlight = positive(options.maxInFlight, 32, "maxInFlight");
  const maxBodyBytes = positive(options.maxBodyBytes, 1048576, "maxBodyBytes");
  const maxResponseBytes = options.maxResponseBytes == null ? null : positive(options.maxResponseBytes, 1048576, "maxResponseBytes");
  const shutdownMs = positive(options.shutdownMs, 5000, "shutdownMs");
  const callTimeoutMs = options.callTimeoutMs == null ? 0 : positive(options.callTimeoutMs, 30000, "callTimeoutMs");
  if (callTimeoutMs > 86400000) throw new RangeError("callTimeoutMs exceeds wire maximum");
  const maxHeaderBytes = positive(options.maxHeaderBytes, 16384, "maxHeaderBytes");
  class BoundedRequest extends http.IncomingMessage {
    push(chunk, encoding) {
      if (chunk) {
        this.receivedBytes = (this.receivedBytes ?? 0) + chunk.length;
        if (this.receivedBytes > maxBodyBytes) {
          this.destroy(new Error("request body limit exceeded"));
          return false;
        }
      }
      return super.push(chunk, encoding);
    }
  }
  let inFlight = 0, activeHandlers = 0, closing = false;
  let upgradeHandler;
  let changed = () => {};
  const sockets = new Set();
  const headerTimeoutMs = positive(options.headerTimeoutMs, 5000, "headerTimeoutMs");
  const requestTimeoutMs = positive(options.requestTimeoutMs, Math.max(headerTimeoutMs, callTimeoutMs || 30000), "requestTimeoutMs");
  const server = (options.tls ? https : http).createServer({
    ...options.tls,
    IncomingMessage: BoundedRequest,
    maxHeaderSize: maxHeaderBytes,
    headersTimeout: headerTimeoutMs,
    requestTimeout: requestTimeoutMs,
    keepAliveTimeout: positive(options.idleTimeoutMs, 30000, "idleTimeoutMs"),
    connectionsCheckingInterval: 250,
  }, (req, res) => {
    req.on("error", () => { emit("transport_failed", { category: req.receivedBytes > maxBodyBytes ? "resource_exhausted" : "unavailable" }); res.destroy(); });
    if (closing) {
      emit("call_rejected", { category: "unavailable", in_flight: inFlight });
      res.writeHead(503, { Connection: "close" });
      res.end();
      return;
    }
    if (inFlight >= maxInFlight || Number(req.headers["content-length"] ?? 0) > maxBodyBytes) {
      emit("call_rejected", { category: "resource_exhausted", in_flight: inFlight });
      res.writeHead(inFlight >= maxInFlight ? 429 : 413, { Connection: "close" });
      res.end();
      return;
    }
    inFlight++;
    activeHandlers++;
    const started = performance.now();
    emit("call_started", { in_flight: inFlight });
    let released = false, responseDone = false, handlerDone = false;
    const release = () => {
      if (!released && responseDone && handlerDone) {
        released = true;
        inFlight--;
        emit("call_completed", { elapsed_ms: performance.now() - started, in_flight: inFlight });
        // Native finish listeners mark this keepalive socket idle after the
        // response event. Reap it on that completion boundary during drain.
        if (closing) setImmediate(() => server.closeIdleConnections());
        changed();
      }
    };
    const responseFinished = () => { responseDone = true; release(); };
    const handlerFinished = () => { handlerDone = true; activeHandlers--; release(); changed(); };
    res.once("finish", responseFinished);
    res.once("close", responseFinished);
    const setHeader = res.setHeader.bind(res), appendHeader = res.appendHeader.bind(res), writeHead = res.writeHead.bind(res), addTrailers = res.addTrailers.bind(res);
    const responseHeadFits = (entries, reason = res.statusMessage) => {
      let bytes = 128 + Buffer.byteLength(String(reason ?? "")); // Native Date/connection/framing overhead.
      for (const [name, raw] of entries) {
        for (const value of Array.isArray(raw) ? raw : [raw]) bytes += Buffer.byteLength(String(name)) + Buffer.byteLength(String(value)) + 4;
        if (bytes > maxHeaderBytes) throw new RangeError("response headers exceed limit");
      }
    };
    res.setHeader = (name, value) => {
      const current = Object.entries(res.getHeaders()).filter(([key]) => key.toLowerCase() !== String(name).toLowerCase());
      responseHeadFits([...current, [name, value]]);
      return setHeader(name, value);
    };
    res.appendHeader = (name, value) => {
      responseHeadFits([...Object.entries(res.getHeaders()), [name, value]]);
      return appendHeader(name, value);
    };
    res.writeHead = (status, reasonOrHeaders, headers) => {
      const added = typeof reasonOrHeaders === "string" ? headers : reasonOrHeaders;
      const entries = added == null ? [] : Array.isArray(added) ? added.reduce((all, value, i) => {
        if (i % 2 === 0) all.push([value, added[i + 1]]); return all;
      }, []) : Object.entries(added);
      const replaced = new Set(entries.map(([name]) => String(name).toLowerCase()));
      responseHeadFits([...Object.entries(res.getHeaders()).filter(([name]) => !replaced.has(name.toLowerCase())), ...entries], typeof reasonOrHeaders === "string" ? reasonOrHeaders : res.statusMessage);
      return writeHead(status, reasonOrHeaders, headers);
    };
    res.addTrailers = (headers) => {
      const entries = Array.isArray(headers) ? headers : Object.entries(headers);
      responseHeadFits([...Object.entries(res.getHeaders()), ...entries]);
      return addTrailers(headers);
    };
    if (maxResponseBytes) {
      let responseBytes = 0;
      const write = res.write.bind(res), end = res.end.bind(res);
      const account = (chunk, encoding) => {
        if (req.method === "HEAD") return true; // Native HTTP emits no body.
        if (chunk != null) responseBytes += Buffer.byteLength(chunk, typeof encoding === "string" ? encoding : undefined);
        if (responseBytes > maxResponseBytes) { res.destroy(new Error("response body limit exceeded")); return false; }
        return true;
      };
      res.write = (chunk, encoding, callback) => account(chunk, encoding) ? write(chunk, encoding, callback) : false;
      res.end = (chunk, encoding, callback) => account(chunk, encoding) ? end(chunk, encoding, callback) : res;
    }
    // Response streams can live longer than one request-body budget. Owners
    // opt into a finite handler budget for routes without long-lived streams.
    let timer;
    if (callTimeoutMs) {
      timer = setTimeout(() => { emit("deadline_exceeded", { budget_ms: callTimeoutMs }); res.destroy(); }, callTimeoutMs);
      timer.unref();
      res.once("close", () => clearTimeout(timer));
    }
    try {
      const suppliedId = req.headers["x-request-id"];
      res.setHeader("X-Request-ID", typeof suppliedId === "string" && /^[A-Za-z0-9._:-]{1,128}$/.test(suppliedId) ? suppliedId : randomUUID());
      Promise.resolve(handler(req, res)).catch(() => { emit("handler_failed", { category: "internal" }); res.destroy(); }).finally(handlerFinished);
    } catch {
      emit("handler_failed", { category: "internal" });
      res.destroy();
      handlerFinished();
    }
  });
  if ("keepAliveTimeoutBuffer" in server) server.keepAliveTimeoutBuffer = 0;
  const nativeListen = server.listen.bind(server);
  server.listen = function (...args) {
    const first = args[0];
    if ((typeof first === "object" && first?.path != null) || (typeof first === "string" && !/^\d+$/.test(first))) {
      throw new TypeError("Node supplemental hosts do not implement a Unix lease; use a formal SDK provider");
    }
    return nativeListen(...args);
  };
  server.on("connection", (socket) => {
    if (closing || sockets.size >= maxConnections) { emit("connection_rejected", { connections: sockets.size, category: "resource_exhausted" }); socket.destroy(); return; }
    sockets.add(socket);
    emit("connection_accepted", { connections: sockets.size });
    socket.once("close", () => { sockets.delete(socket); emit("connection_closed", { connections: sockets.size }); });
  });
  server.on("upgrade", (req, socket, head) => {
    if (closing || !upgradeHandler) { socket.destroy(); return; }
    try { upgradeHandler(req, socket, head); }
    catch { socket.destroy(); }
  });
  let shutdown, networkClosed = false, closeStarted = false, networkError;
  function close() {
    if (shutdown) return shutdown;
    closing = true;
    emit("shutdown_started", { in_flight: inFlight, budget_ms: shutdownMs });
    shutdown = new Promise((resolve, reject) => {
      let expired = false;
      const finish = () => {
        if (expired && activeHandlers) {
          changed = () => {};
          shutdown = undefined; // A later close can wait for actual quiescence.
          emit("shutdown_failed", { category: "deadline_exceeded", in_flight: inFlight });
          reject(new Error(`shutdown deadline: ${activeHandlers} domain handlers remain active`));
        } else if (networkClosed && !inFlight) {
          clearTimeout(timer);
          changed = () => {};
          if (networkError && networkError.code !== "ERR_SERVER_NOT_RUNNING") { emit("shutdown_failed", { category: "unavailable" }); reject(networkError); }
          else { emit("shutdown_completed", { in_flight: inFlight }); resolve(); }
        }
      };
      changed = finish;
      const timer = setTimeout(() => {
        expired = true;
        for (const socket of sockets) socket.destroy();
        finish();
      }, shutdownMs);
      timer.unref();
      if (!closeStarted) {
        closeStarted = true;
        server.close((error) => { networkClosed = true; networkError = error; changed(); });
        server.closeIdleConnections();
      }
      finish();
    });
    return shutdown;
  }
  return {
    server, close, stats: () => ({ connections: sockets.size, inFlight }),
    onUpgrade(handler) {
      if (closing) throw new Error("host is closing");
      if (upgradeHandler) throw new Error("upgrade handler already registered");
      if (typeof handler !== "function") throw new TypeError("upgrade handler must be a function");
      upgradeHandler = handler;
    },
  };
}

// The maintained ws parser owns both handshakes and frames. One bounded message
// is sent at a time in each direction; pause/resume supplies backpressure while
// preserving message boundaries and text/binary identity.
function proxyWebSocket(request, socket, head, address, options = {}) {
  if (socket.destroyed) return { close() {} };
  const maxPayload = positive(options.maxPayload, 16 * 1024 * 1024, "maxPayload");
  const handshakeTimeout = positive(options.handshakeTimeoutMs, 5000, "handshakeTimeoutMs");
  const sendTimeout = positive(options.sendTimeoutMs, 10000, "sendTimeoutMs");
  const maxPendingMessages = positive(options.maxPendingMessages, 64, "maxPendingMessages");
  const protocols = String(request.headers["sec-websocket-protocol"] || "")
    .split(",").map((p) => p.trim()).filter(Boolean);
  const headers = {};
  // Policy decides whether these credentials may reach the selected upstream.
  for (const name of options.forwardHeaders ?? ["authorization", "cookie", "origin"]) {
    if (request.headers[name] != null) headers[name] = request.headers[name];
  }
  let upstream, downstream, ended = false, upgraded = false;
  let pendingBytes = 0;
  const pending = [];
  const timers = new Set();
  const writes = new Map();
  const cleanup = () => {
    if (ended) return;
    ended = true;
    for (const timer of timers) clearTimeout(timer);
    timers.clear();
    pending.length = 0;
    upstream?.terminate();
    downstream?.terminate();
    socket.destroy();
  };
  const fail = () => {
    if (!upgraded && !socket.destroyed) {
      socket.end("HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    }
    upstream?.terminate();
    downstream?.terminate();
  };
  socket.once("close", cleanup);
  socket.once("error", cleanup);
  try {
    upstream = new WebSocket(address, protocols, {
      headers, handshakeTimeout, maxPayload, perMessageDeflate: false,
      autoPong: false, rejectUnauthorized: true,
    });
  } catch {
    fail();
    return { close: cleanup };
  }
  const bridge = (source, destination, data, binary, kind = "message") => {
    if (ended || destination.readyState !== WebSocket.OPEN) { cleanup(); return; }
    // Pausing takes effect at the socket boundary; ws can emit more than one
    // already-parsed message. Account queued writes as well as a single frame.
    const state = writes.get(destination) ?? { messages: 0, bytes: 0 };
    writes.set(destination, state);
    if (state.messages >= maxPendingMessages || state.bytes + data.length > maxPayload) { cleanup(); return; }
    state.messages++;
    state.bytes += data.length;
    source.pause();
    const timer = setTimeout(cleanup, sendTimeout);
    timers.add(timer);
    timer.unref();
    const completed = (error) => {
      clearTimeout(timer);
      timers.delete(timer);
      state.messages--;
      state.bytes -= data.length;
      if (error) cleanup();
      else if (!ended && state.messages === 0) source.resume();
    };
    if (kind === "ping") destination.ping(data, undefined, completed);
    else if (kind === "pong") destination.pong(data, undefined, completed);
    else destination.send(data, { binary }, completed);
  };
  const relayControl = (source, destination) => {
    source.on("ping", (data) => {
      bridge(source, destination, data, true, "ping");
    });
    source.on("pong", (data) => {
      bridge(source, destination, data, true, "pong");
    });
    source.on("close", (code, reason) => {
      if (destination.readyState === WebSocket.OPEN) {
        destination.close(code === 1005 || code === 1006 ? 1011 : code, reason);
        const timer = setTimeout(cleanup, 1000);
        timers.add(timer);
        timer.unref();
      }
    });
  };
  upstream.on("error", fail);
  upstream.on("unexpected-response", (_req, response) => { response.destroy(); fail(); });
  upstream.on("message", (data, binary) => {
    if (downstream) bridge(upstream, downstream, data, binary);
    else {
      pendingBytes += data.length;
      if (pendingBytes > maxPayload || pending.length >= maxPendingMessages) { cleanup(); return; }
      pending.push({ data, binary });
      upstream.pause();
    }
  });
  upstream.once("open", () => {
    if (ended || socket.destroyed) { cleanup(); return; }
    const acceptor = new WebSocketServer({
      noServer: true, clientTracking: false, maxPayload,
      perMessageDeflate: false, autoPong: false,
      handleProtocols: () => upstream.protocol || false,
    });
    acceptor.on("error", cleanup);
    acceptor.handleUpgrade(request, socket, head, (client) => {
      downstream = client;
      upgraded = true;
      client.on("error", cleanup);
      client.on("message", (data, binary) => bridge(client, upstream, data, binary));
      relayControl(client, upstream);
      relayControl(upstream, client);
      for (const message of pending) bridge(upstream, client, message.data, message.binary);
      pending.length = 0;
      pendingBytes = 0;
      upstream.resume();
      options.onOpen?.({ protocol: upstream.protocol });
    });
    acceptor.close();
  });
  upstream.once("close", () => { if (!upgraded) cleanup(); });
  return { close: cleanup };
}

// Fetch-style product routes use the same bounded listener and shutdown path.
// Bodies stay streams; disconnect cancellation reaches the domain Request.
function createFetchHost(handler, options = {}) {
  return createHTTPHost(async (req, res) => {
    const abort = new AbortController();
    const disconnected = () => abort.abort();
    res.once("close", disconnected);
    try {
      const headers = new Headers();
      for (let i = 0; i < req.rawHeaders.length; i += 2) {
        headers.append(req.rawHeaders[i], req.rawHeaders[i + 1]);
      }
      const init = { method: req.method, headers, signal: abort.signal };
      if (req.method !== "GET" && req.method !== "HEAD") {
        init.body = Readable.toWeb(req);
        init.duplex = "half";
      }
      const request = new Request(new URL(req.url, `http://${req.headers.host || "localhost"}`), init);
      const response = await handler(request);
      res.statusCode = response.status;
      for (const [name, value] of response.headers) {
        if (name !== "set-cookie") res.setHeader(name, value);
      }
      const cookies = response.headers.getSetCookie();
      if (cookies.length) res.setHeader("set-cookie", cookies);
      if (req.method === "HEAD" || !response.body) {
        await response.body?.cancel();
        res.end();
      } else {
        await pipeline(Readable.fromWeb(response.body), res, { signal: abort.signal });
      }
    } finally {
      res.removeListener("close", disconnected);
    }
  }, options);
}

// A bound internal host uses the same native listener and resource owner as a
// public edge, with strict metadata before any domain dispatch.
function createRPCHost(handler, options = {}) {
  options = policyOptions(options, { CALL_TIMEOUT_MS: "callTimeoutMs", MAX_RESPONSE_BYTES: "maxResponseBytes" });
  const instanceId = options.instanceId;
  if (typeof instanceId !== "string" || !/^[A-Za-z0-9._:-]{1,128}$/.test(instanceId)) throw new TypeError("canonical instanceId required");
  const discovery = new Set(options.discoveryPaths ?? []);
  const maximum = options.policy?.fields.CALL_TIMEOUT_MS?.value ?? options.callTimeoutMs ?? 30000;
  if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 86400000) throw new RangeError("finite RPC callTimeoutMs 1..86400000 required");
  return createHTTPHost(async (req, res) => {
    const entries = (name) => {
      const values = [];
      for (let i = 0; i < req.rawHeaders.length; i += 2) if (req.rawHeaders[i].toLowerCase() === name) values.push(req.rawHeaders[i + 1]);
      return values;
    };
    res.setHeader("X-Xrpc-Instance-ID", instanceId);
    const ids = entries("x-request-id"), timeouts = entries("x-xrpc-timeout-ms"), instances = entries("x-xrpc-instance-id");
    let code;
    if (ids.length !== 1 || !/^[A-Za-z0-9._:-]{1,128}$/.test(ids[0]) || timeouts.length !== 1 || !/^[1-9][0-9]{0,7}$/.test(timeouts[0]) || Number(timeouts[0]) > 86400000 || instances.length > 1) code = "invalid_argument";
    else if (!(req.method === "GET" && discovery.has(req.url) && instances.length === 0) && (instances.length !== 1 || instances[0] !== instanceId)) code = "conflict";
    if (code) {
      options.policy?.diagnostics?.emit("call_rejected", { category: code, instance_id: instanceId });
      res.writeHead(code === "conflict" ? 409 : 400, { "Content-Type": "application/json", Connection: "close" });
      res.end(JSON.stringify({ error: { code, message: "invalid RPC metadata" } }));
      return;
    }
    res.setHeader("X-Request-ID", ids[0]);
    const abort = new AbortController();
    const timeoutMs = Math.min(Number(timeouts[0]), maximum);
    const deadline = Date.now() + timeoutMs;
    const timer = setTimeout(() => { options.policy?.diagnostics?.emit("deadline_exceeded", { request_id: ids[0], instance_id: instanceId, budget_ms: timeoutMs }); abort.abort(); res.destroy(); }, timeoutMs);
    timer.unref();
    const ended = () => { clearTimeout(timer); if (!res.writableFinished && !abort.signal.aborted) options.policy?.diagnostics?.emit("cancelled", { request_id: ids[0], instance_id: instanceId }); abort.abort(); };
    res.once("close", ended); res.once("finish", () => clearTimeout(timer));
    try { return await handler(req, res, { requestId: ids[0], instanceId, deadline, signal: abort.signal }); }
    finally { if (res.writableFinished || res.destroyed) clearTimeout(timer); }
  }, { ...options, maxResponseBytes: options.maxResponseBytes ?? 1048576, callTimeoutMs: maximum });
}

function createBoundHTTPHost(handler, options) {
  if (!(options?.binding instanceof BootstrapBinding)) throw new TypeError("validated BootstrapBinding required");
  if (options.binding.profile !== "http.v1" || options.binding.endpoint.kind !== "https") throw new TypeError("Node native bound host requires HTTPS BootstrapBinding");
  const credentials = options.binding.resolveCredentials(options.resolveGrant, "server");
  if (options.tls != null) throw new TypeError("bound TLS comes from bootstrap grants");
  return createRPCHost(async (req,res,context) => {
    const authorized = await credentials.authorization.authorize(req, context);
    if (context.signal.aborted || res.destroyed || Date.now() >= context.deadline) return;
    if (authorized !== true) {
      options.policy?.diagnostics?.emit("call_rejected", { operation: "authorization", request_id: context.requestId, instance_id: context.instanceId });
      res.writeHead(403, { "Content-Type": "application/json", Connection: "close" });
      res.end('{"error":{"code":"permission_denied","message":"caller is not authorized"}}');
      return;
    }
    return handler(req,res,context);
  }, { ...options, tls: credentials.tls });
}
module.exports = { createHTTPHost, createFetchHost, createRPCHost, createBoundHTTPHost, BootstrapBinding, readBootstrapBinding, loadBootstrapInput, Diagnostics, DiagnosticCloseError, DiagnosticSinkError, proxyWebSocket, resolvePolicy, PolicyError, HTTPClient, TransportError };
