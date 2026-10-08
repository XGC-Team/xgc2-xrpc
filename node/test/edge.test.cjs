"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const net = require("node:net");
const { once } = require("node:events");
const { WebSocket, WebSocketServer } = require("ws");
const { createHTTPHost, proxyWebSocket } = require("..");

async function listen(server) {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return server.address().port;
}
async function get(port, agent) {
  return new Promise((resolve, reject) => {
    http.get({ host: "127.0.0.1", port, agent }, (res) => {
      res.resume();
      res.once("end", () => resolve(res));
    }).on("error", reject);
  });
}

test("HTTP reuses a connection and cleans up idle keepalive", async () => {
  const host = createHTTPHost((_req, res) => res.end("ok"));
  const port = await listen(host.server);
  const agent = new http.Agent({ keepAlive: true });
  try {
    for (let i = 0; i < 3; i++) {
      const res = await get(port, agent);
      assert.equal(res.statusCode, 200);
      assert.ok(res.headers["x-request-id"]);
      assert.equal(host.stats().connections, 1);
    }
    await host.close();
    await host.close();
    assert.equal(host.stats().inFlight, 0);
  } finally { agent.destroy(); await host.close(); }
});

test("HTTP admission rejects excess work without dispatch", async () => {
  let held;
  const host = createHTTPHost((_req, res) => { held = res; }, { maxInFlight: 1 });
  const port = await listen(host.server);
  const first = get(port);
  while (!held) await new Promise((r) => setImmediate(r));
  try {
    const rejected = await get(port);
    assert.equal(rejected.statusCode, 429);
    assert.equal(host.stats().inFlight, 1);
    held.end();
    await first;
  } finally { held?.end(); await host.close(); }
});

test("drain rejects pipelined requests on an already busy connection", async () => {
  let firstResponse, entered;
  const started = new Promise((r) => { entered = r; });
  const calls = [];
  const host = createHTTPHost((req, res) => {
    calls.push(req.url);
    firstResponse = res;
    entered();
  }, { shutdownMs: 200 });
  const port = await listen(host.server);
  const client = net.connect(port, "127.0.0.1");
  client.on("error", () => {});
  client.resume();
  try {
    await once(client, "connect");
    client.write("GET /first HTTP/1.1\r\nHost: local\r\n\r\n");
    await started;
    const closing = host.close();
    const nextRequest = once(host.server, "request");
    client.write("GET /after-close HTTP/1.1\r\nHost: local\r\n\r\n");
    const [, response] = await nextRequest;
    assert.equal(response.statusCode, 503);
    assert.deepEqual(calls, ["/first"]);
    firstResponse.end();
    await closing;
  } finally { firstResponse?.end(); client.destroy(); await host.close(); }
});

for (const kind of ["ping", "pong"]) {
  test(`WS ${kind} flood shares data admission and closes on overload`, { timeout: 5000 }, async () => {
    const upstream = new WebSocketServer({ port: 0, host: "127.0.0.1", autoPong: false });
    await once(upstream, "listening");
    let peer;
    upstream.on("connection", (connection) => {
      peer = connection;
      peer.on("error", () => {});
      peer.pause();
    });
    const host = createHTTPHost((_req, res) => res.end(), { shutdownMs: 200 });
    host.onUpgrade((req, socket, head) => proxyWebSocket(req, socket, head, `ws://127.0.0.1:${upstream.address().port}`, {
      maxPayload: 1024, maxPendingMessages: 2, sendTimeoutMs: 30,
    }));
    const port = await listen(host.server);
    const client = new WebSocket(`ws://127.0.0.1:${port}`, { autoPong: false });
    client.on("error", () => {});
    let timer;
    try {
      await once(client, "open");
      const closed = once(client, "close");
      const payload = Buffer.alloc(125);
      for (let batch = 0; batch < 100 && client.readyState === WebSocket.OPEN; batch++) {
        for (let i = 0; i < 1000; i++) client[kind](payload);
        await new Promise((r) => setImmediate(r));
      }
      await Promise.race([
        closed,
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("control-frame queue did not terminate")), 1000); }),
      ]);
      assert.equal(client.readyState, WebSocket.CLOSED);
    } finally {
      clearTimeout(timer);
      client.terminate(); peer?.terminate();
      await host.close();
      await new Promise((r) => upstream.close(r));
    }
  });
}

test("disconnect cannot release admission while async domain work survives", async () => {
  let entered, release;
  const started = new Promise((r) => { entered = r; });
  const held = new Promise((r) => { release = r; });
  const host = createHTTPHost(async (_req, res) => { entered(); await held; res.end(); }, { maxInFlight: 1 });
  const port = await listen(host.server);
  const first = http.get({ host: "127.0.0.1", port });
  first.on("error", () => {});
  try {
    await started;
    first.destroy();
    await new Promise((r) => setTimeout(r, 20));
    const rejected = await get(port);
    assert.equal(rejected.statusCode, 429);
    assert.equal(host.stats().inFlight, 1);
  } finally {
    release();
    first.destroy();
    await host.close();
  }
});

test("shutdown reports domain work that ignored cancellation", async () => {
  let release, entered;
  const held = new Promise((r) => { release = r; });
  const started = new Promise((r) => { entered = r; });
  const host = createHTTPHost(async (_req, res) => { entered(); await held; res.end(); }, { shutdownMs: 30 });
  const port = await listen(host.server);
  const req = http.get({ host: "127.0.0.1", port });
  req.on("error", () => {});
  try {
    await started;
    await assert.rejects(host.close(), /domain handlers/);
    assert.equal(host.stats().inFlight, 1);
  } finally { release(); req.destroy(); }
});

test("chunked body budget also stops requests without Content-Length", async () => {
  let dispatched = false;
  const host = createHTTPHost(async (req, res) => {
    for await (const _chunk of req) { /* domain streaming reader */ }
    dispatched = true;
    res.end();
  }, { maxBodyBytes: 32 });
  const port = await listen(host.server);
  try {
    await new Promise((resolve, reject) => {
      const req = http.request({ host: "127.0.0.1", port, method: "POST" }, () => reject(new Error("unexpected response")));
      req.on("error", resolve);
      req.write(Buffer.alloc(64));
      req.end();
    });
    assert.equal(dispatched, false);
  } finally { await host.close(); }
});

test("WS preserves selected protocol, text/binary, and ping end to end", async () => {
  const upstream = new WebSocketServer({ port: 0, host: "127.0.0.1", handleProtocols: (p) => p.has("foxglove.websocket.v1") ? "foxglove.websocket.v1" : false });
  await once(upstream, "listening");
  upstream.on("connection", (peer) => peer.on("message", (data, binary) => peer.send(data, { binary })));
  const host = createHTTPHost((_req, res) => res.end());
  host.onUpgrade((req, socket, head) => proxyWebSocket(req, socket, head, `ws://127.0.0.1:${upstream.address().port}`));
  const port = await listen(host.server);
  const client = new WebSocket(`ws://127.0.0.1:${port}`, ["foxglove.websocket.v1"]);
  try {
    await once(client, "open");
    assert.equal(client.protocol, "foxglove.websocket.v1");
    for (const [data, binary] of [["text", false], [Buffer.from([0, 255, 10, 0]), true]]) {
      const reply = once(client, "message");
      client.send(data, { binary });
      const [actual, actualBinary] = await reply;
      assert.deepEqual(actual, Buffer.from(data));
      assert.equal(actualBinary, binary);
    }
    const pong = once(client, "pong");
    client.ping("heartbeat");
    assert.equal((await pong)[0].toString(), "heartbeat");
  } finally {
    client.terminate();
    for (const peer of upstream.clients) peer.terminate();
    await host.close();
    await new Promise((r) => upstream.close(r));
  }
});

test("WS handshake is bounded and shutdown releases upgraded connections", async () => {
  const peers = new Set();
  const silent = net.createServer((peer) => { peers.add(peer); peer.on("close", () => peers.delete(peer)); });
  await listen(silent);
  const host = createHTTPHost((_req, res) => res.end(), { shutdownMs: 50 });
  host.onUpgrade((req, socket, head) => proxyWebSocket(req, socket, head, `ws://127.0.0.1:${silent.address().port}`, { handshakeTimeoutMs: 50 }));
  const port = await listen(host.server);
  const client = new WebSocket(`ws://127.0.0.1:${port}`);
  client.on("error", () => {});
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("unbounded handshake")), 2000);
      client.once("close", () => { clearTimeout(timer); resolve(); });
    });
  } finally {
    client.terminate();
    await host.close();
    for (const peer of peers) peer.destroy();
    await new Promise((r) => silent.close(r));
  }
});
