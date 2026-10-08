"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const net = require("node:net");
const tls = require("node:tls");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { once } = require("node:events");
const { execFileSync } = require("node:child_process");
const { HTTPClient, createRPCHost, resolvePolicy, createHTTPHost } = require("..");
const corpus = require("../../contracts/fixtures/wire.json");
const env = require("../../contracts/fixtures/environment.json");
const registry = require("../runtime-policy.json");
const boot = corpus.instance_id;
const certDir = fs.mkdtempSync(path.join(os.tmpdir(), "xrpc-node-tls-"));
execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", path.join(certDir, "key.pem"), "-out", path.join(certDir, "cert.pem"), "-days", "1", "-subj", "/CN=fixture", "-addext", "subjectAltName=IP:127.0.0.1"], { stdio: "ignore" });
const key = fs.readFileSync(path.join(certDir, "key.pem")), cert = fs.readFileSync(path.join(certDir, "cert.pem"));
fs.rmSync(certDir, { recursive: true });
function ref(address, instance_id = boot) { return { target_id: "local", service: "fixture", api_version: "v1", instance_id, profile: "http.v1", endpoint: { kind: "https", address } }; }
async function httpsHost(handler, options = {}) {
  const host = createRPCHost(handler, { instanceId: boot, tls: { key, cert }, shutdownMs: 100, ...options });
  host.server.listen(0, "127.0.0.1"); await once(host.server, "listening");
  const address = `https://127.0.0.1:${host.server.address().port}`;
  return { host, address, async close() { await host.close(); } };
}
async function raw(address, entry) {
  const socket = tls.connect({ port: new URL(address).port, host: "127.0.0.1", ca: cert }), chunks = [];
  socket.on("error", () => {});
  socket.on("data", (data) => chunks.push(data));
  await once(socket, "secureConnect");
  socket.write(`${entry.method} ${entry.path} HTTP/1.1\r\nHost: fixture\r\nConnection: close\r\n${entry.headers.map(([k,v]) => `${k}: ${v}\r\n`).join("")}\r\n`);
  await once(socket, "close");
  return Number(Buffer.concat(chunks).toString().match(/^HTTP\/1.1 (\d+)/)?.[1]);
}
test("all shared wire fixtures use the native HTTP parser and gate dispatch", async () => {
  let dispatch = 0;
  const fixture = await httpsHost((req, res) => { dispatch++; res.setHeader("Content-Type", "application/json"); res.end('{"ok":true}'); }, { discoveryPaths: ["/v1/describe"] });
  try {
    for (const entry of corpus.cases) {
      const before = dispatch;
      assert.equal(await raw(fixture.address, entry), entry.status, entry.name);
      assert.equal(dispatch - before, entry.dispatch ? 1 : 0, entry.name);
    }
  } finally { await fixture.close(); }
});
test("shared environment corpus resolves sources/ceilings and rejects unsupported fields", () => {
  const supported = new Set(registry.fields.filter((f) => !["diagnostics", "grpc"].includes(f.capability)).map((f) => f.name));
  for (const entry of env.cases) {
    const options = { environment: entry.environment, defaults: entry.defaults, ceilings: entry.ceilings, capabilities: entry.capabilities };
    if (entry.error_field) assert.throws(() => resolvePolicy(options), (e) => e.field === entry.error_field, entry.name);
    else {
      const policy = resolvePolicy(options);
      for (const [field,value] of Object.entries(entry.values ?? {})) if (supported.has(field)) assert.equal(policy.fields[field]?.value, value, `${entry.name}:${field}`);
      for (const [field,source] of Object.entries(entry.sources ?? {})) if (supported.has(field)) assert.equal(policy.fields[field]?.source, source, `${entry.name}:${field}`);
      assert.equal(policy.revision, 1);
      assert.ok(!JSON.stringify(policy).includes("not-returned"));
    }
  }
  assert.throws(() => resolvePolicy(), /snapshot/);
  const policy = resolvePolicy({ environment: { XGC2_XRPC_HOST_MAX_CONNECTIONS: "2" } });
  const host = createHTTPHost((_req,res) => res.end(), { policy });
  assert.throws(() => createHTTPHost(() => {}, { policy, maxConnections: 3 }), /conflicts/);
  void host.close();
});
test("one shared client reuses connections and rejects stale/remote references", async () => {
  const fixture = await httpsHost((_req,res) => res.end('{"ok":true}'));
  const client = new HTTPClient({ localTarget: "local", tls: { ca: cert } });
  try {
    for (let i = 0; i < 4; i++) assert.equal((await client.call(ref(fixture.address), "/v1/echo", { timeoutMs: 1000, json: { n: i }, requestId: `caller:${i}` })).status, 200);
    assert.equal(fixture.host.stats().connections, 1);
    assert.equal(client.stats().references, 1);
    await assert.rejects(client.call(ref(fixture.address, "stale"), "/v1/echo", { timeoutMs: 1000 }), (e) => e.disposition === "outcome_unknown" && e.code === "conflict");
    await assert.rejects(client.call({ ...ref(fixture.address), target_id: "remote", endpoint: { kind: "unix", address: "/does/not/exist.sock" } }, "/v1/echo", { timeoutMs: 1000 }), (e) => e.disposition === "not_sent");
    client.close();
    await assert.rejects(client.call(ref(fixture.address), "/v1/echo", { timeoutMs: 1000 }), (e) => e.disposition === "not_sent");
  } finally { client.close(); await fixture.close(); }
});
test("effect applied then reply lost executes exactly once without replay", async () => {
  let effects = 0;
  const fixture = await httpsHost((_req,res) => { effects++; res.socket.destroy(); });
  const client = new HTTPClient({ localTarget: "local", tls: { ca: cert } });
  try {
    await assert.rejects(client.call(ref(fixture.address), "/v1/mutate", { timeoutMs: 300, method: "PUT", json: { effect: true } }), (e) => e.disposition === "outcome_unknown");
    assert.equal(effects, 1);
  } finally { client.close(); await fixture.close(); }
});
test("client admission and whole-call timeout stop native I/O", async () => {
  let entered;
  const started = new Promise((r) => { entered = r; });
  const fixture = await httpsHost((_req,_res) => entered());
  const client = new HTTPClient({ localTarget: "local", tls: { ca: cert }, maxInFlight: 1 });
  const first = client.call(ref(fixture.address), "/v1/held", { timeoutMs: 80 });
  const rejected = assert.rejects(first, (e) => e.code === "deadline_exceeded" && e.disposition === "outcome_unknown");
  try {
    await started;
    await assert.rejects(client.call(ref(fixture.address), "/v1/held", { timeoutMs: 80 }), (e) => e.code === "resource_exhausted" && e.disposition === "not_sent");
    await rejected;
  } finally { client.close(); await fixture.close(); }
});
test("response size and stream ownership bound slow consumers", async () => {
  const fixture = await httpsHost((_req,res) => res.end(Buffer.alloc(100)));
  const client = new HTTPClient({ localTarget: "local", tls: { ca: cert }, maxResponseBytes: 32, maxInFlight: 1 });
  try {
    await assert.rejects(client.call(ref(fixture.address), "/v1/echo", { timeoutMs: 1000 }), (e) => e.code === "resource_exhausted");
  } finally { client.close(); await fixture.close(); }
});
test("deadline and terminal close reclaim a completed but unread stream", async () => {
  const fixture = await httpsHost((_req,res) => res.end("ok"));
  const client = new HTTPClient({ localTarget: "local", tls: { ca: cert }, maxInFlight: 1 });
  try {
    const first = await client.stream(ref(fixture.address), "/v1/echo", { timeoutMs: 30 });
    assert.equal(client.stats().inFlight, 1);
    await new Promise((resolve) => first.body.once("close", resolve));
    assert.equal(client.stats().inFlight, 0);
    const second = await client.stream(ref(fixture.address), "/v1/echo", { timeoutMs: 1000 });
    const closed = once(second.body, "close").catch(() => {});
    client.close(); await closed;
    assert.equal(client.stats().inFlight, 0);
  } finally { client.close(); await fixture.close(); }
});
test("HEAD resource Content-Length does not consume response-body policy", async () => {
  const fixture = await httpsHost((_req,res) => { res.setHeader("Content-Length", "100"); res.end(Buffer.alloc(100)); }, { maxResponseBytes: 32 });
  const client = new HTTPClient({ localTarget: "local", tls: { ca: cert }, maxResponseBytes: 32 });
  try {
    const response = await client.call(ref(fixture.address), "/v1/echo", { method: "HEAD", timeoutMs: 1000 });
    assert.equal(response.status, 200); assert.equal(response.body.length, 0);
  } finally { client.close(); await fixture.close(); }
});
test("retrying close waits for a previously noncooperative handler", async () => {
  let entered, release;
  const started = new Promise((r) => entered = r), held = new Promise((r) => release = r);
  const fixture = await httpsHost(async (_req,res) => { entered(); await held; res.end(); }, { shutdownMs: 20 });
  const req = require("node:https").get(fixture.address, { ca: cert, headers: Object.fromEntries(corpus.cases[0].headers) });
  req.on("error", () => {});
  await started;
  await assert.rejects(fixture.host.close(), /domain handlers/);
  release();
  await new Promise((r) => setImmediate(r));
  await fixture.close();
});
test("policy conflicts, unsupported Unix host and native timeout overflow fail early", async () => {
  const policy = resolvePolicy({ environment: { XGC2_XRPC_CALL_TIMEOUT_MS: "60000", XGC2_XRPC_HEADER_TIMEOUT_MS: "45000" } });
  assert.throws(() => createRPCHost(() => {}, { instanceId: boot, policy, callTimeoutMs: 50000 }), /conflicts/);
  assert.throws(() => createRPCHost(() => {}, { instanceId: boot, callTimeoutMs: 2147483648 }), /wire maximum|finite/);
  const host = createRPCHost(() => {}, { instanceId: boot, policy });
  assert.equal(host.server.requestTimeout, 60000);
  assert.equal(host.server.headersTimeout, 45000);
  if ("keepAliveTimeoutBuffer" in host.server) assert.equal(host.server.keepAliveTimeoutBuffer, 0);
  assert.throws(() => host.server.listen("/tmp/foreign.sock"), /Unix lease/);
  await host.close();
});
test("request target, runtime header types and response headers are bounded", async () => {
  let dispatch = 0;
  const fixture = await httpsHost((_req,res) => { dispatch++; res.setHeader("X-Large", "a".repeat(2000)); res.end(); }, { maxHeaderBytes: 1024 });
  const client = new HTTPClient({ localTarget: "local", tls: { ca: cert }, maxHeaderBytes: 1024 });
  try {
    await assert.rejects(client.call(ref(fixture.address), "/" + "a".repeat(2000), { timeoutMs: 1000 }), (e) => e.code === "resource_exhausted" && e.disposition === "not_sent");
    await assert.rejects(client.call(ref(fixture.address), "/v1/echo", { timeoutMs: 1000, headers: { "X-Large": Array(1000).fill("") } }), (e) => e.code === "invalid_argument" && e.disposition === "not_sent");
    assert.equal(dispatch, 0);
    await assert.rejects(client.call(ref(fixture.address), "/v1/echo", { timeoutMs: 1000 }), (e) => e.disposition === "outcome_unknown");
    assert.equal(dispatch, 1);
  } finally { client.close(); await fixture.close(); }
});
test("tiny positive response-header budget fails a request without process exception", async () => {
  const host = createHTTPHost((_req,res) => res.end("ok"), { maxHeaderBytes: 128 });
  host.server.listen(0, "127.0.0.1"); await once(host.server, "listening");
  try {
    await new Promise((resolve, reject) => {
      http.get({ host: "127.0.0.1", port: host.server.address().port }, (response) => { response.resume(); response.on("end", () => reject(new Error("head limit should reject"))); }).on("error", resolve);
    });
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(host.stats().inFlight, 0);
  } finally { await host.close(); }
});
for (const kind of ["reason", "trailer"]) test(`${kind} output cannot bypass the response head budget`, async () => {
  const host = createHTTPHost((_req,res) => {
    if (kind === "reason") res.statusMessage = "a".repeat(10000);
    else res.addTrailers({ "X-Payload": "a".repeat(10000) });
    res.end("ok");
  }, { maxHeaderBytes: 512 });
  host.server.listen(0, "127.0.0.1"); await once(host.server, "listening");
  try {
    await new Promise((resolve, reject) => {
      const request = http.get({ host: "127.0.0.1", port: host.server.address().port }, () => reject(new Error("oversized response head was sent")));
      request.on("error", resolve);
    });
  } finally { await host.close(); }
});
