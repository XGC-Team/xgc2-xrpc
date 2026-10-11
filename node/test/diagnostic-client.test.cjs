"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const https = require("node:https");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { once } = require("node:events");
const { execFile, execFileSync } = require("node:child_process");
const { HTTPClient } = require("../client.cjs");
const { Diagnostics } = require("../diagnostics.cjs");
const sink = { kind: "supervisor_stderr", rotationOwner: "supervisor" };
const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "xrpc-client-diagnostic-"));
function certificate(name, alternative) {
  const keyFile = path.join(temporary, `${name}.key`), certFile = path.join(temporary, `${name}.pem`);
  execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", keyFile, "-out", certFile, "-days", "1", "-subj", `/CN=${name}`, "-addext", `subjectAltName=${alternative}`], { stdio: "ignore" });
  return { key: fs.readFileSync(keyFile), cert: fs.readFileSync(certFile) };
}
const valid = certificate("fixture", "IP:127.0.0.1"), mismatch = certificate("wrong.fixture", "DNS:wrong.fixture");
fs.rmSync(temporary, { recursive: true });
function reference(address, instance_id = "fixture:1") { return { target_id: "local", service: "fixture", api_version: "v1", instance_id, profile: "http.v1", endpoint: { kind: "https", address } }; }
function diagnostics(limits = {}) {
  const owner = new Diagnostics({ sink, level: "error" });
  return { owner, options: { diagnostics: owner, ...limits } };
}
async function server(handler, identity = valid, options = {}) {
  const native = https.createServer({ ...identity, ...options }, (req, res) => { res.setHeader("X-Xrpc-Instance-Id", "fixture:1"); handler(req, res); });
  native.listen(0, "127.0.0.1"); await once(native, "listening");
  return { native, ref: reference(`https://127.0.0.1:${native.address().port}`), async close() { native.closeAllConnections(); await new Promise((resolve) => native.close(resolve)); } };
}
async function eventually(predicate) {
  for (let attempt = 0; attempt < 200; attempt++) { if (predicate()) return; await new Promise((resolve) => setTimeout(resolve, 5)); }
  assert.ok(predicate(), "maintained local lifecycle state did not settle");
}

test("clients share one diagnostics owner and complete only consumed streams", async () => {
  const { owner, options } = diagnostics();
  const fixture = await server((_req, res) => res.end("bounded"));
  const first = new HTTPClient({ ...options, tls: { ca: valid.cert } }), second = new HTTPClient({ ...options, tls: { ca: valid.cert } });
  try {
    const reply = await first.stream(fixture.ref, "/private-route", { timeoutMs: 1000, requestId: "stream:owned" });
    assert.equal(first.stats().inFlight, 1);
    assert.equal(owner.status().eventCounts.call_started, 1);
    assert.equal(owner.status().eventCounts.call_completed, 0);
    const closed = once(reply.body, "close"); reply.body.resume(); await closed;
    assert.equal(first.stats().inFlight, 0);
    assert.equal(owner.status().eventCounts.call_completed, 1);
    await second.call(fixture.ref, "/other-private-route", { timeoutMs: 1000 });
    await eventually(() => owner.status().eventCounts.call_completed === 2);
    first.close(); second.close();
    await eventually(() => owner.status().eventCounts.shutdown_completed === 2);
    assert.equal(owner.status().eventCounts.call_started, 2);
    assert.equal(owner.status().eventCounts.shutdown_started, 2);
    assert.equal(owner.status().state, "running", "clients must not close the shared sink");
    assert.equal(first.options.diagnostics, second.options.diagnostics);
  } finally { first.close(); second.close(); await fixture.close(); await owner.close(); }
});

test("closing an unread completed response reports cancellation rather than success", async () => {
  const { owner, options } = diagnostics();
  const fixture = await server((_req, res) => res.end("unread"));
  const client = new HTTPClient({ ...options, tls: { ca: valid.cert } });
  try {
    const reply = await client.stream(fixture.ref, "/unread", { timeoutMs: 1000 });
    const closed = once(reply.body, "close"); reply.close(); await closed;
    assert.equal(client.stats().inFlight, 0);
    assert.equal(owner.status().eventCounts.cancelled, 1);
    assert.equal(owner.status().eventCounts.call_completed, 0);
    client.close(); await eventually(() => owner.status().eventCounts.shutdown_completed === 1);
    assert.equal(owner.status().state, "running");
  } finally { client.close(); await fixture.close(); await owner.close(); }
});

for (const cause of ["caller", "deadline", "owner_close"]) test(`${cause} termination emits one terminal event and retains ownership until close`, async () => {
  const { owner, options } = diagnostics();
  let entered;
  const started = new Promise((resolve) => entered = resolve);
  const fixture = await server(() => entered());
  const client = new HTTPClient({ ...options, tls: { ca: valid.cert } });
  const abort = new AbortController();
  const call = client.call(fixture.ref, "/held", { timeoutMs: cause === "deadline" ? 150 : 1000, signal: abort.signal });
  const checked = assert.rejects(call, (error) => error.code === (cause === "deadline" ? "deadline_exceeded" : "cancelled"));
  try {
    await started;
    assert.equal(owner.status().eventCounts.call_started, 1);
    assert.equal(owner.status().eventCounts.call_completed, 0);
    if (cause === "caller") abort.abort(); else if (cause === "owner_close") client.close();
    await checked;
    await eventually(() => client.stats().inFlight === 0);
    const counts = owner.status().eventCounts;
    assert.equal(counts[cause === "deadline" ? "deadline_exceeded" : "cancelled"], 1);
    assert.equal(counts.transport_failed, 0);
    assert.equal(counts.call_completed, 0);
    client.close(); await eventually(() => owner.status().eventCounts.shutdown_completed === 1);
    assert.equal(owner.status().state, "running");
  } finally { client.close(); await fixture.close(); await owner.close(); }
});

test("global rejection is logged before body serialization and without a second start", async () => {
  const { owner, options } = diagnostics({ maxInFlight: 1 });
  let entered, serialized = 0;
  const started = new Promise((resolve) => entered = resolve);
  const fixture = await server(() => entered());
  const client = new HTTPClient({ ...options, tls: { ca: valid.cert } });
  const abort = new AbortController();
  const first = client.call(fixture.ref, "/held", { timeoutMs: 1000, signal: abort.signal });
  const checked = assert.rejects(first, (error) => error.code === "cancelled");
  try {
    await started;
    await assert.rejects(client.call(fixture.ref, "/rejected-private", { timeoutMs: 1000, json: { toJSON() { serialized++; return "secret"; } } }), (error) => error.code === "resource_exhausted");
    assert.equal(serialized, 0);
    assert.equal(owner.status().eventCounts.call_started, 1);
    assert.equal(owner.status().eventCounts.call_rejected, 1);
    assert.equal(owner.status().eventCounts.client_pool_full, 1);
    abort.abort(); await checked;
  } finally { client.close(); await fixture.close(); await owner.close(); }
});

test("per-reference and cache admission report fixed pool saturation facts", async () => {
  for (const limit of ["maxConnections", "maxReferences"]) {
    const { owner, options } = diagnostics({ [limit]: 1 });
    let entered;
    const started = new Promise((resolve) => entered = resolve);
    const firstFixture = await server(() => entered()), secondFixture = await server((_req, res) => res.end("not reached"));
    const client = new HTTPClient({ ...options, tls: { ca: valid.cert } });
    const abort = new AbortController();
    const first = client.call(firstFixture.ref, "/held", { timeoutMs: 1000, signal: abort.signal });
    const checked = assert.rejects(first, (error) => error.code === "cancelled");
    try {
      await started;
      const ref = limit === "maxReferences" ? secondFixture.ref : firstFixture.ref;
      await assert.rejects(client.call(ref, "/not-sent", { timeoutMs: 1000 }), (error) => error.code === "resource_exhausted" && error.disposition === "not_sent");
      assert.equal(owner.status().eventCounts.client_pool_full, 1);
      assert.equal(owner.status().eventCounts.call_rejected, 1);
      assert.equal(owner.status().eventCounts.call_started, 1);
      abort.abort(); await checked;
    } finally { client.close(); await firstFixture.close(); await secondFixture.close(); await owner.close(); }
  }
});

test("lost replies, stale instances and oversized responses count failures once", async () => {
  const { owner, options } = diagnostics({ maxResponseBytes: 32 });
  let effects = 0;
  const fixture = await server((req, res) => {
    if (req.url === "/lost") { effects++; req.socket.destroy(); }
    else if (req.url === "/stale") { res.setHeader("X-Xrpc-Instance-Id", "other:instance"); res.end("stale"); }
    else res.end(Buffer.alloc(100));
  });
  const client = new HTTPClient({ ...options, tls: { ca: valid.cert } });
  try {
    await assert.rejects(client.call(fixture.ref, "/lost", { timeoutMs: 1000, method: "POST", json: { mutation: "private" } }), (error) => error.code === "unavailable" && error.disposition === "outcome_unknown");
    await eventually(() => client.stats().inFlight === 0);
    await assert.rejects(client.call(fixture.ref, "/stale", { timeoutMs: 1000 }), (error) => error.code === "conflict");
    await eventually(() => client.stats().inFlight === 0);
    await assert.rejects(client.call(fixture.ref, "/large", { timeoutMs: 1000 }), (error) => error.code === "resource_exhausted");
    await eventually(() => client.stats().inFlight === 0);
    assert.equal(effects, 1);
    assert.equal(owner.status().eventCounts.call_started, 3);
    assert.equal(owner.status().eventCounts.transport_failed, 2);
    assert.equal(owner.status().eventCounts.call_rejected, 1);
    assert.equal(owner.status().eventCounts.call_completed, 0);
  } finally { client.close(); await fixture.close(); await owner.close(); }
});

test("actual client records exclude paths, endpoints, credentials and payload/error content", async () => {
  async function childMain(configuration) {
    const https = require("node:https"), { once } = require("node:events");
    const { HTTPClient } = require(configuration.client), { Diagnostics } = require(configuration.diagnostics);
    const key = Buffer.from(configuration.key, "base64"), cert = Buffer.from(configuration.cert, "base64");
    const native = https.createServer({ key, cert }, (req, res) => { req.resume(); res.setHeader("X-Xrpc-Instance-Id", "fixture:1"); res.end("response-payload-secret"); });
    native.listen(0, "127.0.0.1"); await once(native, "listening");
    const owner = new Diagnostics({ sink: configuration.sink, level: "debug" });
    const client = new HTTPClient({ diagnostics: owner, tls: { ca: cert } });
    const ref = { target_id: "local", service: "fixture", api_version: "v1", instance_id: "fixture:1", profile: "http.v1", endpoint: { kind: "https", address: `https://127.0.0.1:${native.address().port}` } };
    await client.call(ref, "/private-route-secret", { timeoutMs: 1000, headers: { Authorization: "Bearer credential-secret", Cookie: "cookie-secret" }, json: { body: "request-payload-secret" } });
    const bad = {}; bad.self = bad;
    try { await client.call(ref, "/private-error-secret", { timeoutMs: 1000, json: bad }); } catch {}
    client.close();
    while (!owner.status().eventCounts.shutdown_completed) await new Promise((resolve) => setTimeout(resolve, 5));
    await owner.close(); await new Promise((resolve) => native.close(resolve));
    process.stdout.write(JSON.stringify(owner.status()));
  }
  const configuration = { client: require.resolve("../client.cjs"), diagnostics: require.resolve("../diagnostics.cjs"), sink, key: valid.key.toString("base64"), cert: valid.cert.toString("base64") };
  const script = `(${childMain.toString()})(${JSON.stringify(configuration)}).catch(()=>process.exitCode=1);`;
  const { stdout, stderr } = await new Promise((resolve, reject) => execFile(process.execPath, ["-e", script], { timeout: 10000, maxBuffer: 1048576 }, (error, stdout, stderr) => error ? reject(error) : resolve({ stdout, stderr })));
  const records = stderr.trim().split("\n").map((line) => JSON.parse(line));
  for (const secret of ["private-route-secret", "private-error-secret", "credential-secret", "cookie-secret", "request-payload-secret", "response-payload-secret", "127.0.0.1", "circular", "self"]) assert.ok(!stderr.includes(secret), secret);
  assert.ok(records.every((record) => record.operation === "http_client"));
  const status = JSON.parse(stdout);
  assert.equal(status.eventCounts.call_started, 1);
  assert.equal(status.eventCounts.call_completed, 1);
  assert.equal(status.eventCounts.call_rejected, 1);
  assert.equal(status.eventCounts.shutdown_completed, 1);
  assert.equal(status.dropped, 0);
});

test("client TLS rejects verification overrides, connection factories and legacy protocols", () => {
  for (const tls of [
    { rejectUnauthorized: false }, { checkServerIdentity: () => undefined }, { servername: "wrong.fixture" },
    { secureProtocol: "TLSv1_method" }, { minVersion: "TLSv1" }, { minVersion: "TLSv1.1" }, { maxVersion: "TLSv1.1" },
    { minVersion: "TLSv1.3", maxVersion: "TLSv1.2" }, { createConnection: () => {} }, { secureOptions: 0 },
    { secureContext: require("node:tls").createSecureContext() }, { ciphers: "DEFAULT:@SECLEVEL=0" }, { session: Buffer.alloc(1) },
  ]) assert.throws(() => new HTTPClient({ tls }), /TLS verification|unsupported client TLS|TLS versions/);
});

test("trusted CA does not permit a mismatched HTTPS hostname", async () => {
  const { owner, options } = diagnostics();
  let dispatch = 0;
  const fixture = await server((_req, res) => { dispatch++; res.end("must not execute"); }, mismatch);
  const client = new HTTPClient({ ...options, tls: { ca: mismatch.cert } });
  try {
    await assert.rejects(client.call(fixture.ref, "/identity", { timeoutMs: 1000 }), (error) => error.code === "unavailable" && error.disposition === "not_sent" && error.cause?.code === "ERR_TLS_CERT_ALTNAME_INVALID");
    assert.equal(dispatch, 0);
    assert.equal(owner.status().eventCounts.transport_failed, 1);
    assert.equal(owner.status().eventCounts.call_completed, 0);
  } finally { client.close(); await fixture.close(); await owner.close(); }
});

test("startup TLS retains CA/key/cert identity after caller buffer mutation", async () => {
  const fixture = await server((req, res) => { assert.equal(req.socket.authorized, true); res.end("verified identity"); }, valid, { ca: valid.cert, requestCert: true, rejectUnauthorized: true });
  const raw = { ca: Buffer.from(valid.cert), key: Buffer.from(valid.key), cert: Buffer.from(valid.cert), minVersion: "TLSv1.2" };
  const client = new HTTPClient({ tls: raw });
  raw.ca.fill(0); raw.key.fill(0); raw.cert.fill(0); raw.minVersion = "TLSv1";
  try { assert.equal((await client.call(fixture.ref, "/identity", { timeoutMs: 1000 })).body.toString(), "verified identity"); }
  finally { client.close(); await fixture.close(); }
});
