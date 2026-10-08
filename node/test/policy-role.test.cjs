"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const { once } = require("node:events");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { resolvePolicy, derivePolicy, policyOptions } = require("../policy.cjs");
const { Diagnostics } = require("../diagnostics.cjs");
const { createHTTPHost, HTTPClient } = require("..");

test("role queries preserve parent provenance and never expand budgets", () => {
  const environment = { XGC2_XRPC_MAX_RESPONSE_BYTES: "64", OTHER_SECRET: "never-exposed" };
  const parent = resolvePolicy({ environment, ceilings: { MAX_RESPONSE_BYTES: 64 } });
  const original = parent.effective();
  const caps = { MAX_RESPONSE_BYTES: 4 };
  const role = derivePolicy(parent, { role: "public-edge", ceilings: caps });
  caps.MAX_RESPONSE_BYTES = 999;
  environment.XGC2_XRPC_MAX_RESPONSE_BYTES = "1";
  const query = role.effective(), field = query.fields.MAX_RESPONSE_BYTES;
  assert.equal(query.role, "public-edge");
  assert.equal(query.revision, 1);
  assert.equal(query.parent, original);
  assert.equal(query.roleCaps.MAX_RESPONSE_BYTES, 4);
  assert.equal(field.value, 4);
  assert.equal(field.source, "role_cap");
  assert.equal(field.parentValue, 64);
  assert.equal(field.parentSource, "environment");
  assert.equal(field.parentCeiling, 64);
  assert.equal(field.roleCap, 4);
  assert.equal(field.ceiling, 4);
  assert.equal(role.fields.HOST_MAX_CONNECTIONS.roleCap, null);
  assert.equal(role.fields.HOST_MAX_CONNECTIONS.source, "sdk_default");
  assert.equal(role.fields.HOST_MAX_CONNECTIONS.ceiling, null);
  assert.equal(role.effective(), query, "unchanged parent queries reuse the frozen role snapshot");
  assert.equal(parent.effective(), original);
  assert.equal(parent.fields.MAX_RESPONSE_BYTES.value, 64);
  assert.ok(!JSON.stringify(query).includes("never-exposed"));
  assert.ok(Object.isFrozen(role) && Object.isFrozen(query) && Object.isFrozen(query.fields) && Object.isFrozen(field) && Object.isFrozen(query.roleCaps));
  assert.throws(() => { field.value = 8; }, TypeError);
  assert.throws(() => { query.roleCaps.MAX_RESPONSE_BYTES = 8; }, TypeError);
  assert.equal(Object.hasOwn(role, "update"), false);
  const larger = derivePolicy(parent, { role: "larger-cap", ceilings: { MAX_RESPONSE_BYTES: 128 } });
  assert.equal(larger.fields.MAX_RESPONSE_BYTES.value, 64);
  assert.equal(larger.fields.MAX_RESPONSE_BYTES.source, "environment");
  assert.equal(larger.fields.MAX_RESPONSE_BYTES.ceiling, 64);
  assert.equal(larger.fields.MAX_RESPONSE_BYTES.roleCap, 128);
  const unconstrained = resolvePolicy({ environment: { XGC2_XRPC_MAX_RESPONSE_BYTES: "64" } });
  assert.equal(derivePolicy(unconstrained, { role: "bounded", ceilings: { MAX_RESPONSE_BYTES: 8 } }).fields.MAX_RESPONSE_BYTES.ceiling, 8);
});

test("role caps reject invalid parent/global policy and unsupported fields before effects", () => {
  assert.throws(() => resolvePolicy({ environment: { XGC2_XRPC_MAX_RESPONSE_BYTES: "64" }, ceilings: { MAX_RESPONSE_BYTES: 32 } }), (error) => error.field === "MAX_RESPONSE_BYTES");
  const parent = resolvePolicy({ environment: {} });
  for (const role of [undefined, null, "", " space ", "r/a", "中文", "a".repeat(129), new String("role")]) assert.throws(() => derivePolicy(parent, { role }), (error) => error.field === "role");
  assert.doesNotThrow(() => derivePolicy(parent, { role: "r" }));
  assert.doesNotThrow(() => derivePolicy(parent, { role: "a".repeat(128) }));
  for (const value of [0, -1, 1.5, NaN, Infinity, 1n, "4", "04", true, 2147483648]) assert.throws(() => derivePolicy(parent, { role: "bad", ceilings: { MAX_RESPONSE_BYTES: value } }), (error) => error.field === "MAX_RESPONSE_BYTES");
  assert.throws(() => derivePolicy(parent, { role: "bad", ceilings: { CALL_TIMEOUT_MS: 86400001 } }), (error) => error.field === "CALL_TIMEOUT_MS");
  for (const field of ["LOG_LEVEL", "LOG_FORMAT", "UNKNOWN", "GRPC_MAX_STREAMS_PER_CONNECTION", "TLS_MIN_VERSION", "INSTANCE_ID"]) assert.throws(() => derivePolicy(parent, { role: "bad", ceilings: { [field]: 1 } }), (error) => error.field === field);
  for (const name of ["environment", "defaults", "diagnostics", "tls", "instanceId", "update"]) assert.throws(() => derivePolicy(parent, { role: "bad", [name]: {} }), (error) => error.field === name);
  for (const ceilings of [null, [], 3, "caps", false]) assert.throws(() => derivePolicy(parent, { role: "bad", ceilings }), /plain data object/);
  assert.throws(() => derivePolicy(derivePolicy(parent, { role: "first" }), { role: "second" }), /genuine resolved parent/);
});

test("role descriptions cannot execute accessors or Proxy reentry and retain no supplied cap object", () => {
  const parent = resolvePolicy({ environment: {} });
  let executed = 0;
  const accessor = { get MAX_RESPONSE_BYTES() { executed++; return 4; } };
  assert.throws(() => derivePolicy(parent, { role: "accessor", ceilings: accessor }), /own data value/);
  assert.throws(() => derivePolicy(parent, { get role() { executed++; return "getter"; } }), /own data value/);
  const proxy = new Proxy({ MAX_RESPONSE_BYTES: 4 }, { ownKeys() { executed++; throw Error("must not execute"); } });
  assert.throws(() => derivePolicy(parent, { role: "proxy", ceilings: proxy }), /plain data object/);
  assert.throws(() => derivePolicy(parent, new Proxy({ role: "proxy" }, { getPrototypeOf() { executed++; throw Error("must not execute"); } })), /plain data object/);
  assert.equal(executed, 0);
  const before = parent.effective();
  assert.equal(derivePolicy(parent, { role: "safe", ceilings: Object.assign(Object.create(null), { MAX_RESPONSE_BYTES: 4 }) }).fields.MAX_RESPONSE_BYTES.value, 4);
  assert.equal(parent.effective(), before);
});

test("policy options accept genuine resolved/role owners and reject shaped or copied policies", async () => {
  const parent = resolvePolicy({ environment: {} });
  const role = derivePolicy(parent, { role: "small", ceilings: { MAX_RESPONSE_BYTES: 4 } });
  assert.equal(policyOptions({ policy: parent }, { MAX_RESPONSE_BYTES: "maxResponseBytes" }).maxResponseBytes, parent.fields.MAX_RESPONSE_BYTES.value);
  assert.equal(policyOptions({ policy: role }, { MAX_RESPONSE_BYTES: "maxResponseBytes" }).maxResponseBytes, 4);
  assert.throws(() => policyOptions({ policy: role, maxResponseBytes: 8 }, { MAX_RESPONSE_BYTES: "maxResponseBytes" }), /conflicts/);
  for (const fake of [{ revision: 1, fields: {} }, parent.effective(), { ...parent }, { ...role }, new Proxy(parent, {}), false, 1]) {
    assert.throws(() => policyOptions({ policy: fake }, {}), /genuine resolved or derived/);
    assert.throws(() => createHTTPHost(() => {}, { policy: fake }), /genuine resolved or derived/);
  }
  const ordinary = createHTTPHost((_req, res) => res.end(), { policy: parent });
  const derived = createHTTPHost((_req, res) => res.end(), { policy: role });
  await Promise.all([ordinary.close(), derived.close()]);
});

test("role views follow the parent's LOG_LEVEL revision and shared sink without an independent update", async () => {
  const diagnostics = new Diagnostics({ sink: { kind: "supervisor_stderr", rotationOwner: "supervisor" } });
  const parent = resolvePolicy({ environment: {}, diagnostics });
  const role = derivePolicy(parent, { role: "edge", ceilings: { MAX_RESPONSE_BYTES: 4 } });
  const original = role.effective();
  try {
    assert.equal(role.diagnostics, diagnostics);
    assert.equal(original.fields.LOG_LEVEL.value, "info");
    assert.equal(role.update, undefined);
    parent.update({ LOG_LEVEL: "debug" }, { expectedRevision: 1 });
    const current = role.effective();
    assert.equal(role.revision, 2);
    assert.equal(current.revision, parent.revision);
    assert.equal(current.parent, parent.effective());
    assert.equal(current.fields.LOG_LEVEL.value, "debug");
    assert.equal(current.fields.LOG_LEVEL.parentValue, "debug");
    assert.equal(current.fields.LOG_LEVEL.parentSource, "administrative");
    assert.equal(current.fields.LOG_LEVEL.source, "administrative");
    assert.equal(current.fields.LOG_LEVEL.roleCap, null);
    assert.equal(current.fields.MAX_RESPONSE_BYTES.value, 4);
    assert.equal(role.effective(), current);
    assert.equal(original.revision, 1);
    assert.equal(original.fields.LOG_LEVEL.value, "info");
    assert.equal(diagnostics.status().policyRevision, 2);
    assert.equal(diagnostics.status().workerStarted, false, "a role creates no diagnostic writer");
  } finally { await diagnostics.close(); }
});

test("two real hosts enforce distinct role budgets from one unchanged parent snapshot", async () => {
  const parent = resolvePolicy({ environment: { XGC2_XRPC_MAX_RESPONSE_BYTES: "64" } });
  const before = parent.effective();
  const small = derivePolicy(parent, { role: "four-bytes", ceilings: { MAX_RESPONSE_BYTES: 4 } });
  const large = derivePolicy(parent, { role: "eight-bytes", ceilings: { MAX_RESPONSE_BYTES: 8 } });
  const hosts = [small, large].map((policy) => createHTTPHost((_req, res) => res.end("12345"), { policy }));
  try {
    for (const host of hosts) { host.server.listen(0, "127.0.0.1"); await once(host.server, "listening"); }
    const request = (host) => new Promise((resolve, reject) => {
      const call = http.get({ host: "127.0.0.1", port: host.server.address().port }, (response) => {
        let body = "";
        response.on("data", (chunk) => body += chunk);
        response.once("error", reject);
        response.once("end", () => resolve({ status: response.statusCode, body }));
      });
      call.once("error", reject);
    });
    await assert.rejects(request(hosts[0]), (error) => error.code === "ECONNRESET");
    assert.deepEqual(await request(hosts[1]), { status: 200, body: "12345" });
    assert.equal(parent.effective(), before);
    assert.equal(parent.fields.MAX_RESPONSE_BYTES.value, 64);
    assert.equal(small.fields.MAX_RESPONSE_BYTES.value, 4);
    assert.equal(large.fields.MAX_RESPONSE_BYTES.value, 8);
  } finally { await Promise.all(hosts.map((host) => host.close())); }
});

test("client role deadlines constrain actual native I/O despite a longer caller budget", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "xrpc-role-client-"));
  const address = path.join(directory, "host.sock");
  const parent = resolvePolicy({ environment: { XGC2_XRPC_CALL_TIMEOUT_MS: "2000" } });
  const short = derivePolicy(parent, { role: "short-client", ceilings: { CALL_TIMEOUT_MS: 20 } });
  const long = derivePolicy(parent, { role: "long-client", ceilings: { CALL_TIMEOUT_MS: 1000 } });
  const clients = [short, long].map((policy) => new HTTPClient({ policy, localTarget: "test" }));
  const observedBudgets = [];
  const server = http.createServer((request, response) => {
    observedBudgets.push({ requestId: request.headers["x-request-id"], timeout: Number(request.headers["x-xrpc-timeout-ms"]) });
    response.setHeader("X-Xrpc-Instance-ID", "role:client");
    response.setHeader("X-Request-ID", request.headers["x-request-id"]);
    setTimeout(() => response.end("ok"), 80);
  });
  try {
    assert.throws(() => new HTTPClient({ policy: short, callTimeoutMs: 1000 }), /conflicts/);
    assert.throws(() => new HTTPClient({ callTimeoutMs: 86400001 }), /wire maximum/);
    server.listen(address); await once(server, "listening");
    const ref = { target_id: "test", service: "role", api_version: "v1", instance_id: "role:client", profile: "http.v1", endpoint: { kind: "unix", address } };
    const small = assert.rejects(clients[0].call(ref, "/echo", { requestId: "short", timeoutMs: 2000 }), (error) => error.code === "deadline_exceeded");
    const large = clients[1].call(ref, "/echo", { requestId: "long", timeoutMs: 2000 });
    await small;
    const reply = await large;
    assert.equal(reply.status, 200); assert.equal(reply.body.toString(), "ok");
    for (const observation of observedBudgets) assert.ok(observation.timeout <= (observation.requestId === "short" ? 20 : 1000));
    assert.ok(observedBudgets.some((entry) => entry.requestId === "long"));
    assert.equal(parent.fields.CALL_TIMEOUT_MS.value, 2000);
    assert.equal(short.fields.CALL_TIMEOUT_MS.value, 20);
  } finally {
    clients.forEach((client) => client.close());
    await new Promise((resolve) => server.close(resolve));
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
