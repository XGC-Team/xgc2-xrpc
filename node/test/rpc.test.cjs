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
const { HTTPClient, createRPCHost, createBoundHTTPHost, BootstrapBinding, readBootstrapBinding, loadBootstrapInput, Diagnostics, resolvePolicy, createHTTPHost } = require("..");
const corpus = require("../../contracts/fixtures/wire.json");
const env = require("../../contracts/fixtures/environment.json");
const registry = require("../runtime-policy.json");
const bootstraps = require("../../contracts/fixtures/bootstrap.json");
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
  assert.throws(() => host.server.listen("/tmp/foreign.sock"), /unixPath/);
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
test("shared bootstrap corpus and safe explicit file loading reject legacy/unsafe inputs", () => {
  for (const entry of bootstraps.cases) {
    if (entry.valid) {
      const binding = new BootstrapBinding(entry.binding);
      assert.equal(binding.serviceRef("actual:fresh-boot").instance_id, "actual:fresh-boot");
    } else assert.throws(() => new BootstrapBinding(entry.binding), undefined, entry.name);
  }
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "xrpc-bootstrap-"));
  const file = path.join(directory, "binding.json");
  try {
    fs.writeFileSync(file, JSON.stringify(bootstraps.cases[0].binding), { mode: 0o600 });
    assert.equal(readBootstrapBinding(file).serviceRef("fresh:1").service, "fixture");
    fs.chmodSync(file, 0o644); assert.throws(() => readBootstrapBinding(file), /mode0600/);
    fs.chmodSync(file, 0o600); fs.linkSync(file, path.join(directory,"hardlink")); assert.throws(() => readBootstrapBinding(file), /single-link/);
    fs.unlinkSync(path.join(directory,"hardlink"));
    fs.symlinkSync(file,path.join(directory,"symlink")); assert.throws(() => readBootstrapBinding(path.join(directory,"symlink")));
    const fifo = path.join(directory, "fifo"); execFileSync("mkfifo", ["-m", "600", fifo]);
    assert.throws(() => execFileSync(process.execPath, ["-e", "require(process.argv[1]).readBootstrapBinding(process.argv[2])", path.resolve(__dirname,".."), fifo], { timeout: 1000, stdio: "pipe" }), (error) => error.code !== "ETIMEDOUT" && error.status === 1);
    fs.writeFileSync(file, " ".repeat(16385)); assert.throws(() => readBootstrapBinding(file), /16KiB/);
  } finally { fs.rmSync(directory, { recursive: true }); }
});
test("public startup loader reads actual bounded grants and authenticates native mutual TLS", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "xrpc-bootstrap-input-"));
  const write = (name, bytes) => { const file = path.join(directory,name); fs.writeFileSync(file,bytes,{mode:0o600}); return file; };
  const keyFile = write("key.pem",key), certFile = write("cert.pem",cert), tokenFile = write("token","startup-fixture");
  const binding = bootstraps.cases[0].binding;
  const input = { schema_version:1, binding, grants: {
    [binding.secret_handles.tls_identity]: { kind:"tls_identity",cert_file:certFile,key_file:keyFile },
    [binding.secret_handles.tls_trust]: { kind:"tls_trust",ca_file:certFile },
    [binding.secret_handles.authorization]: { kind:"bearer",token_file:tokenFile },
    "storage-auth": {kind:"bearer",token_file:tokenFile},
  }, application: { storage: { authorization:"storage-auth" } } };
  const inputFile = write("input.json",JSON.stringify(input));
  let host, client, dispatch = 0;
  try {
    const loaded = loadBootstrapInput(inputFile,{role:"server"});
    assert.ok(Object.isFrozen(loaded.application.storage));
    assert.equal(loaded.resolveGrant("storage-auth","authorization").headers.authorization,"Bearer startup-fixture");
    assert.throws(() => loaded.resolveGrant("ungranted","authorization"));
    assert.throws(() => loaded.resolveGrant("storage-auth","tls_trust"));
    const credentials = loaded.binding.resolveCredentials(loaded.resolveGrant,"client");
    host = createBoundHTTPHost((_req,res) => { dispatch++; res.end("ready"); },{...loaded,instanceId:boot});
    host.server.listen(0,"127.0.0.1"); await once(host.server,"listening");
    client = new HTTPClient({tls:credentials.tls});
    const reference = ref(`https://127.0.0.1:${host.server.address().port}`);
    assert.equal((await client.call(reference,"/echo",{timeoutMs:1000})).status,403); assert.equal(dispatch,0);
    assert.equal((await client.call(reference,"/echo",{timeoutMs:1000,headers:credentials.authorization.headers})).body.toString(),"ready");
    assert.equal(dispatch,1);
    input.grants["unused-trust"] = {kind:"tls_trust",ca_file:write("invalid-ca","not a certificate")};
    fs.writeFileSync(inputFile,JSON.stringify(input));
    assert.throws(() => loadBootstrapInput(inputFile,{role:"server"}));
    delete input.grants["unused-trust"]; fs.writeFileSync(inputFile,JSON.stringify(input));
    fs.writeFileSync(certFile,"not a certificate");
    assert.throws(() => loadBootstrapInput(inputFile,{role:"server"}));
  } finally { client?.close(); await host?.close(); fs.rmSync(directory,{recursive:true}); }
});
test("authorization must return true before the live RPC deadline to dispatch", async () => {
  for (const verdict of ["false", "late"]) {
    let dispatch = 0;
    const binding = new BootstrapBinding(bootstraps.cases[0].binding);
    const host = createBoundHTTPHost((_req,res) => { dispatch++; res.end("bad"); }, { binding,instanceId:boot,
      resolveGrant: (_handle,kind) => kind === "tls_identity" ? {key,cert} : kind === "tls_trust" ? {ca:cert} : {authorize:async () => {
        if (verdict === "late") { await new Promise(resolve=>setTimeout(resolve,100)); return true; }
        return "false";
      }} });
    host.server.listen(0,"127.0.0.1"); await once(host.server,"listening");
    const client = new HTTPClient({tls:{ca:cert,key,cert}});
    try {
      const call = client.call(ref(`https://127.0.0.1:${host.server.address().port}`),"/echo",{timeoutMs:30});
      if (verdict === "late") await assert.rejects(call); else assert.equal((await call).status,403);
      await new Promise(resolve=>setTimeout(resolve,110)); assert.equal(dispatch,0);
    } finally {client.close();await host.close();}
  }
});
test("empty and malformed trust grants fail before a native host is created", () => {
  const binding = new BootstrapBinding(bootstraps.cases[0].binding);
  for (const ca of ["not a certificate",Buffer.alloc(0),[],cert.toString()+"garbage"]) {
    assert.throws(()=>binding.resolveCredentials((_handle,kind)=>kind === "tls_identity" ? {key,cert} : kind === "tls_trust" ? {ca} : {authorize:()=>true},"server"));
  }
});
test("common bootstrap resolves native TLS/auth grants once and gates domain dispatch", async () => {
  const binding = new BootstrapBinding(bootstraps.cases[0].binding);
  const resolved = [], authorization = { authorize: (req) => req.headers.authorization === "Bearer fixture", headers: { Authorization: "Bearer fixture" } };
  function resolveGrant(handle, kind) {
    resolved.push([handle,kind]);
    return kind === "tls_identity" ? { key, cert } : kind === "tls_trust" ? { ca: cert } : authorization;
  }
  let dispatch = 0;
  const host = createBoundHTTPHost((_req,res) => { dispatch++; res.end('{"ok":true}'); }, { binding, instanceId: boot, resolveGrant });
  assert.equal(resolved.length,3);
  host.server.listen(0, "127.0.0.1"); await once(host.server,"listening");
  const address = `https://127.0.0.1:${host.server.address().port}`;
  const client = new HTTPClient({ localTarget: "local", tls: { ca:cert,key,cert } });
  try {
    assert.equal((await client.call(ref(address),"/v1/echo",{timeoutMs:1000})).status,403);
    assert.equal(dispatch,0);
    assert.equal((await client.call(ref(address),"/v1/echo",{timeoutMs:1000,headers:authorization.headers})).status,200);
    assert.equal(dispatch,1); assert.equal(resolved.length,3);
  } finally { client.close(); await host.close(); }
});
