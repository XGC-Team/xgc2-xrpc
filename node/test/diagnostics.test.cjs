"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { spawn, execFile } = require("node:child_process");
const { once } = require("node:events");
const { openSync, closeSync } = require("node:fs");
const { Diagnostics, DiagnosticCloseError } = require("../diagnostics.cjs");
const { resolvePolicy, policyOptions } = require("../policy.cjs");
const registry = require("../runtime-policy.json");
const corpus = require("../../contracts/fixtures/environment.json");
const diagnosticsPath = require.resolve("../diagnostics.cjs");
const policyPath = require.resolve("../policy.cjs");
const sink = { kind: "supervisor_stderr", rotationOwner: "supervisor" };
const preamble = `const {Diagnostics}=require(${JSON.stringify(diagnosticsPath)}); const {resolvePolicy}=require(${JSON.stringify(policyPath)}); const sink=${JSON.stringify(sink)};`;

function captured(script) {
  return new Promise((resolve, reject) => {
    execFile(process.execPath, ["-e", preamble + script], { timeout: 10000, maxBuffer: 1048576 }, (error, stdout, stderr) => {
      if (error) reject(error); else resolve({ stdout, stderr });
    });
  });
}

test("diagnostics capability requires the explicitly owned declared sink", async () => {
  assert.throws(() => new Diagnostics(), /explicit supervisor/);
  assert.throws(() => new Diagnostics({ sink: { kind: "file", rotationOwner: "supervisor" } }), /explicit supervisor/);
  for (const options of [{ maxPendingRecords: 0 }, { maxRecordBytes: 255 }, { maxPendingRecords: Infinity }, { closeTimeoutMs: 0 }, { repeatIntervalMs: 0 }]) assert.throws(() => new Diagnostics({ sink, ...options }), /finite integer/);
  assert.throws(() => resolvePolicy({ environment: { XGC2_XRPC_LOG_LEVEL: "debug" } }), (error) => error.field === "LOG_LEVEL");
  assert.throws(() => resolvePolicy({ environment: {}, capabilities: ["diagnostics"] }), (error) => error.field === "diagnostics");
  assert.throws(() => resolvePolicy({ environment: {}, diagnostics: {} }), /explicit Diagnostics owner/);
  const d = new Diagnostics({ sink });
  assert.throws(() => resolvePolicy({ environment: {}, diagnostics: d, capabilities: ["host"] }), /requires diagnostics capability/);
  const policy = resolvePolicy({ environment: {}, diagnostics: d });
  assert.equal(policy.diagnostics, d);
  assert.equal(d.status().level, registry.fields.find((field) => field.name === "LOG_LEVEL").default);
  assert.equal(d.status().format, "json");
  assert.equal(d.status().workerStarted, false);
  assert.throws(() => resolvePolicy({ environment: {}, diagnostics: d }), /bound once/);
  await d.close();
  assert.throws(() => policy.update({ LOG_LEVEL: "debug" }, { expectedRevision: 1 }), /not running/);
  assert.equal(d.status().workerStarted, false);
});

test("shared environment corpus enforces diagnostics sources and parsing", async () => {
  const supported = new Set(registry.fields.filter((field) => field.capability !== "grpc").map((field) => field.name));
  for (const entry of corpus.cases) {
    const d = new Diagnostics({ sink });
    const options = { environment: entry.environment, defaults: entry.defaults, ceilings: entry.ceilings, diagnostics: d };
    // The explicit unsupported test intentionally excludes diagnostics as well.
    if (entry.capabilities) { options.capabilities = entry.capabilities; delete options.diagnostics; }
    try {
      if (entry.error_field) assert.throws(() => resolvePolicy(options), (error) => error.field === entry.error_field, entry.name);
      else {
        const policy = resolvePolicy(options);
        for (const [field, value] of Object.entries(entry.values ?? {})) if (supported.has(field)) assert.equal(policy.fields[field]?.value, value, `${entry.name}:${field}`);
        for (const [field, source] of Object.entries(entry.sources ?? {})) if (supported.has(field)) assert.equal(policy.fields[field]?.source, source, `${entry.name}:${field}`);
        assert.ok(!JSON.stringify(policy.effective()).includes("not-returned"));
      }
    } finally { await d.close(); }
  }
});

test("live LOG_LEVEL CAS changes the shared sink and preserves previous snapshots", async () => {
  const d = new Diagnostics({ sink });
  const environment = { XGC2_XRPC_LOG_LEVEL: "warn", SECRET: "never-retained" };
  const policy = resolvePolicy({ environment, defaults: { LOG_FORMAT: "text", HOST_MAX_CONNECTIONS: 8 }, ceilings: { HOST_MAX_CONNECTIONS: 8 }, diagnostics: d });
  environment.XGC2_XRPC_LOG_LEVEL = "trace";
  const before = policy.effective();
  assert.equal(before.fields.LOG_LEVEL.value, "warn");
  assert.equal(before.fields.LOG_LEVEL.source, "environment");
  assert.equal(before.fields.LOG_FORMAT.source, "deployment");
  assert.equal(before.fields.HOST_MAX_CONNECTIONS.ceiling, 8);
  assert.ok(Object.isFrozen(before) && Object.isFrozen(before.fields.LOG_LEVEL));
  assert.throws(() => { before.fields.LOG_LEVEL.value = "error"; }, TypeError);
  assert.throws(() => policy.update({ LOG_FORMAT: "json" }, { expectedRevision: 1 }), /only LOG_LEVEL/);
  assert.throws(() => policy.update({ HOST_MAX_CONNECTIONS: 9 }, { expectedRevision: 1 }), /only LOG_LEVEL/);
  assert.throws(() => policy.update({ LOG_LEVEL: "DEBUG" }, { expectedRevision: 1 }), /unsupported enum/);
  assert.throws(() => policy.update({ LOG_LEVEL: { toString() { throw Error("never-called"); } } }, { expectedRevision: 1 }), /exact enum string/);
  assert.throws(() => policy.update({ LOG_LEVEL: "trace" }), /revision/);
  assert.throws(() => policy.update({ LOG_LEVEL: "trace", SECRET: "never-retained" }, { expectedRevision: 1 }), /only LOG_LEVEL/);
  let reads = 0;
  assert.throws(() => policy.update({ get LOG_LEVEL() { reads++; return "trace"; } }, { expectedRevision: 1 }), /own data/);
  assert.equal(reads, 0);
  const outcomes = await Promise.allSettled(["debug", "error"].map((level) => Promise.resolve().then(() => policy.update({ LOG_LEVEL: level }, { expectedRevision: 1 }))));
  assert.equal(outcomes.filter((result) => result.status === "fulfilled").length, 1);
  assert.equal(outcomes.filter((result) => result.status === "rejected").length, 1);
  assert.equal(policy.revision, 2);
  assert.equal(policy.fields.LOG_LEVEL.value, "debug");
  assert.equal(policy.fields.LOG_LEVEL.source, "administrative");
  assert.equal(d.status().level, "debug");
  assert.equal(d.status().policyRevision, 2);
  assert.equal(before.revision, 1);
  assert.equal(before.fields.LOG_LEVEL.value, "warn");
  assert.equal(policyOptions({ policy }, { HOST_MAX_CONNECTIONS: "maxConnections" }).maxConnections, 8);
  assert.throws(() => policyOptions({ policy, maxConnections: 9 }, { HOST_MAX_CONNECTIONS: "maxConnections" }), /conflicts/);
  assert.ok(!JSON.stringify(policy.effective()).includes("never-retained"));
  await d.close();
});

test("reentrant local input cannot reuse a stale policy revision", async () => {
  const d = new Diagnostics({ sink });
  const policy = resolvePolicy({ environment: {}, diagnostics: d });
  let entered = false;
  const changes = new Proxy({ LOG_LEVEL: "trace" }, {
    ownKeys(target) {
      if (!entered) { entered = true; policy.update({ LOG_LEVEL: "error" }, { expectedRevision: 1 }); }
      return Reflect.ownKeys(target);
    },
  });
  assert.throws(() => policy.update(changes, { expectedRevision: 1 }), /revision conflicts/);
  assert.equal(policy.revision, 2);
  assert.equal(policy.fields.LOG_LEVEL.value, "error");
  await d.close();
});

for (const format of ["json", "text"]) test(`${format} records exclude payloads, error text and executable fields`, async () => {
  const { stdout, stderr } = await captured(`
    (async()=>{
      const d=new Diagnostics({sink,maxRecordBytes:256});
      const p=resolvePolicy({environment:{XGC2_XRPC_LOG_FORMAT:${JSON.stringify(format)}},diagnostics:d});
      const fields={service:"fixture",instance_id:"instance:1",request_id:"request:1",operation:"read",category:"internal",elapsed_ms:1,trace_id:"x".repeat(128),body:"body-secret",Authorization:"credential-secret",cookie:"cookie-secret",error:new Error("error-secret"),config:{token:"config-secret"},state:"bad-secret"};
      Object.defineProperty(fields,"payload",{get(){throw Error("payload getter accessed")}});
      Object.defineProperty(fields,"limit",{get(){throw Error("allowed getter accessed")}});
      d.emit("handler_failed",fields);
      await d.close();
      process.stdout.write(JSON.stringify(d.status()));
    })().catch(()=>process.exitCode=1);
  `);
  assert.ok(stderr.length > 0);
  assert.ok(Buffer.byteLength(stderr) <= 256);
  for (const secret of ["body-secret", "credential-secret", "cookie-secret", "error-secret", "config-secret", "bad-secret", "payload getter", "allowed getter"]) assert.ok(!stderr.includes(secret), secret);
  assert.ok(stderr.includes("handler_failed"));
  const status = JSON.parse(stdout);
  assert.equal(status.written, 1);
  assert.ok(status.redactedFields >= 2);
  assert.equal(status.sink.opensFiles, false);
  if (format === "json") assert.equal(JSON.parse(stderr).event, "handler_failed");
  else assert.match(stderr, /^\d+ error handler_failed /);
});

test("live level affects actual records and format stays startup-only", async () => {
  const { stdout, stderr } = await captured(`
    (async()=>{
      const d=new Diagnostics({sink});const p=resolvePolicy({environment:{},diagnostics:d});
      d.emit("call_started",{request_id:"before"});
      p.update({LOG_LEVEL:"debug"},{expectedRevision:1});
      d.emit("call_started",{request_id:"after"});
      await d.close();process.stdout.write(JSON.stringify(d.status()));
    })().catch(()=>process.exitCode=1);
  `);
  assert.ok(!stderr.includes("before"));
  assert.equal(JSON.parse(stderr).request_id, "after");
  assert.equal(JSON.parse(stdout).filtered, 1);
});

test("ACKed admission bounds cross-thread records and fixed-cardinality counters", async () => {
  const { stdout, stderr } = await captured(`
    (async()=>{
      const d=new Diagnostics({sink,maxPendingRecords:2});resolvePolicy({environment:{XGC2_XRPC_LOG_LEVEL:"debug"},diagnostics:d});
      const results=[];for(let i=0;i<100;i++)results.push(d.emit("/private/arbitrary/"+i,{body:"not-written",robot_id:String(i)}));
      const before=d.status();await d.close();const after=d.status();
      process.stdout.write(JSON.stringify({results,before,after}));
    })().catch(()=>process.exitCode=1);
  `);
  const { results, before, after } = JSON.parse(stdout);
  assert.equal(results.filter(Boolean).length, 2);
  assert.equal(before.pendingRecords, 2);
  assert.equal(before.dropped, 98);
  assert.equal(before.saturations, 1);
  assert.equal(after.pendingRecords, 0);
  assert.equal(after.recoveries, 1);
  assert.equal(after.eventCounts.unclassified, 100);
  assert.deepEqual(Object.keys(before.eventCounts), Object.keys(after.eventCounts));
  assert.ok(!stderr.includes("/private") && !stderr.includes("not-written") && !stderr.includes("robot_id"));
  assert.ok(stderr.includes("diagnostic_saturated") && stderr.includes("diagnostic_recovered"));
});

test("repeated warning errors are rate limited with bounded aggregate counts", async () => {
  const { stdout, stderr } = await captured(`
    (async()=>{
      const d=new Diagnostics({sink,repeatIntervalMs:1000});resolvePolicy({environment:{},diagnostics:d});
      d.emit("transport_failed",{category:"unavailable"});
      for(let i=0;i<10;i++)d.emit("transport_failed",{category:"unavailable"});
      await new Promise(r=>setTimeout(r,1100));
      d.emit("transport_failed",{category:"unavailable"});await d.close();
      process.stdout.write(JSON.stringify(d.status()));
    })().catch(()=>process.exitCode=1);
  `);
  const records = stderr.trim().split("\n").map((line) => JSON.parse(line));
  assert.equal(records.length, 2);
  assert.equal(records[1].repeat_count, 10);
  assert.equal(JSON.parse(stdout).rateSuppressed, 10);
});

test("real blocked stderr retains worker, fails finite close, then drains on retry", { timeout: 15000 }, async (t) => {
  const script = preamble + `
    (async()=>{
      const d=new Diagnostics({sink,maxPendingRecords:1024,maxRecordBytes:2048});resolvePolicy({environment:{XGC2_XRPC_LOG_LEVEL:"debug"},diagnostics:d});
      const fields={service:"s".repeat(128),instance_id:"i".repeat(128),request_id:"r".repeat(128),trace_id:"t".repeat(128),operation:"o".repeat(128)};
      for(let i=0;i<1024;i++)d.emit("call_started",fields);
      let ticks=0;const ticker=setInterval(()=>ticks++,1);
      await new Promise(r=>setTimeout(r,150));
      try{await d.close({timeoutMs:40});process.send({unexpected:"closed",status:d.status()});}
      catch(error){process.send({name:error.name,status:d.status(),ticks});}
      process.once("message",async()=>{await d.close({timeoutMs:5000});clearInterval(ticker);process.send({done:true,status:d.status()});process.disconnect();});
    })().catch(()=>process.exitCode=1);
  `;
  const child = spawn(process.execPath, ["-e", script], { stdio: ["ignore", "ignore", "pipe", "ipc"] });
  t.after(() => { if (child.exitCode === null) child.kill("SIGKILL"); });
  const first = await once(child, "message");
  const report = first[0];
  assert.equal(report.name, "DiagnosticCloseError", JSON.stringify(report));
  assert.equal(report.status.state, "closing");
  assert.equal(report.status.workerAlive, true);
  assert.ok(report.status.pendingRecords > 0 && report.status.pendingRecords <= 1024);
  assert.ok(report.ticks > 5, "main event loop continues while worker stderr is blocked");
  let bytes = 0; child.stderr.on("data", (buffer) => bytes += buffer.length);
  const done = once(child, "message"); child.send("resume");
  const completed = (await done)[0];
  assert.equal(completed.done, true);
  assert.equal(completed.status.state, "closed");
  assert.equal(completed.status.workerAlive, false);
  assert.equal(completed.status.pendingRecords, 0);
  assert.equal(completed.status.written, 1024);
  assert.ok(bytes > 65536);
  const [code] = await once(child, "exit"); assert.equal(code, 0);
});

test("close is terminal and DiagnosticsCloseError is a bounded public error", async () => {
  const d = new Diagnostics({ sink });
  await d.close(); await d.close();
  assert.equal(d.emit("handler_failed", { body: "discarded" }), false);
  assert.equal(d.status().dropped, 1);
  assert.equal(d.status().state, "closed");
  assert.throws(() => d.close({ timeoutMs: Infinity }), /finite integer/);
  const error = new DiagnosticCloseError();
  assert.equal(error.code, "deadline_exceeded");
  assert.ok(!error.message.includes("body"));
});

test("hard stderr I/O failure accounts for loss and rejects successful drain", { timeout: 10000 }, async (t) => {
  // A genuinely read-only inherited fd2 fails write(2); no synthetic sink hook
  // or swallowed event-loop exception substitutes for this negative case.
  const fd = openSync("/dev/null", "r");
  const script = preamble + `
    (async()=>{
      const d=new Diagnostics({sink});const p=resolvePolicy({environment:{},diagnostics:d});
      d.emit("handler_failed",{category:"internal"});
      try{await d.close();process.stdout.write(JSON.stringify({unexpected:"success"}));}
      catch(error){process.stdout.write(JSON.stringify({name:error.name,code:error.code,status:d.status()}));}
    })().catch(()=>process.exitCode=1);
  `;
  let child;
  try { child = spawn(process.execPath, ["-e", script], { stdio: ["ignore", "pipe", fd] }); } finally { closeSync(fd); }
  t.after(() => { if (child.exitCode === null) child.kill("SIGKILL"); });
  let stdout = ""; child.stdout.on("data", (chunk) => stdout += chunk);
  const [code] = await once(child, "exit"); assert.equal(code, 0);
  const report = JSON.parse(stdout);
  assert.equal(report.name, "DiagnosticSinkError");
  assert.equal(report.code, "unavailable");
  assert.equal(report.status.state, "closed");
  assert.equal(report.status.failed, true);
  assert.equal(report.status.workerAlive, false);
  assert.equal(report.status.written, 0);
  assert.equal(report.status.dropped, 1);
  assert.equal(report.status.sinkFailures, 1);
});
