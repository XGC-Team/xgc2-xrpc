"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { spawn, execFile } = require("node:child_process");
const { once } = require("node:events");
const { openSync, closeSync } = require("node:fs");
const { Diagnostics, DiagnosticCloseError } = require("../diagnostics.cjs");
const diagnosticsPath = require.resolve("../diagnostics.cjs");
const sink = { kind: "supervisor_stderr", rotationOwner: "supervisor" };
const preamble = `const {Diagnostics}=require(${JSON.stringify(diagnosticsPath)}); const sink=${JSON.stringify(sink)};`;

function captured(script) {
  return new Promise((resolve, reject) => {
    execFile(process.execPath, ["-e", preamble + script], { timeout: 10000, maxBuffer: 1048576 }, (error, stdout, stderr) => {
      if (error) reject(error); else resolve({ stdout, stderr });
    });
  });
}

test("diagnostics requires the explicitly owned declared sink and validated plain options", async () => {
  assert.throws(() => new Diagnostics(), /explicit supervisor/);
  assert.throws(() => new Diagnostics({ sink: { kind: "file", rotationOwner: "supervisor" } }), /explicit supervisor/);
  for (const options of [{ maxPendingRecords: 0 }, { maxRecordBytes: 255 }, { maxPendingRecords: Infinity }, { closeTimeoutMs: 0 }, { repeatIntervalMs: 0 }]) assert.throws(() => new Diagnostics({ sink, ...options }), /finite integer/);
  for (const level of ["", "INFO", "verbose", null, 3]) assert.throws(() => new Diagnostics({ sink, level }), /level must be/);
  for (const format of ["", "JSON", "logfmt", 1]) assert.throws(() => new Diagnostics({ sink, format }), /format must be/);
  const d = new Diagnostics({ sink });
  assert.equal(d.status().level, "info");
  assert.equal(d.status().format, "json");
  assert.equal(d.status().workerStarted, false);
  assert.equal(new Diagnostics({ sink, level: "trace", format: "text" }).status().level, "trace");
  assert.equal(Object.hasOwn(d.status(), "policyRevision"), false);
  await d.close();
  assert.equal(d.status().workerStarted, false);
});

for (const format of ["json", "text"]) test(`${format} records exclude payloads, error text and executable fields`, async () => {
  const { stdout, stderr } = await captured(`
    (async()=>{
      const d=new Diagnostics({sink,maxRecordBytes:256,format:${JSON.stringify(format)}});
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

test("records below the configured level are filtered and counted", async () => {
  const { stdout, stderr } = await captured(`
    (async()=>{
      const d=new Diagnostics({sink,level:"warn"});
      d.emit("call_started",{request_id:"below"});
      d.emit("transport_failed",{category:"unavailable",request_id:"above"});
      await d.close();process.stdout.write(JSON.stringify(d.status()));
    })().catch(()=>process.exitCode=1);
  `);
  assert.ok(!stderr.includes("below"));
  assert.equal(JSON.parse(stderr).request_id, "above");
  assert.equal(JSON.parse(stdout).filtered, 1);
});

test("ACKed admission bounds cross-thread records and fixed-cardinality counters", async () => {
  const { stdout, stderr } = await captured(`
    (async()=>{
      const d=new Diagnostics({sink,maxPendingRecords:2,level:"debug"});
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
      const d=new Diagnostics({sink,repeatIntervalMs:1000});
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
      const d=new Diagnostics({sink,maxPendingRecords:1024,maxRecordBytes:2048,level:"debug"});
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
      const d=new Diagnostics({sink});
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
