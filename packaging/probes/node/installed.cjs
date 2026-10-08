"use strict";

// Run from the installed consumer's directory so ordinary ancestor lookup owns
// package resolution. The caller also disables runtime global search paths.
delete process.env.NODE_PATH;
delete process.env.NODE_OPTIONS;

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const runtime = {
  implementation: process.versions.bun ? "bun" : "node",
  node: process.versions.node,
  bun: process.versions.bun ?? null,
};
const schema = "xgc2.xrpc.node-installed-probe.v1";
let phase = "arguments";
let missingWorker = false;

function regularReadable(file) {
  assert.equal(fs.statSync(file).isFile(), true);
  fs.accessSync(file, fs.constants.R_OK);
}

async function main() {
  const [expectedSdkArgument, option, ...extra] = process.argv.slice(2);
  assert.equal(extra.length, 0);
  assert.ok(expectedSdkArgument && path.isAbsolute(expectedSdkArgument));
  assert.ok(option === undefined || option === "--missing-worker");
  missingWorker = option === "--missing-worker";
  const expectedSdk = fs.realpathSync(expectedSdkArgument);

  phase = "installed-package-resolution";
  const sdkEntry = fs.realpathSync(require.resolve("@xgc2/xrpc"));
  assert.equal(sdkEntry, path.join(expectedSdk, "index.cjs"));
  const wsEntry = fs.realpathSync(require.resolve("ws", { paths: [expectedSdk] }));
  assert.equal(wsEntry, path.join(expectedSdk, "node_modules", "ws", "index.js"));
  const sdkPackageFile = path.join(expectedSdk, "package.json");
  const wsPackageFile = path.join(expectedSdk, "node_modules", "ws", "package.json");
  const sdkPackage = JSON.parse(fs.readFileSync(sdkPackageFile, "utf8"));
  const wsPackage = JSON.parse(fs.readFileSync(wsPackageFile, "utf8"));
  assert.equal(sdkPackage.name, "@xgc2/xrpc");
  assert.equal(sdkPackage.version, "0.1.0");
  assert.equal(wsPackage.name, "ws");
  assert.equal(wsPackage.version, "8.22.0");
  assert.equal(sdkPackage.dependencies.ws, "8.22.0");

  phase = "installed-runtime-files";
  for (const filename of ["index.cjs", "index.d.cts", "client.cjs", "bootstrap.cjs", "policy.cjs", "diagnostics.cjs", "runtime-policy.json"]) {
    regularReadable(path.join(expectedSdk, filename));
  }
  const workerFile = path.join(expectedSdk, "diagnostic-worker.cjs");
  if (missingWorker) {
    assert.throws(() => fs.statSync(workerFile), (error) => error.code === "ENOENT");
  } else {
    regularReadable(workerFile);
    assert.equal(fs.realpathSync(workerFile), workerFile);
  }

  phase = "public-exports";
  const sdk = require("@xgc2/xrpc");
  const publicFunctions = [
    "createHTTPHost", "createFetchHost", "createRPCHost", "createBoundHTTPHost",
    "BootstrapBinding", "readBootstrapBinding", "loadBootstrapInput",
    "Diagnostics", "DiagnosticCloseError", "DiagnosticSinkError",
    "proxyWebSocket", "resolvePolicy", "PolicyError", "HTTPClient", "TransportError",
  ];
  for (const name of publicFunctions) assert.equal(typeof sdk[name], "function", name);
  for (const name of ["serviceRef", "resolveCredentials"]) assert.equal(typeof sdk.BootstrapBinding.prototype[name], "function", name);

  phase = "diagnostic-policy-startup";
  const diagnostics = new sdk.Diagnostics({ sink: { kind: "supervisor_stderr", rotationOwner: "supervisor" } });
  const policy = sdk.resolvePolicy({ environment: { XGC2_XRPC_LOG_LEVEL: "debug" }, diagnostics });
  const initial = diagnostics.status();
  assert.equal(policy.diagnostics, diagnostics);
  assert.equal(policy.revision, 1);
  assert.equal(policy.fields.LOG_LEVEL.value, "debug");
  assert.equal(policy.fields.LOG_LEVEL.source, "environment");
  assert.equal(initial.state, "running");
  assert.equal(initial.workerStarted, false);
  assert.equal(initial.policyRevision, 1);
  assert.equal(initial.level, "debug");
  assert.equal(initial.sink.backend, "supervisor_stderr");
  assert.equal(initial.sink.rotationOwner, "supervisor");
  assert.equal(initial.sink.opensFiles, false);

  phase = missingWorker ? "missing-worker-emission" : "worker-emission";
  const emitted = diagnostics.emit("handler_failed", { category: "internal", operation: "installed-probe" });
  assert.equal(emitted, !missingWorker);

  const resolvedPaths = { sdk: sdkEntry, ws: wsEntry, sdkPackage: sdkPackageFile, wsPackage: wsPackageFile, worker: missingWorker ? null : workerFile };
  const packages = { sdk: { name: sdkPackage.name, version: sdkPackage.version }, ws: { name: wsPackage.name, version: wsPackage.version } };
  if (missingWorker) {
    const failed = diagnostics.status();
    assert.equal(failed.failed, true);
    assert.equal(failed.state, "failed");
    assert.equal(failed.workerStarted, false);
    assert.equal(failed.pendingRecords, 0);
    assert.ok(failed.workerFailures >= 1);
    phase = "missing-worker-close-rejection";
    await assert.rejects(diagnostics.close({ timeoutMs: 1000 }), (error) => error instanceof sdk.DiagnosticSinkError && error.code === "unavailable");
    const status = diagnostics.status();
    assert.equal(status.state, "closed");
    assert.equal(status.failed, true);
    assert.equal(status.workerAlive, false);
    assert.equal(status.pendingRecords, 0);
    assert.equal(status.written, 0);
    assert.ok(status.workerFailures >= 1);
    return { schema, ok: true, mode: "missing-worker", negative: { verified: true, emit: emitted, closeErrorCode: "unavailable" }, runtime, resolvedPaths, packages, status };
  }

  phase = "diagnostic-policy-cas";
  const started = diagnostics.status();
  assert.equal(started.workerStarted, true);
  assert.equal(started.workerAlive, true);
  assert.ok(started.pendingRecords >= 1);
  const changed = policy.update({ LOG_LEVEL: "warn" }, { expectedRevision: 1 });
  assert.equal(changed.revision, 2);
  assert.equal(policy.revision, 2);
  assert.equal(policy.fields.LOG_LEVEL.value, "warn");
  assert.equal(policy.fields.LOG_LEVEL.source, "administrative");
  assert.equal(diagnostics.status().level, "warn");
  assert.throws(() => policy.update({ LOG_LEVEL: "error" }, { expectedRevision: 1 }), (error) => error instanceof sdk.PolicyError && error.field === "revision");
  assert.equal(policy.revision, 2);
  assert.equal(policy.fields.LOG_LEVEL.value, "warn");

  phase = "worker-ack-and-close";
  await diagnostics.close({ timeoutMs: 1000 });
  const status = diagnostics.status();
  assert.equal(status.state, "closed");
  assert.equal(status.failed, false);
  assert.equal(status.workerStarted, true);
  assert.equal(status.workerAlive, false);
  assert.equal(status.pendingRecords, 0);
  assert.ok(status.admitted >= 1);
  assert.ok(status.written >= 1);
  assert.equal(status.workerFailures, 0);
  assert.equal(status.sinkFailures, 0);
  assert.equal(status.eventCounts.handler_failed, 1);
  assert.equal(status.policyRevision, 2);
  assert.equal(status.level, "warn");
  return { schema, ok: true, mode: "positive", runtime, resolvedPaths, packages, policy: { revision: policy.revision, level: policy.fields.LOG_LEVEL.value, staleRevisionRejected: true }, status };
}

main().then((report) => {
  process.stdout.write(JSON.stringify(report) + "\n");
}).catch(() => {
  // Failure locations are bounded probe labels; arbitrary SDK errors, stacks,
  // environment values and credential material are never copied to output.
  process.stderr.write(JSON.stringify({ schema, ok: false, mode: missingWorker ? "missing-worker" : "positive", runtime, check: phase }) + "\n");
  process.exitCode = 1;
});
