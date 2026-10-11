"use strict";

// Run from the installed consumer's directory so ordinary ancestor lookup owns
// package resolution. The caller also disables runtime global search paths.
//
//   node installed.cjs <installed SDK directory> <expected SDK version> [--missing-worker]
//
// Prints one JSON report on success. With --missing-worker the diagnostics worker
// file is expected to be absent and the probe verifies that this is a bounded,
// reported failure rather than a hang or a silent success.
delete process.env.NODE_PATH;
delete process.env.NODE_OPTIONS;

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const runtime = {
  implementation: process.versions.bun ? "bun" : "node",
  node: process.versions.node,
  bun: process.versions.bun ?? null,
};
const schema = "xgc2.xrpc.node-installed-probe.v2";
let phase = "arguments";
let missingWorker = false;

function regularReadable(file) {
  assert.equal(fs.statSync(file).isFile(), true);
  fs.accessSync(file, fs.constants.R_OK);
}

async function main() {
  const [expectedSdkArgument, expectedVersion, option, ...extra] = process.argv.slice(2);
  assert.equal(extra.length, 0);
  assert.ok(expectedSdkArgument && path.isAbsolute(expectedSdkArgument));
  assert.match(expectedVersion ?? "", /^[0-9]+\.[0-9]+\.[0-9]+$/);
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
  assert.equal(sdkPackage.version, expectedVersion);
  assert.equal(sdkPackage.license, "Apache-2.0");
  assert.equal(wsPackage.name, "ws");
  assert.equal(wsPackage.version, "8.22.0");
  assert.equal(sdkPackage.dependencies.ws, "8.22.0");

  phase = "installed-runtime-files";
  for (const filename of ["index.cjs", "index.d.cts", "client.cjs", "bootstrap.cjs", "diagnostics.cjs", "unix.cjs"]) {
    regularReadable(path.join(expectedSdk, filename));
  }
  for (const removed of ["policy.cjs", "runtime-policy.json"]) {
    assert.equal(fs.existsSync(path.join(expectedSdk, removed)), false, removed);
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
    "createHTTPHost", "createFetchHost", "createRPCHost", "createBoundHTTPHost", "newInstanceId",
    "BootstrapBinding", "readBootstrapBinding", "loadBootstrapInput",
    "Diagnostics", "DiagnosticCloseError", "DiagnosticSinkError",
    "proxyWebSocket", "HTTPClient", "TransportError",
  ];
  for (const name of publicFunctions) assert.equal(typeof sdk[name], "function", name);
  for (const name of ["resolvePolicy", "derivePolicy", "PolicyError"]) assert.equal(sdk[name], undefined, name);
  for (const name of ["serviceRef", "resolveCredentials"]) assert.equal(typeof sdk.BootstrapBinding.prototype[name], "function", name);

  phase = "unix-host-and-client";
  // A real fenced call over a private Unix socket with the installed code.
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "xrpc-installed-"));
  const socket = path.join(directory, "control.sock");
  const instanceId = sdk.newInstanceId();
  assert.match(instanceId, /^[0-9a-f]{32}$/);
  const host = sdk.createRPCHost((_request, response, context) => {
    response.setHeader("Content-Type", "application/json");
    response.end(JSON.stringify({ requestId: context.requestId }));
  }, { instanceId, unixPath: socket });
  const client = new sdk.HTTPClient({ localTarget: "local" });
  try {
    await host.listen();
    assert.equal(fs.lstatSync(socket).mode & 0o777, 0o600);
    const reference = { target_id: "local", service: "probe", api_version: "v1", instance_id: instanceId, profile: "http.v1", endpoint: { kind: "unix", address: socket } };
    const reply = await client.call(reference, "/v1/echo", { timeoutMs: 2000, requestId: "probe:1" });
    assert.equal(reply.status, 200);
    assert.equal(reply.disposition, "response_received");
    assert.equal(JSON.parse(reply.body).requestId, "probe:1");
    await assert.rejects(client.call({ ...reference, instance_id: "stale" }, "/v1/echo", { timeoutMs: 2000 }),
      (error) => error.code === "conflict" && error.disposition === "outcome_unknown");
  } finally {
    client.close();
    await host.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }

  phase = "diagnostics-startup";
  const diagnostics = new sdk.Diagnostics({ sink: { kind: "supervisor_stderr", rotationOwner: "supervisor" }, level: "debug" });
  const initial = diagnostics.status();
  assert.equal(initial.state, "running");
  assert.equal(initial.workerStarted, false);
  assert.equal(initial.level, "debug");
  assert.equal(initial.format, "json");
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

  phase = "worker-ack-and-close";
  const started = diagnostics.status();
  assert.equal(started.workerStarted, true);
  assert.equal(started.workerAlive, true);
  assert.ok(started.pendingRecords >= 1);
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
  assert.equal(status.level, "debug");
  return { schema, ok: true, mode: "positive", runtime, resolvedPaths, packages, status };
}

main().then((report) => {
  process.stdout.write(JSON.stringify(report) + "\n");
}).catch(() => {
  // Failure locations are bounded probe labels; arbitrary SDK errors, stacks,
  // environment values and credential material are never copied to output.
  process.stderr.write(JSON.stringify({ schema, ok: false, mode: missingWorker ? "missing-worker" : "positive", runtime, check: phase }) + "\n");
  process.exitCode = 1;
});
