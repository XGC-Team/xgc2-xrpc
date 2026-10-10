"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const net = require("node:net");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { once } = require("node:events");
const { HTTPClient, createHTTPHost, createRPCHost, newInstanceId } = require("..");

const boot = "boot:1";
function privateDirectory() { return fs.mkdtempSync(path.join(os.tmpdir(), "xrpc-unix-")); }
function ref(socket, instance_id = boot) { return { target_id: "local", service: "fixture", api_version: "v1", instance_id, profile: "http.v1", endpoint: { kind: "unix", address: socket } }; }
function get(socketPath) {
  return new Promise((resolve, reject) => {
    http.get({ socketPath, path: "/", headers: { Connection: "close" } }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString() }));
    }).on("error", reject);
  });
}
async function withDirectory(run) {
  const directory = privateDirectory();
  try { await run(directory); } finally { fs.rmSync(directory, { recursive: true, force: true }); }
}

test("a Unix host serves fenced RPC calls on a 0600 socket and removes it on close", async () => {
  await withDirectory(async (directory) => {
    const socket = path.join(directory, "control.sock");
    const host = createRPCHost((_req, res, context) => {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ requestId: context.requestId }));
    }, { instanceId: boot, unixPath: socket });
    assert.equal(host.unixPath, socket);
    await host.listen();
    const client = new HTTPClient({ localTarget: "local" });
    try {
      const stat = fs.lstatSync(socket);
      assert.ok(stat.isSocket());
      assert.equal(stat.mode & 0o777, 0o600);
      assert.equal(fs.statSync(directory).mode & 0o777, 0o700);
      const reply = await client.call(ref(socket), "/v1/echo", { timeoutMs: 1000, requestId: "unix:1" });
      assert.equal(reply.status, 200);
      assert.equal(JSON.parse(reply.body).requestId, "unix:1");
      await assert.rejects(client.call(ref(socket, "stale"), "/v1/echo", { timeoutMs: 1000 }), (e) => e.code === "conflict" && e.disposition === "outcome_unknown");
      assert.equal(host.stats().inFlight, 0);
    } finally { client.close(); await host.close(); }
    assert.equal(fs.existsSync(socket), false, "close removes the socket");
    await host.close();
    await assert.rejects(get(socket), (e) => e.code === "ENOENT" || e.code === "ECONNREFUSED");
  });
});

test("a plain HTTP host listens on a Unix path too", async () => {
  await withDirectory(async (directory) => {
    const socket = path.join(directory, "edge.sock");
    const host = createHTTPHost((_req, res) => res.end("edge"), { unixPath: socket });
    await host.listen();
    try { assert.deepEqual(await get(socket), { status: 200, body: "edge" }); }
    finally { await host.close(); }
    assert.equal(fs.existsSync(socket), false);
  });
});

test("a stale socket left by a killed process is reclaimed", async () => {
  await withDirectory(async (directory) => {
    const socket = path.join(directory, "stale.sock");
    const child = spawn(process.execPath, ["-e", `require("net").createServer().listen(${JSON.stringify(socket)}, () => process.stdout.write("ready"))`], { stdio: ["ignore", "pipe", "inherit"] });
    await once(child.stdout, "data");
    child.kill("SIGKILL");
    await once(child, "exit");
    assert.ok(fs.lstatSync(socket).isSocket(), "the killed process left its socket behind");
    await assert.rejects(get(socket), (e) => e.code === "ECONNREFUSED");
    const host = createHTTPHost((_req, res) => res.end("reclaimed"), { unixPath: socket });
    await host.listen();
    try { assert.equal((await get(socket)).body, "reclaimed"); }
    finally { await host.close(); }
  });
});

test("a socket with a live owner is never taken over", async () => {
  await withDirectory(async (directory) => {
    const socket = path.join(directory, "live.sock");
    const first = createHTTPHost((_req, res) => res.end("first"), { unixPath: socket });
    await first.listen();
    try {
      const before = fs.lstatSync(socket, { bigint: true });
      const second = createHTTPHost((_req, res) => res.end("second"), { unixPath: socket });
      await assert.rejects(second.listen(), (e) => e.code === "EADDRINUSE" && /address in use/.test(e.message));
      await second.close();
      const after = fs.lstatSync(socket, { bigint: true });
      assert.equal(after.ino, before.ino);
      assert.equal((await get(socket)).body, "first");
    } finally { await first.close(); }
  });
});

test("only an unreachable socket may be replaced: files, symlinks and directories are refused", async () => {
  await withDirectory(async (directory) => {
    const target = path.join(directory, "target.sock");
    const cases = {
      file: () => fs.writeFileSync(target, "precious"),
      symlink: () => fs.symlinkSync("/etc/hostname", target),
      directory: () => fs.mkdirSync(target),
    };
    for (const [kind, create] of Object.entries(cases)) {
      create();
      const host = createHTTPHost((_req, res) => res.end(), { unixPath: target });
      await assert.rejects(host.listen(), (e) => e.code === "EADDRINUSE", kind);
      await host.close();
      assert.ok(fs.lstatSync(target, { throwIfNoEntry: false }), `${kind} survived`);
      fs.rmSync(target, { recursive: true, force: true });
    }
    fs.writeFileSync(target, "precious");
    const host = createHTTPHost((_req, res) => res.end("late"), { unixPath: target });
    await assert.rejects(host.listen());
    assert.equal(fs.readFileSync(target, "utf8"), "precious");
    // A failed listen can be retried once the path is free.
    fs.unlinkSync(target);
    await host.listen();
    try { assert.equal((await get(target)).body, "late"); } finally { await host.close(); }
  });
});

test("the runtime directory must be private, present and not behind a symlink", async () => {
  await withDirectory(async (directory) => {
    const open = path.join(directory, "open");
    fs.mkdirSync(open, { mode: 0o755 });
    fs.chmodSync(open, 0o755);
    const group = path.join(directory, "group");
    fs.mkdirSync(group);
    fs.chmodSync(group, 0o770);
    const real = path.join(directory, "real");
    fs.mkdirSync(real);
    const link = path.join(directory, "link");
    fs.symlinkSync(real, link);
    for (const [name, socket, expected] of [
      ["mode 0755", path.join(open, "x.sock"), /0700/],
      ["mode 0770", path.join(group, "x.sock"), /0700/],
      ["missing", path.join(directory, "absent", "x.sock"), /ENOENT/],
      ["symlinked directory", path.join(link, "x.sock"), /ELOOP|ENOTDIR/],
    ]) {
      const host = createHTTPHost((_req, res) => res.end(), { unixPath: socket });
      await assert.rejects(host.listen(), expected, name);
      await host.close();
    }
    assert.deepEqual(fs.readdirSync(real), []);
  });
});

test("invalid addresses and option combinations fail before any effect", () => {
  const handler = (_req, res) => res.end();
  for (const unixPath of ["relative.sock", "/tmp/a/../b.sock", "/tmp/dir/", "/tmp/a\0b.sock", "/tmp/" + "x".repeat(110), "/", ""]) {
    assert.throws(() => createHTTPHost(handler, { unixPath }), /canonical absolute Unix socket path/, JSON.stringify(unixPath));
  }
  assert.throws(() => createHTTPHost(handler, { unixPath: "/tmp/x.sock", tls: {} }), /does not use TLS/);
});

test("listen state: single use, no TCP hosts, closing hosts and raw listens are refused", async () => {
  await withDirectory(async (directory) => {
    const socket = path.join(directory, "state.sock");
    const host = createHTTPHost((_req, res) => res.end(), { unixPath: socket });
    assert.throws(() => host.server.listen(0), /unixPath/);
    await host.listen();
    await assert.rejects(host.listen(), /already listening/);
    await host.close();
    await assert.rejects(host.listen(), /closing/);
    const tcp = createHTTPHost((_req, res) => res.end());
    assert.equal(tcp.unixPath, null);
    await assert.rejects(tcp.listen(), /unixPath/);
    await tcp.close();
    const idle = createHTTPHost((_req, res) => res.end(), { unixPath: path.join(directory, "never.sock") });
    await idle.close();
    assert.equal(fs.existsSync(path.join(directory, "never.sock")), false);
  });
});

test("close lets admitted work finish, then removes the socket and refuses new callers", async () => {
  await withDirectory(async (directory) => {
    const socket = path.join(directory, "drain.sock");
    let release, entered;
    const held = new Promise((resolve) => { release = resolve; });
    const started = new Promise((resolve) => { entered = resolve; });
    const host = createHTTPHost(async (_req, res) => { entered(); await held; res.end("done"); }, { unixPath: socket, shutdownMs: 2000 });
    await host.listen();
    const pending = get(socket);
    await started;
    const closing = host.close();
    await assert.rejects(get(socket), (e) => e.code === "ECONNREFUSED" || e.code === "ENOENT");
    release();
    assert.deepEqual(await pending, { status: 200, body: "done" });
    await closing;
    assert.equal(fs.existsSync(socket), false);
  });
});

test("a socket held by any live process is protected, even with a long name", async () => {
  await withDirectory(async (directory) => {
    const socket = path.join(directory, "x".repeat(60) + ".sock");
    const holder = net.createServer().listen(socket);
    await once(holder, "listening");
    const host = createHTTPHost((_req, res) => res.end(), { unixPath: socket });
    await assert.rejects(host.listen(), (e) => e.code === "EADDRINUSE");
    await host.close();
    assert.ok(fs.lstatSync(socket).isSocket(), "the other owner's socket is untouched");
    await new Promise((resolve) => holder.close(resolve));
  });
});

test("instance identities are 128 random bits as hex", () => {
  const first = newInstanceId(), second = newInstanceId();
  assert.match(first, /^[0-9a-f]{32}$/);
  assert.notEqual(first, second);
});
