// Run from the installed consumer's directory so that ordinary ancestor lookup finds
// the package; the caller also disables runtime global search paths.
//
//   node installed.mjs <installed client directory> <expected version>
//
// Prints one JSON report on success.
import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import { createRequire } from "node:module";
import path from "node:path";

const schema = "xgc2.xrpc.ts-installed-probe.v1";
const runtime = {
  implementation: process.versions.bun ? "bun" : "node",
  node: process.versions.node,
  bun: process.versions.bun ?? null,
};
let phase = "arguments";

async function main() {
  const [expectedArgument, expectedVersion, ...extra] = process.argv.slice(2);
  assert.equal(extra.length, 0);
  assert.ok(expectedArgument && path.isAbsolute(expectedArgument));
  assert.match(expectedVersion ?? "", /^[0-9]+\.[0-9]+\.[0-9]+$/);
  const expected = fs.realpathSync(expectedArgument);

  phase = "installed-package-resolution";
  const entry = fs.realpathSync(createRequire(import.meta.url).resolve("@xgc2/xrpc-client"));
  assert.equal(entry, path.join(expected, "dist", "index.js"));
  const manifest = JSON.parse(fs.readFileSync(path.join(expected, "package.json"), "utf8"));
  assert.equal(manifest.name, "@xgc2/xrpc-client");
  assert.equal(manifest.version, expectedVersion);
  assert.equal(manifest.license, "Apache-2.0");
  assert.equal(manifest.type, "module");
  assert.equal(manifest.dependencies, undefined);
  for (const file of ["index.js", "index.d.ts", "call.js", "events.js", "sse.js", "errors.js", "wire.js"]) {
    assert.equal(fs.statSync(path.join(expected, "dist", file)).isFile(), true, file);
  }

  phase = "public-exports";
  const client = await import("@xgc2/xrpc-client");
  for (const name of ["call", "events", "XrpcError"]) assert.equal(typeof client[name], "function", name);

  phase = "local-server";
  const server = http.createServer((request, response) => {
    const id = request.headers["x-request-id"];
    const echo = id === undefined ? {} : { "X-Request-ID": id };
    if (request.url === "/events") {
      const resumed = request.headers["last-event-id"];
      response.writeHead(200, { "Content-Type": "text/event-stream", ...echo });
      if (resumed === undefined) response.write("id: 1\nevent: first\ndata: {\"n\":1}\n\n");
      else response.write("id: 2\nevent: second\ndata: {\"n\":2}\n\n");
      if (resumed === undefined) response.end();
      return;
    }
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      response.writeHead(request.url === "/v1/call/probe.v1/Echo" ? 200 : 404, { "Content-Type": "application/json", ...echo });
      response.end(request.url === "/v1/call/probe.v1/Echo" ? Buffer.concat(chunks) : '{"error":{"code":"not_found","message":"no such method"}}');
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    phase = "call";
    const answer = await client.call(`${base}/v1/call/probe.v1/Echo`, "POST", { robot: "scout-1" }, { deadlineMs: 3000 });
    assert.equal(answer.ok, true);
    assert.equal(answer.disposition, "response_received");
    assert.deepEqual(answer.body, { robot: "scout-1" });
    const refused = await client.call(`${base}/v1/call/probe.v1/Absent`, "POST", {}, { deadlineMs: 3000 });
    assert.equal(refused.ok, false);
    assert.equal(refused.disposition, "response_received");
    assert.equal(refused.error.code, "not_found");

    phase = "events-resume";
    const seen = [];
    const abort = new AbortController();
    await client.events(`${base}/events`, {
      signal: abort.signal,
      backoff: { initialMs: 1, maxMs: 5 },
      onEvent: (event) => {
        seen.push([event.event, event.id, event.data]);
        if (seen.length === 2) abort.abort();
      },
    });
    // The second connection resumed after the cursor of the first event.
    assert.deepEqual(seen, [["first", "1", '{"n":1}'], ["second", "2", '{"n":2}']]);
  } finally {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
  return { schema, ok: true, mode: "positive", runtime, resolvedPaths: { entry }, package: { name: manifest.name, version: manifest.version } };
}

main().then((report) => {
  process.stdout.write(JSON.stringify(report) + "\n");
}).catch(() => {
  // Failure locations are bounded probe labels; arbitrary errors and stacks are not copied.
  process.stderr.write(JSON.stringify({ schema, ok: false, mode: "positive", runtime, check: phase }) + "\n");
  process.exitCode = 1;
});
