import assert from "node:assert/strict";
import { test } from "node:test";
import { call, XrpcError } from "../dist/index.js";
import { delay, json, serve, until } from "./helpers.mjs";

const ID = /^[A-Za-z0-9._:-]{1,128}$/;

test("a JSON call carries the XRPC request headers and returns the parsed answer", async () => {
  const server = await serve((req, res, record) => {
    json(res, 200, { echo: JSON.parse(record.body) }, {
      "X-Request-ID": req.headers["x-request-id"],
      "X-Xrpc-Instance-ID": "boot:1",
    });
  });
  try {
    const result = await call({ url: `${server.url}/v1/echo`, instanceId: "boot:1" }, "POST", { n: 1 }, {
      deadlineMs: 5000,
      requestId: "req:1",
      headers: { Authorization: "Bearer token" },
    });
    assert.equal(result.ok, true);
    assert.equal(result.disposition, "response_received");
    assert.equal(result.status, 200);
    assert.equal(result.requestId, "req:1");
    assert.equal(result.instanceId, "boot:1");
    assert.deepEqual(result.body, { echo: { n: 1 } });
    const [request] = server.requests;
    assert.equal(request.method, "POST");
    assert.equal(request.url, "/v1/echo");
    assert.equal(request.headers["x-request-id"], "req:1");
    assert.equal(request.headers["x-xrpc-instance-id"], "boot:1");
    assert.equal(request.headers["content-type"], "application/json");
    assert.equal(request.headers.authorization, "Bearer token");
    const budget = Number(request.headers["x-xrpc-timeout-ms"]);
    assert.ok(Number.isInteger(budget) && budget >= 1 && budget <= 5000, `budget ${budget}`);
  } finally {
    await server.close();
  }
});

test("request IDs are generated in the shared grammar and an empty GET has no body", async () => {
  const server = await serve((_req, res) => {
    res.writeHead(204);
    res.end();
  });
  try {
    const first = await call(`${server.url}/v1/describe`, "get", undefined, { deadlineMs: 2000 });
    const second = await call(new URL(`${server.url}/v1/describe`), "GET", undefined, { deadlineMs: 2000 });
    assert.equal(first.ok, true);
    assert.equal(first.status, 204);
    assert.equal(first.body, null);
    assert.match(first.requestId, ID);
    assert.notEqual(first.requestId, second.requestId);
    assert.equal(server.requests[0].method, "GET");
    assert.equal(server.requests[0].headers["content-type"], undefined);
    assert.equal(server.requests[0].headers["x-xrpc-instance-id"], undefined);
    assert.equal(server.requests[0].headers["x-request-id"], first.requestId);
  } finally {
    await server.close();
  }
});

test("the standard error envelope becomes a response_received failure", async () => {
  const server = await serve((req, res) => {
    json(res, 409, { error: { code: "conflict", message: "revision moved", details: { revision: 7 } } }, {
      "X-Request-ID": req.headers["x-request-id"],
    });
  });
  try {
    const result = await call(`${server.url}/v1/x`, "POST", {}, { deadlineMs: 2000, requestId: "req:2" });
    assert.equal(result.ok, false);
    assert.equal(result.disposition, "response_received");
    assert.ok(result.error instanceof XrpcError);
    assert.equal(result.error.code, "conflict");
    assert.equal(result.error.message, "revision moved");
    assert.equal(result.error.status, 409);
    assert.equal(result.error.requestId, "req:2");
    assert.equal(result.error.disposition, "response_received");
    assert.deepEqual(result.error.details, { revision: 7 });
  } finally {
    await server.close();
  }
});

test("domain codes pass through and bodies without an envelope map by status", async () => {
  const server = await serve((req, res) => {
    const status = Number(req.url.slice(1));
    if (status === 418) json(res, 418, { error: { code: "native_busy", message: "provider busy" } });
    else if (status === 502) {
      res.writeHead(502, { "Content-Type": "text/html" });
      res.end("<html>Bad Gateway</html>");
    } else {
      res.writeHead(status);
      res.end();
    }
  });
  try {
    const expectations = [
      [418, "native_busy"],
      [400, "invalid_argument"],
      [401, "unauthenticated"],
      [403, "permission_denied"],
      [404, "not_found"],
      [409, "conflict"],
      [413, "resource_exhausted"],
      [429, "resource_exhausted"],
      [504, "deadline_exceeded"],
      [499, "cancelled"],
      [502, "unavailable"],
      [503, "unavailable"],
      [500, "internal"],
    ];
    for (const [status, code] of expectations) {
      const result = await call(`${server.url}/${status}`, "GET", undefined, { deadlineMs: 2000 });
      assert.equal(result.ok, false, String(status));
      assert.equal(result.disposition, "response_received", String(status));
      assert.equal(result.error.code, code, String(status));
      assert.equal(result.error.status, status);
    }
    const html = await call(`${server.url}/502`, "GET", undefined, { deadlineMs: 2000 });
    assert.match(html.error.message, /Bad Gateway/);
  } finally {
    await server.close();
  }
});

test("a changed or missing server instance fails with conflict and an unknown outcome", async () => {
  const server = await serve((req, res) => {
    const headers = req.url === "/missing" ? {} : { "X-Xrpc-Instance-ID": "boot:2" };
    json(res, 200, { ok: true }, headers);
  });
  try {
    for (const path of ["/other", "/missing"]) {
      const result = await call({ url: server.url + path, instanceId: "boot:1" }, "GET", undefined, { deadlineMs: 2000 });
      assert.equal(result.ok, false, path);
      assert.equal(result.error.code, "conflict", path);
      assert.equal(result.disposition, "outcome_unknown", path);
    }
    const unpinned = await call(server.url + "/other", "GET", undefined, { deadlineMs: 2000 });
    assert.equal(unpinned.ok, true);
    assert.equal(unpinned.instanceId, "boot:2");
  } finally {
    await server.close();
  }
});

test("an answer that echoes another request ID is rejected", async () => {
  const server = await serve((_req, res) => json(res, 200, {}, { "X-Request-ID": "someone-else" }));
  try {
    const result = await call(server.url, "GET", undefined, { deadlineMs: 2000, requestId: "mine" });
    assert.equal(result.ok, false);
    assert.equal(result.disposition, "outcome_unknown");
    assert.equal(result.error.code, "internal");
  } finally {
    await server.close();
  }
});

test("oversized answers are refused as response_received", async () => {
  const server = await serve((req, res) => {
    if (req.url === "/sized") {
      res.writeHead(200, { "Content-Type": "application/json", "Content-Length": 2000 });
      res.end(" ".repeat(2000));
    } else {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.write(" ".repeat(600));
      setTimeout(() => res.end(" ".repeat(600) + "{}"), 20);
    }
  });
  try {
    for (const path of ["/sized", "/chunked"]) {
      const result = await call(server.url + path, "GET", undefined, { deadlineMs: 2000, maxResponseBytes: 1024 });
      assert.equal(result.ok, false, path);
      assert.equal(result.disposition, "response_received", path);
      assert.equal(result.error.code, "resource_exhausted", path);
    }
    const fits = await call(server.url + "/chunked", "GET", undefined, { deadlineMs: 2000, maxResponseBytes: 4096 });
    assert.equal(fits.ok, true);
  } finally {
    await server.close();
  }
});

test("a success status with a body that is not JSON is an internal error", async () => {
  const server = await serve((_req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("plain");
  });
  try {
    const result = await call(server.url, "GET", undefined, { deadlineMs: 2000 });
    assert.equal(result.ok, false);
    assert.equal(result.disposition, "response_received");
    assert.equal(result.error.code, "internal");
    assert.equal(result.error.status, 200);
  } finally {
    await server.close();
  }
});

test("redirects are not followed", async () => {
  const server = await serve((req, res) => {
    if (req.url === "/start") {
      res.writeHead(302, { Location: "/elsewhere" });
      res.end();
    } else {
      json(res, 200, {});
    }
  });
  try {
    const result = await call(`${server.url}/start`, "POST", {}, { deadlineMs: 2000 });
    assert.equal(result.ok, false);
    assert.equal(result.disposition, "response_received");
    assert.equal(server.requests.length, 1);
  } finally {
    await server.close();
  }
});

test("the deadline covers the whole call and leaves an unknown outcome", async () => {
  const server = await serve(() => {});
  try {
    const started = Date.now();
    const result = await call(server.url, "POST", {}, { deadlineMs: 150 });
    const elapsed = Date.now() - started;
    assert.equal(result.ok, false);
    assert.equal(result.error.code, "deadline_exceeded");
    assert.equal(result.disposition, "outcome_unknown");
    assert.ok(elapsed >= 140 && elapsed < 1500, `elapsed ${elapsed}`);
    assert.equal(server.requests.length, 1);
  } finally {
    await server.close();
  }
});

test("the deadline also bounds a body that stalls after the head", async () => {
  const server = await serve((_req, res) => {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.write("{");
  });
  try {
    const result = await call(server.url, "GET", undefined, { deadlineMs: 200 });
    assert.equal(result.ok, false);
    assert.equal(result.error.code, "deadline_exceeded");
    assert.equal(result.disposition, "outcome_unknown");
  } finally {
    await server.close();
  }
});

test("aborting during the call reports cancelled with an unknown outcome", async () => {
  const server = await serve(() => {});
  try {
    const controller = new AbortController();
    const pending = call(server.url, "POST", {}, { deadlineMs: 5000, signal: controller.signal });
    await until(() => server.requests.length === 1, 3000, "the request to arrive");
    controller.abort();
    const result = await pending;
    assert.equal(result.ok, false);
    assert.equal(result.error.code, "cancelled");
    assert.equal(result.disposition, "outcome_unknown");
    await until(() => server.requests[0].closed, 3000, "the server to see the closed request");
  } finally {
    await server.close();
  }
});

test("an already aborted signal sends nothing", async () => {
  const server = await serve((_req, res) => json(res, 200, {}));
  try {
    const result = await call(server.url, "POST", {}, { deadlineMs: 5000, signal: AbortSignal.abort() });
    assert.equal(result.ok, false);
    assert.equal(result.error.code, "cancelled");
    assert.equal(result.disposition, "not_sent");
    assert.equal(server.requests.length, 0);
  } finally {
    await server.close();
  }
});

test("a refused connection proves nothing was sent", async () => {
  const server = await serve((_req, res) => json(res, 200, {}));
  const url = server.url;
  await server.close();
  const result = await call(url, "POST", {}, { deadlineMs: 2000 });
  assert.equal(result.ok, false);
  assert.equal(result.error.code, "unavailable");
  assert.equal(result.disposition, "not_sent");
});

test("a connection lost after the request arrived is an unknown outcome and is never replayed", async () => {
  const server = await serve((_req, res) => res.socket.destroy());
  try {
    const result = await call(server.url, "PUT", { effect: true }, { deadlineMs: 2000 });
    assert.equal(result.ok, false);
    assert.equal(result.error.code, "unavailable");
    assert.equal(result.disposition, "outcome_unknown");
    await delay(50);
    assert.equal(server.requests.length, 1);
  } finally {
    await server.close();
  }
});

test("invalid arguments fail as not_sent without touching the network", async () => {
  const server = await serve((_req, res) => json(res, 200, {}));
  try {
    const circular = {};
    circular.self = circular;
    const cases = [
      ["missing options", server.url, "GET", undefined, undefined],
      ["missing deadline", server.url, "GET", undefined, {}],
      ["zero deadline", server.url, "GET", undefined, { deadlineMs: 0 }],
      ["fractional deadline", server.url, "GET", undefined, { deadlineMs: 1.5 }],
      ["infinite deadline", server.url, "GET", undefined, { deadlineMs: Infinity }],
      ["deadline above the wire maximum", server.url, "GET", undefined, { deadlineMs: 86_400_001 }],
      ["bad request id", server.url, "GET", undefined, { deadlineMs: 100, requestId: "has space" }],
      ["bad method", server.url, "GET /", undefined, { deadlineMs: 100 }],
      ["body on GET", server.url, "GET", {}, { deadlineMs: 100 }],
      ["circular body", server.url, "POST", circular, { deadlineMs: 100 }],
      ["unserializable body", server.url, "POST", () => {}, { deadlineMs: 100 }],
      ["bigint body", server.url, "POST", 1n, { deadlineMs: 100 }],
      ["not a URL", "not a url", "GET", undefined, { deadlineMs: 100 }],
      ["unsupported scheme", "ftp://127.0.0.1/x", "GET", undefined, { deadlineMs: 100 }],
      ["credentials in the URL", `http://user:pw@127.0.0.1:1/x`, "GET", undefined, { deadlineMs: 100 }],
      ["bad instance id", { url: server.url, instanceId: "a b" }, "GET", undefined, { deadlineMs: 100 }],
      ["bad response limit", server.url, "GET", undefined, { deadlineMs: 100, maxResponseBytes: 0 }],
      ["bad header name", server.url, "GET", undefined, { deadlineMs: 100, headers: { "bad name": "x" } }],
      ["header injection", server.url, "GET", undefined, { deadlineMs: 100, headers: { "X-A": "x\r\nX-B: y" } }],
      ["duplicate header", server.url, "GET", undefined, { deadlineMs: 100, headers: { "X-A": "1", "x-a": "2" } }],
      ["header value outside ISO-8859-1", server.url, "GET", undefined, { deadlineMs: 100, headers: { "X-A": "\u20ac" } }],
    ];
    for (const name of ["X-Request-ID", "x-xrpc-timeout-ms", "X-Xrpc-Instance-ID", "Content-Type", "Content-Length", "Host", "Accept"]) {
      cases.push([`owned header ${name}`, server.url, "GET", undefined, { deadlineMs: 100, headers: { [name]: "x" } }]);
    }
    for (const [name, target, method, body, options] of cases) {
      const result = await call(target, method, body, options);
      assert.equal(result.ok, false, name);
      assert.equal(result.disposition, "not_sent", name);
      assert.equal(result.error.code, "invalid_argument", name);
    }
    assert.equal(server.requests.length, 0);
  } finally {
    await server.close();
  }
});

test("a caller-supplied fetch is used", async () => {
  let seen;
  const result = await call("http://example.invalid/v1/x", "POST", { a: 1 }, {
    deadlineMs: 1000,
    fetch: async (url, init) => {
      seen = { url: String(url), init };
      return new Response('{"fake":true}', { status: 200, headers: { "Content-Type": "application/json" } });
    },
  });
  assert.equal(result.ok, true);
  assert.deepEqual(result.body, { fake: true });
  assert.equal(seen.url, "http://example.invalid/v1/x");
  assert.equal(seen.init.method, "POST");
  assert.equal(seen.init.redirect, "manual");
});
