import assert from "node:assert/strict";
import { test } from "node:test";
import { events, XrpcError } from "../dist/index.js";
import { reconnectDelay } from "../dist/events.js";
import { collector, delay, json, serve, stream, until } from "./helpers.mjs";

/** Fast reconnects for tests. */
const FAST = { initialMs: 5, maxMs: 20, random: () => 0 };

function frame({ id, event, data }) {
  return (id === undefined ? "" : `id: ${id}\n`) + (event === undefined ? "" : `event: ${event}\n`) + `data: ${data}\n\n`;
}

test("events arrive in order with their id, type and data", async () => {
  const server = await serve((_req, res) => {
    const out = stream(res);
    out.write(frame({ id: 1, event: "update", data: '{"n":1}' }));
    out.write(frame({ id: 2, data: "second" }));
    out.write(": hb\n\n");
  });
  const { seen, options } = collector();
  const abort = new AbortController();
  try {
    const done = events(`${server.url}/v1/events`, {
      ...options,
      signal: abort.signal,
      backoff: { ...FAST, initialMs: 60_000, maxMs: 60_000, random: () => 0.5 },
      onEvent: (event) => {
        seen.events.push(event);
        if (seen.events.length === 2) abort.abort();
      },
    });
    await done;
    assert.deepEqual(seen.events, [
      { event: "update", data: '{"n":1}', id: "1" },
      { event: "message", data: "second", id: "2" },
    ]);
    assert.equal(seen.opens, 1);
    const [request] = server.requests;
    assert.equal(request.method, "GET");
    assert.equal(request.headers.accept, "text/event-stream");
    assert.equal(request.headers["last-event-id"], undefined);
    assert.equal(request.headers["x-xrpc-timeout-ms"], undefined);
  } finally {
    await server.close();
  }
});

test("multi-line data, CRLF line ends and heartbeat comments", async () => {
  const server = await serve((_req, res) => {
    const out = stream(res);
    out.write(": hb\r\n\r\n");
    out.write("id: 7\r\nevent: note\r\ndata: line one\r\ndata: line two\r\n\r\n");
    out.write(": hb\n\n");
    out.write("data: tail\n\n");
  });
  const { seen, options } = collector();
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      ...options,
      signal: abort.signal,
      onEvent: (event) => {
        seen.events.push(event);
        if (seen.events.length === 2) abort.abort();
      },
    });
    assert.deepEqual(seen.events, [
      { event: "note", data: "line one\nline two", id: "7" },
      { event: "message", data: "tail", id: undefined },
    ]);
  } finally {
    await server.close();
  }
});

test("a stream written one byte at a time, splitting UTF-8 sequences and CRLF, parses the same", async () => {
  const text = frame({ id: "é-1", event: "greeting", data: "héllo wörld \u2713 \u{1f600}" }) + "data: a\r\ndata: b\r\n\r\n";
  const bytes = Buffer.from(text, "utf8");
  const server = await serve(async (_req, res) => {
    const out = stream(res);
    for (const byte of bytes) {
      out.write(Buffer.from([byte]));
      await delay(1);
    }
  });
  const { seen, options } = collector();
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      ...options,
      signal: abort.signal,
      onEvent: (event) => {
        seen.events.push(event);
        if (seen.events.length === 2) abort.abort();
      },
    });
    assert.deepEqual(seen.events, [
      { event: "greeting", data: "héllo wörld \u2713 \u{1f600}", id: "é-1" },
      { event: "message", data: "a\nb", id: undefined },
    ]);
  } finally {
    await server.close();
  }
});

test("a dropped connection resumes with Last-Event-ID and delivers each event once", async () => {
  const server = await serve(async (_req, res, record) => {
    const out = stream(res);
    if (record.index === 0) {
      out.write(frame({ id: 1, data: "a" }) + frame({ id: 2, data: "b" }));
      out.end();
    } else if (record.index === 1) {
      await out.write(frame({ id: 3, data: "c" }));
      res.socket.destroy();
    } else {
      out.write(frame({ id: 4, data: "d" }));
    }
  });
  const { seen, options } = collector();
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      ...options,
      signal: abort.signal,
      backoff: FAST,
      onEvent: (event) => {
        seen.events.push(event);
        if (event.data === "d") abort.abort();
      },
    });
    assert.deepEqual(seen.events.map((event) => event.data), ["a", "b", "c", "d"]);
    assert.deepEqual(server.requests.map((request) => request.headers["last-event-id"]), [undefined, "2", "3"]);
    assert.equal(seen.opens, 3);
    assert.equal(seen.errors.length, 2);
    assert.equal(seen.errors[0].error.code, "unavailable");
    assert.equal(seen.errors[0].error.disposition, "outcome_unknown");
  } finally {
    await server.close();
  }
});

test("the after option is sent as Last-Event-ID on the first request", async () => {
  const server = await serve((_req, res) => {
    stream(res).write(frame({ id: 43, data: "next" }));
  });
  const { options } = collector();
  const abort = new AbortController();
  try {
    await events(`${server.url}/?x=1`, {
      ...options,
      after: "42",
      signal: abort.signal,
      onEvent: () => abort.abort(),
    });
    assert.equal(server.requests[0].headers["last-event-id"], "42");
    assert.equal(server.requests[0].url, "/?x=1");
  } finally {
    await server.close();
  }
});

test("frames without an id keep the cursor, an empty id clears it", async () => {
  const server = await serve((_req, res, record) => {
    const out = stream(res);
    if (record.index === 0) out.write(frame({ id: 5, data: "a" }) + frame({ data: "no id" }));
    else if (record.index === 1) out.write("id: 9\ndata: b\n\nid:\ndata: cleared\n\n");
    else out.write(frame({ data: "end" }));
    out.end();
  });
  const { seen, options } = collector();
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      ...options,
      signal: abort.signal,
      backoff: FAST,
      onEvent: (event) => {
        seen.events.push(event);
        if (event.data === "end") abort.abort();
      },
    });
    assert.deepEqual(server.requests.map((request) => request.headers["last-event-id"]), [undefined, "5", undefined]);
    assert.equal(seen.events.find((event) => event.data === "cleared").id, undefined);
  } finally {
    await server.close();
  }
});

test("reset forgets the cursor, calls onReset and keeps the stream", async () => {
  const server = await serve((_req, res, record) => {
    const out = stream(res);
    if (record.index === 0) {
      out.write(frame({ id: 1, data: "a" }));
      out.end();
    } else if (record.index === 1) {
      out.write(frame({ event: "reset", data: '{"reason":"cursor gone"}' }));
      out.write(frame({ data: "snapshot" }));
      out.end();
    } else if (record.index === 2) {
      out.write(frame({ event: "reset", id: 100, data: "{}" }));
      out.write(frame({ id: 101, data: "after reset" }));
      out.end();
    } else {
      out.write(frame({ data: "end" }));
    }
  });
  const { seen, options } = collector();
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      ...options,
      signal: abort.signal,
      backoff: FAST,
      onEvent: (event) => {
        seen.events.push(event);
        if (event.data === "end") abort.abort();
      },
    });
    assert.deepEqual(seen.resets.map((event) => event.data), ['{"reason":"cursor gone"}', "{}"]);
    assert.deepEqual(seen.resets.map((event) => event.id), [undefined, "100"]);
    assert.deepEqual(seen.events.map((event) => event.data), ["a", "snapshot", "after reset", "end"]);
    // Request 1 resumes after 1; the reset frame without an id empties the
    // cursor, so request 2 sends none; request 3 resumes after reset id 100,
    // moved on to 101 by the next event.
    assert.deepEqual(server.requests.map((request) => request.headers["last-event-id"]), [undefined, "1", undefined, "101"]);
  } finally {
    await server.close();
  }
});

test("closing is announced to onClosing, then the client reconnects and resumes", async () => {
  const server = await serve((_req, res, record) => {
    const out = stream(res);
    if (record.index === 0) {
      out.write(frame({ id: 1, data: "a" }));
      out.write(frame({ event: "closing", data: '{"drain":true}' }));
      out.end();
    } else {
      out.write(frame({ id: 2, data: "b" }));
    }
  });
  const { seen, options } = collector();
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      ...options,
      signal: abort.signal,
      backoff: FAST,
      onEvent: (event) => {
        seen.events.push(event);
        if (event.data === "b") abort.abort();
      },
    });
    assert.deepEqual(seen.closings, [{ event: "closing", data: '{"drain":true}', id: undefined }]);
    assert.deepEqual(seen.events.map((event) => event.data), ["a", "b"]);
    assert.deepEqual(server.requests.map((request) => request.headers["last-event-id"]), [undefined, "1"]);
    assert.equal(seen.errors.length, 0, "an announced close is not an error");
  } finally {
    await server.close();
  }
});

test("without dedicated callbacks reset and closing are delivered to onEvent", async () => {
  const server = await serve((_req, res) => {
    const out = stream(res);
    out.write(frame({ event: "reset", data: "{}" }) + frame({ event: "closing", data: "{}" }) + frame({ data: "end" }));
  });
  const seen = [];
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      signal: abort.signal,
      onEvent: (event) => {
        seen.push(event.event);
        if (event.data === "end") abort.abort();
      },
    });
    assert.deepEqual(seen, ["reset", "closing", "message"]);
  } finally {
    await server.close();
  }
});

test("aborting inside onEvent closes the connection and does not reconnect", async () => {
  const server = await serve((_req, res) => {
    stream(res).write(frame({ id: 1, data: "a" }));
  });
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, { signal: abort.signal, backoff: FAST, onEvent: () => abort.abort() });
    await until(() => server.requests[0].closed, 3000, "the server to see the stream close");
    await delay(60);
    assert.equal(server.requests.length, 1);
  } finally {
    await server.close();
  }
});

test("aborting while waiting to reconnect resolves promptly", async () => {
  const server = await serve((_req, res) => {
    stream(res).end();
  });
  const abort = new AbortController();
  try {
    const started = Date.now();
    const done = events(`${server.url}/`, {
      signal: abort.signal,
      backoff: { initialMs: 60_000, maxMs: 60_000, random: () => 0.99 },
      onEvent: () => {},
      onError: () => setTimeout(() => abort.abort(), 20),
    });
    await done;
    assert.ok(Date.now() - started < 3000);
    assert.equal(server.requests.length, 1);
  } finally {
    await server.close();
  }
});

test("an already aborted signal does nothing", async () => {
  const server = await serve((_req, res) => stream(res));
  try {
    await events(`${server.url}/`, { signal: AbortSignal.abort(), onEvent: () => assert.fail("no event expected") });
    assert.equal(server.requests.length, 0);
  } finally {
    await server.close();
  }
});

test("a client error answer is final and carries the XRPC error envelope", async () => {
  const server = await serve((_req, res) => {
    json(res, 404, { error: { code: "not_found", message: "no such session" } });
  });
  try {
    await assert.rejects(
      events(`${server.url}/`, { backoff: FAST, onEvent: () => {} }),
      (error) =>
        error instanceof XrpcError &&
        error.code === "not_found" &&
        error.message === "no such session" &&
        error.status === 404 &&
        error.disposition === "response_received",
    );
    assert.equal(server.requests.length, 1);
    for (const status of [400, 401, 403]) {
      const gate = await serve((_req, res) => {
        res.writeHead(status);
        res.end();
      });
      try {
        await assert.rejects(events(`${gate.url}/`, { onEvent: () => {} }), (error) => error.status === status);
        assert.equal(gate.requests.length, 1);
      } finally {
        await gate.close();
      }
    }
  } finally {
    await server.close();
  }
});

test("draining or overloaded servers are retried with backoff", async () => {
  const server = await serve((_req, res, record) => {
    if (record.index < 3) {
      json(res, [503, 429, 500][record.index], { error: { code: "unavailable", message: "host stopping" } });
    } else {
      stream(res).write(frame({ id: 1, data: "finally" }));
    }
  });
  const { seen, options } = collector();
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      ...options,
      signal: abort.signal,
      backoff: { initialMs: 10, maxMs: 40, random: () => 0.5 },
      onEvent: (event) => {
        seen.events.push(event);
        abort.abort();
      },
    });
    assert.deepEqual(seen.events.map((event) => event.data), ["finally"]);
    assert.deepEqual(seen.errors.map(({ error }) => error.status), [503, 429, 500]);
    assert.deepEqual(seen.errors.map(({ next }) => next.failures), [1, 2, 3]);
    assert.deepEqual(seen.errors.map(({ next }) => next.delayMs), [5, 10, 20]);
    assert.equal(seen.errors[0].error.message, "host stopping");
  } finally {
    await server.close();
  }
});

test("the failure count resets once a stream was established", async () => {
  const server = await serve((_req, res, record) => {
    if (record.index === 0) res.destroy();
    else if (record.index === 1) stream(res).end();
    else if (record.index === 2) res.destroy();
    else stream(res).write(frame({ data: "end" }));
  });
  const { seen, options } = collector();
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      ...options,
      signal: abort.signal,
      backoff: { initialMs: 8, maxMs: 64, random: () => 0.5 },
      onEvent: (event) => {
        seen.events.push(event);
        abort.abort();
      },
    });
    assert.deepEqual(seen.errors.map(({ next }) => next.failures), [1, 0, 1]);
    assert.deepEqual(seen.errors.map(({ next }) => next.delayMs), [4, 4, 4]);
  } finally {
    await server.close();
  }
});

test("reconnect delays are full jitter between 0 and a cap that grows from 500 ms to 5 s", () => {
  const caps = [];
  for (let failures = 0; failures <= 8; failures++) {
    // random() just below 1 exposes the cap: delay = floor(cap * 0.999999).
    caps.push(reconnectDelay(failures, 500, 5000, () => 0.999999) + 1);
  }
  assert.deepEqual(caps, [500, 500, 1000, 2000, 4000, 5000, 5000, 5000, 5000]);
  assert.equal(reconnectDelay(3, 500, 5000, () => 0), 0);
  assert.equal(reconnectDelay(3, 500, 5000, () => 0.5), 1000);
  for (let i = 0; i < 1000; i++) {
    const delayMs = reconnectDelay(4, 500, 5000, Math.random);
    assert.ok(delayMs >= 0 && delayMs < 4000);
  }
});

test("an answer that is not an event stream is final", async () => {
  const server = await serve((_req, res) => {
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end("<html>login</html>");
  });
  try {
    await assert.rejects(
      events(`${server.url}/`, { onEvent: () => {} }),
      (error) => error instanceof XrpcError && error.code === "internal" && /text\/event-stream/.test(error.message),
    );
    assert.equal(server.requests.length, 1);
  } finally {
    await server.close();
  }
});

test("a silent connection is abandoned after the idle timeout and heartbeats keep it alive", async () => {
  const server = await serve(async (_req, res, record) => {
    const out = stream(res);
    if (record.index === 0) return; // silent
    for (let i = 0; i < 6; i++) {
      out.write(": hb\n\n");
      await delay(40);
    }
    out.write(frame({ id: 1, data: "alive" }));
  });
  const { seen, options } = collector();
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      ...options,
      signal: abort.signal,
      backoff: FAST,
      idleTimeoutMs: 150,
      onEvent: (event) => {
        seen.events.push(event);
        abort.abort();
      },
    });
    assert.equal(seen.errors.length, 1);
    assert.match(seen.errors[0].error.message, /no data/);
    assert.equal(server.requests.length, 2, "heartbeats every 40 ms kept the second connection open past 150 ms");
    assert.deepEqual(seen.events.map((event) => event.data), ["alive"]);
  } finally {
    await server.close();
  }
});

test("an oversized frame is final", async () => {
  const server = await serve((_req, res) => {
    stream(res).write("data: " + "x".repeat(5000));
  });
  try {
    await assert.rejects(
      events(`${server.url}/`, { maxEventChars: 1024, onEvent: () => {} }),
      (error) => error instanceof XrpcError && error.code === "resource_exhausted",
    );
    assert.equal(server.requests.length, 1);
  } finally {
    await server.close();
  }
});

test("a throwing callback stops the stream and rejects", async () => {
  const server = await serve((_req, res) => {
    stream(res).write(frame({ id: 1, data: "boom" }));
  });
  try {
    await assert.rejects(
      events(`${server.url}/`, {
        backoff: FAST,
        onEvent: () => {
          throw new Error("handler failed");
        },
      }),
      /handler failed/,
    );
    await until(() => server.requests[0].closed, 3000, "the stream to close");
    await delay(60);
    assert.equal(server.requests.length, 1);
  } finally {
    await server.close();
  }
});

test("callbacks are awaited in order", async () => {
  const server = await serve((_req, res) => {
    stream(res).write(frame({ data: "1" }) + frame({ data: "2" }) + frame({ data: "3" }));
  });
  const order = [];
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      signal: abort.signal,
      onEvent: async (event) => {
        order.push(`start ${event.data}`);
        await delay(event.data === "1" ? 30 : 1);
        order.push(`end ${event.data}`);
        if (event.data === "3") abort.abort();
      },
    });
    assert.deepEqual(order, ["start 1", "end 1", "start 2", "end 2", "start 3", "end 3"]);
  } finally {
    await server.close();
  }
});

test("caller headers are sent and protocol headers are refused", async () => {
  const server = await serve((_req, res) => {
    stream(res).write(frame({ data: "x" }));
  });
  const abort = new AbortController();
  try {
    await events(`${server.url}/`, {
      signal: abort.signal,
      headers: { Authorization: "Bearer t" },
      onEvent: () => abort.abort(),
    });
    assert.equal(server.requests[0].headers.authorization, "Bearer t");
    for (const name of ["Accept", "last-event-id"]) {
      await assert.rejects(
        events(`${server.url}/`, { headers: { [name]: "x" }, onEvent: () => {} }),
        (error) => error instanceof XrpcError && error.code === "invalid_argument" && error.disposition === "not_sent",
      );
    }
    await assert.rejects(events("ftp://example.invalid/", { onEvent: () => {} }), (error) => error.code === "invalid_argument");
    await assert.rejects(events(server.url, {}), (error) => error.code === "invalid_argument");
    assert.equal(server.requests.length, 1);
  } finally {
    await server.close();
  }
});

test("a cursor that cannot be a header value is refused instead of crashing", async () => {
  const server = await serve((_req, res, record) => {
    if (record.index === 0) {
      const out = stream(res);
      out.write(frame({ id: "\u20ac1", data: "euro" }));
      out.end();
    } else {
      stream(res);
    }
  });
  try {
    await assert.rejects(
      events(`${server.url}/`, { backoff: FAST, onEvent: () => {} }),
      (error) => error instanceof XrpcError && error.code === "invalid_argument",
    );
    await assert.rejects(events(`${server.url}/`, { after: "bad\nvalue", onEvent: () => {} }), (error) => error.code === "invalid_argument");
    await assert.rejects(events(`${server.url}/`, { after: 7, onEvent: () => {} }), (error) => error.code === "invalid_argument");
    assert.equal(server.requests.length, 1, "only the first connection was made");
  } finally {
    await server.close();
  }
});
