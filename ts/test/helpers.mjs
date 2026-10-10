import http from "node:http";

/** A local HTTP server that records every request it receives. */
export async function serve(handler) {
  const requests = [];
  const sockets = new Set();
  const server = http.createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const record = {
      method: req.method,
      url: req.url,
      headers: req.headers,
      body: Buffer.concat(chunks).toString("utf8"),
      closed: false,
    };
    record.index = requests.length;
    requests.push(record);
    res.on("close", () => {
      record.closed = true;
    });
    await handler(req, res, record);
  });
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${server.address().port}`;
  return {
    url,
    requests,
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise((resolve) => server.close(resolve));
    },
  };
}

/** Reply with JSON. */
export function json(res, status, value, headers = {}) {
  const body = JSON.stringify(value);
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(body),
    ...headers,
  });
  res.end(body);
}

/** Start an event-stream response and return a writer for raw frames. */
export function stream(res, headers = {}) {
  res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-store", ...headers });
  res.flushHeaders();
  return {
    /** Resolves once the bytes were handed to the kernel. */
    write: (text) => new Promise((resolve) => res.write(text, resolve)),
    end: () => res.end(),
  };
}

export const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Wait until `condition()` is truthy, failing after `ms`. */
export async function until(condition, ms = 3000, what = "condition") {
  const deadline = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await delay(5);
  }
}

/** Run `events()` and collect what it reports. */
export function collector() {
  const seen = { events: [], resets: [], closings: [], errors: [], opens: 0 };
  return {
    seen,
    options: {
      onEvent: (event) => void seen.events.push(event),
      onReset: (event) => void seen.resets.push(event),
      onClosing: (event) => void seen.closings.push(event),
      onError: (error, next) => void seen.errors.push({ error, next }),
      onOpen: () => void seen.opens++,
    },
  };
}
