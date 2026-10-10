"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const { once } = require("node:events");
const { Diagnostics, createHTTPHost } = require("..");

function get(port, route) {
  return new Promise((resolve, reject) => {
    const request = http.get({ host: "127.0.0.1", port, path: route }, response => {
      response.resume(); response.once("end", () => resolve(response.statusCode)); response.once("error", reject);
    });
    request.once("error", reject);
  });
}
test("shared diagnostics observe real host work and remain owned after host drain", async () => {
  const diagnostics = new Diagnostics({ sink: {kind:"supervisor_stderr",rotationOwner:"supervisor"}, level: "debug" });
  let release, admitted;
  const held = new Promise(resolve => { release = resolve; });
  const dispatched = new Promise(resolve => { admitted = resolve; });
  const host = createHTTPHost(async (request,response) => {
    if (request.url === "/held") { admitted(); await held; }
    response.end("bounded");
  }, {diagnostics});
  host.server.listen(0,"127.0.0.1"); await once(host.server,"listening");
  try {
    const port = host.server.address().port;
    assert.equal(await get(port,"/ready"),200);
    const call = get(port,"/held"); await dispatched;
    assert.equal(host.stats().inFlight,1);
    const drain = host.close();
    assert.equal(diagnostics.status().state,"running");
    assert.equal(diagnostics.status().eventCounts.call_completed,1);
    release(); assert.equal(await call,200); await drain;
    const status = diagnostics.status();
    assert.equal(status.eventCounts.call_started,2); assert.equal(status.eventCounts.call_completed,2);
    assert.equal(status.eventCounts.shutdown_started,1); assert.equal(status.eventCounts.shutdown_completed,1);
    assert.ok(status.eventCounts.connection_accepted >= 1);
    assert.equal(status.state,"running"); assert.equal(host.stats().inFlight,0);
  } finally {release();await host.close();await diagnostics.close();}
  assert.equal(diagnostics.status().written,diagnostics.status().admitted);
});
