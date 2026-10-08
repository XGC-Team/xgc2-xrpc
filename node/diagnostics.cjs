"use strict";
const { Worker } = require("node:worker_threads");
const { performance } = require("node:perf_hooks");
const registry = require("./runtime-policy.json");

const LEVELS = Object.freeze({ trace: 0, debug: 1, info: 2, warn: 3, error: 4 });
const EVENTS = Object.freeze({
  connection_accepted: "debug", connection_closed: "debug", connection_rejected: "warn",
  call_started: "debug", call_completed: "debug", call_rejected: "warn",
  deadline_exceeded: "warn", cancelled: "info", handler_failed: "error",
  transport_failed: "warn", client_pool_full: "warn", shutdown_started: "info",
  shutdown_completed: "info", shutdown_failed: "error", diagnostic_saturated: "warn",
  diagnostic_recovered: "info", sink_failed: "error", unclassified: "debug",
});
const IDENTITIES = Object.freeze(["service", "instance_id", "request_id", "trace_id", "operation"]);
const NUMBERS = Object.freeze(["elapsed_ms", "budget_ms", "connections", "in_flight", "queue_depth", "bytes", "limit", "count", "repeat_count", "dropped"]);
const CATEGORIES = new Set(["invalid_argument", "not_found", "conflict", "resource_exhausted", "deadline_exceeded", "cancelled", "unavailable", "internal", "not_sent", "outcome_unknown"]);
const STATES = new Set(["starting", "running", "closing", "closed", "saturated", "recovered", "failed"]);
const TOKEN = /^[A-Za-z0-9._:-]{1,128}$/;
const DEFAULT_LEVEL = registry.fields.find((field) => field.name === "LOG_LEVEL").default;
const DEFAULT_FORMAT = registry.fields.find((field) => field.name === "LOG_FORMAT").default;
function increment(object, field, amount = 1) { object[field] = Math.min(Number.MAX_SAFE_INTEGER, object[field] + amount); }
function positive(name, value, minimum, maximum) {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) throw new RangeError(`${name}: finite integer in ${minimum}..${maximum} required`);
  return value;
}
class DiagnosticCloseError extends Error {
  constructor() { super("diagnostic shutdown deadline; writer and pending records retained"); this.name = "DiagnosticCloseError"; this.code = "deadline_exceeded"; }
}
class DiagnosticSinkError extends Error {
  constructor() { super("diagnostic sink failed; consult bounded diagnostic status"); this.name = "DiagnosticSinkError"; this.code = "unavailable"; }
}

// This object is explicitly created once by the process composition root and
// passed to its shared policy. There is no ambient owner or file allocation.
// Worker ACKs release admission only after a complete write (or a counted
// failure), so the cross-thread message channel cannot become a hidden queue.
class Diagnostics {
  #policy = null;
  #used = false;
  #worker = null;
  #state = "running";
  #exited = false;
  #failed = false;
  #pending = new Set();
  #sequence = 0;
  #saturated = false;
  #pendingSaturation = false;
  #pendingRecovery = false;
  #closeSent = false;
  #closeWait = null;
  #last = Object.fromEntries(Object.keys(EVENTS).map((name) => [name, -Infinity]));
  #repeated = Object.fromEntries(Object.keys(EVENTS).map((name) => [name, 0]));
  #counts = Object.fromEntries(Object.keys(EVENTS).map((name) => [name, 0]));
  #totals = { admitted: 0, written: 0, filtered: 0, dropped: 0, rateSuppressed: 0, redactedFields: 0, sinkFailures: 0, workerFailures: 0, saturations: 0, recoveries: 0 };
  #capacity;
  #recordBytes;
  #closeMs;
  #repeatMs;

  constructor({ sink, maxPendingRecords = 128, maxRecordBytes = 2048, closeTimeoutMs = 5000, repeatIntervalMs = 1000 } = {}) {
    if (!sink || sink.kind !== "supervisor_stderr" || sink.rotationOwner !== "supervisor") throw new TypeError("explicit supervisor_stderr sink with supervisor rotation owner required");
    this.#capacity = positive("maxPendingRecords", maxPendingRecords, 1, 4096);
    this.#recordBytes = positive("maxRecordBytes", maxRecordBytes, 256, 65536);
    this.#closeMs = positive("closeTimeoutMs", closeTimeoutMs, 1, 2147483647);
    this.#repeatMs = positive("repeatIntervalMs", repeatIntervalMs, 1, 2147483647);
  }

  // Called by the startup resolver. Reusing this sink with another resolved
  // policy would create conflicting process owners and is rejected.
  _bindPolicy(policy) {
    if (this.#policy === policy) return;
    if (this.#policy || this.#used || this.#state !== "running") throw new TypeError("diagnostics must be bound once before first emission");
    if (!policy?.fields?.LOG_LEVEL || !policy.fields.LOG_FORMAT || !Object.hasOwn(LEVELS, policy.fields.LOG_LEVEL.value) || !["json", "text"].includes(policy.fields.LOG_FORMAT.value)) throw new TypeError("resolved diagnostic policy required");
    this.#policy = policy;
  }
  #level() { return this.#policy?.fields.LOG_LEVEL.value ?? DEFAULT_LEVEL; }
  #format() { return this.#policy?.fields.LOG_FORMAT.value ?? DEFAULT_FORMAT; }

  #fields(input) {
    const out = {};
    if (input == null) return out;
    try {
      if (typeof input !== "object" || Array.isArray(input)) { increment(this.#totals, "redactedFields"); return out; }
    } catch { increment(this.#totals, "redactedFields"); return out; }
    // Inspect only a fixed set of own data properties. Unknown fields (including
    // body, Authorization, cookies, config and Error.message) are never read,
    // enumerated, stringified or sent to another thread. Do not pass a Proxy or
    // other executable object: transport integrations supply plain scalars.
    for (const name of [...IDENTITIES, ...NUMBERS, "category", "state"]) {
      let descriptor;
      try { descriptor = Object.getOwnPropertyDescriptor(input, name); } catch { increment(this.#totals, "redactedFields"); continue; }
      if (!descriptor) continue;
      const value = descriptor.value;
      const valid = Object.hasOwn(descriptor, "value") && (IDENTITIES.includes(name)
        ? typeof value === "string" && value.length <= 128 && TOKEN.test(value)
        : NUMBERS.includes(name) ? typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER
          : name === "category" ? CATEGORIES.has(value) : STATES.has(value));
      if (valid) out[name] = value; else increment(this.#totals, "redactedFields");
    }
    return out;
  }

  #encode(event, fields) {
    const base = { time_unix_ms: Date.now(), severity: EVENTS[event], event };
    const names = Object.keys(fields); // At most the fixed whitelist size.
    for (;;) {
      const line = this.#format() === "json" ? JSON.stringify({ ...base, ...fields }) + "\n"
        : `${base.time_unix_ms} ${base.severity} ${event}` + Object.entries(fields).map(([name, value]) => ` ${name}=${JSON.stringify(value)}`).join("") + "\n";
      if (Buffer.byteLength(line) <= this.#recordBytes) return line;
      // Inputs are already bounded primitives. Trimming preserves the stable
      // event envelope without serializing an unbounded caller object.
      delete fields[names.pop()]; increment(this.#totals, "redactedFields");
    }
  }

  #start() {
    if (this.#worker) return;
    this.#worker = new Worker(require.resolve("./diagnostic-worker.cjs"), {
      workerData: { maxRecordBytes: this.#recordBytes, maxPendingRecords: this.#capacity }, env: {}, execArgv: [],
    });
    this.#worker.on("message", (message) => {
      if (message?.kind !== "ack" || !this.#pending.delete(message.id)) return;
      if (message.ok) increment(this.#totals, "written");
      else {
        increment(this.#totals, "dropped"); increment(this.#totals, "sinkFailures"); increment(this.#counts, "sink_failed"); this.#failed = true;
        if (this.#state === "running") this.#state = "failed";
      }
      if (this.#saturated && this.#pending.size <= Math.floor(this.#capacity / 2)) {
        this.#saturated = false; this.#pendingRecovery = true;
        increment(this.#totals, "recoveries"); increment(this.#counts, "diagnostic_recovered");
      }
      this.#pumpTransitions(); this.#maybeClose();
    });
    this.#worker.on("error", () => {
      this.#failed = true; increment(this.#totals, "workerFailures");
      if (this.#state === "running") this.#state = "failed";
      // Never copy arbitrary worker error text, stack, filenames or secrets.
    });
    this.#worker.on("exit", (code) => {
      this.#exited = true;
      if (code !== 0 || !this.#closeSent) { this.#failed = true; increment(this.#totals, "workerFailures"); }
      increment(this.#totals, "dropped", this.#pending.size); this.#pending.clear();
      this.#state = this.#closeSent ? "closed" : "failed";
      this.#finishClose();
    });
  }

  #enqueue(event, fields) {
    const line = this.#encode(event, fields);
    // Reuse small sequence numbers only when no corresponding item is pending.
    do { this.#sequence = this.#sequence === Number.MAX_SAFE_INTEGER ? 1 : this.#sequence + 1; } while (this.#pending.has(this.#sequence));
    const id = this.#sequence;
    this.#pending.add(id);
    try {
      this.#start(); this.#worker.postMessage({ kind: "record", id, line });
      increment(this.#totals, "admitted"); return true;
    } catch {
      this.#pending.delete(id); increment(this.#totals, "dropped"); increment(this.#totals, "workerFailures"); this.#failed = true;
      if (!this.#worker) this.#state = "failed";
      return false;
    }
  }

  #pumpTransitions() {
    for (const [flag, event] of [["saturation", "diagnostic_saturated"], ["recovery", "diagnostic_recovered"]]) {
      if (!(flag === "saturation" ? this.#pendingSaturation : this.#pendingRecovery)) continue;
      if (this.#pending.size >= this.#capacity || this.#exited || this.#failed) return;
      if (flag === "saturation") this.#pendingSaturation = false; else this.#pendingRecovery = false;
      if (LEVELS[EVENTS[event]] < LEVELS[this.#level()]) { increment(this.#totals, "filtered"); continue; }
      this.#enqueue(event, { dropped: this.#totals.dropped, queue_depth: this.#pending.size, state: flag === "saturation" ? "saturated" : "recovered" });
    }
  }

  emit(event, input) {
    this.#used = true;
    if (typeof event !== "string" || !Object.hasOwn(EVENTS, event)) event = "unclassified";
    increment(this.#counts, event);
    if (this.#state !== "running" || this.#failed) { increment(this.#totals, "dropped"); return false; }
    if (LEVELS[EVENTS[event]] < LEVELS[this.#level()]) { increment(this.#totals, "filtered"); return true; }
    const now = performance.now();
    if (LEVELS[EVENTS[event]] >= LEVELS.warn && now - this.#last[event] < this.#repeatMs) {
      increment(this.#repeated, event); increment(this.#totals, "rateSuppressed"); return true;
    }
    if (this.#pending.size >= this.#capacity) {
      increment(this.#totals, "dropped");
      if (!this.#saturated) {
        this.#saturated = true; this.#pendingSaturation = true;
        increment(this.#totals, "saturations"); increment(this.#counts, "diagnostic_saturated");
      }
      return false;
    }
    const fields = this.#fields(input);
    if (this.#repeated[event]) { fields.repeat_count = this.#repeated[event]; this.#repeated[event] = 0; }
    if (!this.#enqueue(event, fields)) return false;
    this.#last[event] = performance.now(); return true;
  }

  status() {
    return Object.freeze({
      sourceTimeUnixMs: Date.now(), state: this.#state, failed: this.#failed, workerStarted: !!this.#worker,
      workerAlive: !!this.#worker && !this.#exited, pendingRecords: this.#pending.size,
      queueCapacity: this.#capacity, maxRecordBytes: this.#recordBytes,
      identityFieldByteLimit: 128, identityFieldCountLimit: IDENTITIES.length,
      saturated: this.#saturated, level: this.#level(), format: this.#format(),
      policyRevision: this.#policy?.revision ?? null,
      eventCounts: Object.freeze({ ...this.#counts }), repeatPending: Object.freeze({ ...this.#repeated }),
      ...this.#totals,
      sink: Object.freeze({ backend: "supervisor_stderr", rotationOwner: "supervisor", opensFiles: false }),
    });
  }

  #maybeClose() {
    if (this.#state !== "closing" || this.#closeSent || !this.#worker || this.#exited || this.#pending.size) return;
    // Transition alerts consume the same bounded ACKed admission. They are
    // flushed before the close marker rather than posted behind it.
    if (!this.#failed && (this.#pendingSaturation || this.#pendingRecovery)) { this.#pumpTransitions(); if (this.#pending.size) return; }
    this.#closeSent = true;
    try { this.#worker.postMessage({ kind: "close" }); } catch { this.#failed = true; }
  }
  #finishClose() {
    if (!this.#closeWait || !this.#exited) return;
    clearTimeout(this.#closeWait.timer);
    const waiter = this.#closeWait; this.#closeWait = null;
    if (this.#failed) waiter.reject(new DiagnosticSinkError()); else waiter.resolve();
  }

  close({ timeoutMs = this.#closeMs } = {}) {
    positive("timeoutMs", timeoutMs, 1, 2147483647);
    if (this.#closeWait) return this.#closeWait.promise; // One owner waiter/timer.
    if (!this.#worker) { this.#state = "closed"; return this.#failed ? Promise.reject(new DiagnosticSinkError()) : Promise.resolve(); }
    if (this.#exited) { this.#state = "closed"; return this.#failed ? Promise.reject(new DiagnosticSinkError()) : Promise.resolve(); }
    this.#state = "closing";
    let resolve, reject;
    const promise = new Promise((done, fail) => { resolve = done; reject = fail; });
    const timer = setTimeout(() => { this.#closeWait = null; reject(new DiagnosticCloseError()); }, timeoutMs);
    this.#closeWait = { promise, resolve, reject, timer };
    this.#maybeClose(); return promise;
  }
}

module.exports = { Diagnostics, DiagnosticCloseError, DiagnosticSinkError };
