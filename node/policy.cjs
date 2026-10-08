"use strict";
const registry = require("./runtime-policy.json");
const { Diagnostics } = require("./diagnostics.cjs");
const supported = ["host", "http", "rpc", "transport", "client_pool", "client_registry"];
class PolicyError extends Error {
  constructor(field, reason) { super(`${field}: ${reason}`); this.name = "PolicyError"; this.field = field; }
}
function parse(field, value) {
  if (typeof value !== "string") value = String(value);
  if (field.type === "enum") {
    if (!field.values.includes(value)) throw new PolicyError(field.name, "unsupported enum value");
    return value;
  }
  if (!/^[1-9][0-9]*$/.test(value) || value.length > 10) throw new PolicyError(field.name, "canonical positive ASCII decimal required");
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number > Math.min(registry.integer_max, field.maximum ?? registry.integer_max)) {
    throw new PolicyError(field.name, "integer exceeds maximum");
  }
  return number;
}
// The composition root supplies a startup snapshot. No ambient getenv here.
function resolvePolicy({ environment, defaults = {}, ceilings = {}, capabilities, diagnostics } = {}) {
  if (environment == null || typeof environment !== "object" || Array.isArray(environment)) throw new TypeError("explicit environment snapshot required");
  if (diagnostics != null && !(diagnostics instanceof Diagnostics)) throw new TypeError("explicit Diagnostics owner required");
  const enabled = new Set(capabilities ?? (diagnostics ? [...supported, "diagnostics"] : supported));
  for (const capability of enabled) {
    if (capability === "diagnostics") {
      if (!diagnostics) throw new PolicyError(capability, "explicit Diagnostics owner required");
    } else if (!supported.includes(capability)) throw new PolicyError(capability, "SDK capability is not implemented");
  }
  if (diagnostics && !enabled.has("diagnostics")) throw new PolicyError("diagnostics", "provided owner requires diagnostics capability");
  const known = new Map(registry.fields.map((f) => [f.name, f]));
  const values = new Map();
  for (const field of registry.fields) if (enabled.has(field.capability)) values.set(field.name, {
    value: field.default, source: "sdk_default", dynamic: field.dynamic, ceiling: null,
  });
  const apply = (name, raw, source) => {
    const field = known.get(name);
    if (!field) throw new PolicyError(source === "environment" ? registry.prefix + name : name, "unknown runtime setting");
    if (!enabled.has(field.capability)) throw new PolicyError(name, "setting is not enforced by selected capabilities");
    values.get(name).value = parse(field, raw);
    values.get(name).source = source;
  };
  for (const [name, raw] of Object.entries(defaults)) apply(name, raw, "deployment");
  for (const [name, raw] of Object.entries(environment)) if (name.startsWith(registry.prefix)) apply(name.slice(registry.prefix.length), raw, "environment");
  for (const [name, raw] of Object.entries(ceilings)) {
    const field = known.get(name), entry = values.get(name);
    if (!field || field.type !== "integer" || !entry) throw new PolicyError(name, "ceiling requires a supported integer field");
    const ceiling = parse(field, raw);
    if (entry.value > ceiling) throw new PolicyError(name, "resolved value exceeds ceiling");
    entry.ceiling = ceiling;
  }
  // Freeze individual entries as well as the enclosing value. Snapshots never
  // retain the supplied environment object or unrelated secret values.
  const snapshot = (revision, entries) => Object.freeze({ revision, fields: Object.freeze(Object.fromEntries([...entries].map(([name, entry]) => [name, Object.freeze({ ...entry })]))) });
  let current = snapshot(1, values);
  const owner = Object.freeze({
    get revision() { return current.revision; },
    get fields() { return current.fields; },
    diagnostics,
    effective() { return current; },
    // This is a local administrative primitive. The owning product must
    // authenticate and authorize its caller before invoking it; no port,
    // credentials or Boolean authorization shortcut is introduced here.
    update(changes, { expectedRevision } = {}) {
      const prior = current;
      if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 1 || expectedRevision !== current.revision) throw new PolicyError("revision", "expected policy revision conflicts");
      if (!changes || typeof changes !== "object" || Array.isArray(changes)) throw new TypeError("runtime policy update object required");
      // Update input is bounded to this single implemented dynamic field.
      const keys = Reflect.ownKeys(changes);
      if (keys.length !== 1 || keys[0] !== "LOG_LEVEL") throw new PolicyError(typeof keys[0] === "string" ? keys[0] : "update", "only LOG_LEVEL is dynamically mutable");
      if (!diagnostics || !current.fields.LOG_LEVEL) throw new PolicyError("LOG_LEVEL", "setting is not enforced by selected capabilities");
      if (diagnostics.status().state !== "running") throw new PolicyError("LOG_LEVEL", "diagnostics owner is not running");
      if (current.revision === Number.MAX_SAFE_INTEGER) throw new PolicyError("revision", "policy revision exhausted");
      const descriptor = Object.getOwnPropertyDescriptor(changes, "LOG_LEVEL");
      if (!Object.hasOwn(descriptor, "value")) throw new PolicyError("LOG_LEVEL", "update requires an own data value");
      if (typeof descriptor.value !== "string") throw new PolicyError("LOG_LEVEL", "exact enum string required");
      const value = parse(known.get("LOG_LEVEL"), descriptor.value);
      // A local Proxy can execute code during introspection; it must not turn
      // reentry into an unchecked second update with the same expected revision.
      if (current !== prior) throw new PolicyError("revision", "expected policy revision conflicts");
      const next = new Map(Object.entries(current.fields));
      next.set("LOG_LEVEL", { ...current.fields.LOG_LEVEL, value, source: "administrative" });
      // Atomic in one owner event loop: no await/callback can interleave this
      // compare-and-swap, and the shared sink reads this same current owner.
      current = snapshot(current.revision + 1, next);
      return current;
    },
  });
  if (diagnostics) diagnostics._bindPolicy(owner);
  return owner;
}
function policyOptions(options, mapping) {
  const out = { ...options };
  if (!options.policy) return out;
  if (!Number.isSafeInteger(options.policy.revision) || options.policy.revision < 1 || !options.policy.fields) throw new TypeError("resolved policy required");
  for (const [field, option] of Object.entries(mapping)) {
    const entry = options.policy.fields[field];
    if (!entry) continue;
    if (options[option] != null && options[option] !== entry.value) throw new PolicyError(field, `conflicts with ${option}`);
    out[option] = entry.value;
  }
  return out;
}
module.exports = { resolvePolicy, PolicyError, policyOptions };
