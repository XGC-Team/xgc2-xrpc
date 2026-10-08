"use strict";
const registry = require("./runtime-policy.json");
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
function resolvePolicy({ environment, defaults = {}, ceilings = {}, capabilities = supported } = {}) {
  if (environment == null || typeof environment !== "object" || Array.isArray(environment)) throw new TypeError("explicit environment snapshot required");
  const enabled = new Set(capabilities);
  for (const capability of enabled) if (!supported.includes(capability)) throw new PolicyError(capability, "SDK capability is not implemented");
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
  const fields = Object.freeze(Object.fromEntries([...values].map(([name, entry]) => [name, Object.freeze(entry)])));
  return Object.freeze({ revision: 1, fields, effective() { return { revision: 1, fields }; } });
}
function policyOptions(options, mapping) {
  const out = { ...options };
  if (!options.policy) return out;
  if (options.policy.revision !== 1 || !options.policy.fields) throw new TypeError("resolved policy required");
  for (const [field, option] of Object.entries(mapping)) {
    const entry = options.policy.fields[field];
    if (!entry) continue;
    if (options[option] != null && options[option] !== entry.value) throw new PolicyError(field, `conflicts with ${option}`);
    out[option] = entry.value;
  }
  return out;
}
module.exports = { resolvePolicy, PolicyError, policyOptions };
