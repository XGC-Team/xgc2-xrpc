"use strict";
const path = require("node:path");
const tls = require("node:tls");
const fs = require("node:fs");
const crypto = require("node:crypto");
const NAME = /^[A-Za-z0-9._:-]{1,128}$/;
function object(value, keys, required = keys) {
  if (value == null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("bootstrap object required");
  for (const key of Object.keys(value)) if (!keys.includes(key)) throw new TypeError(`unknown bootstrap field ${key}`);
  for (const key of required) if (!Object.hasOwn(value, key)) throw new TypeError(`missing bootstrap field ${key}`);
}
function name(value) { if (typeof value !== "string" || !NAME.test(value)) throw new TypeError("canonical bootstrap name required"); return value; }
function validateTrust(ca) {
  const bundles = Array.isArray(ca) ? ca : [ca];
  let bytes = 0;
  if (!bundles.length || bundles.length > 128) throw new TypeError("bounded certificate trust bundle required");
  for (const bundle of bundles) {
    if (typeof bundle !== "string" && !Buffer.isBuffer(bundle)) throw new TypeError("PEM certificate trust bundle required");
    bytes += Buffer.byteLength(bundle);
    if (bytes > 131072) throw new TypeError("trust bundle exceeds 128KiB");
    const pem = bundle.toString(), blocks = pem.match(/-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g);
    if (!blocks?.length || blocks.length > 128 || pem.replace(/-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g, "").trim()) throw new TypeError("valid PEM certificate trust bundle required");
    for (const block of blocks) new crypto.X509Certificate(block);
  }
}
function freezePayload(value, depth = 0) {
  if (depth > 32) throw new TypeError("application startup payload exceeds depth limit");
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) freezePayload(child, depth + 1);
    Object.freeze(value);
  }
  return value;
}
class BootstrapBinding {
  constructor(value) {
    object(value, ["schema_version", "target_id", "service", "api_version", "profile", "endpoint", "runtime_grant", "authentication", "secret_handles", "storage_grants"]);
    if (value.schema_version !== 1) throw new TypeError("unsupported bootstrap schema_version");
    for (const key of ["target_id", "service", "api_version", "runtime_grant"]) name(value[key]);
    if (!["http.v1", "grpc.v1"].includes(value.profile)) throw new TypeError("unsupported bootstrap profile");
    object(value.endpoint, ["kind", "address"]);
    const endpoint = value.endpoint;
    if (typeof endpoint.address !== "string" || !endpoint.address || endpoint.address.length > 2048 || /[\x00-\x20\x7f]/.test(endpoint.address)) throw new TypeError("canonical bootstrap endpoint required");
    if (endpoint.kind === "unix") {
      if (!path.isAbsolute(endpoint.address) || path.normalize(endpoint.address) !== endpoint.address || Buffer.byteLength(endpoint.address) >= 108 || value.authentication !== "local_private") throw new TypeError("private canonical Unix bootstrap required");
    } else if (endpoint.kind === "https") {
      const url = new URL(endpoint.address);
      if (value.profile !== "http.v1" || url.protocol !== "https:" || !url.hostname || url.username || url.password || url.search || url.hash || url.pathname !== "/") throw new TypeError("HTTP bootstrap requires an HTTPS origin");
    } else if (endpoint.kind === "tls") {
      if (value.profile !== "grpc.v1" || !/^(?:\[[0-9a-fA-F:]+\]|[A-Za-z0-9.-]+):[1-9][0-9]{0,4}$/.test(endpoint.address) || Number(endpoint.address.slice(endpoint.address.lastIndexOf(":") + 1)) > 65535) throw new TypeError("gRPC bootstrap requires TLS host:port");
    } else throw new TypeError("unsupported bootstrap endpoint");
    if (!["local_private", "server_tls", "mutual_tls"].includes(value.authentication)) throw new TypeError("unsupported bootstrap authentication");
    object(value.secret_handles, ["tls_identity", "tls_trust", "authorization"], []);
    for (const handle of Object.values(value.secret_handles)) name(handle);
    if (endpoint.kind !== "unix") {
      if (value.authentication === "local_private") throw new TypeError("remote bootstrap requires authenticated TLS");
      for (const key of ["tls_identity", "tls_trust", "authorization"]) name(value.secret_handles[key]);
    }
    if (!Array.isArray(value.storage_grants) || value.storage_grants.length > 32 || new Set(value.storage_grants).size !== value.storage_grants.length) throw new TypeError("bounded unique storage grants required");
    value.storage_grants.forEach(name);
    this.schema_version = 1;
    for (const key of ["target_id", "service", "api_version", "profile", "runtime_grant", "authentication"]) this[key] = value[key];
    this.endpoint = Object.freeze({ ...endpoint });
    this.secret_handles = Object.freeze({ ...value.secret_handles });
    this.storage_grants = Object.freeze([...value.storage_grants]);
    Object.freeze(this);
  }
  serviceRef(instanceId) {
    name(instanceId);
    return Object.freeze({ target_id: this.target_id, service: this.service, api_version: this.api_version,
      profile: this.profile, instance_id: instanceId, endpoint: this.endpoint });
  }
  resolveCredentials(resolve, role) {
    if (typeof resolve !== "function" || !["server", "client"].includes(role)) throw new TypeError("explicit grant resolver and role required");
    if (this.endpoint.kind === "unix") throw new TypeError("local private bindings carry no TLS grants; host them with the unixPath option");
    const identity = resolve(this.secret_handles.tls_identity, "tls_identity");
    const trust = resolve(this.secret_handles.tls_trust, "tls_trust");
    const authorization = resolve(this.secret_handles.authorization, "authorization");
    if (!identity?.cert || !identity?.key || !trust?.ca || !authorization) throw new TypeError("unresolved bootstrap credentials");
    if (role === "server" && typeof authorization.authorize !== "function") throw new TypeError("server authorization grant requires a verifier");
    if (role === "client" && (authorization.headers == null || typeof authorization.headers !== "object" || Array.isArray(authorization.headers))) throw new TypeError("client authorization grant requires explicit headers");
    const options = { cert: identity.cert, key: identity.key, ca: trust.ca, minVersion: "TLSv1.2", rejectUnauthorized: true };
    if (role === "server") options.requestCert = this.authentication === "mutual_tls";
    // Native parsing validates PEM/material before a listener or pool is opened.
    validateTrust(options.ca);
    tls.createSecureContext(options);
    return Object.freeze({ tls: Object.freeze(options), authorization });
  }
}
function readPrivateFile(filePath, maximum) {
  if (process.platform !== "linux" || typeof filePath !== "string" || !path.isAbsolute(filePath) || path.normalize(filePath) !== filePath) throw new TypeError("canonical explicitly granted Linux bootstrap file required");
  const parts = filePath.split("/").filter(Boolean), filename = parts.pop();
  let directory, file;
  try {
    directory = fs.openSync("/", fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    for (const component of parts) {
      const next = fs.openSync(`/proc/self/fd/${directory}/${component}`, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
      fs.closeSync(directory); directory = next;
    }
    const parent = fs.fstatSync(directory), uid = process.geteuid();
    if (parent.uid !== uid || (parent.mode & 0o777) !== 0o700) throw new TypeError("bootstrap parent must be owned mode0700");
    file = fs.openSync(`/proc/self/fd/${directory}/${filename}`, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
    const stat = fs.fstatSync(file);
    if (!stat.isFile() || stat.uid !== uid || stat.nlink !== 1 || (stat.mode & 0o777) !== 0o600 || stat.size > maximum) throw new TypeError(`grant must be an owned single-link mode0600 regular file within ${maximum === 16384 ? "16KiB" : maximum + " bytes"}`);
    const bytes = Buffer.alloc(maximum + 1);
    let count = 0, read;
    while (count < bytes.length && (read = fs.readSync(file, bytes, count, bytes.length - count, count)) !== 0) count += read;
    if (count > maximum) throw new TypeError("grant exceeds its byte limit");
    return bytes.subarray(0, count);
  } finally {
    if (file != null) fs.closeSync(file);
    if (directory != null) fs.closeSync(directory);
  }
}
function readBootstrapBinding(filePath) {
  return new BootstrapBinding(JSON.parse(readPrivateFile(filePath, 16384).toString("utf8")));
}

// One explicit input from the process owner. Resolve only named grants in this
// document, once at startup; never search directories or read ambient env vars.
function loadBootstrapInput(filePath, { role } = {}) {
  if (!["server", "client"].includes(role)) throw new TypeError("explicit bootstrap role required");
  const input = JSON.parse(readPrivateFile(filePath, 16384).toString("utf8"));
  object(input, ["schema_version", "binding", "grants", "application"], ["schema_version", "binding", "grants"]);
  if (input.schema_version !== 1) throw new TypeError("unsupported bootstrap input schema_version");
  const binding = new BootstrapBinding(input.binding);
  if (binding.endpoint.kind === "unix") throw new TypeError("Node startup input requires authenticated TLS binding");
  const purposes = Object.entries(binding.secret_handles);
  if (!input.grants || typeof input.grants !== "object" || Array.isArray(input.grants) || Object.keys(input.grants).length > 32) throw new TypeError("bounded named bootstrap grants required");
  for (const [, handle] of purposes) if (!Object.hasOwn(input.grants, handle)) throw new TypeError("missing bootstrap secret grant");
  if (new Set(purposes.map(([, handle]) => handle)).size !== purposes.length) throw new TypeError("bootstrap secret handles must be distinct");
  const grants = new Map();
  for (const [handle, descriptor] of Object.entries(input.grants)) {
    name(handle);
    const purpose = descriptor?.kind === "bearer" ? "authorization" : descriptor?.kind;
    let grant;
    if (purpose === "tls_identity") {
      object(descriptor, ["kind", "cert_file", "key_file"]);
      if (descriptor.kind !== "tls_identity") throw new TypeError("grant purpose mismatch");
      grant = Object.freeze({ cert: readPrivateFile(descriptor.cert_file, 131072), key: readPrivateFile(descriptor.key_file, 65536) });
      tls.createSecureContext(grant);
    } else if (purpose === "tls_trust") {
      object(descriptor, ["kind", "ca_file"]);
      if (descriptor.kind !== "tls_trust") throw new TypeError("grant purpose mismatch");
      grant = Object.freeze({ ca: readPrivateFile(descriptor.ca_file, 131072) });
      validateTrust(grant.ca);
    } else {
      object(descriptor, ["kind", "token_file"]);
      if (descriptor.kind !== "bearer") throw new TypeError("unsupported authorization grant");
      const token = readPrivateFile(descriptor.token_file, 1024).toString("utf8");
      if (!/^[A-Za-z0-9._~+\/-]+={0,}$/.test(token)) throw new TypeError("canonical bounded bearer token required");
      {
        const digest = crypto.createHash("sha256").update(`Bearer ${token}`).digest();
        grant = Object.freeze({ headers: Object.freeze({ authorization: `Bearer ${token}` }), authorize(request) {
          const header = request.headers.authorization;
          if (typeof header !== "string" || header.length > 1031) return false;
          let count = 0;
          for (let index = 0; index < request.rawHeaders.length; index += 2) if (request.rawHeaders[index].toLowerCase() === "authorization") count++;
          return count === 1 && crypto.timingSafeEqual(digest, crypto.createHash("sha256").update(header).digest());
        } });
      }
    }
    grants.set(handle, { purpose, grant });
  }
  const resolveGrant = (handle, purpose) => {
    const entry = grants.get(handle);
    if (!entry || entry.purpose !== purpose) throw new TypeError("ungranted bootstrap handle or purpose");
    return entry.grant;
  };
  // Fail native certificate/key parsing before returning a usable startup input.
  binding.resolveCredentials(resolveGrant, role);
  return Object.freeze({ binding, resolveGrant, application: freezePayload(input.application) });
}
module.exports = { BootstrapBinding, readBootstrapBinding, loadBootstrapInput };
