"use strict";
const fs = require("node:fs");
const net = require("node:net");
const path = require("node:path");

// Unix endpoint ownership for the Node host. Node has no flock, so exclusivity
// rests on the private runtime directory (owned by the effective user, mode
// 0700, created by the supervisor) plus a connect probe of any existing socket:
// refused -> stale, unlink and bind; accepted -> the address is in use.

const SUN_PATH_LIMIT = 108;

function checkUnixAddress(address) {
  if (typeof address !== "string" || !path.isAbsolute(address) || path.normalize(address) !== address
    || address.endsWith("/") || [".", ".."].includes(path.basename(address))
    || /[\x00\r\n]/.test(address) || Buffer.byteLength(address) >= SUN_PATH_LIMIT) {
    throw new TypeError("canonical absolute Unix socket path shorter than 108 bytes required");
  }
}

function inUse(address, reason) {
  const error = new Error(`listen EADDRINUSE: ${reason}: ${address}`);
  error.code = "EADDRINUSE";
  error.address = address;
  return error;
}

// Walk from / without following symlinks; only the final directory must be
// private. Returns a descriptor that pins it, so a later rename cannot redirect
// the bind. /proc/self/fd/N/name addresses entries relative to that pin.
function openPrivateDirectory(components) {
  const flags = fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW;
  let directory = fs.openSync("/", fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
  try {
    for (const component of components) {
      const next = fs.openSync(`/proc/self/fd/${directory}/${component}`, flags);
      fs.closeSync(directory);
      directory = next;
    }
    const stat = fs.fstatSync(directory);
    if (stat.uid !== process.geteuid() || (stat.mode & 0o7777) !== 0o700) {
      throw new TypeError("runtime directory must be owned by the effective user with mode 0700");
    }
    return directory;
  } catch (error) {
    fs.closeSync(directory);
    throw error;
  }
}

class UnixEndpoint {
  #descriptor;
  #name;
  #identity = null;

  constructor(address, descriptor, name) {
    this.address = address;
    this.#descriptor = descriptor;
    this.#name = name;
  }

  // Path to bind and unlink, relative to the pinned directory.
  get bindPath() { return `/proc/self/fd/${this.#descriptor}/${this.#name}`; }

  static async reserve(address, { probeTimeoutMs = 250 } = {}) {
    if (process.platform !== "linux") throw new TypeError("Unix endpoints require Linux");
    checkUnixAddress(address);
    const components = address.split("/").filter(Boolean);
    const name = components.pop();
    const endpoint = new UnixEndpoint(address, openPrivateDirectory(components), name);
    try {
      if (Buffer.byteLength(endpoint.bindPath) >= SUN_PATH_LIMIT) throw new RangeError("socket name is too long to bind relative to its directory");
      await endpoint.#reclaim(probeTimeoutMs);
      return endpoint;
    } catch (error) {
      endpoint.release();
      throw error;
    }
  }

  async #reclaim(probeTimeoutMs) {
    let before;
    try { before = fs.lstatSync(this.bindPath, { bigint: true }); }
    catch (error) { if (error.code === "ENOENT") return; throw error; }
    if (!before.isSocket()) throw inUse(this.address, "path exists and is not a socket");
    await new Promise((resolve, reject) => {
      const probe = net.connect({ path: this.bindPath });
      const timer = setTimeout(() => { probe.destroy(); reject(inUse(this.address, "existing endpoint is not provably unreachable")); }, probeTimeoutMs);
      probe.once("connect", () => { clearTimeout(timer); probe.destroy(); reject(inUse(this.address, "address in use")); });
      probe.once("error", (error) => {
        clearTimeout(timer);
        probe.destroy();
        if (error.code === "ECONNREFUSED") resolve();
        else reject(inUse(this.address, `existing endpoint is not provably unreachable (${error.code})`));
      });
    });
    const current = fs.lstatSync(this.bindPath, { bigint: true });
    if (current.dev !== before.dev || current.ino !== before.ino) throw inUse(this.address, "endpoint changed during the probe");
    fs.unlinkSync(this.bindPath);
  }

  // The directory is private, so the socket is unreachable to others until the
  // mode is tightened here.
  recordBound() {
    const stat = fs.lstatSync(this.bindPath, { bigint: true });
    if (!stat.isSocket()) throw new Error("bound endpoint is not a socket");
    this.#identity = { dev: stat.dev, ino: stat.ino };
    fs.chmodSync(this.bindPath, 0o600);
  }

  // Unlink only the socket this endpoint bound, then drop the directory pin.
  release() {
    if (this.#descriptor == null) return;
    try {
      if (this.#identity) {
        let current;
        try { current = fs.lstatSync(this.bindPath, { bigint: true }); }
        catch (error) { if (error.code !== "ENOENT") throw error; }
        if (current?.isSocket() && current.dev === this.#identity.dev && current.ino === this.#identity.ino) fs.unlinkSync(this.bindPath);
        this.#identity = null;
      }
    } finally {
      fs.closeSync(this.#descriptor);
      this.#descriptor = null;
    }
  }
}

module.exports = { UnixEndpoint, checkUnixAddress };
