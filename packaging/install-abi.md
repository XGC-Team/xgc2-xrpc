# XRPC package and deployment ABI

`tools/build-packages.py` snapshots its inputs before invoking native builders. It writes
only to a new directory outside the checkout. The default build includes every language and
every C++ component the target distribution ships (see below); a selected language or C++
component subset is an explicit partial validation, not complete release coverage. Builds
and installation checks run offline with at most two native build jobs.

`.xgc2/product.yml` owns Debian package identity, the distributions and suite-specific
revisions. `packaging/runtime-abi.json` owns the per-distribution package set (languages and
C++ components), the glibc ceiling and the SOVERSION generation of each library. Language
versions, dependencies and public APIs remain in their language manifests. One product
version (0.2.0) is the version of the CMake project, the Python wheel, the Rust crate, both
npm packages and every Debian package (`0.2.0-1~<dist>`); the Go module is versioned by the
tag `go/v0.2.0` of this repository (the module lives in `go/`).

## Distributions

| Distribution | What it ships |
| --- | --- |
| bionic (Ubuntu 18.04, amd64 and arm64) | the `udp` C++ component only: `libxgc2-xrpc-udp1`, `libxgc2-xrpc-udp-dev`. Its Boost is 1.65, below what the Beast components need, and no robot consumer uses them. Built with the stock g++ 7.5, CMake 3.10 and Python 3.6 of the image |
| focal, jammy, noble (amd64 and arm64) | every C++ component, the Python wheel, the Go source archive, the Rust crate and both npm packages with their Debian packages |

## Artifacts

| Artifact | Installation interface |
| --- | --- |
| `libxgc2-xrpc-udp1` | the `udp.v1` shared library (`libxgc2_xrpc_udp.so.1`); its dependencies are derived by `dpkg-shlibdeps` (libssl, libstdc++, libc) |
| `libxgc2-xrpc-udp-dev` | the root development package: the `udp` headers and the header-only method router (`method.hpp`, `method_udp.hpp`), the unversioned linker symlink, the CMake package files (`XgcXrpcConfig.cmake`, `XgcXrpcConfigVersion.cmake`) and the `udp` export. Exact-version runtime dependency |
| `libxgc2-xrpc2` | the `unix`, `diagnostics`, `bootstrap`, `http` and `json_http` shared libraries, SONAME 2; generated Debian shlibs dependencies |
| `libxgc2-xrpc-dev` | their headers (and `method_http.hpp`), linker symlinks and relocatable CMake exports; exact-version dependencies on `libxgc2-xrpc2` and `libxgc2-xrpc-udp-dev`, and on `nlohmann-json3-dev (>= 3.7)` |
| `libxgc2-xrpc-grpc2` | the optional gRPC shared library, SONAME 2; generated Debian runtime dependencies |
| `libxgc2-xrpc-grpc-dev` | the gRPC header and CMake component; exact-version dependency on `libxgc2-xrpc-dev` and `libxgc2-xrpc-grpc2`, and the native gRPC and protobuf development packages |
| Python wheel | an explicitly selected Python >=3.8 virtual environment; offline native dependency wheels, no extras |
| Go source archive | standalone module `github.com/XGC-Team/xgc2-xrpc/go` with the `httpx`, `grpcx`, `udpx` and `unix` packages |
| Rust crate | standalone Cargo package with the `http.v1` host and client and the C ABI |
| Node npm tarball | `@xgc2/xrpc` with the declared `ws` dependency |
| `node-xgc2-xrpc` | Architecture `all` library from that installed npm artifact: `/usr/lib/xgc2/node_modules/@xgc2/xrpc` with private pinned `ws` |
| TypeScript npm tarball | `@xgc2/xrpc-client`: the compiled ESM client for browsers and Node, no dependencies. The tarball is built from `ts/` with the locked compiler (`npm ci`, `npm run build`, `npm pack`, no lifecycle script) |
| `node-xgc2-xrpc-client` | Architecture `all` library from that installed npm artifact: `/usr/lib/xgc2/node_modules/@xgc2/xrpc-client` |

The CMake build installs every file under its component name (`unix`, `diagnostics`,
`bootstrap`, `http`, `json_http`, `grpc`, `udp`; the package files under `config`) and the
builder splits one build into packages with `cmake -DCMAKE_INSTALL_COMPONENT=<name> -P
cmake_install.cmake`, which also works with CMake 3.10. A file that no package takes fails
the build. Development files and versioned runtime ELF files have disjoint owners.

### SONAME policy

`SOVERSION` is the ABI generation of one library, not the product version: it changes when a
release breaks that library's C++ ABI, and the runtime package name carries it. Release 0.2.0
broke the ABI of every library that 0.1.0 shipped (removed and changed interfaces, no policy
library), so those libraries are generation 2 (`libxgc2-xrpc2`, `libxgc2-xrpc-grpc2`); the
`udp` library is new in 0.2.0 and is generation 1 (`libxgc2-xrpc-udp1`). Packages built
against 0.1.0 depend on `libxgc2-xrpc1`, which stays installable beside the new ones.

## What the checks establish

HTTP and gRPC packages do not depend on each other beyond the CMake component graph. A real
installed CMake consumer is configured as a project that asks for the oldest policies in use
(`cmake_minimum_required(VERSION 3.0.2)`, as catkin packages on ROS Melodic and Noetic do),
compiled as C++17 and run: `packaging/probes/cpp/udp_consumer.cpp` serves a method through
`MethodRouter` on a `udp.v1` server and calls it with the client, and `consumer.cpp` (where
the suite ships `json_http`, optionally with gRPC) parses and answers JSON through the
router's `http.v1` adapter and exercises the stop-token wait. The installation checker
records actual loader resolution and rejects missing libraries, absolute build RPATHs, wrong
architecture and ELF glibc requirements above the declared target suite.

The wheel check installs into a fresh venv with `--no-index`, checks dependencies, imports
with `-I` and makes a real Unix-socket round trip with the installed code; removing a module
must fail. Go, Rust and Node consumers build outside both the checkout and the build snapshot,
against installed archives only (the Go consumer starts a `udp.v1` server and calls it through
`Dispatcher.CallMethod`). Their native dependency caches are preprovisioned; the checker never
downloads missing dependencies or installs toolchains. The TypeScript client is installed from
its tarball and exercised against a local HTTP server: a call, an error answer and a resumed
event stream.

The two Node Debs contain the npm-installed SDK, not a language source tree. Their identity
receipts are `/usr/share/xgc2/xrpc/node/install-identity.json` and
`/usr/share/xgc2/xrpc/ts/install-identity.json`; each binds the npm SHA256, the dependency
integrity (for `ws`) and every installed file hash. The consumer supplies a controlled Node >=20
interpreter (for example Lichtblick's embedded Node). A library package depends on
`ca-certificates` and contains no interpreter, launcher, lifecycle install script or
target-side npm download. Ordinary `require("@xgc2/xrpc")` from `/usr/lib/xgc2/lichtblick-web`
resolves the shared ancestor path, and `import "@xgc2/xrpc-client"` resolves it from any
directory below `/usr/lib/xgc2`. Other layouts must explicitly consume the installed library.
No source copy, module alias or global search path is an installation contract.

The Node Deb checker starts the installed diagnostics worker, awaits write ACK and close and
makes a fenced call over a private Unix socket; missing worker, private `ws` and Unix host, and
modified installed bytes, are negative controls. The TypeScript Deb has its own negative
controls (a missing module, modified bytes). `tools/check-install-node-runtime.py --sdk node|ts`
verifies actual `dpkg -i` and package-owned paths in fresh offline containers. Node and Bun
receipts are separate and name their exact runtime binary hashes; a Node result does not
establish Bun support. Build each suite's architecture-independent packages once and reuse
those exact bytes in both native architecture indexes.

`tools/check-install-runtime.py` performs actual `dpkg -i` in two fresh, offline,
resource-limited containers from an already cached immutable runtime image: a positive base
installation (every C++ package of the suite except the gRPC pair) and a missing-runtime
dependency negative. It records ldconfig/loader selection and executes the installed consumers
(`--installed-consumer` for `json_http`, `--installed-udp-consumer`). It runs on the target
architecture's native Docker host and never starts a product service. `--dry-run` prints the
container programs without Docker.

## Language floors

Python 3.8 syntax with pinned compatible aiohttp 3.10.11, httpx 0.28.1 and httpcore 1.0.9; the
owning build/runtime images provide the same module lock and capability checks, and products
do not replace the interpreter or download these dependencies at startup. Go 1.24, Rust 1.85,
Node 20 (browsers and Node for the TypeScript client).

C++ is C++17. GNU 7.5 builds `udp`; the Boost.Beast components (`http`, `json_http`,
`bootstrap`, `diagnostics`, `unix`) and `grpc` need GNU 9 and clang 10 or newer, Boost 1.70 and
gRPC 1.16. The controlled Clang10/libstdc++10 route supports the distribution gRPC 1.16.1 and
Protobuf 3.6.1 via pkg-config. ELF version requirements and actual loader selection are checked
separately from compiler version. Never relabel Noble ELF as Focal or Focal ELF as Bionic: each
distribution is built on its own image. A process loading C++ plugins must select one
compatible standard-library runtime from startup; an SDK `$ORIGIN` RPATH does not select the
process's libstdc++.

## Local validation and release evidence

Inside the controlled target-suite builder, with source readonly and a dedicated writable
scratch mount:

```bash
python3 tools/build-packages.py --output /scratch/packages \
  --distribution focal --architecture amd64 \
  --cxx /path/to/controlled/c++ --python /usr/bin/python3
python3 tools/check-install.py --artifacts /scratch/packages \
  --work-dir /scratch/install --distribution focal --architecture amd64 \
  --cxx /path/to/controlled/c++ --python /usr/bin/python3 \
  --wheelhouse /preprovisioned/wheels --negative-controls
```

`package-evidence.json` binds input file hashes, platform, selected languages and C++
components, artifact hashes and ELF requirements. `install-evidence.json` binds the exact
package receipt and records checks and negative controls. These local receipts are explicitly
untrusted release evidence. They never impersonate a successful push CI run or a clean source
SHA. Local Debian probe revisions include a source-snapshot hash, so partial and full probes
cannot reuse a published package/version/architecture identity. `--release-source-sha` requires
every language and C++ component the distribution ships, a clean exact committed checkout before
and after the build, and provisioned image dependencies. Its output is a release candidate,
still not trusted push evidence. For a local consumer probe against an already published npm
artifact, the builder accepts the complete tuple `--node-tarball`, `--node-tarball-sha256` and
`--node-source-sha`. It checks every tar payload file against that commit's Git blob and reads
its exact dependency lock. It does not identify a dirty working tree as that commit. This
override cannot be used with release mode.

CI (`.github/workflows/ci.yml`) builds every suite and architecture with `.xgc2/scripts/build-ci.sh`
in the owning build image, runs the installation checker with negative controls, and on push emits
the strict `xgc2.build-artifact.v1` manifest with `.xgc2/scripts/emit-build-artifact.py`; it also
runs every test suite (see `docs/capability-matrix.md`). Ordinary products use trusted push
artifacts. Only the central release-scoped DAG can dispatch a compatibility/release worker; it
cannot replace missing push evidence. Production APT and parent gitlinks are changed by the
central release owner.
