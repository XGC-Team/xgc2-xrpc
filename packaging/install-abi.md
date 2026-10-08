# XRPC package and deployment ABI

`tools/build-packages.py` snapshots its inputs before invoking native builders.
It writes only to a new directory outside the checkout. The default build includes
all five languages and C++ gRPC. A selected language or HTTP-only build is an
explicit partial validation, not complete release coverage. Builds and installation
checks run offline with at most two native build jobs.

`.xgc2/product.yml` owns Debian package identity and suite-specific revisions.
`packaging/runtime-abi.json` owns the installation checker platform ceilings.
Language versions, dependencies and public APIs remain in their language manifests.

## Artifacts

| Artifact | Installation interface |
| --- | --- |
| `libxgc2-xrpc1` | HTTP, policy, diagnostics and Unix shared libraries, SONAME 1; generated Debian shlibs dependencies |
| `libxgc2-xrpc-dev` | Headers, unversioned linker symlinks, relocatable CMake exports; exact-version runtime dependency |
| `libxgc2-xrpc-grpc1` | Optional gRPC shared library, SONAME 1; generated Debian runtime dependencies |
| `libxgc2-xrpc-grpc-dev` | gRPC header and CMake component; exact base development/runtime dependencies and native gRPC/protobuf development dependencies |
| Python wheel | An explicitly selected Python >=3.10 virtual environment; offline native dependency wheels |
| Go source archive | Standalone module with embedded policy assets; native Go module consumer |
| Rust crate | Standalone Cargo package with policy assets and optional gRPC feature |
| Node npm tarball | Native npm installation with the declared `ws` dependency |
| `node-xgc2-xrpc` | Architecture `all` library from that installed npm artifact; `/usr/lib/xgc2/node_modules/@xgc2/xrpc` with private pinned `ws` |

HTTP packages do not depend on gRPC. Development files and versioned runtime ELF
files have disjoint owners. A real installed CMake consumer invokes the shared
policy and transport API; its C++20 probe executes span and stop-token-aware
condition-variable waiting. The installation checker records actual loader
resolution and rejects missing libraries, absolute build RPATHs, wrong architecture
and ELF glibc requirements above the declared target suite.

The wheel check installs into a fresh venv with `--no-index`, checks dependencies,
imports with `-I`, resolves the installed registry and verifies native aiohttp/gRPC
capabilities. Removing the policy asset must fail. Go/Rust/Node consumers build
outside both the checkout and build snapshot, against installed archives only.
Their native dependency caches are preprovisioned; the checker never downloads
missing dependencies or installs toolchains.
The Node Deb contains the npm-installed SDK and complete private `ws` payload,
not a language source tree. Its identity receipt is
`/usr/share/xgc2/xrpc/node/install-identity.json`; it binds npm SHA256,
dependency integrity and every installed file hash. The consumer supplies a
controlled Node >=20 interpreter (for example Lichtblick's embedded Node).
The library package depends on `ca-certificates` and contains no interpreter,
launcher, lifecycle install script or target-side npm download. Ordinary
`require("@xgc2/xrpc")` from `/usr/lib/xgc2/lichtblick-web` resolves this shared
ancestor path. Other layouts must explicitly consume the installed library.
No source copy, module alias or global search path is an installation contract.

The Node Deb checker starts the installed diagnostics worker, awaits write ACK
and close, and verifies policy CAS. Missing worker, private `ws` and registry,
plus modified installed bytes, are negative controls. The separate
`tools/check-install-node-runtime.py` verifies actual `dpkg -i` and package-owned
paths in fresh offline containers. Node and Bun receipts are separate and name
their exact runtime binary hashes; a Node result does not establish Bun support.
Build each suite's architecture-independent package once and reuse those exact
bytes in both native architecture indexes.
`--cpp-install-profile http` validates the base subset of the same canonical
full-profile Debian bundle. `--python-install-profile http` validates a fresh venv
without the optional gRPC dependency; default profiles validate gRPC too.

`tools/check-install-runtime.py` performs actual `dpkg -i` in two fresh, offline,
resource-limited containers from an already cached immutable runtime image: a
positive base installation and a missing-runtime dependency negative. It records
ldconfig/loader selection and executes the installed consumer. It runs on the
target architecture's native Docker host and never starts a product service.

## Focal runtime

Focal remains a target. Its system GCC9 and Python3.8 are not SDK toolchains.
Select a controlled C++20 compiler explicitly and run the installed feature probe.
The Clang10/libstdc++10 route is applicable to the currently exercised HTTP feature
set; it does not establish the native gRPC route. GNU builds require GCC >=11 as
declared by the SDK. ELF version requirements and actual runtime loader selection
are checked separately from compiler version.

Use an explicitly provisioned modern Python, with the 3.12 deployment line, instead
of replacing `/usr/bin/python3`. The controlled Focal builder's managed Python is
a build resource, not proof of runtime-image deployment. Pin its exact release,
archive hash and architecture, plus all native wheel hashes, in the owning images
build/runtime layer. The same interpreter and module capability checks apply in
the deployment image. No leaf PPA, system-Python replacement or dependency download
is an installation fallback.

The native gRPC route requires >=1.51 and matching protobuf/Abseil dependencies
built for the target glibc. Focal's archive gRPC1.16 cannot satisfy that route.
Never relabel Noble-built ELF as Focal. Controlled images must supply the compiler,
standard library and gRPC/protobuf together for both amd64 and arm64. A Gazebo or
adapter process loading C++ plugins must use one compatible libstdc++ selection
from process startup; an SDK `$ORIGIN` RPATH alone does not select the process's
standard library. Static libstdc++ copies are not a substitute for this check.
[GCC ABI policy](https://gcc.gnu.org/onlinedocs/libstdc++/manual/abi.html) describes
the standard-library symbol version boundary. [Managed Python documentation](https://docs.astral.sh/uv/concepts/python-versions/)
describes the interpreter distribution used by the controlled builder.

## Local validation and release evidence

Inside the controlled target-suite builder, with source readonly and a dedicated
writable scratch mount:

```bash
python3 tools/build-packages.py --output /scratch/packages \
  --distribution focal --architecture amd64 \
  --cxx /path/to/controlled/c++ --python /path/to/managed/python
python3 tools/check-install.py --artifacts /scratch/packages \
  --work-dir /scratch/install --distribution focal --architecture amd64 \
  --cxx /path/to/controlled/c++ --python /path/to/managed/python \
  --wheelhouse /preprovisioned/wheels --negative-controls
```

`package-evidence.json` binds input file hashes, platform, selected languages,
artifact hashes and ELF requirements. `install-evidence.json` binds the exact
package receipt and records checks and negative controls. These local receipts
are explicitly untrusted release evidence. They never impersonate a successful
push CI run or a clean source SHA.
Local Debian probe revisions include a source-snapshot hash, so HTTP-only and
full-profile probes cannot reuse a published package/version/architecture identity.
`--release-source-sha` requires all SDKs plus C++ gRPC, a clean exact committed
checkout before and after the build, and provisioned image dependencies. Its
output is a release candidate, still not trusted push evidence.
For a local consumer probe against an already published npm artifact, the
builder accepts the complete tuple `--node-tarball`, `--node-tarball-sha256`
and `--node-source-sha`. It checks every tar payload file against that commit's
Git blob and reads its exact dependency lock. It does not identify a dirty
working tree as that commit. This override cannot be used with release mode.

Cloud workflow identity, runner, triggers, immutable builder digest, artifact
budget and execution authorization must be established before enabling a new CI.
No automatic cloud workflow is introduced while those prerequisites are absent.
The existing application CI freeze is unchanged. The owning image pipeline
provides toolchains; the leaf only builds, installs, tests and emits strict
`xgc2.build-artifact.v1` artifacts from the final clean exact source. Ordinary
products use trusted push artifacts. Only the central release-scoped DAG can
dispatch a compatibility/release worker; it cannot replace missing push evidence.
Production APT and parent gitlinks are changed by the central release owner.
