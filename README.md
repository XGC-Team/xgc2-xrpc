# XGC2 XRPC

XRPC is the communication layer of XGC2: how one process or host calls a function of
another. It owns the mechanics only (listening and dialing, framing, request correlation,
deadlines, cancellation, one error vocabulary, admission limits, drain, instance fencing and
the authentication of the carrier). Domains own their state, the meaning of readiness,
completion and trust, and every device effect. A capability is a function first; XRPC is the
thin adapter that lets another process call it, and a caller in the same process calls the
function directly.

Version 0.2.0. Apache-2.0 ([LICENSE](LICENSE)).

## Profiles

| Profile | Encoding | Carriers | Modes |
| --- | --- | --- | --- |
| `http.v1` | JSON over HTTP/1.1 | private Unix socket; TLS | unary; server event stream with resume |
| `grpc.v1` | Protobuf over HTTP/2 | Unix socket; TLS | unary; streaming; long-lived sessions (Go) |
| `udp.v1` | binary header and a JSON body, one datagram per request and per reply | UDP on a LAN | unary, authenticated with HMAC-SHA256 |

A callable operation is named `<service>/<Method>` and carries the same name on every profile
(`POST /v1/call/<service>/<Method>` on `http.v1`, the datagram's method on `udp.v1`, the native
full method on `grpc.v1`). Every client reports one of three dispositions (`not_sent`,
`outcome_unknown`, `response_received`) and nothing replays a mutation.

## Languages

| Directory | Package | What it provides |
| --- | --- | --- |
| [`go/`](go/README.md) | module `github.com/XGC-Team/xgc2-xrpc/go` (Go 1.24) | `http.v1`, `grpc.v1` (hosts, clients, sessions), `udp.v1` client and server and its fault proxy, event streams, method addressing, bootstrap input |
| [`cpp/`](cpp/README.md) | CMake project `XgcXrpc`, C++17 | one shared library per component: `unix`, `diagnostics`, `bootstrap`, `http`, `json_http`, `grpc`, `udp`; the header-only method router. `udp` builds on Ubuntu 18.04 |
| [`rust/`](rust/README.md) | crate `xgc2-xrpc` (Rust 1.85) | `http.v1` host and client over Unix sockets, endpoint ownership, a C ABI |
| [`python/`](python/README.md) | wheel `xgc2-xrpc` (Python 3.8) | `http.v1` host and client over Unix sockets, native aiohttp edges, HTTPS client |
| [`node/`](node/README.md) | npm `@xgc2/xrpc` (Node 20, CommonJS) | `http.v1` hosts (TCP, TLS, private Unix socket) and client |
| [`ts/`](ts/README.md) | npm `@xgc2/xrpc-client` (ESM) | `http.v1` calls and resumable event streams for browsers and Node, no dependencies |

Which language has which capability, for which consumer, and the test that shows it, is in the
[capability matrix](docs/capability-matrix.md); `python3 tools/check-matrix.py` checks it against
the tests in the tree. The contracts every SDK follows are in [`contracts/`](contracts):
[runtime](contracts/runtime.md) (profiles, service references, the instance fence, deadlines,
errors, dispositions, describe and method addressing), [udp.v1](contracts/udp-v1.md),
[event streams](contracts/events.md), [sessions](contracts/sessions.md),
[operations](contracts/operations.md), [bootstrap input](contracts/bootstrap.md) and the
[fault and interoperability tests](contracts/fault-conformance.md).

## Build and test

Limits are plain options with documented defaults; nothing reads the environment.

```sh
# Go
(cd go && go vet ./... && go test ./... && CGO_ENABLED=1 go test -race ./...)

# C++ (CMake 3.10 or newer; add ";grpc" to the components where gRPC 1.16 or newer is installed)
cmake -S . -B build -DXGC2_XRPC_COMPONENTS="unix;diagnostics;bootstrap;http;json_http;udp"
cmake --build build -j2 && (cd build && ctest --output-on-failure)
cmake -S . -B build-udp -DXGC2_XRPC_COMPONENTS=udp     # the udp.v1 library alone (g++ 7.5 is enough)

# Rust, Python, Node, TypeScript
(cd rust && cargo test --locked)
pip install 'aiohttp==3.10.11' 'httpx==0.28.1' 'httpcore==1.0.9' && PYTHONPATH=python python3 -m unittest discover -s python/tests
(cd node && npm ci && npm test)
(cd ts && npm ci && npm test)

# The capability matrix and the tool's own tests
python3 tools/check-matrix.py && python3 -m unittest discover -s tools -p 'test_*.py'
```

The cross-language tests start a C++ server and call it from Go; `.xgc2/scripts/ci-interop.sh`
builds the servers and runs them.

## Packaging and versions

One product version covers every package: the CMake project, the Python wheel, the Rust crate,
the npm packages and the Debian packages (`0.2.0-1~<distribution>`); the Go module is versioned
by the tag `go/v0.2.0` of this repository, and a release is the tag `v0.2.0`. Distributions:
Ubuntu 18.04 (bionic) gets the `udp.v1` C++ library only (`libxgc2-xrpc-udp1`,
`libxgc2-xrpc-udp-dev`); Ubuntu 20.04, 22.04 and 24.04 (focal, jammy, noble) get every C++
component, the Node and TypeScript libraries as Debian packages and the language artifacts.
[`packaging/install-abi.md`](packaging/install-abi.md) describes the packages, the SONAME policy
and the installation checks; `tools/build-packages.py` and `tools/check-install.py` build and
verify them offline. `.github/workflows/ci.yml` runs every test suite and builds the packages for
each distribution and architecture.
