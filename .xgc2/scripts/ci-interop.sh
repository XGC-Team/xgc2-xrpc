#!/usr/bin/env bash
# Build the C++ interoperability servers and run the Go tests that drive them:
# the udp.v1 server through the Go fault proxy, and one method router served on
# http.v1 (Unix socket) and udp.v1 and called through the Go Dispatcher.
# Needs CMake, a C++17 compiler with Boost and nlohmann-json, and Go with cgo for -race.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
build="${XRPC_BUILD_DIR:-$root/.ci/interop}"
rm -rf "$build"
mkdir -p "$build"
cd "$build"
read -r -a extra <<< "${XRPC_CMAKE_ARGS:-}"
cmake "$root" -DCMAKE_BUILD_TYPE=Release "-DXGC2_XRPC_COMPONENTS=unix;diagnostics;bootstrap;http;json_http;udp" "${extra[@]}"
for target in xgc2-xrpc-udp-interop-server xgc2-xrpc-method-interop-server; do
  cmake --build . --target "$target" -- "-j${XRPC_JOBS:-2}"
done
export XGC2_XRPC_UDP_INTEROP_SERVER="$build/cpp/xgc2-xrpc-udp-interop-server"
export XGC2_XRPC_METHOD_INTEROP_SERVER="$build/cpp/xgc2-xrpc-method-interop-server"
cd "$root/go"
export CGO_ENABLED=1
go test -race -count=1 -timeout 600s ./udpx -run 'TestInterop' -v
go test -race -count=1 -timeout 600s ./conformance -run 'TestMethodInterop' -v
