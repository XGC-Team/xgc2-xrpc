#!/usr/bin/env bash
# Configure, build and test the C++ SDK in the current image:
#   ci-cpp.sh full [CXX]   every component, gRPC included (focal, jammy and noble images)
#   ci-cpp.sh udp  [CXX]   the udp component alone (any image, Ubuntu 18.04 included)
# Only commands that CMake 3.10, the version of Ubuntu 18.04, knows are used. Extra CMake
# arguments come from XRPC_CMAKE_ARGS (a cross sysroot, for one), the build directory from
# XRPC_BUILD_DIR and the number of build jobs from XRPC_JOBS (default 2).
set -euo pipefail
mode="${1:?full or udp}"
cxx="${2:-}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
case "$mode" in
  full) components="unix;diagnostics;bootstrap;http;json_http;grpc;udp" ;;
  udp) components="udp" ;;
  *) echo "usage: ci-cpp.sh full|udp [CXX]" >&2; exit 2 ;;
esac
build="${XRPC_BUILD_DIR:-$root/.ci/cpp-$mode-${cxx##*/}}"
rm -rf "$build"
mkdir -p "$build"
cd "$build"
args=("-DCMAKE_BUILD_TYPE=Release" "-DXGC2_XRPC_COMPONENTS=$components")
if [[ -n "$cxx" ]]; then args+=("-DCMAKE_CXX_COMPILER=$cxx"); fi
read -r -a extra <<< "${XRPC_CMAKE_ARGS:-}"
cmake "$root" "${args[@]}" "${extra[@]}"
cmake --build . -- "-j${XRPC_JOBS:-2}"
ctest --output-on-failure
