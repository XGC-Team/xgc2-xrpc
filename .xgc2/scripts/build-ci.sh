#!/usr/bin/env bash
set -euo pipefail
suite="${1:?suite}"
arch="$(dpkg --print-architecture)"
root="$(pwd)"
scratch="${RUNNER_TEMP:?}/xrpc-${suite}-${arch}"
mkdir -p "$scratch" "$root/.ci/artifacts"
export CARGO_HOME="$scratch/cargo"
export GOMODCACHE="$scratch/go-mod"
export GOCACHE="$scratch/go-build"
export GOMAXPROCS=2
export npm_config_cache="$scratch/npm-cache"
mkdir -p "$CARGO_HOME" "$GOMODCACHE" "$GOCACHE"
cp -a /opt/xgc2/npm-cache "$npm_config_cache"
# Resolve only the SDK's committed native language locks before the offline
# package and installed-consumer steps. Toolchains remain image-owned.
cargo fetch --locked --manifest-path rust/Cargo.toml
(cd go && go mod download)
export SOURCE_DATE_EPOCH="$(git show -s --format=%ct HEAD)"
cxx=g++
if [[ "$suite" = focal ]]; then cxx=clang++-10; fi
python3 tools/build-packages.py --output "$scratch/packages" \
  --distribution "$suite" --architecture "$arch" --cxx "$cxx" \
  --release-source-sha "$GITHUB_SHA"
python3 tools/check-install.py --artifacts "$scratch/packages" \
  --work-dir "$scratch/installed" --distribution "$suite" \
  --architecture "$arch" --cxx "$cxx" \
  --wheelhouse /opt/xgc2/python-xrpc-wheels
if [[ "${GITHUB_EVENT_NAME:-}" = push ]]; then
  python3 .xgc2/scripts/emit-build-artifact.py \
    --artifacts "$scratch/packages" \
    --install-receipt "$scratch/installed/install-evidence.json" \
    --output "$scratch/packages/build-manifest.json"
fi
find "$scratch/packages" -maxdepth 1 -type f \
  -exec cp '{}' "$root/.ci/artifacts/" ';'
cp "$scratch/installed/install-evidence.json" "$root/.ci/artifacts/"
