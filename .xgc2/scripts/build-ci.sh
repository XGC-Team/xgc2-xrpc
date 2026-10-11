#!/usr/bin/env bash
# Build the Debian packages and language artifacts of one suite from the checked-out
# commit, check that they install, and (on push) emit the build manifest. Runs inside
# the suite's build image; see .github/workflows/ci.yml. Ubuntu 18.04 (bionic) builds
# the udp.v1 C++ library only, with its stock CMake 3.10 and Python 3.6. The image's
# preprovisioned npm cache and Python wheelhouse are named by XGC2_NPM_CACHE_SEED and
# XGC2_WHEELHOUSE (defaults below); PYTHON picks the interpreter of the packaging tools.
set -euo pipefail
suite="${1:?suite}"
arch="$(dpkg --print-architecture)"
root="$(pwd)"
scratch="${RUNNER_TEMP:?}/xrpc-${suite}-${arch}"
mkdir -p "$scratch" "$root/.ci/artifacts"
check_args=()
if [[ "$suite" != bionic ]]; then
  export CARGO_HOME="$scratch/cargo"
  export GOMODCACHE="$scratch/go-mod"
  export GOCACHE="$scratch/go-build"
  export GOMAXPROCS=2
  export npm_config_cache="$scratch/npm-cache"
  mkdir -p "$CARGO_HOME" "$GOMODCACHE" "$GOCACHE"
  cp -a "${XGC2_NPM_CACHE_SEED:-/opt/xgc2/npm-cache}" "$npm_config_cache"
  # Resolve only the SDK's committed native language locks before the offline
  # package and installed-consumer steps. Toolchains remain image-owned.
  cargo fetch --locked --manifest-path rust/Cargo.toml
  mapfile -t go_modules < <(awk '$2 !~ /\/go.mod$/ {print $1 "@" $2}' go/go.sum)
  (cd go && go mod download "${go_modules[@]}")
  # The TypeScript compiler comes from its lock into the npm cache, so that the offline
  # build finds it. This runs in a copy: the checkout stays exactly the committed source.
  mkdir -p "$scratch/ts-lock"
  cp ts/package.json ts/package-lock.json "$scratch/ts-lock/"
  (cd "$scratch/ts-lock" && npm ci --ignore-scripts --no-audit --no-fund)
  check_args+=(--wheelhouse "${XGC2_WHEELHOUSE:-/opt/xgc2/python-xrpc-wheels}")
fi
SOURCE_DATE_EPOCH="$(git show -s --format=%ct HEAD)"
export SOURCE_DATE_EPOCH
cxx=g++
if [[ "$suite" = focal ]]; then cxx=clang++-10; fi
"${PYTHON:-python3}" tools/build-packages.py --output "$scratch/packages" \
  --distribution "$suite" --architecture "$arch" --cxx "$cxx" \
  --release-source-sha "$GITHUB_SHA"
"${PYTHON:-python3}" tools/check-install.py --artifacts "$scratch/packages" \
  --work-dir "$scratch/installed" --distribution "$suite" \
  --architecture "$arch" --cxx "$cxx" --negative-controls "${check_args[@]}"
if [[ "${GITHUB_EVENT_NAME:-}" = push ]]; then
  python3 .xgc2/scripts/emit-build-artifact.py \
    --artifacts "$scratch/packages" \
    --install-receipt "$scratch/installed/install-evidence.json" \
    --output "$scratch/packages/build-manifest.json"
fi
find "$scratch/packages" -maxdepth 1 -type f \
  -exec cp '{}' "$root/.ci/artifacts/" ';'
cp "$scratch/installed/install-evidence.json" "$root/.ci/artifacts/"
