#!/usr/bin/env bash
# Run a command of this checkout in a build image, as the runner's own user so that
# nothing in the workspace becomes root-owned: in-image.sh IMAGE command [argument...]
# The command runs in /work (the checkout); /out is the runner's temporary directory.
set -euo pipefail
image="${1:?image}"
shift
exec docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -e GITHUB_SHA -e GITHUB_RUN_ID -e GITHUB_EVENT_NAME \
  -e GITHUB_REPOSITORY -e GITHUB_WORKFLOW_REF \
  -e RUNNER_TEMP=/out \
  -v "${GITHUB_WORKSPACE:-$PWD}:/work" -v "${RUNNER_TEMP:-/tmp}:/out" -w /work \
  "$image" "$@"
