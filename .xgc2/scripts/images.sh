#!/usr/bin/env bash
# The build image of a suite, one place for every workflow job: images.sh bionic|focal|jammy|noble
# The Ubuntu 20.04 to 24.04 images are named by digest. The Ubuntu 18.04 image is named by its
# tag, because its digest has not been recorded yet (it needs a pull from the registry).
set -euo pipefail
case "${1:-}" in
  bionic) echo ghcr.io/xgc-team/xgc2-images/xgc2-build-bionic-dev:1.0.0 ;;
  focal) echo ghcr.io/xgc-team/xgc2-images/xgc2-build-focal-full-noetic@sha256:fce2d76fddf4f6439bf0a188249b731650febdc163befc360bed186b269d252a ;;
  jammy) echo ghcr.io/xgc-team/xgc2-images/xgc2-build-jammy-full-humble@sha256:94b723df918e4eb04c6d4ef4e4ae6ce7364deb8087909eb710b6847ee6ba7421 ;;
  noble) echo ghcr.io/xgc-team/xgc2-images/xgc2-build-noble-full-jazzy@sha256:1f9ba87ad4f6fd088493693b70d61e9987a65ce94fb36ee19e3a8e6dbc8829c9 ;;
  *) echo "usage: images.sh bionic|focal|jammy|noble" >&2; exit 2 ;;
esac
