#!/usr/bin/env python3
"""Install HTTP Debs in fresh offline containers; never mutate a live runtime."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "packaging"))
from support import ABI, ROOT, digest, run, utc_now, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--installed-consumer", required=True, type=Path)
    parser.add_argument("--runtime-image", required=True, help="already cached immutable image ID or registry digest")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"(?:sha256:|[A-Za-z0-9._:/-]+@sha256:)[0-9a-f]{64}", args.runtime_image):
        raise ValueError("runtime image must use an immutable full digest")
    directory = args.artifacts.resolve()
    evidence_file = directory / "package-evidence.json"
    evidence = json.loads(evidence_file.read_text())
    consumer = args.installed_consumer.resolve()
    if args.output.exists() or args.output.resolve() == ROOT or ROOT in args.output.resolve().parents:
        raise ValueError("runtime receipt must be a new file outside the checkout")
    if evidence["architecture"] not in ABI["architectures"] or evidence["distribution"] not in ABI["distributions"]:
        raise ValueError("unsupported runtime platform cell")
    for item in evidence["artifacts"]:
        name = item["path"]
        if Path(name).name != name or digest(directory / name) != item["sha256"]:
            raise ValueError("runtime artifact digest mismatch")
    image = json.loads(run(["docker", "image", "inspect", args.runtime_image], capture=True))[0]
    if image["Architecture"] != evidence["architecture"]:
        raise ValueError("runtime image architecture mismatch")
    native = run(["docker", "info", "--format", "{{.Architecture}}"], capture=True)
    if {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(native, native) != evidence["architecture"]:
        raise ValueError("runtime ABI checks require the target architecture's native Docker runner")
    command = ["docker", "run", "--rm", "--network", "none", "--memory", "512m", "--cpus", "2", "--pids-limit", "128",
               "--mount", "type=bind,src=" + str(directory) + ",dst=/artifacts,readonly",
               "--mount", "type=bind,src=" + str(consumer) + ",dst=/probe/consumer,readonly",
               "--entrypoint", "/bin/bash", args.runtime_image, "-c"]
    positive = """set -euo pipefail
. /etc/os-release
test "$VERSION_CODENAME" = """ + evidence["distribution"] + """
dpkg -i /artifacts/libxgc2-xrpc1_*.deb /artifacts/libxgc2-xrpc-dev_*.deb
dpkg-query -W libxgc2-xrpc1 libxgc2-xrpc-dev
ldd /probe/consumer
/probe/consumer
"""
    positive_result = run(command + [positive], capture=True)
    for name in ("http", "unix", "policy", "diagnostics"):
        if not re.search(r"libxgc2_xrpc_" + name + r"\.so\.1\s+=>\s+/(?:usr/)?lib/", positive_result):
            raise ValueError("runtime consumer did not load installed " + name)
    negative = """set -euo pipefail
if dpkg -i /artifacts/libxgc2-xrpc-dev_*.deb; then
  echo 'missing-runtime installation incorrectly succeeded' >&2
  exit 1
fi
test "$(dpkg-query -W -f='${Status}' libxgc2-xrpc-dev)" = 'install ok unpacked'
"""
    negative_result = run(command + [negative], capture=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.output, {"schema": "xgc2.xrpc.runtime-install-evidence.v1", "created_at": utc_now(),
               "package_evidence_sha256": digest(evidence_file), "consumer_sha256": digest(consumer),
               "image_id": image["Id"], "image_digests": image.get("RepoDigests", []),
               "distribution": evidence["distribution"], "architecture": evidence["architecture"],
               "profile": "http", "positive": positive_result, "negative_missing_runtime": negative_result,
               "trusted_release": False})


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as error:
        sys.exit(str(error))
