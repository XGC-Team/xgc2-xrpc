#!/usr/bin/env python3
"""Install the C++ Debs in fresh offline containers; never mutate a live runtime.

A positive installation (every C++ package except the optional gRPC pair, whose build
dependencies the runtime image need not carry) with the installed consumers run, and a
negative one: a development package without its runtime must be refused.
"""
import argparse
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "packaging"))
from support import ABI, ROOT, cpp_package_set, digest, run, utc_now, write_json


def packages_to_install(evidence):
    """The packages of the base installation, in dependency order (udp first)."""
    return [p for p in cpp_package_set(evidence["cpp_components"]) if "grpc" not in p["components"]]


def scripts(evidence, consumers):
    """The shell programs of the positive and the negative container."""
    packages = packages_to_install(evidence)
    files = {item["package"]: item["path"] for item in evidence["artifacts"] if item["kind"] == "deb"}
    names = [name for p in packages for name in (p["runtime"], p["dev"])]
    debs = " ".join(shlex.quote("/artifacts/" + files[name]) for name in names)
    commands = ["set -euo pipefail", ". /etc/os-release", 'test "$VERSION_CODENAME" = ' + shlex.quote(evidence["distribution"]),
                "dpkg -i " + debs, "dpkg-query -W " + " ".join(names)]
    for consumer in consumers:
        commands += ["ldd /probe/" + consumer, "/probe/" + consumer]
    root = next(p for p in packages if p["root"])
    negative = ["set -euo pipefail",
                "if dpkg -i " + shlex.quote("/artifacts/" + files[root["dev"]]) + "; then",
                "  echo 'missing-runtime installation incorrectly succeeded' >&2",
                "  exit 1",
                "fi",
                "test \"$(dpkg-query -W -f='${Status}' " + root["dev"] + ")\" = 'install ok unpacked'"]
    return "\n".join(commands) + "\n", "\n".join(negative) + "\n", packages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--installed-consumer", type=Path,
                        help="the json_http consumer of packaging/probes/cpp (required where the suite ships http)")
    parser.add_argument("--installed-udp-consumer", required=True, type=Path, help="the udp consumer of packaging/probes/cpp")
    parser.add_argument("--runtime-image", required=True, help="already cached immutable image ID or registry digest")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true", help="print the container programs and stop (no Docker)")
    args = parser.parse_args()
    if not re.fullmatch(r"(?:sha256:|[A-Za-z0-9._:/-]+@sha256:)[0-9a-f]{64}", args.runtime_image):
        raise ValueError("runtime image must use an immutable full digest")
    directory = args.artifacts.resolve()
    evidence_file = directory / "package-evidence.json"
    evidence = json.loads(evidence_file.read_text())
    if "cpp" not in evidence["languages"]:
        raise ValueError("the artifacts carry no C++ packages")
    http = "json_http" in evidence["cpp_components"]
    if http and not args.installed_consumer:
        raise ValueError("this suite ships http: pass the json_http consumer too")
    consumers = {"udp_consumer": args.installed_udp_consumer.resolve()}
    if http:
        consumers["consumer"] = args.installed_consumer.resolve()
    if args.output.exists() or args.output.resolve() == ROOT or ROOT in args.output.resolve().parents:
        raise ValueError("runtime receipt must be a new file outside the checkout")
    if evidence["architecture"] not in ABI["architectures"] or evidence["distribution"] not in ABI["distributions"]:
        raise ValueError("unsupported runtime platform cell")
    for item in evidence["artifacts"]:
        name = item["path"]
        if Path(name).name != name or digest(directory / name) != item["sha256"]:
            raise ValueError("runtime artifact digest mismatch")
    positive, negative, packages = scripts(evidence, list(consumers))
    if args.dry_run:
        print(positive)
        print(negative)
        return
    image = json.loads(run(["docker", "image", "inspect", args.runtime_image], capture=True))[0]
    if image["Architecture"] != evidence["architecture"]:
        raise ValueError("runtime image architecture mismatch")
    native = run(["docker", "info", "--format", "{{.Architecture}}"], capture=True)
    if {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(native, native) != evidence["architecture"]:
        raise ValueError("runtime ABI checks require the target architecture's native Docker runner")
    command = ["docker", "run", "--rm", "--network", "none", "--memory", "512m", "--cpus", "2", "--pids-limit", "128",
               "--mount", "type=bind,src=" + str(directory) + ",dst=/artifacts,readonly"]
    for name, path in consumers.items():
        command += ["--mount", "type=bind,src=" + str(path) + ",dst=/probe/" + name + ",readonly"]
    command += ["--entrypoint", "/bin/bash", args.runtime_image, "-c"]
    positive_result = run(command + [positive], capture=True)
    # The libraries each consumer must have loaded from the installation.
    base = [c for p in packages for c in p["components"]]
    generations = ABI["cpp"]["soname"]
    for component in base:
        if not re.search(r"libxgc2_xrpc_" + component + r"\.so\." + str(generations[component]) + r"\s+=>\s+/(?:usr/)?lib/", positive_result):
            raise ValueError("runtime consumers did not load installed " + component)
    negative_result = run(command + [negative], capture=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.output, {"schema": "xgc2.xrpc.runtime-install-evidence.v2", "created_at": utc_now(),
               "package_evidence_sha256": digest(evidence_file),
               "consumer_sha256": {name: digest(path) for name, path in consumers.items()},
               "image_id": image["Id"], "image_digests": image.get("RepoDigests", []),
               "distribution": evidence["distribution"], "architecture": evidence["architecture"],
               "packages": [name for p in packages for name in (p["runtime"], p["dev"])],
               "positive": positive_result, "negative_missing_runtime": negative_result,
               "trusted_release": False})


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
