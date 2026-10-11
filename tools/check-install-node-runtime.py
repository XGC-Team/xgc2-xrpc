#!/usr/bin/env python3
"""Verify an architecture-independent npm SDK Deb (node-xgc2-xrpc or node-xgc2-xrpc-client)
with Node or Bun in fresh offline containers."""
import argparse
import json
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "packaging"))
from support import ABI, ROOT, digest, utc_now
from node_deb import SDKS

# What differs between the two SDKs: where the probe lives, how it is called and which
# installed files it and the negative controls depend on. Paths are relative to the SDK root.
PROBES = {
    "node": {"source": ROOT / "packaging/probes/node/installed.cjs", "consumer": "/usr/lib/xgc2/lichtblick-web/xrpc-install-probe.cjs",
             "owned": {"sdk_entry": "index.cjs", "worker": "diagnostic-worker.cjs", "ws_entry": "node_modules/ws/index.js",
                       "ws_identity": "node_modules/ws/package.json", "sdk_identity": "package.json", "unix_host": "unix.cjs"},
             "resolved": {"sdk": "index.cjs", "ws": "node_modules/ws/index.js"},
             "negatives": [["missing-worker", "diagnostic-worker.cjs", "missing-worker"], ["missing-ws", "node_modules/ws", "fails"],
                           ["missing-unix-host", "unix.cjs", "fails"]]},
    "ts": {"source": ROOT / "packaging/probes/ts/installed.mjs", "consumer": "/usr/lib/xgc2/xrpc-client-probe/xrpc-install-probe.mjs",
           "owned": {"sdk_entry": "dist/index.js", "calls": "dist/call.js", "events": "dist/events.js", "sdk_identity": "package.json"},
           "resolved": {"entry": "dist/index.js"},
           "negatives": [["missing-call-module", "dist/call.js", "fails"]]},
}

# This program runs inside each new container. Its only mounted inputs are the
# artifact directory and the single consumer probe; installation uses dpkg.
CONTAINER_PROGRAM = r'''
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile

config = json.loads(sys.argv[1])
phase = "runtime-platform"

def sha256(file):
    h = hashlib.sha256()
    with Path(file).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def execute(argv, check=True, **options):
    result = subprocess.run(argv, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=30, **options)
    if check and result.returncode != 0:
        raise ValueError("command failed")
    return result

def output(argv):
    return execute(argv).stdout.strip()

def main():
    global phase
    release = dict(line.split("=", 1) for line in Path("/etc/os-release").read_text().splitlines() if "=" in line)
    suite = release.get("VERSION_CODENAME", "").strip('"')
    if suite != config["runtime_distribution"]:
        raise ValueError("runtime distribution mismatch")
    architecture = output(["dpkg", "--print-architecture"])
    if architecture != config["architecture"]:
        raise ValueError("runtime architecture mismatch")
    package = config["package"]
    root = Path(config["sdk_root"])
    before = execute(["dpkg-query", "-W", "-f=${Status}", package], check=False)
    if before.returncode == 0 or root.exists() or root.is_symlink():
        raise ValueError("runtime image already contains the SDK")

    phase = "runtime-binary"
    binary = shutil.which(config["runtime"])
    if not binary:
        raise ValueError("preprovisioned runtime is missing")
    binary = str(Path(binary).resolve(strict=True))
    with tempfile.TemporaryDirectory(prefix="xrpc-runtime-version-", dir="/tmp") as home:
        runtime_version = execute([binary, "--version"], env={"HOME": home, "PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"}).stdout.strip()
    if config["runtime"] == "node":
        version = re.fullmatch(r"v([0-9]+)\.[0-9]+\.[0-9]+", runtime_version)
        if not version or int(version.group(1)) < 20:
            raise ValueError("Node >=20 is required")
    elif not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9._-]+)?", runtime_version):
        raise ValueError("invalid Bun version")
    runtime = {"kind": config["runtime"], "binary": binary, "version": runtime_version,
               "binary_sha256": sha256(binary)}

    phase = "deb-control-and-install"
    deb = Path("/artifacts") / config["deb_path"]
    if sha256(deb) != config["deb_sha256"] or deb.stat().st_size != config["deb_bytes"]:
        raise ValueError("mounted Deb identity changed")
    control = {field.lower(): output(["dpkg-deb", "-f", str(deb), field])
               for field in ("Package", "Version", "Architecture", "Depends")}
    if (control["package"], control["version"], control["architecture"]) != (package, config["deb_version"], "all"):
        raise ValueError("unexpected Node Deb identity")
    if control["depends"] != "ca-certificates":
        raise ValueError("unexpected Node Deb dependencies")
    install = execute(["dpkg", "-i", str(deb)])
    query = output(["dpkg-query", "-W", "-f=${Status}\t${Version}\t${Architecture}\n", package])
    if query.split("\t") != ["install ok installed", config["deb_version"], "all"]:
        raise ValueError("SDK Deb was not fully installed")

    phase = "dpkg-file-ownership"
    paths = {name: root / relative for name, relative in config["owned"].items()}
    paths["install_identity"] = Path(config["identity"])
    owners = {}
    for name, file in paths.items():
        phase = "dpkg-file-ownership:" + name
        if not file.is_file() or file.is_symlink():
            raise ValueError("installed runtime file is absent or linked")
        owned = output(["dpkg-query", "-S", str(file)])
        if owned != package + ": " + str(file):
            raise ValueError("runtime file has unexpected ownership")
        owners[name] = {"path": str(file), "owner": package, "query": owned, "sha256": sha256(file)}
    identity = paths["install_identity"]
    phase = "installed-identity-json"
    if identity.stat().st_size > 65536:
        raise ValueError("invalid installed identity document")
    installed_identity = json.loads(identity.read_text())
    if not isinstance(installed_identity, dict):
        raise ValueError("invalid installed identity document")
    expected_identity = {"schema": "xgc2.xrpc.node-deb-identity.v1", "package": package,
                         "installedPath": str(root), "npmTarSha256": config["npm_tar_sha256"],
                         "nodeSourceSha": config["node_source_sha"], "debVersion": config["deb_version"]}
    for name, value in expected_identity.items():
        if installed_identity.get(name) != value:
            raise ValueError("installed identity differs from package evidence")

    def installed_hashes():
        hashes = {}
        if root.is_symlink() or not root.is_dir():
            raise ValueError("invalid installed SDK directory")
        for file in sorted(root.rglob("*")):
            metadata = file.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                raise ValueError("installed SDK contains a symbolic link")
            if stat.S_ISREG(metadata.st_mode):
                if metadata.st_nlink != 1 or re.search(r"(?:\.node|\.so(?:\.[^/]*)?)$", file.name):
                    raise ValueError("Architecture all SDK contains a hardlink or native addon")
                hashes[str(file.relative_to(root))] = sha256(file)
            elif not stat.S_ISDIR(metadata.st_mode):
                raise ValueError("installed SDK contains a special file")
        return hashes

    phase = "installed-payload-identity"
    files = installed_identity.get("files")
    if not isinstance(files, dict) or not files or installed_hashes() != files:
        raise ValueError("complete installed SDK file map differs from identity")
    payload_identity = dict(expected_identity, path=str(identity), sha256=sha256(identity),
                            files_count=len(files), files_sha256=hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest())

    phase = "consumer-placement"
    consumer = Path(config["consumer"])
    consumer.parent.mkdir(parents=True, exist_ok=True)
    if consumer.exists() or consumer.is_symlink():
        raise ValueError("consumer path already exists in runtime image")
    shutil.copyfile("/probe.src", consumer)
    if sha256(consumer) != config["probe_sha256"]:
        raise ValueError("copied probe identity changed")

    def probe(case, missing_worker=False):
        home = tempfile.mkdtemp(prefix="xrpc-installed-" + case + "-", dir="/tmp")
        environment = {"HOME": home, "PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"}
        command = [binary]
        if config["runtime"] == "node":
            command.append("--no-global-search-paths")
        command += [str(consumer), str(root), config["sdk_version"]]
        if missing_worker:
            command.append("--missing-worker")
        try:
            result = execute(command, check=False, cwd=str(consumer.parent), env=environment)
            if len(result.stdout) > 65536 or len(result.stderr) > 65536:
                raise ValueError("probe output exceeded bounded receipt size")
            report = json.loads(result.stdout) if result.stdout.strip() else None
            return {"case": case, "returncode": result.returncode, "report": report,
                    "stderr": result.stderr, "environment": {"isolated_home": True, "node_path": False,
                    "node_options": False, "no_global_search_paths": config["runtime"] == "node"}}
        finally:
            shutil.rmtree(home)

    def require_positive(result):
        report = result["report"]
        if result["returncode"] != 0 or not isinstance(report, dict) or report.get("ok") is not True or report.get("mode") != "positive":
            raise ValueError("installed consumer failed")
        if report["runtime"]["implementation"] != config["runtime"]:
            raise ValueError("probe runtime differs from selected binary")
        for key, relative in config["resolved"].items():
            if report["resolvedPaths"][key] != str(root / relative):
                raise ValueError("probe escaped the installed dependency closure")

    receipt = {"distribution": suite, "architecture": architecture, "runtime": runtime,
               "dpkg": {"control": control, "install_stdout": install.stdout, "install_stderr": install.stderr,
                        "status_query": query, "ownership": owners},
               "probe_sha256": config["probe_sha256"], "fresh_image_without_sdk": True}
    receipt["installed_identity"] = payload_identity
    if config["case"] == "positive":
        phase = "positive-installed-consumer"
        result = probe("positive")
        require_positive(result)
        receipt["positive"] = result
        return receipt

    negative = []
    # Each runtime is a new process. Files are restored before the next control,
    # so a later rejection cannot be explained by an earlier missing asset.
    for name, relative, mode in config["negatives"]:
        phase = name
        target = root / relative
        hidden = target.with_name(target.name + ".installed-probe-hidden")
        if hidden.exists() or hidden.is_symlink():
            raise ValueError("negative control backup already exists")
        target.rename(hidden)
        try:
            result = probe(name, missing_worker=mode == "missing-worker")
            if mode == "missing-worker":
                report = result["report"]
                if result["returncode"] != 0 or not isinstance(report, dict) or report.get("ok") is not True or report.get("mode") != name:
                    raise ValueError("missing worker boundary was not verified")
                if report.get("negative") != {"verified": True, "emit": False, "closeErrorCode": "unavailable"}:
                    raise ValueError("missing worker did not reject admission and close")
                if report["status"]["workerFailures"] < 1 or report["status"]["written"] != 0:
                    raise ValueError("missing worker status does not account for failure")
            elif result["returncode"] == 0 or result["report"] is not None:
                raise ValueError("missing packaged dependency unexpectedly succeeded")
            result["verified"] = True
            negative.append(result)
        finally:
            hidden.rename(target)
    phase = "restored-installed-consumer"
    restored = probe("restored")
    require_positive(restored)
    for name, file in paths.items():
        if sha256(file) != owners[name]["sha256"]:
            raise ValueError("negative control did not restore installed bytes")
    if installed_hashes() != files:
        raise ValueError("negative controls did not restore the full installed payload")
    receipt["negative_controls"] = negative
    receipt["restored_positive"] = restored
    return receipt

try:
    print(json.dumps(main(), sort_keys=True))
except Exception:
    # Keep subprocess failures bounded and never copy arbitrary error text.
    print(json.dumps({"ok": False, "check": phase}), file=sys.stderr)
    sys.exit(1)
'''


def captured(command, timeout=30):
    result = subprocess.run([str(part) for part in command], check=False, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if result.returncode != 0:
        raise ValueError("command failed: " + str(command[0]) + "; " + result.stderr.strip()[:4096])
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--sdk", choices=sorted(SDKS), default="node", help="which npm SDK's Debian package to check")
    parser.add_argument("--runtime-image", required=True, help="already cached immutable full image ID or registry digest")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--runtime", choices=("node", "bun"), default="node")
    parser.add_argument("--expected-distribution", choices=ABI["distributions"],
                        help="explicit runtime suite for cross-suite Architecture: all validation")
    args = parser.parse_args()
    sdk, probe_config = SDKS[args.sdk], PROBES[args.sdk]
    probe_source = probe_config["source"]
    if not re.fullmatch(r"(?:sha256:|[A-Za-z0-9._:/-]+@sha256:)[0-9a-f]{64}", args.runtime_image):
        raise ValueError("runtime image must use an immutable full digest")
    directory = args.artifacts.resolve(strict=True)
    if not directory.is_dir() or directory == ROOT or ROOT in directory.parents:
        raise ValueError("artifacts must be outside the checkout")
    output_file = args.output.resolve()
    if args.output.exists() or args.output.is_symlink() or output_file == ROOT or ROOT in output_file.parents:
        raise ValueError("runtime receipt must be a new file outside the checkout")
    evidence_file = directory / "package-evidence.json"
    if evidence_file.is_symlink():
        raise ValueError("package evidence must be a regular file")
    evidence_sha256 = digest(evidence_file)
    evidence = json.loads(evidence_file.read_text())
    if evidence["distribution"] not in ABI["distributions"] or evidence["architecture"] not in ABI["architectures"]:
        raise ValueError("unsupported package platform cell")
    seen = set()
    for item in evidence["artifacts"]:
        name = item["path"]
        file = directory / name
        if not name or Path(name).name != name or name in seen or file.is_symlink() or not file.is_file():
            raise ValueError("unsafe, duplicate or nonregular artifact path")
        seen.add(name)
        if digest(file) != item["sha256"] or file.stat().st_size != item["bytes"]:
            raise ValueError("runtime artifact hash/size mismatch")
    node_debs = [item for item in evidence["artifacts"] if item["kind"] == args.sdk + "-deb"]
    if len(node_debs) != 1 or node_debs[0]["package"] != sdk.deb:
        raise ValueError("expected exactly one %s Deb" % sdk.deb)
    artifact = node_debs[0]
    if not artifact["path"].endswith(".deb"):
        raise ValueError("Deb artifact has an invalid suffix")
    if not re.fullmatch(r"[0-9a-f]{64}", artifact["npm_tar_sha256"]) or not re.fullmatch(r"[0-9a-f]{40}", artifact["node_source_sha"]):
        raise ValueError("the Deb requires exact npm and committed source identities")
    npm_artifacts = [item for item in evidence["artifacts"] if item["kind"] == args.sdk]
    if len(npm_artifacts) != 1 or npm_artifacts[0]["sha256"] != artifact["npm_tar_sha256"]:
        raise ValueError("Deb origin differs from the declared npm artifact")
    image = json.loads(captured(["docker", "image", "inspect", args.runtime_image]))[0]
    if image["Architecture"] != evidence["architecture"]:
        raise ValueError("runtime image architecture mismatch")
    native = captured(["docker", "info", "--format", "{{.Architecture}}"])
    native = {"x86_64": "amd64", "aarch64": "arm64"}.get(native, native)
    if native != evidence["architecture"]:
        raise ValueError("runtime checks require the target architecture's native Docker host")
    runtime_distribution = args.expected_distribution or evidence["distribution"]
    probe_sha256 = digest(probe_source)
    config = {"package": sdk.deb, "sdk_root": "/" + str(sdk.install_path), "identity": "/" + str(sdk.identity_path),
              "runtime": args.runtime, "runtime_distribution": runtime_distribution,
              "architecture": evidence["architecture"], "deb_version": evidence["deb_version"],
              "sdk_version": evidence["version"].split("-", 1)[0],
              "deb_path": artifact["path"], "deb_sha256": artifact["sha256"], "deb_bytes": artifact["bytes"],
              "npm_tar_sha256": artifact["npm_tar_sha256"], "node_source_sha": artifact["node_source_sha"],
              "probe_sha256": probe_sha256, "consumer": probe_config["consumer"], "owned": probe_config["owned"],
              "resolved": probe_config["resolved"], "negatives": probe_config["negatives"]}
    results = {}
    # Build outputs also contain work/source snapshots. Only declared artifacts
    # enter the mounted directory, so source trees cannot satisfy the consumer.
    with tempfile.TemporaryDirectory(prefix="xrpc-node-runtime-artifacts-") as staged:
        staged = Path(staged)
        shutil.copyfile(evidence_file, staged / "package-evidence.json")
        for item in evidence["artifacts"]:
            target = staged / item["path"]
            shutil.copyfile(directory / item["path"], target)
            if digest(target) != item["sha256"] or target.stat().st_size != item["bytes"]:
                raise ValueError("artifact changed while preparing isolated mounts")
        if digest(staged / "package-evidence.json") != evidence_sha256:
            raise ValueError("package evidence changed while preparing isolated mounts")
        command = ["docker", "run", "--rm", "--pull", "never", "--network", "none", "--memory", "512m",
                   "--cpus", "2", "--pids-limit", "128", "--user", "0:0", "--workdir", "/",
                   "--mount", "type=bind,src=" + str(staged) + ",dst=/artifacts,readonly",
                   "--mount", "type=bind,src=" + str(probe_source) + ",dst=/probe.src,readonly",
                   "--entrypoint", "/usr/bin/python3", image["Id"], "-c", CONTAINER_PROGRAM]
        for case in ("positive", "negative"):
            print("Checking installed %s Deb: %s / %s" % (args.sdk, args.runtime, case), flush=True)
            results[case] = json.loads(captured(command + [json.dumps(dict(config, case=case))], timeout=150))
    if results["positive"]["runtime"] != results["negative"]["runtime"]:
        raise ValueError("fresh containers used different runtime binaries")
    if digest(evidence_file) != evidence_sha256 or digest(probe_source) != probe_sha256:
        raise ValueError("verification inputs changed during runtime checks")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": "xgc2.xrpc.node-runtime-install-evidence.v2", "created_at": utc_now(), "sdk": args.sdk,
               "package_evidence_sha256": evidence_sha256, "probe_sha256": probe_sha256,
               "deb_sha256": artifact["sha256"], "deb_bytes": artifact["bytes"], "deb_path": artifact["path"],
               "deb_version": evidence["deb_version"], "package": sdk.deb,
               "image_requested": args.runtime_image, "image_id": image["Id"], "image_digests": image.get("RepoDigests", []),
               "architecture": evidence["architecture"], "build_distribution": evidence["distribution"],
               "runtime_distribution": runtime_distribution, "distribution_override_explicit": args.expected_distribution is not None,
               "cross_suite_architecture_all": runtime_distribution != evidence["distribution"], "runtime": args.runtime,
               "isolation": {"fresh_containers": 2, "network": "none", "memory": "512m", "cpus": 2,
                             "pids_limit": 128, "mounts": ["artifacts:readonly", "probe:readonly"],
                             "artifact_mount_declared_files_only": True,
                             "source_mount": False, "runtime_installation": False},
               "positive": results["positive"], "negative": results["negative"],
               "trusted": False, "trusted_release": False}
    with output_file.open("x") as stream:
        stream.write(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print("Node runtime evidence: " + str(output_file))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, subprocess.TimeoutExpired) as error:
        sys.exit(str(error))
