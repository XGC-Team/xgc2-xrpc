#!/usr/bin/env python3
"""Emit strict-v1 Debian evidence only from verified exact-source push builds."""
import argparse
import json
import os
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "packaging"))
from support import ROOT, cpp_package_set, digest, run, suite_cpp_components, suite_languages, utc_now, write_json
from node_deb import SDKS


def require_npm_installation(package, install, language):
    """The npm tarball and its Debian package of one npm SDK were installed and probed."""
    sdk = SDKS[language]
    npm_artifacts = [artifact for artifact in package["artifacts"] if artifact["kind"] == language]
    node_debs = [artifact for artifact in package["artifacts"] if artifact["kind"] == language + "-deb"]
    if len(npm_artifacts) != 1 or len(node_debs) != 1:
        raise ValueError("one %s npm tarball and one Debian artifact required" % language)
    npm_sha = npm_artifacts[0]["sha256"]
    if not isinstance(npm_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", npm_sha):
        raise ValueError("invalid %s npm tarball identity" % language)
    if node_debs[0].get("npm_tar_sha256") != npm_sha:
        raise ValueError("%s Debian artifact is not bound to the same npm tarball" % language)
    if any(artifact.get("node_source_sha") != package["source_sha"]
           for artifact in (npm_artifacts[0], node_debs[0])):
        raise ValueError("%s npm and Debian artifacts differ from the release source" % language)
    checks = [check for check in install["checks"] if check.get("language") == language]
    if not any(check.get("format") == "npm" for check in checks):
        raise ValueError("missing actual %s npm installation consumer" % language)
    deb_checks = [check for check in checks if check.get("format") == "deb"]
    if not deb_checks:
        raise ValueError("missing actual %s Debian installation consumer" % language)
    for check in deb_checks:
        if check.get("installedPath") != "/" + str(sdk.install_path):
            raise ValueError("%s Debian consumer used a different installation prefix" % language)
        if check.get("npmTarSha256") != npm_sha:
            raise ValueError("%s Debian consumer is not bound to the same npm tarball" % language)
        consumer = check.get("consumer")
        if not isinstance(consumer, dict) or consumer.get("ok") is not True:
            raise ValueError("missing successful %s Debian installation probe" % language)
        if language == "node":
            status = consumer.get("status")
            if (consumer.get("mode") != "positive" or not isinstance(status, dict) or status.get("workerStarted") is not True or
                    status.get("state") != "closed" or type(status.get("pendingRecords")) is not int or
                    status["pendingRecords"] != 0 or type(status.get("written")) is not int or
                    status["written"] < 1 or type(status.get("workerFailures")) is not int or
                    status["workerFailures"] != 0):
                raise ValueError("Node Debian diagnostic worker did not write and close successfully")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--install-receipt", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    package_file = args.artifacts / "package-evidence.json"
    package = json.loads(package_file.read_text())
    install = json.loads(args.install_receipt.read_text())
    sha = os.environ.get("GITHUB_SHA", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    workflow_ref = os.environ.get("GITHUB_WORKFLOW_REF", "")
    if os.environ.get("GITHUB_EVENT_NAME") != "push" or os.environ.get("GITHUB_REPOSITORY") != "XGC-Team/xgc2-xrpc":
        raise ValueError("trusted-v1 requires the product's native push CI identity")
    if not re.fullmatch(r"[0-9a-f]{40}", sha) or not re.fullmatch(r"[1-9][0-9]*", run_id):
        raise ValueError("missing exact push/run identity")
    if not workflow_ref.startswith("XGC-Team/xgc2-xrpc/.github/workflows/ci.yml@"):
        raise ValueError("wrong trusted CI workflow identity")
    if package.get("schema") != "xgc2.xrpc.package-evidence.v1" or not package.get("release_candidate") or package.get("source_sha") != sha:
        raise ValueError("artifacts are not an exact committed release candidate")
    distribution = package["distribution"]
    if package["languages"] != suite_languages(distribution) or package["cpp_components"] != suite_cpp_components(distribution):
        raise ValueError("every SDK and C++ component the distribution ships is required")
    if install.get("schema") != "xgc2.xrpc.install-evidence.v1" or install["package_evidence_sha256"] != digest(package_file):
        raise ValueError("installation receipt is not bound to these exact artifacts")
    if (install["distribution"], install["architecture"], install["languages"]) != (package["distribution"], package["architecture"], package["languages"]):
        raise ValueError("installation coverage differs from package cell")
    if {check["language"] for check in install["checks"] if "language" in check} != set(package["languages"]):
        raise ValueError("missing actual SDK installation consumer")
    if any(check.get("components") != package["cpp_components"] for check in install["checks"] if check.get("language") == "cpp"):
        raise ValueError("full C++ component installation coverage required")
    for language in ("node", "ts"):
        if language in package["languages"]:
            require_npm_installation(package, install, language)
    if run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture=True) != sha or run(["git", "status", "--porcelain"], cwd=ROOT, capture=True):
        raise ValueError("source changed after build/install checks")
    if any(digest(ROOT / name) != value for name, value in package["source_files"].items()):
        raise ValueError("source bytes differ from the verified snapshot")
    canonical = set()
    if "cpp" in package["languages"]:
        for item in cpp_package_set(package["cpp_components"]):
            canonical |= {item["runtime"], item["dev"]}
    canonical |= {SDKS[language].deb for language in ("node", "ts") if language in package["languages"]}
    node_debs = {SDKS[language].deb: language + "-deb" for language in SDKS}
    debs = []
    for artifact in package["artifacts"]:
        file = args.artifacts / artifact["path"]
        if Path(artifact["path"]).name != artifact["path"] or digest(file) != artifact["sha256"]:
            raise ValueError("artifact bytes changed after installation checks")
        if artifact["kind"] not in ("deb", "node-deb", "ts-deb"):
            continue
        name, version, arch = [run(["dpkg-deb", "-f", file, field], capture=True)
                               for field in ("Package", "Version", "Architecture")]
        expected_arch = "all" if name in node_debs else package["architecture"]
        expected_kind = node_debs.get(name, "deb")
        if version != package["version"] + "~" + package["distribution"] or arch != expected_arch or artifact["kind"] != expected_kind:
            raise ValueError("Debian release identity mismatch")
        debs.append({"file": file.name, "package": name, "version": version, "architecture": arch,
                     "sha256": digest(file), "size": file.stat().st_size})
    if {deb["package"] for deb in debs} != canonical or len(debs) != len(canonical):
        raise ValueError("canonical Debian package set incomplete")
    manifest = {"schema": "xgc2.build-artifact.v1", "product": "xgc2-xrpc",
                "source_sha": sha, "version": package["version"], "distribution": package["distribution"],
                "architecture": package["architecture"], "ci": {"run_id": run_id, "workflow": "ci.yml", "workflow_ref": workflow_ref},
                "created_at": utc_now(), "debs": debs}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.output, manifest)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as error:
        sys.exit(str(error))
