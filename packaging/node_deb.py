"""Turn a pinned, natively npm-installed SDK into a library-only Debian package.

Two npm packages ship this way, both as architecture-independent packages: the
CommonJS SDK `@xgc2/xrpc` (with a private, pinned `ws`) and the ESM client
`@xgc2/xrpc-client` (no dependencies). A NodeSdk describes one of them.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tarfile

from support import ROOT, digest, run, write_json

WS_VERSION = "8.22.0"
WS_URL = "https://registry.npmjs.org/ws/-/ws-8.22.0.tgz"


class NodeSdk:
    """One npm package and the Debian package that carries its installed tree."""

    def __init__(self, language, deb, npm_name, source, description, required, dependencies):
        self.language = language            # key in the package evidence: "node" or "ts"
        self.deb = deb                      # Debian package name
        self.npm_name = npm_name            # npm package name
        self.source = source                # directory of the sources
        self.install_path = Path("usr/lib/xgc2/node_modules") / npm_name
        self.identity_path = Path("usr/share/xgc2/xrpc") / language / "install-identity.json"
        self.description = description
        self.required = set(required)       # files the installed tree must contain
        self.dependencies = dict(dependencies)  # exact runtime dependencies of package.json


NODE = NodeSdk(
    "node", "node-xgc2-xrpc", "@xgc2/xrpc", "node", "Pinned npm SDK and private ws; consumer provides a controlled Node >=20 runtime.",
    ["index.cjs", "index.d.cts", "client.cjs", "bootstrap.cjs", "diagnostics.cjs", "diagnostic-worker.cjs", "unix.cjs", "package.json"],
    {"ws": WS_VERSION})
TS = NodeSdk(
    "ts", "node-xgc2-xrpc-client", "@xgc2/xrpc-client", "ts", "ESM http.v1 client for browsers and Node; consumer provides a controlled Node >=20 runtime.",
    ["dist/index.js", "dist/index.d.ts", "dist/call.js", "dist/events.js", "dist/sse.js", "dist/errors.js", "dist/wire.js", "package.json"], {})
SDKS = {sdk.language: sdk for sdk in (NODE, TS)}


def npm_payload(file):
    """Validate before extraction; npm payloads here contain regular files only."""
    files = {}
    total = 0
    seen = set()
    with tarfile.open(file) as archive:
        for member in archive:
            parts = Path(member.name).parts
            if (len(seen) >= 10000 or len(member.name) > 512 or len(parts) > 16 or
                    not parts or parts[0] != "package" or ".." in parts or member.name in seen):
                raise ValueError("unsafe/duplicate npm member")
            seen.add(member.name)
            if member.isdir():
                continue
            if not member.isfile() or len(parts) < 2:
                raise ValueError("npm payload must contain only directories and regular files")
            total += member.size
            if total > 256 * 1024 * 1024:
                raise ValueError("npm payload exceeds size ceiling")
            name = str(Path(*parts[1:]))
            if name in files:
                raise ValueError("duplicate normalized npm member")
            files[name] = hashlib.sha256(archive.extractfile(member).read()).hexdigest()
    if "package.json" not in files:
        raise ValueError("npm payload lacks package identity")
    return files


def regular_hashes(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("installed npm payload contains a symlink")
        if path.is_file():
            if path.stat().st_nlink != 1 or path.suffix in (".node", ".so"):
                raise ValueError("arch-all payload contains a link or native addon")
            result[str(path.relative_to(root))] = digest(path)
        elif not path.is_dir():
            raise ValueError("unsupported installed npm file")
    return result


def integrity(file):
    return "sha512-" + base64.b64encode(hashlib.sha512(Path(file).read_bytes()).digest()).decode()


def verify_source_tar(file, source_sha):
    """A local probe may reuse a published Node npm tarball: bind every file to its commit."""
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("Node source SHA must be a full lowercase commit ID")
    payload = npm_payload(file)
    for name, expected in payload.items():
        # git blob IDs independently bind every actual npm payload file.
        blob = run(["git", "rev-parse", source_sha + ":" + NODE.source + "/" + name], cwd=ROOT, capture=True)
        with tarfile.open(file) as archive:
            data = archive.extractfile("package/" + name).read()
        actual = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        if actual != blob or hashlib.sha256(data).hexdigest() != expected:
            raise ValueError("npm payload differs from exact Node commit: " + name)


def verify_installed(sdk, root, expected_tar_sha=None):
    """Check an installed (or staged) tree against its identity receipt."""
    identity = json.loads((root / sdk.identity_path).read_text())
    if (identity.get("schema"), identity.get("package"), identity.get("installedPath")) != (
            "xgc2.xrpc.node-deb-identity.v1", sdk.deb, "/" + str(sdk.install_path)):
        raise ValueError("invalid installed Node Deb identity")
    if expected_tar_sha and identity["npmTarSha256"] != expected_tar_sha:
        raise ValueError("installed npm tar identity mismatch")
    if regular_hashes(root / sdk.install_path) != identity["files"]:
        raise ValueError("installed Node Deb payload differs from identity")
    manifest = json.loads((root / sdk.install_path / "package.json").read_text())
    if (manifest["name"], manifest["version"], manifest["engines"], manifest.get("dependencies", {})) != (
            sdk.npm_name, identity["sdkVersion"], {"node": ">=20"}, sdk.dependencies):
        raise ValueError("installed SDK runtime/dependency identity differs")
    if "ws" in sdk.dependencies:
        ws = json.loads((root / sdk.install_path / "node_modules/ws/package.json").read_text())
        if (ws["name"], ws["version"]) != ("ws", WS_VERSION):
            raise ValueError("installed private ws identity differs")
    if not sdk.required <= set(identity["files"]):
        raise ValueError("installed SDK lacks a required runtime or type asset")
    return identity


def locked_ws_archive(lock_file, work):
    # Resolve the committed archive URL directly: an offline cache need not contain
    # the registry's mutable package manifest.
    ws_lock = json.loads(lock_file.read_text())["packages"]["node_modules/ws"]
    if ws_lock["version"] != WS_VERSION or ws_lock["resolved"] != WS_URL:
        raise ValueError("unexpected ws lock identity")
    packed = json.loads(run(["npm", "pack", ws_lock["resolved"], "--offline", "--ignore-scripts", "--json",
                             "--pack-destination", work], cwd=work, capture=True))[0]
    ws_tar = work / packed["filename"]
    if integrity(ws_tar) != ws_lock["integrity"]:
        raise ValueError("preprovisioned ws archive differs from lock integrity")
    return ws_tar, ws_lock


def copyright_text(sdk):
    text = ("Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/\n"
            "\nFiles: " + "/" + str(sdk.install_path) + "/*\nCopyright: XGC Team\nLicense: Apache-2.0\n"
            " Licensed under the Apache License, Version 2.0. On Debian systems the full\n"
            " text is in /usr/share/common-licenses/Apache-2.0.\n")
    if "ws" in sdk.dependencies:
        text += ("\nFiles: /" + str(sdk.install_path) + "/node_modules/ws/*\n"
                 "Copyright: 2011 Einar Otto Stangvik <einaros@gmail.com>\n"
                 "License: MIT\n See /" + str(sdk.install_path) + "/node_modules/ws/LICENSE.\n")
    return text


def build_node_deb(sdk, tarball, lock_file, work, out, version, sdk_version, epoch, source_sha=None):
    # Preprovisioned npm cache only; no registry access, lifecycle scripts or native peers.
    payload = npm_payload(tarball)
    consumer = work / (sdk.language + "-deb-npm-install")
    consumer.mkdir()
    dependencies = {sdk.npm_name: "file:" + str(tarball.resolve())}
    ws_payload = ws_lock = None
    if "ws" in sdk.dependencies:
        ws_tar, ws_lock = locked_ws_archive(lock_file, work)
        ws_payload = npm_payload(ws_tar)
        dependencies["ws"] = "file:" + str(ws_tar.resolve())
    write_json(consumer / "package.json", {"private": True, "dependencies": dependencies})
    run(["npm", "install", "--offline", "--ignore-scripts", "--omit=dev", "--omit=optional",
         "--no-audit", "--no-fund"], cwd=consumer, env={"npm_config_engine_strict": "true"})
    installed = consumer / "node_modules" / sdk.npm_name
    if regular_hashes(installed) != payload:
        raise ValueError("native npm installation differs from the pinned archive")
    npm_lock = json.loads((consumer / "package-lock.json").read_text())["packages"]
    expected = {"", "node_modules/" + sdk.npm_name}
    if ws_lock:
        expected.add("node_modules/ws")
    if set(npm_lock) != expected:
        raise ValueError("unexpected npm-installed dependency")
    if npm_lock["node_modules/" + sdk.npm_name]["integrity"] != integrity(tarball):
        raise ValueError("installed npm lock integrity mismatch")
    stage = work / "deb-roots" / sdk.deb
    target = stage / sdk.install_path
    target.parent.mkdir(parents=True)
    shutil.copytree(installed, target)
    identity = {"schema": "xgc2.xrpc.node-deb-identity.v1", "package": sdk.deb,
                "debVersion": version, "sdkVersion": sdk_version, "installedPath": "/" + str(sdk.install_path),
                "npmTarSha256": digest(tarball), "nodeSourceSha": source_sha,
                "runtime": {"node": ">=20", "owner": "consumer", "includesInterpreter": False}}
    if ws_lock:
        ws = consumer / "node_modules/ws"
        if regular_hashes(ws) != ws_payload or npm_lock["node_modules/ws"]["integrity"] != ws_lock["integrity"]:
            raise ValueError("native npm installation differs from the pinned ws archive")
        # Preserve the complete installed private dependency; no npm/network is needed on target.
        shutil.copytree(ws, target / "node_modules/ws")
        identity.update({"wsVersion": WS_VERSION, "wsIntegrity": ws_lock["integrity"]})
    identity["files"] = regular_hashes(target)
    (stage / sdk.identity_path.parent).mkdir(parents=True)
    write_json(stage / sdk.identity_path, identity)
    doc = stage / "usr/share/doc" / sdk.deb
    doc.mkdir(parents=True)
    (doc / "copyright").write_text(copyright_text(sdk))
    control = stage / "DEBIAN"
    control.mkdir()
    (control / "control").write_text("Package: " + sdk.deb + "\nVersion: " + version +
        "\nArchitecture: all\nMulti-Arch: foreign\nSection: javascript\nPriority: optional\n"
        "Maintainer: XGC Team <apt@example.com>\nDepends: ca-certificates\n"
        "Description: XGC2 XRPC " + sdk.npm_name + " installed Node library\n " + sdk.description + "\n")
    verify_installed(sdk, stage, digest(tarball))
    for path in [stage] + sorted(stage.rglob("*")):
        os.chmod(path, 0o755 if path.is_dir() else 0o644)
        os.utime(path, (epoch, epoch))
    deb = out / (sdk.deb + "_" + version + "_all.deb")
    run(["dpkg-deb", "--root-owner-group", "--build", stage, deb], env={"SOURCE_DATE_EPOCH": str(epoch)})
    return {"kind": sdk.language + "-deb", "path": deb.name, "package": sdk.deb, "architecture": "all",
            "npm_tar_sha256": identity["npmTarSha256"], "node_source_sha": source_sha}
