"""Turn a pinned, natively npm-installed SDK into a library-only Debian package."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tarfile

from support import ROOT, digest, run, write_json

PACKAGE = "node-xgc2-xrpc"
SDK_PATH = Path("usr/lib/xgc2/node_modules/@xgc2/xrpc")
IDENTITY_PATH = Path("usr/share/xgc2/xrpc/node/install-identity.json")


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
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("Node source SHA must be a full lowercase commit ID")
    payload = npm_payload(file)
    for name, expected in payload.items():
        # git blob IDs independently bind every actual npm payload file.
        blob = run(["git", "rev-parse", source_sha + ":node/" + name], cwd=ROOT, capture=True)
        with tarfile.open(file) as archive:
            data = archive.extractfile("package/" + name).read()
        actual = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        if actual != blob or hashlib.sha256(data).hexdigest() != expected:
            raise ValueError("npm payload differs from exact Node commit: " + name)


def verify_installed(root, expected_tar_sha=None):
    identity = json.loads((root / IDENTITY_PATH).read_text())
    if (identity.get("schema"), identity.get("package"), identity.get("installedPath")) != (
            "xgc2.xrpc.node-deb-identity.v1", PACKAGE, "/" + str(SDK_PATH)):
        raise ValueError("invalid installed Node Deb identity")
    if expected_tar_sha and identity["npmTarSha256"] != expected_tar_sha:
        raise ValueError("installed npm tar identity mismatch")
    if regular_hashes(root / SDK_PATH) != identity["files"]:
        raise ValueError("installed Node Deb payload differs from identity")
    sdk = json.loads((root / SDK_PATH / "package.json").read_text())
    ws = json.loads((root / SDK_PATH / "node_modules/ws/package.json").read_text())
    if (sdk["name"], sdk["version"], sdk["engines"], sdk["dependencies"]) != (
            "@xgc2/xrpc", identity["sdkVersion"], {"node": ">=20"}, {"ws": "8.22.0"}):
        raise ValueError("installed SDK runtime/dependency identity differs")
    if (ws["name"], ws["version"]) != ("ws", "8.22.0"):
        raise ValueError("installed private ws identity differs")
    required = {"index.cjs", "index.d.cts", "client.cjs", "bootstrap.cjs", "policy.cjs",
                "diagnostics.cjs", "diagnostic-worker.cjs", "runtime-policy.json", "package.json"}
    if not required <= set(identity["files"]):
        raise ValueError("installed SDK lacks a required runtime or type asset")
    return identity


def locked_ws_archive(lock_file, work):
    # Resolve the committed archive URL directly: an offline cache need not contain
    # the registry's mutable package manifest.
    ws_lock = json.loads(lock_file.read_text())["packages"]["node_modules/ws"]
    if ws_lock["version"] != "8.22.0" or ws_lock["resolved"] != "https://registry.npmjs.org/ws/-/ws-8.22.0.tgz":
        raise ValueError("unexpected ws lock identity")
    packed = json.loads(run(["npm", "pack", ws_lock["resolved"], "--offline", "--ignore-scripts", "--json",
                             "--pack-destination", work], cwd=work, capture=True))[0]
    ws_tar = work / packed["filename"]
    if integrity(ws_tar) != ws_lock["integrity"]:
        raise ValueError("preprovisioned ws archive differs from lock integrity")
    return ws_tar, ws_lock


def build_node_deb(tarball, lock_file, work, out, version, sdk_version, epoch, source_sha=None):
    # Preprovisioned npm cache only; no registry access, lifecycle scripts or native peers.
    ws_tar, ws_lock = locked_ws_archive(lock_file, work)
    payload = npm_payload(tarball)
    ws_payload = npm_payload(ws_tar)
    consumer = work / "node-deb-npm-install"
    consumer.mkdir()
    write_json(consumer / "package.json", {"private": True, "dependencies": {
        "@xgc2/xrpc": "file:" + str(tarball.resolve()), "ws": "file:" + str(ws_tar.resolve())}})
    run(["npm", "install", "--offline", "--ignore-scripts", "--omit=dev", "--omit=optional",
         "--no-audit", "--no-fund"], cwd=consumer, env={"npm_config_engine_strict": "true"})
    installed = consumer / "node_modules/@xgc2/xrpc"
    ws = consumer / "node_modules/ws"
    if regular_hashes(installed) != payload or regular_hashes(ws) != ws_payload:
        raise ValueError("native npm installation differs from pinned archives")
    npm_lock = json.loads((consumer / "package-lock.json").read_text())["packages"]
    if set(npm_lock) != {"", "node_modules/@xgc2/xrpc", "node_modules/ws"}:
        raise ValueError("unexpected npm-installed dependency")
    if (npm_lock["node_modules/@xgc2/xrpc"]["integrity"] != integrity(tarball) or
            npm_lock["node_modules/ws"]["integrity"] != ws_lock["integrity"]):
        raise ValueError("installed npm lock integrity mismatch")
    stage = work / "deb-roots" / PACKAGE
    target = stage / SDK_PATH
    target.parent.mkdir(parents=True)
    shutil.copytree(installed, target)
    # Preserve the complete installed private dependency; no npm/network is needed on target.
    shutil.copytree(ws, target / "node_modules/ws")
    (stage / IDENTITY_PATH.parent).mkdir(parents=True)
    doc = stage / "usr/share/doc" / PACKAGE
    doc.mkdir(parents=True)
    identity = {"schema": "xgc2.xrpc.node-deb-identity.v1", "package": PACKAGE,
        "debVersion": version, "sdkVersion": sdk_version, "installedPath": "/" + str(SDK_PATH),
        "npmTarSha256": digest(tarball), "nodeSourceSha": source_sha,
        "wsVersion": "8.22.0", "wsIntegrity": ws_lock["integrity"],
        "runtime": {"node": ">=20", "owner": "consumer", "includesInterpreter": False},
        "files": regular_hashes(target)}
    write_json(stage / IDENTITY_PATH, identity)
    (doc / "copyright").write_text("Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/\n"
        "\nFiles: /usr/lib/xgc2/node_modules/@xgc2/xrpc/*\nCopyright: XGC Team\nLicense: MIT\n"
        " Permission is hereby granted, free of charge, to any person obtaining a copy\n"
        " of this software and associated documentation files (the Software), to deal\n"
        " in the Software without restriction, including without limitation the rights\n"
        " to use, copy, modify, merge, publish, distribute, sublicense, and/or sell\n"
        " copies of the Software, and to permit persons to whom the Software is\n"
        " furnished to do so, subject to the following conditions:\n .\n"
        " The above copyright notice and this permission notice shall be included in\n"
        " all copies or substantial portions of the Software.\n .\n"
        " THE SOFTWARE IS PROVIDED AS IS, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR\n"
        " IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,\n"
        " FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL\n"
        " THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER\n"
        " LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING\n"
        " FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER\n"
        " DEALINGS IN THE SOFTWARE.\n\nFiles: /usr/lib/xgc2/node_modules/@xgc2/xrpc/node_modules/ws/*\n"
        "Copyright: 2011 Einar Otto Stangvik <einaros@gmail.com>\n"
        "License: MIT\n See /usr/lib/xgc2/node_modules/@xgc2/xrpc/node_modules/ws/LICENSE.\n")
    control = stage / "DEBIAN"
    control.mkdir()
    (control / "control").write_text("Package: " + PACKAGE + "\nVersion: " + version +
        "\nArchitecture: all\nMulti-Arch: foreign\nSection: javascript\nPriority: optional\n"
        "Maintainer: XGC Team <apt@example.com>\nDepends: ca-certificates\n"
        "Description: XGC2 XRPC installed Node library\n"
        " Pinned npm SDK and private ws; consumer provides a controlled Node >=20 runtime.\n")
    verify_installed(stage, digest(tarball))
    for path in [stage] + sorted(stage.rglob("*")):
        os.chmod(path, 0o755 if path.is_dir() else 0o644)
        os.utime(path, (epoch, epoch))
    deb = out / (PACKAGE + "_" + version + "_all.deb")
    run(["dpkg-deb", "--root-owner-group", "--build", stage, deb], env={"SOURCE_DATE_EPOCH": str(epoch)})
    return {"kind": "node-deb", "path": deb.name, "package": PACKAGE, "architecture": "all",
            "npm_tar_sha256": identity["npmTarSha256"], "node_source_sha": source_sha}
