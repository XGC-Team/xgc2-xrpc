#!/usr/bin/env python3
"""Verify SDK artifacts by installing into a new scratch directory, offline."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "packaging"))
from support import ABI, ROOT, digest, elf_info, extract_tar, run, system_suite, utc_now, write_json
from node_deb import IDENTITY_PATH, PACKAGE as NODE_PACKAGE, SDK_PATH, verify_installed


def expected_failure(args, cwd=None, env=None):
    try:
        run(args, cwd=cwd, env=env)
    except subprocess.CalledProcessError:
        return
    raise ValueError("negative control unexpectedly succeeded: " + str(args))


def validate_artifacts(directory, evidence):
    seen = set()
    kinds = set()
    for item in evidence["artifacts"]:
        if item["kind"] not in {"deb", "python", "go", "rust", "node", "node-deb"}:
            raise ValueError("unknown artifact kind")
        kinds.add({"deb": "cpp", "node-deb": "node"}.get(item["kind"], item["kind"]))
        name = item["path"]
        if Path(name).name != name or name in seen:
            raise ValueError("unsafe or duplicate artifact path")
        seen.add(name)
        file = directory / name
        if file.is_symlink() or digest(file) != item["sha256"] or file.stat().st_size != item["bytes"]:
            raise ValueError("artifact hash/size mismatch: " + name)
    if not seen:
        raise ValueError("no installable artifacts")
    if kinds != set(evidence["languages"]) or len(evidence["languages"]) != len(set(evidence["languages"])):
        raise ValueError("declared languages differ from actual artifacts")
    debs = [item["package"] for item in evidence["artifacts"] if item["kind"] == "deb"]
    expected = {"libxgc2-xrpc1", "libxgc2-xrpc-dev"} if "cpp" in kinds else set()
    if "cpp" in kinds and evidence["cpp_profile"] == "grpc":
        expected |= {"libxgc2-xrpc-grpc1", "libxgc2-xrpc-grpc-dev"}
    if set(debs) != expected or len(debs) != len(expected):
        raise ValueError("unexpected or duplicate Debian package set")
    node_debs = [item for item in evidence["artifacts"] if item["kind"] == "node-deb"]
    if len(node_debs) != (1 if "node" in kinds else 0) or any(item["package"] != NODE_PACKAGE for item in node_debs):
        raise ValueError("expected the actual Node Debian SDK alongside its npm artifact")
    if node_debs and node_debs[0]["npm_tar_sha256"] != next(item["sha256"] for item in evidence["artifacts"] if item["kind"] == "node"):
        raise ValueError("Node Debian SDK is not bound to this npm artifact")
    for kind in kinds - {"cpp"}:
        if sum(item["kind"] == kind for item in evidence["artifacts"]) != 1:
            raise ValueError("expected one native language artifact: " + kind)


def validate_deb_paths(file):
    process = subprocess.Popen(["dpkg-deb", "--fsys-tarfile", str(file)], stdout=subprocess.PIPE)
    seen = set()
    total = 0
    try:
        with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
            for member in archive:
                name = member.name
                while name.startswith("./"):
                    name = name[2:]
                if name in ("", ".") and member.isdir():
                    continue
                path = Path(name)
                if len(seen) >= 10000 or len(name) > 512 or len(path.parts) > 16:
                    raise ValueError("Debian member count/path ceiling exceeded")
                if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != "usr" or name in seen:
                    raise ValueError("unsafe/duplicate Debian member: " + member.name)
                seen.add(name)
                if member.issym():
                    # Only flat, relative linker/SONAME links are permitted. Directory links are never copied.
                    if not re.fullmatch(r"usr/lib/[^/]+/libxgc2_xrpc_[a-z]+\.so(?:\.1)?", name):
                        raise ValueError("unsupported Debian symlink: " + name)
                    if not re.fullmatch(r"libxgc2_xrpc_[a-z]+\.so\.(?:1|[0-9]+\.[0-9]+\.[0-9]+)", member.linkname):
                        raise ValueError("unsafe Debian link target: " + member.linkname)
                elif not (member.isfile() or member.isdir()):
                    raise ValueError("unsupported Debian member type: " + name)
                total += member.size
                if total > 256 * 1024 * 1024:
                    raise ValueError("Debian unpacked size exceeds verification ceiling")
        if process.wait() != 0:
            raise ValueError("cannot inspect Debian filesystem archive")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def check_cpp(args, directory, evidence, work, checks):
    profile = args.cpp_install_profile or evidence["cpp_profile"]
    if profile == "grpc" and evidence["cpp_profile"] != "grpc":
        raise ValueError("gRPC installation requested from HTTP-only artifacts")
    prefix = work / "cpp-root"
    prefix.mkdir()
    owners = {}
    runtime = []
    # Validate every package before any package can create directories or links.
    for item in evidence["artifacts"]:
        if item["kind"] == "deb":
            validate_deb_paths(directory / item["path"])
    for item in evidence["artifacts"]:
        if item["kind"] != "deb":
            continue
        if profile == "http" and item["package"].startswith("libxgc2-xrpc-grpc"):
            continue
        file = directory / item["path"]
        package = run(["dpkg-deb", "-f", file, "Package"], capture=True)
        version = run(["dpkg-deb", "-f", file, "Version"], capture=True)
        arch = run(["dpkg-deb", "-f", file, "Architecture"], capture=True)
        if package != item["package"] or version != evidence["deb_version"] or arch != args.architecture:
            raise ValueError("Debian package identity mismatch: " + file.name)
        depends = run(["dpkg-deb", "-f", file, "Depends"], capture=True)
        if package.endswith("-dev"):
            wanted = package[:-4] + "1 (= " + version + ")"
            if wanted not in depends:
                raise ValueError("development package lacks exact runtime dependency")
        package_root = work / "individual" / package
        package_root.mkdir(parents=True)
        run(["dpkg-deb", "-x", file, package_root])
        for path in sorted(package_root.rglob("*")):
            if not (path.is_file() or path.is_symlink()):
                continue
            relative = path.relative_to(package_root)
            if str(relative) in owners:
                raise ValueError("overlapping package ownership: " + str(relative))
            owners[str(relative)] = package
            target = prefix / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target, follow_symlinks=False)
            if ".so." in path.name and path.is_file() and not path.is_symlink():
                runtime.append(target)
    if len(runtime) != (6 if profile == "grpc" else 5):
        raise ValueError("wrong installed native library count")
    for library in runtime:
        info = elf_info(library, args.distribution, args.architecture)
        if info != evidence["elf"][library.name]:
            raise ValueError("ELF ABI differs from build receipt")
        dynamic = run(["readelf", "-d", library], capture=True)
        for path in re.findall(r"\((?:RUNPATH|RPATH)\).*?\[([^]]*)\]", dynamic):
            if any(component not in ("$ORIGIN", "${ORIGIN}") for component in path.split(":")):
                raise ValueError("installed library retains an external build/runtime path: " + path)
        selected = run(["ldd", library], capture=True)
        if "not found" in selected:
            raise ValueError("unresolved installed library dependency")
        checks.append({"elf": library.name, "resolved": selected})
    build = work / "cpp-consumer"
    cmake_prefix = [str(prefix / "usr")] + args.cmake_prefix
    flags = ["-DCMAKE_PREFIX_PATH=" + ";".join(cmake_prefix), "-DCMAKE_CXX_COMPILER=" + args.cxx,
             "-DXRPC_CHECK_GRPC=" + ("ON" if profile == "grpc" else "OFF")]
    run(["cmake", "-S", ROOT / "packaging/probes/cpp", "-B", build] + flags)
    run(["cmake", "--build", build, "--parallel", "2"])
    run([build / "consumer"], cwd=work)
    selected = run(["ldd", build / "consumer"], capture=True)
    if "not found" in selected or str(prefix) not in selected:
        raise ValueError("installed consumer did not load the installed SDK")
    for library in runtime:
        soname = evidence["elf"][library.name]["soname"]
        if not re.search(re.escape(soname) + r"\s+=>\s+" + re.escape(str(library.parent)) + r"/", selected):
            raise ValueError("consumer did not select installed " + soname)
    checks.append({"language": "cpp", "profile": profile, "consumer": selected})
    if args.negative_controls:
        if profile == "http":
            expected_failure(["cmake", "-S", ROOT / "packaging/probes/cpp", "-B", work / "missing-grpc"] +
                             flags[:-1] + ["-DXRPC_CHECK_GRPC=ON"])
            checks.append({"negative": "missing optional gRPC", "result": "rejected"})
        # The dynamic loader must reject an installed consumer with its actual HTTP ABI absent.
        library = next(p for p in runtime if "_http.so." in p.name)
        hidden = library.with_name(library.name + ".hidden")
        library.rename(hidden)
        try:
            expected_failure([build / "consumer"], cwd=work)
        finally:
            hidden.rename(library)
        checks.append({"negative": "missing native HTTP runtime", "result": "rejected"})


def check_python(args, file, work, checks, sdk_version):
    if not args.wheelhouse:
        raise ValueError("Python verification needs a preprovisioned offline --wheelhouse")
    run([args.python, "-c", "import sys; assert sys.version_info >= (3,8)"])
    env = work / "python-venv"
    run([args.python, "-m", "venv", env])
    interpreter = env / "bin/python"
    # No inherited source/dependency path is allowed to make an incomplete wheel appear valid.
    run([interpreter, "-m", "pip", "install", "--no-index", "--only-binary=:all:", "--find-links", args.wheelhouse,
         str(file) + ("[grpc]" if args.python_install_profile == "grpc" else "")], env={"PYTHONPATH": "", "PYTHONNOUSERSITE": "1"})
    run([interpreter, "-m", "pip", "check"], env={"PYTHONPATH": ""})
    probe = """import json, pathlib, sys, importlib.metadata as m
import xgc2_xrpc, aiohttp
from xgc2_xrpc.policy import resolve_policy
pathlib.Path(xgc2_xrpc.__file__).relative_to(pathlib.Path(sys.prefix))
assert resolve_policy({}) is not None
assert aiohttp.__version__ == '3.10.11'
assert m.version('httpx') == '0.28.1' and m.version('httpcore') == '1.0.9'
assert callable(aiohttp.web.Application)
versions = {'python':sys.version.split()[0], 'sdk':m.version('xgc2-xrpc'), 'aiohttp':aiohttp.__version__}
"""
    if args.python_install_profile == "grpc":
        probe += "import xgc2_xrpc.grpc, grpc\nassert callable(grpc.aio.server)\nversions['grpc'] = grpc.__version__\n"
    else:
        probe += "import importlib.util\nassert importlib.util.find_spec('grpc') is None\n"
    probe += "print(json.dumps(versions))\n"
    # -I ignores PYTHONPATH and cwd, proving import from the installed wheel.
    result = json.loads(run([interpreter, "-I", "-c", probe], cwd=work, capture=True))
    if result["sdk"] != sdk_version:
        raise ValueError("installed Python SDK version mismatch")
    checks.append({"language": "python", "profile": args.python_install_profile, "versions": result})
    if args.negative_controls:
        registry = next(env.glob("lib/python*/site-packages/xgc2_xrpc/_runtime_policy.json"))
        registry.rename(registry.with_suffix(".hidden"))
        try:
            expected_failure([interpreter, "-I", "-c", "from xgc2_xrpc.policy import resolve_policy; resolve_policy({})"], cwd=work)
        finally:
            registry.with_suffix(".hidden").rename(registry)
        checks.append({"negative": "missing packaged Python registry", "result": "rejected"})


def check_go(file, work, checks, sdk_version):
    target = work / "go-install"
    target.mkdir()
    extract_tar(file, target)
    if not re.search(r"^module github.com/XGC-Team/xgc2-xrpc/go$", (target / "go/go.mod").read_text(), re.M):
        raise ValueError("installed Go module identity mismatch")
    consumer = work / "go-consumer"
    consumer.mkdir()
    (consumer / "go.mod").write_text("module installed.consumer\n\ngo 1.24.0\n\nrequire github.com/XGC-Team/xgc2-xrpc/go v0.0.0\nreplace github.com/XGC-Team/xgc2-xrpc/go => " + str(target / "go") + "\n")
    (consumer / "main.go").write_text('package main\nimport ("fmt"; x "github.com/XGC-Team/xgc2-xrpc/go"; _ "github.com/XGC-Team/xgc2-xrpc/go/httpx"; _ "github.com/XGC-Team/xgc2-xrpc/go/grpcx")\nfunc main(){fmt.Println(x.HTTP, x.GRPC)}\n')
    env = {"GOPROXY": "off", "GOTOOLCHAIN": "local", "GOSUMDB": "off"}
    run(["go", "mod", "tidy"], cwd=consumer, env=env)
    run(["go", "run", "."], cwd=consumer, env=env)
    checks.append({"language": "go", "install": "standalone source archive; offline HTTP + gRPC consumer"})


def check_rust(file, work, checks, sdk_version):
    target = work / "rust-install"
    target.mkdir()
    extract_tar(file, target)
    crates = list(target.glob("xgc2-xrpc-*"))
    if len(crates) != 1:
        raise ValueError("invalid Rust crate layout")
    manifest = (crates[0] / "Cargo.toml").read_text()
    if crates[0].name != "xgc2-xrpc-" + sdk_version or not re.search(r'^name\s*=\s*"xgc2-xrpc"$', manifest, re.M) or not re.search(r'^version\s*=\s*"' + re.escape(sdk_version) + '"$', manifest, re.M):
        raise ValueError("installed Rust package identity mismatch")
    consumer = work / "rust-consumer"
    (consumer / "src").mkdir(parents=True)
    (consumer / "Cargo.toml").write_text('[package]\nname="installed-xrpc-consumer"\nversion="0.0.0"\nedition="2021"\n[dependencies]\nxgc2-xrpc={path=' + json.dumps(str(crates[0])) + ',features=["grpc"]}\n')
    (consumer / "src/main.rs").write_text('fn main() { let _: xgc2_xrpc::Limits = Default::default(); println!("installed rust HTTP + gRPC"); }\n')
    run(["cargo", "run", "--offline", "--manifest-path", consumer / "Cargo.toml", "--target-dir", work / "rust-target", "-j", "2"])
    checks.append({"language": "rust", "install": "standalone crate; offline HTTP + gRPC consumer"})


def check_node(file, work, checks, sdk_version):
    consumer = work / "node-consumer"
    consumer.mkdir()
    (consumer / "package.json").write_text('{"private":true,"dependencies":{"@xgc2/xrpc":' + json.dumps(str(file)) + '}}\n')
    run(["npm", "install", "--offline", "--ignore-scripts", "--omit=dev", "--omit=optional", "--no-audit", "--no-fund"], cwd=consumer)
    manifest = json.loads((consumer / "node_modules/@xgc2/xrpc/package.json").read_text())
    if (manifest["name"], manifest["version"]) != ("@xgc2/xrpc", sdk_version):
        raise ValueError("installed Node package identity mismatch")
    run(["node", "-e", "const x=require('@xgc2/xrpc'); if(!x || require('ws/package.json').version!=='8.22.0') process.exit(1); console.log('installed Node SDK, ws 8.22.0');"], cwd=consumer)
    checks.append({"language": "node", "format": "npm", "install": "standalone npm tarball with offline ws dependency"})


def check_node_deb(args, file, evidence, item, work, checks):
    validate_deb_paths(file)
    fields = [run(["dpkg-deb", "-f", file, field], capture=True) for field in ("Package", "Version", "Architecture", "Depends")]
    if fields != [NODE_PACKAGE, evidence["deb_version"], "all", "ca-certificates"]:
        raise ValueError("Node Debian library identity/runtime contract mismatch")
    prefix = work / "node-deb-root"
    prefix.mkdir()
    run(["dpkg-deb", "-x", file, prefix])
    identity = verify_installed(prefix, item["npm_tar_sha256"])
    if identity["debVersion"] != evidence["deb_version"] or identity["nodeSourceSha"] != item["node_source_sha"]:
        raise ValueError("Node Debian embedded source/version identity differs")
    home = work / "node-deb-home"
    home.mkdir()
    env = {"NODE_PATH": "", "NODE_OPTIONS": "", "HOME": str(home)}
    run([args.node, "--no-global-search-paths", "-e", "if(+process.versions.node.split('.')[0]<20)process.exit(1)"], env=env)
    consumer = prefix / "usr/lib/xgc2/lichtblick-web"
    consumer.mkdir()
    probe = consumer / "sol19-install-probe.cjs"
    shutil.copyfile(ROOT / "packaging/probes/node/installed.cjs", probe)
    sdk = prefix / SDK_PATH
    def execute(option=None):
        return json.loads(run([args.node, "--no-global-search-paths", probe, sdk] + ([option] if option else []), cwd=home, env=env, capture=True))
    positive = execute()
    checks.append({"language": "node", "format": "deb", "installedPath": "/" + str(SDK_PATH),
                   "npmTarSha256": identity["npmTarSha256"], "identitySha256": digest(prefix / IDENTITY_PATH),
                   "probeSha256": digest(probe), "consumer": positive})
    if args.negative_controls:
        for name, option in (("diagnostic-worker.cjs", "--missing-worker"), ("node_modules/ws", None), ("runtime-policy.json", None)):
            target = sdk / name
            hidden = target.with_name(target.name + ".hidden")
            target.rename(hidden)
            try:
                if option:
                    negative = execute(option)
                    if not negative["negative"]["verified"]:
                        raise ValueError("missing Node worker failure was not validated")
                else:
                    expected_failure([args.node, "--no-global-search-paths", probe, sdk], cwd=home, env=env)
                try:
                    verify_installed(prefix, item["npm_tar_sha256"])
                except ValueError:
                    pass
                else:
                    raise ValueError("Node install identity accepted a missing asset")
            finally:
                hidden.rename(target)
            checks.append({"negative": "missing installed Node " + name, "result": "rejected"})
        registry = sdk / "runtime-policy.json"
        original = registry.read_bytes()
        try:
            registry.write_bytes(original + b"\n")
            try:
                verify_installed(prefix, item["npm_tar_sha256"])
            except ValueError:
                pass
            else:
                raise ValueError("Node installed hash accepted modified bytes")
        finally:
            registry.write_bytes(original)
        checks.append({"negative": "modified installed Node payload identity", "result": "rejected"})
    verify_installed(prefix, item["npm_tar_sha256"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path, help="new scratch directory")
    parser.add_argument("--distribution", choices=ABI["distributions"], required=True)
    parser.add_argument("--architecture", choices=ABI["architectures"], required=True)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--cxx", default=os.environ.get("CXX", "c++"))
    parser.add_argument("--node", default="node", help="preprovisioned Node >=20 interpreter")
    parser.add_argument("--wheelhouse", type=Path)
    parser.add_argument("--cmake-prefix", action="append", default=[])
    parser.add_argument("--negative-controls", action="store_true")
    parser.add_argument("--cpp-install-profile", choices=("http", "grpc"), help="test the HTTP subset from canonical full-profile Debs")
    parser.add_argument("--python-install-profile", choices=("http", "grpc"), default="grpc")
    args = parser.parse_args()
    directory = args.artifacts.resolve()
    evidence = json.loads((directory / "package-evidence.json").read_text())
    if evidence.get("schema") != "xgc2.xrpc.package-evidence.v1":
        raise ValueError("unsupported package evidence")
    if evidence["distribution"] != args.distribution or system_suite() != args.distribution:
        raise ValueError("installation suite mismatch")
    if evidence["architecture"] != args.architecture or run(["dpkg", "--print-architecture"], capture=True) != args.architecture:
        raise ValueError("installation architecture mismatch")
    validate_artifacts(directory, evidence)
    work = args.work_dir.resolve()
    if work.exists() or work == ROOT or ROOT in work.parents or work == directory or directory in work.parents:
        raise ValueError("installation scratch must be a new directory outside checkout and artifacts")
    work.mkdir(parents=True)
    checks = []
    if "cpp" in evidence["languages"]:
        check_cpp(args, directory, evidence, work, checks)
    handlers = {"python": check_python, "go": check_go, "rust": check_rust, "node": check_node}
    sdk_version = evidence["version"].split("-", 1)[0]
    for item in evidence["artifacts"]:
        kind = item["kind"]
        if kind == "python":
            check_python(args, directory / item["path"], work, checks, sdk_version)
        elif kind == "node-deb":
            check_node_deb(args, directory / item["path"], evidence, item, work, checks)
        elif kind in handlers:
            handlers[kind](directory / item["path"], work, checks, sdk_version)
    receipt = {"schema": "xgc2.xrpc.install-evidence.v1", "created_at": utc_now(),
               "package_evidence_sha256": digest(directory / "package-evidence.json"),
               "distribution": args.distribution, "architecture": args.architecture,
               "languages": evidence["languages"], "checks": checks, "trusted_release": False}
    write_json(work / "install-evidence.json", receipt)
    print("Install evidence: " + str(work / "install-evidence.json"))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as error:
        sys.exit(str(error))
