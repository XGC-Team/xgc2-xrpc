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
from support import ABI, ROOT, cpp_package_set, digest, elf_info, extract_tar, run, system_suite, utc_now, write_json
from node_deb import NODE, SDKS, TS, WS_VERSION, locked_ws_archive, npm_payload, regular_hashes, verify_installed

KIND_LANGUAGE = {"deb": "cpp", "python": "python", "go": "go", "rust": "rust", "node": "node",
                 "node-deb": "node", "ts": "ts", "ts-deb": "ts"}


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
        if item["kind"] not in KIND_LANGUAGE:
            raise ValueError("unknown artifact kind")
        kinds.add(KIND_LANGUAGE[item["kind"]])
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
    expected = set()
    if "cpp" in kinds:
        for package in cpp_package_set(evidence["cpp_components"]):
            expected |= {package["runtime"], package["dev"]}
    if set(debs) != expected or len(debs) != len(expected):
        raise ValueError("unexpected or duplicate Debian package set")
    for sdk in SDKS.values():
        sdk_debs = [item for item in evidence["artifacts"] if item["kind"] == sdk.language + "-deb"]
        if len(sdk_debs) != (1 if sdk.language in kinds else 0) or any(item["package"] != sdk.deb for item in sdk_debs):
            raise ValueError("expected the actual %s Debian SDK alongside its npm artifact" % sdk.language)
        if sdk_debs and sdk_debs[0]["npm_tar_sha256"] != next(item["sha256"] for item in evidence["artifacts"] if item["kind"] == sdk.language):
            raise ValueError("%s Debian SDK is not bound to this npm artifact" % sdk.language)
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
                    if not re.fullmatch(r"usr/lib/[^/]+/libxgc2_xrpc_[a-z_]+\.so(?:\.[0-9]+)?", name):
                        raise ValueError("unsupported Debian symlink: " + name)
                    if not re.fullmatch(r"libxgc2_xrpc_[a-z_]+\.so\.(?:[0-9]+|[0-9]+\.[0-9]+\.[0-9]+)", member.linkname):
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


def cmake_project(source, build, flags, jobs=2):
    # No -S/-B or --parallel: the same commands work with the CMake 3.10 of Ubuntu 18.04.
    build.mkdir(parents=True)
    run(["cmake", source] + flags, cwd=build)
    run(["cmake", "--build", ".", "--", "-j" + str(jobs)], cwd=build)


def component_of(library):
    return re.fullmatch(r"libxgc2_xrpc_([a-z_]+)\.so\..*", library.name).group(1)


def check_cpp(args, directory, evidence, work, checks, sdk_version):
    components = args.cpp_install_components.split(",") if args.cpp_install_components else evidence["cpp_components"]
    if not set(components) <= set(evidence["cpp_components"]):
        raise ValueError("installation requested for components the artifacts do not carry")
    # A subset installs the packages whose components it names completely; the CMake package
    # files and the udp headers sit in the udp development package, which every selection needs.
    packages = [p for p in cpp_package_set(evidence["cpp_components"]) if set(p["components"]) <= set(components)]
    wanted = {name for p in packages for name in (p["runtime"], p["dev"])}
    if "libxgc2-xrpc-udp-dev" not in wanted:
        raise ValueError("the udp component is part of every installation")
    prefix = work / "cpp-root"
    prefix.mkdir()
    owners = {}
    runtime = []
    # Validate every package before any package can create directories or links.
    for item in evidence["artifacts"]:
        if item["kind"] == "deb":
            validate_deb_paths(directory / item["path"])
    for item in evidence["artifacts"]:
        if item["kind"] != "deb" or item["package"] not in wanted:
            continue
        file = directory / item["path"]
        package = run(["dpkg-deb", "-f", file, "Package"], capture=True)
        version = run(["dpkg-deb", "-f", file, "Version"], capture=True)
        arch = run(["dpkg-deb", "-f", file, "Architecture"], capture=True)
        if package != item["package"] or version != evidence["deb_version"] or arch != args.architecture:
            raise ValueError("Debian package identity mismatch: " + file.name)
        depends = run(["dpkg-deb", "-f", file, "Depends"], capture=True)
        if package.endswith("-dev"):
            owner = next(p for p in packages if p["dev"] == package)
            if owner["runtime"] + " (= " + version + ")" not in depends:
                raise ValueError("development package lacks exact runtime dependency")
            # The development packages form a chain (grpc -> http -> udp), each exactly versioned.
            for needed in owner["dev_depends"]:
                if needed.startswith("libxgc2-xrpc") and needed.format(version=version) not in depends:
                    raise ValueError("development package lacks the exact dependency " + needed.split(" ")[0])
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
    if len(runtime) != len(components):
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
    http = "json_http" in components
    grpc = "grpc" in components
    build = work / "cpp-consumer"
    cmake_prefix = [str(prefix / "usr")] + args.cmake_prefix
    flags = ["-DCMAKE_PREFIX_PATH=" + ";".join(cmake_prefix), "-DCMAKE_CXX_COMPILER=" + args.cxx,
             "-DXRPC_VERSION=" + sdk_version, "-DXRPC_CHECK_HTTP=" + ("ON" if http else "OFF"),
             "-DXRPC_CHECK_GRPC=" + ("ON" if grpc else "OFF"), "-DXRPC_CHECK_UDP=ON"]
    cmake_project(ROOT / "packaging/probes/cpp", build, flags)
    # The consumer of the json_http (and grpc) profile loads every library except udp; the
    # udp consumer loads udp alone. Each must select the installed copies.
    executables = {"udp_consumer": [lib for lib in runtime if component_of(lib) == "udp"]}
    if http:
        executables["consumer"] = [lib for lib in runtime if component_of(lib) != "udp"]
    for name, libraries in executables.items():
        run([build / name], cwd=work)
        selected = run(["ldd", build / name], capture=True)
        if "not found" in selected or str(prefix) not in selected:
            raise ValueError("installed consumer did not load the installed SDK: " + name)
        for library in libraries:
            soname = evidence["elf"][library.name]["soname"]
            if not re.search(re.escape(soname) + r"\s+=>\s+" + re.escape(str(library.parent)) + r"/", selected):
                raise ValueError("consumer did not select installed " + soname)
        checks.append({"language": "cpp", "components": components, "consumer": name, "loaded": selected})
    if args.negative_controls:
        if http and not grpc and "grpc" in evidence["cpp_components"]:
            missing = work / "missing-grpc-build"
            missing.mkdir()
            expected_failure(["cmake", ROOT / "packaging/probes/cpp"] + flags + ["-DXRPC_CHECK_GRPC=ON"], cwd=missing)
            checks.append({"negative": "missing optional gRPC", "result": "rejected"})
        # The dynamic loader must reject an installed consumer with its actual ABI absent.
        name = "consumer" if http else "udp_consumer"
        library = next(p for p in runtime if component_of(p) == ("http" if http else "udp"))
        hidden = library.with_name(library.name + ".hidden")
        library.rename(hidden)
        try:
            expected_failure([build / name], cwd=work)
        finally:
            hidden.rename(library)
        checks.append({"negative": "missing native runtime of " + component_of(library), "result": "rejected"})


def check_python(args, file, work, checks, sdk_version):
    if not args.wheelhouse:
        raise ValueError("Python verification needs a preprovisioned offline --wheelhouse")
    run([args.python, "-c", "import sys; assert sys.version_info >= (3,8)"])
    env = work / "python-venv"
    run([args.python, "-m", "venv", env])
    interpreter = env / "bin/python"
    # No inherited source/dependency path is allowed to make an incomplete wheel appear valid.
    # Use the image-owned pip to target the native isolated interpreter. Focal's
    # venv bootstrap pip cannot recognize its modern manylinux arm64 wheels.
    run([args.python, "-m", "pip", "--python", interpreter, "install", "--no-index", "--only-binary=:all:", "--find-links", args.wheelhouse,
         str(file)], env={"PYTHONPATH": "", "PYTHONNOUSERSITE": "1"})
    run([interpreter, "-m", "pip", "check"], env={"PYTHONPATH": ""})
    probe = """import json, os, pathlib, sys, tempfile, importlib.metadata as m
import xgc2_xrpc, aiohttp
from xgc2_xrpc import Client, Host, Limits, Runtime, DISPOSITIONS, new_instance_id
pathlib.Path(xgc2_xrpc.__file__).relative_to(pathlib.Path(sys.prefix))
assert aiohttp.__version__ == '3.10.11'
assert m.version('httpx') == '0.28.1' and m.version('httpcore') == '1.0.9'
assert callable(aiohttp.web.Application)
assert DISPOSITIONS == ('not_sent', 'outcome_unknown', 'response_received') and len(new_instance_id()) == 32
assert Limits().call_timeout == 30.0 and Limits().body_bytes == 1048576
# A real round trip over a private Unix socket with the installed code.
directory = tempfile.mkdtemp()
path = os.path.join(directory, 'rpc.sock')
runtime = Runtime(blocking_workers=2, max_connections=8, max_calls=8)
instance = new_instance_id()
with Host(path, {('POST', '/echo'): lambda context, request: request}, runtime=runtime, instance_id=instance) as host, \\
        Client(path, runtime=runtime, instance_id=instance) as client:
    assert client.json('/echo', {'n': 1}) == {'n': 1}
runtime.close()
for name in ('grpc.py', 'policy.py', '_runtime_policy.json'):
    assert not (pathlib.Path(xgc2_xrpc.__file__).parent / name).exists(), name
versions = {'python': sys.version.split()[0], 'sdk': m.version('xgc2-xrpc'), 'aiohttp': aiohttp.__version__}
print(json.dumps(versions))
"""
    # -I ignores PYTHONPATH and cwd, proving import from the installed wheel.
    result = json.loads(run([interpreter, "-I", "-c", probe], cwd=work, capture=True))
    if result["sdk"] != sdk_version:
        raise ValueError("installed Python SDK version mismatch")
    checks.append({"language": "python", "versions": result})
    if args.negative_controls:
        module = next(env.glob("lib/python*/site-packages/xgc2_xrpc/http.py"))
        module.rename(module.with_suffix(".hidden"))
        try:
            expected_failure([interpreter, "-I", "-c", "import xgc2_xrpc"], cwd=work)
        finally:
            module.with_suffix(".hidden").rename(module)
        checks.append({"negative": "missing packaged Python module", "result": "rejected"})


GO_CONSUMER = '''package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	x "github.com/XGC-Team/xgc2-xrpc/go"
	_ "github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	_ "github.com/XGC-Team/xgc2-xrpc/go/httpx"
	"github.com/XGC-Team/xgc2-xrpc/go/udpx"
)

// A real udp.v1 call through the installed module: a server, a client and the
// Dispatcher's method addressing over loopback.
func main() {
	ring, err := udpx.NewKeyRing(map[uint32][]byte{1: make([]byte, udpx.KeyLen)})
	check(err)
	server, err := udpx.Listen("127.0.0.1:0", udpx.ServerConfig{Keys: ring})
	check(err)
	defer server.Close()
	check(server.Handle("probe.v1/Echo", func(_ context.Context, r udpx.Request, w *udpx.Responder) { _ = w.Reply(r.Body) }))
	client, err := udpx.NewClient(udpx.ClientConfig{Keys: ring})
	check(err)
	profile, err := udpx.NewProfile(client)
	check(err)
	dispatcher, err := x.NewDispatcher(map[string]x.Caller{x.UDP: profile})
	check(err)
	ref := x.ServiceRef{TargetID: "robot", Service: "probe.v1", APIVersion: "v1", Profile: x.UDP, Endpoint: x.Endpoint{Kind: "udp", Address: server.Addr().String()}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := dispatcher.CallMethod(ctx, ref, "probe.v1/Echo", json.RawMessage(`{"ok":true}`))
	check(err)
	if string(result.Payload) != `{"ok":true}` || result.InstanceID != server.InstanceID() {
		panic("unexpected result " + string(result.Payload))
	}
	fmt.Println(x.HTTP, x.GRPC, x.UDP)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
'''


def check_go(file, work, checks, sdk_version):
    target = work / "go-install"
    target.mkdir()
    extract_tar(file, target)
    if not re.search(r"^module github.com/XGC-Team/xgc2-xrpc/go$", (target / "go/go.mod").read_text(), re.M):
        raise ValueError("installed Go module identity mismatch")
    consumer = work / "go-consumer"
    consumer.mkdir()
    (consumer / "go.mod").write_text("module installed.consumer\n\ngo 1.24.0\n\nrequire github.com/XGC-Team/xgc2-xrpc/go v0.0.0\nreplace github.com/XGC-Team/xgc2-xrpc/go => " + str(target / "go") + "\n")
    (consumer / "main.go").write_text(GO_CONSUMER)
    env = {"GOPROXY": "off", "GOTOOLCHAIN": "local", "GOSUMDB": "off"}
    run(["go", "mod", "tidy"], cwd=consumer, env=env)
    run(["go", "run", "."], cwd=consumer, env=env)
    checks.append({"language": "go", "install": "standalone source archive; offline http.v1, grpc.v1 and udp.v1 consumer"})


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
    (consumer / "Cargo.toml").write_text('[package]\nname="installed-xrpc-consumer"\nversion="0.0.0"\nedition="2021"\n[dependencies]\nxgc2-xrpc={path=' + json.dumps(str(crates[0])) + '}\n')
    (consumer / "src/main.rs").write_text('fn main() { let limits: xgc2_xrpc::Limits = Default::default(); assert!(!xgc2_xrpc::new_instance_id().unwrap().is_empty()); drop(limits); println!("installed rust http.v1 host and client"); }\n')
    run(["cargo", "run", "--offline", "--manifest-path", consumer / "Cargo.toml", "--target-dir", work / "rust-target", "-j", "2"])
    checks.append({"language": "rust", "install": "standalone crate; offline http.v1 consumer"})


def check_node(file, work, checks, sdk_version):
    consumer = work / "node-consumer"
    consumer.mkdir()
    ws_tar, _ = locked_ws_archive(ROOT / "node/package-lock.json", consumer)
    write_json(consumer / "package.json", {"private": True, "dependencies": {
        "@xgc2/xrpc": "file:" + str(file.resolve()), "ws": "file:" + str(ws_tar.resolve())}})
    run(["npm", "install", "--offline", "--ignore-scripts", "--omit=dev", "--omit=optional", "--no-audit", "--no-fund"], cwd=consumer)
    if (regular_hashes(consumer / "node_modules/@xgc2/xrpc") != npm_payload(file) or
            regular_hashes(consumer / "node_modules/ws") != npm_payload(ws_tar)):
        raise ValueError("installed Node consumer differs from pinned archives")
    manifest = json.loads((consumer / "node_modules/@xgc2/xrpc/package.json").read_text())
    if (manifest["name"], manifest["version"]) != ("@xgc2/xrpc", sdk_version):
        raise ValueError("installed Node package identity mismatch")
    run(["node", "-e", "const x=require('@xgc2/xrpc'); if(!x || require('ws/package.json').version!=='" + WS_VERSION + "') process.exit(1); console.log('installed Node SDK, ws " + WS_VERSION + "');"], cwd=consumer)
    checks.append({"language": "node", "format": "npm", "install": "standalone npm tarball with offline ws dependency"})


def check_ts(args, file, work, checks, sdk_version):
    consumer = work / "ts-consumer"
    consumer.mkdir()
    write_json(consumer / "package.json", {"private": True, "dependencies": {"@xgc2/xrpc-client": "file:" + str(file.resolve())}})
    run(["npm", "install", "--offline", "--ignore-scripts", "--omit=dev", "--omit=optional", "--no-audit", "--no-fund"], cwd=consumer)
    installed = consumer / "node_modules/@xgc2/xrpc-client"
    if regular_hashes(installed) != npm_payload(file):
        raise ValueError("installed TypeScript client differs from the pinned archive")
    manifest = json.loads((installed / "package.json").read_text())
    if (manifest["name"], manifest["version"], manifest.get("dependencies", {})) != ("@xgc2/xrpc-client", sdk_version, {}):
        raise ValueError("installed TypeScript client identity mismatch")
    probe = consumer / "installed-probe.mjs"
    shutil.copyfile(ROOT / "packaging/probes/ts/installed.mjs", probe)
    report = json.loads(run([args.node, probe, installed.resolve(), sdk_version], cwd=consumer, capture=True))
    checks.append({"language": "ts", "format": "npm", "install": "standalone npm tarball without dependencies", "consumer": report})


def check_node_sdk(args, sdk, file, evidence, item, work, checks, sdk_version):
    """The architecture-independent Debian package of an npm SDK: extracted, verified against
    its identity and run by the probe from the path an ancestor node_modules lookup finds."""
    validate_deb_paths(file)
    fields = [run(["dpkg-deb", "-f", file, field], capture=True) for field in ("Package", "Version", "Architecture", "Depends")]
    if fields != [sdk.deb, evidence["deb_version"], "all", "ca-certificates"]:
        raise ValueError("%s Debian library identity/runtime contract mismatch" % sdk.language)
    prefix = work / (sdk.language + "-deb-root")
    prefix.mkdir()
    run(["dpkg-deb", "-x", file, prefix])
    identity = verify_installed(sdk, prefix, item["npm_tar_sha256"])
    if identity["debVersion"] != evidence["deb_version"] or identity["nodeSourceSha"] != item["node_source_sha"] or identity["sdkVersion"] != sdk_version:
        raise ValueError("%s Debian embedded source/version identity differs" % sdk.language)
    home = work / (sdk.language + "-deb-home")
    home.mkdir()
    env = {"NODE_PATH": "", "NODE_OPTIONS": "", "HOME": str(home)}
    run([args.node, "--no-global-search-paths", "-e", "if(+process.versions.node.split('.')[0]<20)process.exit(1)"], env=env)
    sdk_path = prefix / sdk.install_path
    if sdk.language == "node":
        consumer = prefix / "usr/lib/xgc2/lichtblick-web"
        consumer.mkdir()
        probe = consumer / "xrpc-install-probe.cjs"
        shutil.copyfile(ROOT / "packaging/probes/node/installed.cjs", probe)
    else:
        consumer = prefix / "usr/lib/xgc2/xrpc-client-probe"
        consumer.mkdir()
        probe = consumer / "xrpc-install-probe.mjs"
        shutil.copyfile(ROOT / "packaging/probes/ts/installed.mjs", probe)

    def execute(option=None):
        argv = [args.node, "--no-global-search-paths", probe, sdk_path, sdk_version] + ([option] if option else [])
        return json.loads(run(argv, cwd=home, env=env, capture=True))
    positive = execute()
    checks.append({"language": sdk.language, "format": "deb", "installedPath": "/" + str(sdk.install_path),
                   "npmTarSha256": identity["npmTarSha256"], "identitySha256": digest(prefix / sdk.identity_path),
                   "probeSha256": digest(probe), "consumer": positive})
    if args.negative_controls:
        # (installed name, probe option or None for "the probe must fail")
        controls = ([("diagnostic-worker.cjs", "--missing-worker"), ("node_modules/ws", None), ("unix.cjs", None)]
                    if sdk.language == "node" else [("dist/call.js", None)])
        for name, option in controls:
            target = sdk_path / name
            hidden = target.with_name(target.name + ".hidden")
            target.rename(hidden)
            try:
                if option:
                    negative = execute(option)
                    if not negative["negative"]["verified"]:
                        raise ValueError("missing Node worker failure was not validated")
                else:
                    expected_failure([args.node, "--no-global-search-paths", probe, sdk_path, sdk_version], cwd=home, env=env)
                try:
                    verify_installed(sdk, prefix, item["npm_tar_sha256"])
                except ValueError:
                    pass
                else:
                    raise ValueError("installed identity accepted a missing asset")
            finally:
                hidden.rename(target)
            checks.append({"negative": "missing installed %s %s" % (sdk.language, name), "result": "rejected"})
        modified = sdk_path / ("index.cjs" if sdk.language == "node" else "dist/index.js")
        original = modified.read_bytes()
        try:
            modified.write_bytes(original + b"\n")
            try:
                verify_installed(sdk, prefix, item["npm_tar_sha256"])
            except ValueError:
                pass
            else:
                raise ValueError("installed hash accepted modified bytes")
        finally:
            modified.write_bytes(original)
        checks.append({"negative": "modified installed %s payload identity" % sdk.language, "result": "rejected"})
    verify_installed(sdk, prefix, item["npm_tar_sha256"])


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
    parser.add_argument("--cpp-install-components", help="comma-separated subset of the built C++ components to install and check")
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
    sdk_version = evidence["version"].split("-", 1)[0]
    if "cpp" in evidence["languages"]:
        check_cpp(args, directory, evidence, work, checks, sdk_version)
    for item in evidence["artifacts"]:
        kind = item["kind"]
        file = directory / item["path"]
        if kind == "python":
            check_python(args, file, work, checks, sdk_version)
        elif kind == "go":
            check_go(file, work, checks, sdk_version)
        elif kind == "rust":
            check_rust(file, work, checks, sdk_version)
        elif kind == "node":
            check_node(file, work, checks, sdk_version)
        elif kind == "ts":
            check_ts(args, file, work, checks, sdk_version)
        elif kind in ("node-deb", "ts-deb"):
            check_node_sdk(args, SDKS[KIND_LANGUAGE[kind]], file, evidence, item, work, checks, sdk_version)
    receipt = {"schema": "xgc2.xrpc.install-evidence.v1", "created_at": utc_now(),
               "package_evidence_sha256": digest(directory / "package-evidence.json"),
               "distribution": args.distribution, "architecture": args.architecture,
               "languages": evidence["languages"], "cpp_components": evidence["cpp_components"],
               "checks": checks, "trusted_release": False}
    write_json(work / "install-evidence.json", receipt)
    print("Install evidence: " + str(work / "install-evidence.json"))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as error:
        sys.exit(str(error))
