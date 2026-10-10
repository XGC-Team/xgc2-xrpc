#!/usr/bin/env python3
"""Build offline XRPC SDK artifacts from a frozen source snapshot."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "packaging"))
from support import ABI, ROOT, digest, elf_info, run, source_files, system_suite, tar_source, utc_now, write_json
from node_deb import build_node_deb, verify_source_tar


def build_cpp(args, source, work, out, version, sdk_version):
    build = work / "cpp-build"
    stage = work / "cpp-stage"
    multiarch = run(["dpkg-architecture", "-qDEB_HOST_MULTIARCH"], capture=True)
    flags = ["-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_INSTALL_PREFIX=/usr",
             "-DCMAKE_INSTALL_LIBDIR=lib/" + multiarch, "-DBUILD_TESTING=OFF",
             "-DXGC2_XRPC_ENABLE_GRPC=" + ("ON" if args.cpp_profile == "grpc" else "OFF"),
             "-DCMAKE_CXX_COMPILER=" + args.cxx]
    if args.cmake_prefix:
        flags.append("-DCMAKE_PREFIX_PATH=" + ";".join(args.cmake_prefix))
    run(["cmake", "-S", source, "-B", build] + flags)
    run(["cmake", "--build", build, "--parallel", str(args.jobs)])
    run(["cmake", "--install", build], env={"DESTDIR": str(stage)})
    lib = stage / "usr/lib" / multiarch
    # Bootstrap is part of the base HTTP ABI: http's retained-parent and
    # credential setup calls into it at runtime, so ship its SONAME beside
    # the other non-gRPC libraries and let dpkg-shlibdeps derive OpenSSL.
    names = {"libxgc2-xrpc1": ["unix", "policy", "diagnostics", "bootstrap", "http", "json_http"]}
    if args.cpp_profile == "grpc":
        names["libxgc2-xrpc-grpc1"] = ["grpc"]
    packages = {}
    abi = {}
    for runtime_name, libraries in names.items():
        dev_name = runtime_name.removesuffix("1") + "-dev" if hasattr(str, "removesuffix") else runtime_name[:-1] + "-dev"
        for name in (runtime_name, dev_name):
            packages[name] = work / "deb-roots" / name
            (packages[name] / "DEBIAN").mkdir(parents=True)
            (packages[name] / "usr/lib" / multiarch).mkdir(parents=True)
        for library in libraries:
            actual = lib / ("libxgc2_xrpc_" + library + ".so." + sdk_version)
            info = elf_info(actual, args.distribution, args.architecture)
            expected_soname = "libxgc2_xrpc_" + library + ".so.1"
            if info["soname"] != expected_soname:
                raise ValueError("unexpected SONAME: " + str(info))
            abi[actual.name] = info
            for file in (actual, lib / expected_soname):
                shutil.copy2(file, packages[runtime_name] / "usr/lib" / multiarch / file.name, follow_symlinks=False)
            link = lib / ("libxgc2_xrpc_" + library + ".so")
            shutil.copy2(link, packages[dev_name] / "usr/lib" / multiarch / link.name, follow_symlinks=False)
        (packages[runtime_name] / "DEBIAN/shlibs").write_text("".join(
            "libxgc2_xrpc_" + name + " 1 " + runtime_name + " (>= " + version + ")\n" for name in libraries))
        # ldconfig is run only when dpkg actually installs/removes the runtime package.
        (packages[runtime_name] / "DEBIAN/triggers").write_text("activate-noawait ldconfig\n")
    base_dev = packages["libxgc2-xrpc-dev"]
    include = base_dev / "usr/include/xgc2/xrpc"
    include.mkdir(parents=True)
    for header in (stage / "usr/include/xgc2/xrpc").glob("*"):
        if header.name != "grpc.hpp":
            shutil.copy2(header, include / header.name)
        elif args.cpp_profile == "grpc":
            grpc_include = packages["libxgc2-xrpc-grpc-dev"] / "usr/include/xgc2/xrpc"
            grpc_include.mkdir(parents=True, exist_ok=True)
            shutil.copy2(header, grpc_include / header.name)
    for cmake in (lib / "cmake/XgcXrpc").glob("*"):
        owner = "libxgc2-xrpc-grpc-dev" if cmake.name.startswith("XgcXrpcGrpcTargets") else "libxgc2-xrpc-dev"
        target = packages[owner] / "usr/lib" / multiarch / "cmake/XgcXrpc"
        target.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cmake, target / cmake.name)
    deps_work = work / "shlibdeps"
    (deps_work / "debian").mkdir(parents=True)
    (deps_work / "debian/control").write_text("Source: xgc2-xrpc\nSection: libs\nPriority: optional\nMaintainer: XGC Team <apt@example.com>\n\n" +
        "\n".join("Package: " + name + "\nArchitecture: any\n" for name in names))
    (deps_work / "debian/shlibs.local").write_text("".join((packages[name] / "DEBIAN/shlibs").read_text() for name in names))
    result = []
    for name, root in packages.items():
        if name in names:
            argv = ["dpkg-shlibdeps", "-O", "-x" + name, "-l" + str(lib)]
            argv += ["-S" + str(Path(p).resolve()) for p in args.shlibdeps_package_root]
            argv += ["-l" + str(Path(p).resolve()) for p in args.shlibdeps_library_path]
            argv += ["-e" + str(lib / ("libxgc2_xrpc_" + item + ".so." + sdk_version)) for item in names[name]]
            depends = run(argv, cwd=deps_work, capture=True)
            if not depends.startswith("shlibs:Depends=") or not depends.split("=", 1)[1]:
                raise ValueError("dpkg-shlibdeps produced no runtime dependencies")
            depends = depends.split("=", 1)[1]
        else:
            runtime_name = name.removesuffix("-dev") + "1" if hasattr(str, "removesuffix") else name[:-4] + "1"
            depends = runtime_name + " (= " + version + ")"
            if name == "libxgc2-xrpc-dev":
                depends += ", nlohmann-json3-dev (>= 3.7)"
            if name == "libxgc2-xrpc-grpc-dev":
                depends += ", libxgc2-xrpc-dev (= " + version + "), libgrpc++-dev (>= 1.16), libprotobuf-dev"
        section = "libs" if name in names else "libdevel"
        (root / "DEBIAN/control").write_text(
            "Package: " + name + "\nVersion: " + version + "\nArchitecture: " + args.architecture +
            "\nMulti-Arch: same\nSection: " + section + "\nPriority: optional\nMaintainer: XGC Team <apt@example.com>\nDepends: " + depends +
            "\nDescription: XGC2 XRPC " + ("native shared library" if name in names else "C++20 development interface") + "\n Bounded transport SDK; providers own their domain and lifecycle.\n")
        doc = root / "usr/share/doc" / name
        doc.mkdir(parents=True)
        (doc / "copyright").write_text("XGC Team\nLicense: Apache-2.0\nSee /usr/share/common-licenses/Apache-2.0.\n")
        deb = out / (name + "_" + version + "_" + args.architecture + ".deb")
        run(["dpkg-deb", "--root-owner-group", "--build", root, deb])
        result.append({"kind": "deb", "path": deb.name, "package": name, "depends": depends})
    return result, abi


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="new directory outside the source checkout")
    parser.add_argument("--distribution", choices=ABI["distributions"], required=True)
    parser.add_argument("--architecture", choices=ABI["architectures"], required=True)
    parser.add_argument("--language", choices=("cpp", "python", "go", "rust", "node"), action="append")
    parser.add_argument("--cpp-profile", choices=("http", "grpc"), default="grpc")
    parser.add_argument("--cxx", default=os.environ.get("CXX", "c++"))
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--cmake-prefix", action="append", default=[])
    parser.add_argument("--shlibdeps-package-root", action="append", default=[], help="preprovisioned dependency data + DEBIAN metadata (local isolated validation)")
    parser.add_argument("--shlibdeps-library-path", action="append", default=[])
    parser.add_argument("--release-source-sha", help="require a clean exact committed source for a release candidate")
    parser.add_argument("--node-tarball", type=Path, help="local probe: install an exact committed npm artifact instead of the working Node tree")
    parser.add_argument("--node-tarball-sha256")
    parser.add_argument("--node-source-sha")
    parser.add_argument("--jobs", type=int, choices=(1, 2), default=2)
    parser.add_argument("--source-date-epoch", type=int, default=int(os.environ.get("SOURCE_DATE_EPOCH", "0")))
    args = parser.parse_args()
    if args.source_date_epoch < 0:
        raise ValueError("SOURCE_DATE_EPOCH must be nonnegative")
    out = args.output.resolve()
    if out == ROOT or ROOT in out.parents or out.exists():
        raise ValueError("output must be a new directory outside the checkout")
    if system_suite() != args.distribution:
        raise ValueError("build suite must match /etc/os-release; no cross-suite relabeling")
    actual_arch = run(["dpkg", "--print-architecture"], capture=True)
    if actual_arch != args.architecture:
        raise ValueError("native build architecture must match the requested cell")
    languages = sorted(set(args.language or ("cpp", "python", "go", "rust", "node")))
    if any((args.node_tarball, args.node_tarball_sha256, args.node_source_sha)):
        if not all((args.node_tarball, args.node_tarball_sha256, args.node_source_sha)) or "node" not in languages or args.release_source_sha:
            raise ValueError("pinned Node artifact requires tarball, SHA256 and exact Node commit; local probes only")
        if not re.fullmatch(r"[0-9a-f]{64}", args.node_tarball_sha256) or args.node_tarball.is_symlink() or digest(args.node_tarball) != args.node_tarball_sha256:
            raise ValueError("pinned Node npm artifact SHA256 mismatch")
        verify_source_tar(args.node_tarball, args.node_source_sha)
    source_sha = None
    committed = {}
    if args.release_source_sha:
        if not re.fullmatch(r"[0-9a-f]{40}", args.release_source_sha):
            raise ValueError("release source SHA must be a full lowercase commit ID")
        source_sha = run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture=True)
        if source_sha != args.release_source_sha or run(["git", "status", "--porcelain"], cwd=ROOT, capture=True):
            raise ValueError("release candidate requires clean exact-source checkout")
        if languages != ["cpp", "go", "node", "python", "rust"] or args.cpp_profile != "grpc":
            raise ValueError("release candidate requires all SDKs and native C++ gRPC")
        if args.cmake_prefix or args.shlibdeps_package_root or args.shlibdeps_library_path:
            raise ValueError("release toolchains must be installed in the controlled image; local dependency extraction is not release evidence")
        tree = run(["git", "ls-tree", "-rz", source_sha], cwd=ROOT, capture=True)
        for record in tree.split("\0"):
            if record:
                identity, name = record.split("\t", 1)
                mode, kind, oid = identity.split()
                if kind == "blob" and mode in ("100644", "100755"):
                    committed[name] = oid
    text = (ROOT / ".xgc2/product.yml").read_text()
    product_version = re.search(r"^version: (\S+)$", text, re.M).group(1)
    sdk_version = product_version.split("-", 1)[0]
    version = product_version + "~" + args.distribution
    out.mkdir(parents=True)
    work = out / "work"
    source = work / "source"
    source.mkdir(parents=True)
    inputs = {}
    # Every selected language is snapshotted before any compiler/backend is invoked.
    for directory in languages + ["contracts", "packaging", "tools", ".xgc2"]:
        for path in source_files(ROOT / directory):
            relative = path.relative_to(ROOT)
            if source_sha and str(relative) not in committed:
                continue
            target = source / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
            inputs[str(relative)] = digest(target)
    for name in ("CMakeLists.txt", ".xgc2/product.yml", "packaging/runtime-abi.json"):
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, target)
        inputs[name] = digest(target)
    snapshot_id = hashlib.sha256(json.dumps({"files": inputs, "languages": languages,
        "cpp_profile": args.cpp_profile, "source_date_epoch": args.source_date_epoch,
        "node_artifact_override": {"sha256": args.node_tarball_sha256, "source_sha": args.node_source_sha}},
        sort_keys=True).encode()).hexdigest()
    if source_sha:
        for name in inputs:
            if name not in committed or run(["git", "hash-object", "--no-filters", source / name], cwd=ROOT, capture=True) != committed[name]:
                raise ValueError("build input is not exact committed source: " + name)
    if not args.release_source_sha:
        version = product_version + "+probe." + snapshot_id[:12] + "~" + args.distribution
    evidence = {"schema": "xgc2.xrpc.package-evidence.v1", "created_at": utc_now(),
                "distribution": args.distribution, "architecture": args.architecture,
                "version": product_version, "languages": languages, "cpp_profile": args.cpp_profile,
                "source_files": inputs, "snapshot_id": snapshot_id, "deb_version": version,
                "source_date_epoch": args.source_date_epoch,
        "source_sha": source_sha, "release_candidate": bool(source_sha),
                "artifacts": [], "elf": {}, "trusted_release": False}
    if "cpp" in languages:
        artifacts, evidence["elf"] = build_cpp(args, source, work, out, version, sdk_version)
        evidence["artifacts"] += artifacts
    if "python" in languages:
        run([args.python, "-c", "import sys; assert sys.version_info >= (3,8), 'XRPC requires Python >= 3.8'"])
        wheels = work / "wheels"
        wheels.mkdir()
        run([args.python, "-m", "pip", "wheel", "--no-deps", "--no-index", "--no-build-isolation",
             "--wheel-dir", wheels, source / "python"])
        found = list(wheels.glob("xgc2_xrpc-" + sdk_version + "-*.whl"))
        if len(found) != 1:
            raise ValueError("expected one exact-version Python wheel")
        shutil.copy2(found[0], out / found[0].name)
        evidence["artifacts"].append({"kind": "python", "path": found[0].name})
    if "go" in languages:
        name = "xgc2-xrpc-go-" + sdk_version + ".tar.gz"
        tar_source(source / "go", out / name, "go", args.source_date_epoch)
        evidence["artifacts"].append({"kind": "go", "path": name})
    if "rust" in languages:
        cargo_target = work / "cargo-target"
        run(["cargo", "package", "--offline", "--locked", "--no-verify", "--allow-dirty", "--manifest-path",
             source / "rust/Cargo.toml", "--target-dir", cargo_target])
        crate = cargo_target / "package" / ("xgc2-xrpc-" + sdk_version + ".crate")
        shutil.copy2(crate, out / crate.name)
        evidence["artifacts"].append({"kind": "rust", "path": crate.name})
    if "node" in languages:
        node_sha = args.node_source_sha or source_sha
        if args.node_tarball:
            name = "xgc2-xrpc-" + sdk_version + ".tgz"
            shutil.copyfile(args.node_tarball, out / name)
            if digest(out / name) != args.node_tarball_sha256:
                raise ValueError("pinned Node artifact changed while copying")
            lock = work / "committed-node-package-lock.json"
            lock.write_text(run(["git", "show", node_sha + ":node/package-lock.json"], cwd=ROOT, capture=True) + "\n")
        else:
            pack = run(["npm", "pack", "--offline", "--ignore-scripts", "--json", "--pack-destination", out],
                       cwd=source / "node", capture=True)
            metadata = json.loads(pack)[0]
            if metadata["name"] != "@xgc2/xrpc" or metadata["version"] != sdk_version:
                raise ValueError("Node manifest version differs from product version")
            name = metadata["filename"]
            lock = source / "node/package-lock.json"
        evidence["artifacts"].append({"kind": "node", "path": name, "node_source_sha": node_sha})
        evidence["artifacts"].append(build_node_deb(out / name, lock, work, out, version, sdk_version,
                                                   args.source_date_epoch, node_sha))
    for artifact in evidence["artifacts"]:
        artifact["sha256"] = digest(out / artifact["path"])
        artifact["bytes"] = (out / artifact["path"]).stat().st_size
    evidence["toolchain"] = {"python": run([args.python, "--version"], capture=True),
                             "cxx": run([args.cxx, "--version"], capture=True).splitlines()[0] if "cpp" in languages else None,
                             "node": run(["node", "--version"], capture=True) if "node" in languages else None,
                             "npm": run(["npm", "--version"], capture=True) if "node" in languages else None}
    if source_sha:
        if run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture=True) != source_sha or run(["git", "status", "--porcelain"], cwd=ROOT, capture=True):
            raise ValueError("source changed while constructing release artifacts")
        if any(digest(ROOT / name) != value for name, value in inputs.items()):
            raise ValueError("source bytes changed while constructing release artifacts")
    write_json(out / "package-evidence.json", evidence)
    print("Package evidence: " + str(out / "package-evidence.json"))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, AttributeError) as error:
        sys.exit(str(error))
