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
from support import (ABI, ROOT, cpp_package_set, digest, elf_info, run, source_files, suite_cpp_components,
                     suite_languages, system_suite, tar_source, utc_now, write_json)
from node_deb import NODE, TS, build_node_deb, verify_source_tar

LANGUAGES = ("cpp", "go", "node", "python", "rust", "ts")


def copy_link(source, target_directory):
    target_directory.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target_directory / source.name, follow_symlinks=False)


def build_cpp(args, source, work, out, version, sdk_version, components):
    """Build the selected components once and split the install into Debian packages.

    Every file a component installs carries the component's name as its CMake install
    component (and the CMake package files carry `config`), so each package is assembled
    from exactly the components it ships. Only CMake features of 3.10, the version Ubuntu
    18.04 ships, are used: no -S/-B, --install or --parallel.
    """
    packages = cpp_package_set(components)
    build = work / "cpp-build"
    stage = work / "cpp-stage"
    build.mkdir()
    multiarch = run(["dpkg-architecture", "-qDEB_HOST_MULTIARCH"], capture=True)
    flags = ["-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_INSTALL_PREFIX=/usr",
             "-DCMAKE_INSTALL_LIBDIR=lib/" + multiarch, "-DBUILD_TESTING=OFF",
             "-DXGC2_XRPC_COMPONENTS=" + ";".join(components), "-DCMAKE_CXX_COMPILER=" + args.cxx]
    if args.cmake_prefix:
        flags.append("-DCMAKE_PREFIX_PATH=" + ";".join(args.cmake_prefix))
    run(["cmake", source] + flags, cwd=build)
    run(["cmake", "--build", ".", "--", "-j" + str(args.jobs)], cwd=build)
    for component in list(components) + ["config"]:
        run(["cmake", "-DCMAKE_INSTALL_COMPONENT=" + component, "-P", "cmake_install.cmake"],
            cwd=build, env={"DESTDIR": str(stage / component)})
    lib_relative = Path("usr/lib") / multiarch
    cmake_relative = lib_relative / "cmake/XgcXrpc"
    roots = {}
    accounted = set()
    abi = {}
    shlibs = {}
    for package in packages:
        runtime, dev = package["runtime"], package["dev"]
        generation = ABI["cpp"]["soname"][package["components"][0]]
        for name in (runtime, dev):
            roots[name] = work / "deb-roots" / name
            (roots[name] / "DEBIAN").mkdir(parents=True)
        shlibs[runtime] = ""
        for component in package["components"]:
            installed = stage / component
            lib = installed / lib_relative
            actual = lib / ("libxgc2_xrpc_" + component + ".so." + sdk_version)
            info = elf_info(actual, args.distribution, args.architecture)
            expected_soname = "libxgc2_xrpc_" + component + ".so." + str(generation)
            if info["soname"] != expected_soname:
                raise ValueError("unexpected SONAME: " + str(info))
            abi[actual.name] = info
            for file in (actual, lib / expected_soname):
                copy_link(file, roots[runtime] / lib_relative)
                accounted.add(file)
            link = lib / ("libxgc2_xrpc_" + component + ".so")
            copy_link(link, roots[dev] / lib_relative)
            accounted.add(link)
            shlibs[runtime] += "libxgc2_xrpc_%s %d %s (>= %s)\n" % (component, generation, runtime, version)
            headers = installed / "usr/include/xgc2/xrpc"
            for header in sorted(headers.glob("*")):
                copy_link(header, roots[dev] / "usr/include/xgc2/xrpc")
                accounted.add(header)
            for cmake in sorted((installed / cmake_relative).glob("*")):
                copy_link(cmake, roots[dev] / cmake_relative)
                accounted.add(cmake)
        if package["root"]:
            for cmake in sorted((stage / "config" / cmake_relative).glob("*")):
                copy_link(cmake, roots[dev] / cmake_relative)
                accounted.add(cmake)
    everything = {path for path in stage.rglob("*") if path.is_file() or path.is_symlink()}
    if everything != accounted:
        raise ValueError("the install tree has files no package takes: " +
                         ", ".join(sorted(str(p.relative_to(stage)) for p in everything - accounted)))
    for runtime in shlibs:
        (roots[runtime] / "DEBIAN/shlibs").write_text(shlibs[runtime])
        # ldconfig is run only when dpkg actually installs/removes the runtime package.
        (roots[runtime] / "DEBIAN/triggers").write_text("activate-noawait ldconfig\n")
    deps_work = work / "shlibdeps"
    (deps_work / "debian").mkdir(parents=True)
    (deps_work / "debian/control").write_text("Source: xgc2-xrpc\nSection: libs\nPriority: optional\nMaintainer: XGC Team <apt@example.com>\n\n" +
        "\n".join("Package: " + name + "\nArchitecture: any\n" for name in shlibs))
    (deps_work / "debian/shlibs.local").write_text("".join(shlibs.values()))
    search = [roots[name] / lib_relative for name in shlibs]
    result = []
    for package in packages:
        for name in (package["runtime"], package["dev"]):
            runtime = name == package["runtime"]
            if runtime:
                argv = ["dpkg-shlibdeps", "-O", "-x" + name]
                argv += ["-l" + str(path) for path in search]
                argv += ["-S" + str(Path(p).resolve()) for p in args.shlibdeps_package_root]
                argv += ["-l" + str(Path(p).resolve()) for p in args.shlibdeps_library_path]
                argv += ["-e" + str(roots[name] / lib_relative / ("libxgc2_xrpc_" + c + ".so." + sdk_version)) for c in package["components"]]
                depends = run(argv, cwd=deps_work, capture=True)
                if not depends.startswith("shlibs:Depends=") or not depends.split("=", 1)[1]:
                    raise ValueError("dpkg-shlibdeps produced no runtime dependencies")
                depends = depends.split("=", 1)[1]
            else:
                depends = ", ".join([package["runtime"] + " (= " + version + ")"] +
                                    [item.format(version=version) for item in package["dev_depends"]])
            (roots[name] / "DEBIAN/control").write_text(
                "Package: " + name + "\nVersion: " + version + "\nArchitecture: " + args.architecture +
                "\nMulti-Arch: same\nSection: " + ("libs" if runtime else "libdevel") +
                "\nPriority: optional\nMaintainer: XGC Team <apt@example.com>\nDepends: " + depends +
                "\nDescription: XGC2 XRPC " + ("native shared library" if runtime else "C++17 development interface") +
                " (" + ", ".join(package["components"]) + ")\n Bounded transport SDK; providers own their domain and lifecycle.\n")
            doc = roots[name] / "usr/share/doc" / name
            doc.mkdir(parents=True)
            (doc / "copyright").write_text(
                "Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/\n\nFiles: *\nCopyright: XGC Team\n"
                "License: Apache-2.0\n Licensed under the Apache License, Version 2.0. On Debian systems the full text is in\n"
                " /usr/share/common-licenses/Apache-2.0.\n")
            deb = out / (name + "_" + version + "_" + args.architecture + ".deb")
            run(["dpkg-deb", "--root-owner-group", "--build", roots[name], deb])
            result.append({"kind": "deb", "path": deb.name, "package": name, "depends": depends})
    return result, abi


def pack_npm(directory, out, name, sdk_version):
    pack = run(["npm", "pack", "--offline", "--ignore-scripts", "--json", "--pack-destination", out],
               cwd=directory, capture=True)
    metadata = json.loads(pack)[0]
    if metadata["name"] != name or metadata["version"] != sdk_version:
        raise ValueError(name + " manifest version differs from the product version")
    return metadata["filename"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="new directory outside the source checkout")
    parser.add_argument("--distribution", choices=ABI["distributions"], required=True)
    parser.add_argument("--architecture", choices=ABI["architectures"], required=True)
    parser.add_argument("--language", choices=LANGUAGES, action="append",
                        help="default: every language the distribution ships (Bionic: only the C++ udp component)")
    parser.add_argument("--cpp-components", help="comma-separated C++ components (default: every component of the distribution)")
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
    shipped = suite_languages(args.distribution)
    languages = sorted(set(args.language or shipped))
    if not set(languages) <= set(shipped):
        raise ValueError(args.distribution + " ships only: " + ", ".join(shipped))
    suite_components = suite_cpp_components(args.distribution)
    components = args.cpp_components.split(",") if args.cpp_components else suite_components
    if not set(components) <= set(suite_components):
        raise ValueError(args.distribution + " ships only the C++ components: " + ", ".join(suite_components))
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
        if languages != shipped or components != suite_components:
            raise ValueError("release candidate requires every SDK and C++ component the distribution ships")
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
        "cpp_components": components, "source_date_epoch": args.source_date_epoch,
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
                "version": product_version, "languages": languages, "cpp_components": components if "cpp" in languages else [],
                "source_files": inputs, "snapshot_id": snapshot_id, "deb_version": version,
                "source_date_epoch": args.source_date_epoch,
        "source_sha": source_sha, "release_candidate": bool(source_sha),
                "artifacts": [], "elf": {}, "trusted_release": False}
    if "cpp" in languages:
        artifacts, evidence["elf"] = build_cpp(args, source, work, out, version, sdk_version, components)
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
            name = pack_npm(source / "node", out, NODE.npm_name, sdk_version)
            lock = source / "node/package-lock.json"
        evidence["artifacts"].append({"kind": "node", "path": name, "node_source_sha": node_sha})
        evidence["artifacts"].append(build_node_deb(NODE, out / name, lock, work, out, version, sdk_version,
                                                   args.source_date_epoch, node_sha))
    if "ts" in languages:
        # The package is built TypeScript: install the locked compiler from the npm cache,
        # compile, then pack. Neither step runs a lifecycle script.
        typescript = source / "ts"
        run(["npm", "ci", "--offline", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=typescript)
        run(["npm", "run", "build"], cwd=typescript)
        name = pack_npm(typescript, out, TS.npm_name, sdk_version)
        evidence["artifacts"].append({"kind": "ts", "path": name, "node_source_sha": source_sha})
        evidence["artifacts"].append(build_node_deb(TS, out / name, None, work, out, version, sdk_version,
                                                   args.source_date_epoch, source_sha))
    for artifact in evidence["artifacts"]:
        artifact["sha256"] = digest(out / artifact["path"])
        artifact["bytes"] = (out / artifact["path"]).stat().st_size
    evidence["toolchain"] = {"python": run([args.python, "--version"], capture=True),
                             "cxx": run([args.cxx, "--version"], capture=True).splitlines()[0] if "cpp" in languages else None,
                             "cmake": run(["cmake", "--version"], capture=True).splitlines()[0] if "cpp" in languages else None,
                             "node": run(["node", "--version"], capture=True) if {"node", "ts"} & set(languages) else None,
                             "npm": run(["npm", "--version"], capture=True) if {"node", "ts"} & set(languages) else None}
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
