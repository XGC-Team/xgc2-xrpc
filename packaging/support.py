"""Offline package primitives. Build state never lives in the source checkout."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
ABI = json.loads((ROOT / "packaging/runtime-abi.json").read_text())
SKIP = {".git", "__pycache__", "node_modules", "target", "build", "dist", ".pytest_cache"}


def run(args, cwd=None, env=None, capture=False):
    command_env = os.environ.copy()
    command_env.update(env or {})
    print("+ " + " ".join(str(x) for x in args), flush=True)
    result = subprocess.run([str(x) for x in args], cwd=cwd, env=command_env,
                            check=True, universal_newlines=True, stdout=subprocess.PIPE if capture else None)
    return result.stdout.strip() if capture else ""


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def source_files(root):
    for path in sorted(Path(root).rglob("*")):
        relative = path.relative_to(root)
        if any(part in SKIP or part.endswith(".egg-info") for part in relative.parts):
            continue
        if path.is_symlink():
            raise ValueError("source symlinks must be materialized by their owner: " + str(relative))
        if path.is_file() and path.suffix != ".pyc":
            yield path


def suite_languages(suite):
    """The languages whose artifacts a suite ships (Bionic: only the C++ udp component)."""
    return sorted(ABI["distributions"][suite]["languages"])


def suite_cpp_components(suite):
    return list(ABI["distributions"][suite]["cpp"])


# The C++ Debian packages: the runtime package prefix (the SOVERSION generation of
# its libraries is appended), the development package, the components (shared
# libraries) they carry and what the development package needs beyond its runtime
# package. The udp development package is the root: it also carries the CMake
# package files and the header-only method router, so every other development
# package depends on it, and a Bionic installation needs nothing else.
CPP_PACKAGES = (
    {"runtime": "libxgc2-xrpc-udp", "dev": "libxgc2-xrpc-udp-dev", "components": ("udp",), "root": True,
     "dev_depends": ()},
    {"runtime": "libxgc2-xrpc", "dev": "libxgc2-xrpc-dev", "components": ("unix", "diagnostics", "bootstrap", "http", "json_http"),
     "root": False, "dev_depends": ("libxgc2-xrpc-udp-dev (= {version})", "nlohmann-json3-dev (>= 3.7)")},
    {"runtime": "libxgc2-xrpc-grpc", "dev": "libxgc2-xrpc-grpc-dev", "components": ("grpc",), "root": False,
     "dev_depends": ("libxgc2-xrpc-dev (= {version})", "libgrpc++-dev (>= 1.16)", "libprotobuf-dev")},
)


# What a component links against (cpp/CMakeLists.txt pulls these in itself; a package
# selection must name them, because each of them is shipped by a package).
CPP_DEPENDENCIES = {"bootstrap": ("unix",), "http": ("unix", "diagnostics", "bootstrap"),
                    "json_http": ("http",), "grpc": ("unix", "diagnostics")}


def cpp_package_set(components):
    """The packages that carry the given C++ components: dicts with the runtime package
    name, the development package name and the components the selection includes."""
    result = []
    for package in CPP_PACKAGES:
        selected = tuple(c for c in package["components"] if c in components)
        if not selected:
            continue
        generations = {ABI["cpp"]["soname"][c] for c in package["components"]}
        if len(generations) != 1:
            raise ValueError("the libraries of one runtime package share a SOVERSION generation")
        result.append(dict(package, components=selected, runtime=package["runtime"] + str(generations.pop())))
    unknown = set(components) - {c for p in CPP_PACKAGES for c in p["components"]}
    if unknown:
        raise ValueError("components without a package: " + ", ".join(sorted(unknown)))
    if "udp" not in components:
        raise ValueError("the udp component is required: its development package carries the CMake package files")
    for component in components:
        missing = [c for c in CPP_DEPENDENCIES.get(component, ()) if c not in components]
        if missing:
            raise ValueError("the %s component needs the %s component(s)" % (component, ", ".join(missing)))
    return result


def version_tuple(value):
    return tuple(int(n) for n in value.split("."))


def system_suite():
    fields = dict(line.split("=", 1) for line in Path("/etc/os-release").read_text().splitlines()
                  if "=" in line)
    return fields.get("VERSION_CODENAME", "").strip('"')


def elf_info(path, suite, arch):
    header = run(["readelf", "-h", path], capture=True)
    if ABI["architectures"][arch] not in header:
        raise ValueError("ELF architecture mismatch: " + str(path))
    dynamic = run(["readelf", "-d", path], capture=True)
    versions = run(["readelf", "--version-info", path], capture=True)
    maxima = {}
    for family in ("GLIBC", "GLIBCXX", "CXXABI"):
        found = set(re.findall(r"\b" + family + r"_([0-9.]+)\b", versions))
        if found:
            maxima[family] = max(found, key=version_tuple)
    if version_tuple(maxima.get("GLIBC", "0")) > version_tuple(ABI["distributions"][suite]["glibc_max"]):
        raise ValueError("ELF needs glibc " + maxima["GLIBC"] + ", newer than " + suite + ": " + str(path))
    soname = re.search(r"\(SONAME\).*?\[([^]]+)\]", dynamic)
    return {"soname": soname.group(1) if soname else None, "symbol_versions": maxima}


def tar_source(root, output, prefix, epoch):
    with tarfile.open(output, "w:gz", format=tarfile.PAX_FORMAT) as archive:
        for path in source_files(root):
            info = archive.gettarinfo(str(path), str(Path(prefix) / path.relative_to(root)))
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = epoch
            with path.open("rb") as stream:
                archive.addfile(info, stream)


def extract_tar(path, target):
    # Language packages must not escape the isolated install tree, even on Python 3.8.
    with tarfile.open(path) as archive:
        for member in archive.getmembers():
            dest = (target / member.name).resolve()
            if target.resolve() not in dest.parents or not (member.isfile() or member.isdir()):
                raise ValueError("unsafe archive member: " + member.name)
        archive.extractall(target)


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
