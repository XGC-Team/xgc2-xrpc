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
                            check=True, text=True, stdout=subprocess.PIPE if capture else None)
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
