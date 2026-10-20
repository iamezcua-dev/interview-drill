"""Find a JDK that Spark 3.5 supports (8, 11 or 17; 17 preferred).

If none is installed, `drill setup` offers to download Eclipse Temurin 17
(Adoptium) into drill's own cache folder. Nothing is installed
system-wide and no environment or shell settings are changed.
"""

import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from functools import cache
from pathlib import Path

from drill import CMD, config

SUPPORTED = (17, 11, 8)
ADOPTIUM = (
    "https://api.adoptium.net/v3/binary/latest/17/ga/{os}/{arch}"
    "/jdk/hotspot/normal/eclipse"
)


class JavaNotFound(Exception):
    pass


def cache_dir() -> Path:
    """Per-user cache: ~/Library/Caches, %LOCALAPPDATA% or ~/.cache."""
    system = platform.system()
    if system == "Darwin":
        base = Path.home() / "Library" / "Caches"
    elif system == "Windows":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home()))
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    return base / "de-drill" / "jdk-17"


def _cached_home() -> Path | None:
    root = cache_dir()
    if not root.is_dir():
        return None
    for child in sorted(root.iterdir()):
        for home in (child / "Contents" / "Home", child):
            if (home / "bin").is_dir():
                return home
    return None


def _major(java_home: Path) -> int | None:
    exe = java_home / "bin" / ("java.exe" if os.name == "nt" else "java")
    if not exe.exists():
        return None
    try:
        out = subprocess.run(
            [str(exe), "-version"], capture_output=True, text=True,
            timeout=20,
        ).stderr
    except (OSError, subprocess.TimeoutExpired):
        return None
    match = re.search(r'version "(\d+)(?:\.(\d+))?', out)
    if not match:
        return None
    major = int(match.group(1))
    return int(match.group(2)) if major == 1 else major


def _macos_candidates() -> list[Path]:
    homes = []
    for version in SUPPORTED:
        arg = "1.8" if version == 8 else str(version)
        try:
            out = subprocess.run(
                ["/usr/libexec/java_home", "-v", arg],
                capture_output=True, text=True, timeout=20,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if out.returncode == 0 and out.stdout.strip():
            homes.append(Path(out.stdout.strip()))
    return homes


@cache
def find_java_home() -> tuple[Path, int]:
    candidates: list[Path] = []
    for value in (config.java_home(), os.environ.get("JAVA_HOME")):
        if value:
            candidates.append(Path(value))
    cached = _cached_home()
    if cached:
        candidates.append(cached)
    if platform.system() == "Darwin":
        candidates += _macos_candidates()
    on_path = shutil.which("java")
    if on_path:
        candidates.append(Path(on_path).resolve().parent.parent)
    for home in candidates:
        major = _major(home)
        if major in SUPPORTED:
            return home, major
    raise JavaNotFound(
        f"Spark needs Java 17. Run `{CMD} setup` to download it, or set "
        "java_home in drill.yaml."
    )


def configure_env() -> tuple[Path, int]:
    home, major = find_java_home()
    os.environ["JAVA_HOME"] = str(home)
    return home, major


def download_jdk() -> Path:
    """Download Temurin 17 for this OS/CPU into cache_dir()."""
    os_name = {"Darwin": "mac", "Linux": "linux", "Windows": "windows"}.get(
        platform.system()
    )
    arch = {
        "x86_64": "x64", "amd64": "x64", "arm64": "aarch64",
        "aarch64": "aarch64",
    }.get(platform.machine().lower())
    if not os_name or not arch:
        raise JavaNotFound(
            f"No automatic Java download for {platform.system()} "
            f"{platform.machine()}; install Java 17 and set java_home."
        )
    url = ADOPTIUM.format(os=os_name, arch=arch)
    target = cache_dir()
    with tempfile.TemporaryDirectory() as tmp:
        archive = Path(tmp) / ("jdk.zip" if os_name == "windows"
                               else "jdk.tar.gz")
        # The Adoptium API rejects urllib's default User-Agent with 403.
        request = urllib.request.Request(
            url, headers={"User-Agent": "de-interview-drill (java setup)"}
        )
        with urllib.request.urlopen(request, timeout=60) as resp, \
                open(archive, "wb") as out:
            total = int(resp.headers.get("Content-Length") or 0)
            done = 0
            while chunk := resp.read(1 << 20):
                out.write(chunk)
                done += len(chunk)
                if total:
                    print(f"\r    downloading Java 17: {done >> 20} / "
                          f"{total >> 20} MB", end="", file=sys.stderr)
        print(file=sys.stderr)
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
        if os_name == "windows":
            with zipfile.ZipFile(archive) as zf:
                zf.extractall(target)
        else:
            with tarfile.open(archive) as tf:
                tf.extractall(target, filter="data")
    find_java_home.cache_clear()
    home = _cached_home()
    if not home or _major(home) != 17:
        raise JavaNotFound("the downloaded JDK could not be started")
    return home
