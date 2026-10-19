"""Find a JDK that Spark 3.5 supports (8, 11 or 17; 17 preferred)."""

import os
import platform
import re
import shutil
import subprocess
from functools import cache
from pathlib import Path

from drill import config

SUPPORTED = (17, 11, 8)


class JavaNotFound(Exception):
    pass


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
        "Spark 3.5 needs Java 17 (or 11/8). Install one; if it isn't "
        "found automatically, set java_home in drill.yaml."
    )


def configure_env() -> tuple[Path, int]:
    home, major = find_java_home()
    os.environ["JAVA_HOME"] = str(home)
    return home, major
