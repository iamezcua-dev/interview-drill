"""`drill setup` and the database auto-start used while grading.

The container itself is declared in `compose.yaml`; this module only
orchestrates it, the same way on macOS, Windows and Linux.
"""

import os
import secrets
import shutil
import string
import subprocess
import sys
import time

from drill import CMD, config

CONTAINER = "sql-drill"
HEALTH_TIMEOUT = 180  # first start under Rosetta can be slow


class SetupError(Exception):
    pass


# --------------------------------------------------------------- docker

def engine() -> str | None:
    """`docker` or `podman`, whichever is installed (docker preferred)."""
    for exe in ("docker", "podman"):
        if shutil.which(exe):
            return exe
    return None


def _run(*args: str, env: dict | None = None, timeout: int = 600):
    return subprocess.run(
        list(args), capture_output=True, text=True, cwd=config.ROOT,
        env=env, timeout=timeout,
    )


def engine_running(exe: str) -> bool:
    try:
        return _run(exe, "info", timeout=30).returncode == 0
    except subprocess.TimeoutExpired:
        return False


def container_state(exe: str) -> str | None:
    """'running', 'exited', ... or None when the container doesn't exist."""
    out = _run(exe, "inspect", "-f", "{{.State.Status}}", CONTAINER,
               timeout=30)
    return out.stdout.strip() if out.returncode == 0 else None


def _health(exe: str) -> str:
    out = _run(exe, "inspect", "-f", "{{.State.Health.Status}}", CONTAINER,
               timeout=30)
    return out.stdout.strip()


def wait_healthy(exe: str, timeout: int = HEALTH_TIMEOUT) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = _health(exe)
        if status == "healthy":
            return True
        if status == "unhealthy" or container_state(exe) != "running":
            return False
        time.sleep(2)
    return False


def autostart() -> bool:
    """Start an existing, stopped `sql-drill` container. True if healthy.

    Called by the tsql adapter when it can't connect, so a drill session
    is never interrupted by "start your database first".
    """
    exe = engine()
    if not exe or not engine_running(exe):
        return False
    state = container_state(exe)
    if state is None:
        return False
    if state != "running":
        print(f"Starting the {CONTAINER} container...", file=sys.stderr)
        if _run(exe, "start", CONTAINER, timeout=120).returncode != 0:
            return False
    return wait_healthy(exe)


def _compose(exe: str) -> list[str]:
    if _run(exe, "compose", "version", timeout=30).returncode == 0:
        return [exe, "compose"]
    legacy = f"{exe}-compose"
    if shutil.which(legacy):
        return [legacy]
    raise SetupError(
        f"'{exe} compose' is not available. Install Docker Desktop (or "
        "the compose plugin) and try again."
    )


# --------------------------------------------------------------- password

def _generate_password() -> str:
    # SQL Server requires 3 of 4 character classes; use all four.
    alphabet = string.ascii_letters + string.digits
    body = "".join(secrets.choice(alphabet) for _ in range(24))
    return f"Dr{body}-9x"


def ensure_password() -> tuple[str, str]:
    """Return (password, where it lives), creating one if needed."""
    service, account = config.keychain_item()
    in_file = config.settings()["database"].get("password")
    if in_file:
        return in_file, "drill.yaml / DRILL_DB_PASSWORD"
    existing = config.keychain_password()
    if existing:
        return existing, f"OS keychain (service '{service}')"
    password = _generate_password()
    try:
        import keyring

        keyring.set_password(service, account, password)
        config.keychain_password.cache_clear()
        if config.keychain_password() == password:
            return password, f"OS keychain (service '{service}', new)"
    except Exception:  # noqa: BLE001 - no backend: fall back below
        pass
    config.save_local("database", "password", password)
    return password, (
        "drill.yaml (gitignored), because this machine has no OS keychain"
    )


# --------------------------------------------------------------- java

def _ensure_java() -> None:
    """Find Java 17 for Spark, or offer to download it (asks first)."""
    from drill.java import JavaNotFound, download_jdk, find_java_home

    _step("Java 17 for Spark (pyspark, sparksql)")
    try:
        home, major = find_java_home()
        print(f"    found Java {major}: {home}")
        return
    except JavaNotFound:
        pass
    print("    not found. drill can download Eclipse Temurin 17 (~190 MB)\n"
          "    into its own cache folder; nothing is installed system-wide.")
    if not sys.stdin.isatty():
        print("    skipped (no terminal to ask). Run setup in a terminal.")
        return
    reply = input("    Download it now? [y/N] ").strip().lower()
    if reply not in ("y", "yes"):
        print("    skipped: PySpark and Spark SQL problems are skipped "
              "until Java 17 is available.")
        return
    try:
        print(f"    installed: {download_jdk()}")
    except (JavaNotFound, OSError) as exc:
        print(f"    download failed: {exc}")


# --------------------------------------------------------------- hooks

def _remove_legacy_hook() -> None:
    """Drop the old `.hooks/pre-commit` symlink from before drill.

    pre-commit would otherwise keep running it in "migration mode".
    """
    hook = config.ROOT / ".git" / "hooks" / "pre-commit"
    if hook.is_symlink() and "/.hooks/" in os.readlink(hook):
        hook.unlink()
        print("    removed the old .hooks/pre-commit symlink")


# --------------------------------------------------------------- command

def _step(text: str) -> None:
    print(f"==> {text}")


def setup(skip_db: bool = False, skip_hooks: bool = False) -> int:
    if config.ensure_local_file():
        _step("Created drill.yaml (local settings, not tracked by git)")
    else:
        _step("Using drill.yaml")

    _ensure_java()

    if not skip_db:
        exe = engine()
        if not exe:
            raise SetupError(
                "Docker is not installed: https://docs.docker.com/get-docker/"
                "\n(Or point DRILL_DB_* at any SQL Server you can reach, and "
                f"run `{CMD} setup --skip-db`.)"
            )
        if not engine_running(exe):
            raise SetupError(
                f"{exe} is installed but not running. Start Docker Desktop "
                f"(or the daemon) and run `{CMD} setup` again."
            )
        _step("SQL Server password")
        password, source = ensure_password()
        print(f"    stored in: {source}")

        settings = config.db_settings()
        state = container_state(exe)
        if state is None:
            _step(f"Creating the {CONTAINER} container on port "
                  f"{settings.port} (first time pulls the image)")
            env = dict(os.environ, DRILL_DB_PASSWORD=password,
                       DRILL_DB_PORT=str(settings.port))
            out = _run(*_compose(exe), "up", "-d", env=env, timeout=1800)
            if out.returncode != 0:
                raise SetupError(out.stderr.strip() or out.stdout.strip())
        elif state != "running":
            _step(f"Starting the existing {CONTAINER} container")
            _run(exe, "start", CONTAINER, timeout=120)
        else:
            _step(f"{CONTAINER} is already running")
        _step("Waiting for SQL Server to accept connections")
        if not wait_healthy(exe):
            logs = _run(exe, "logs", "--tail", "15", CONTAINER).stdout
            raise SetupError(
                f"{CONTAINER} did not become healthy.\n{logs}\nIf the "
                "container was created earlier with another password, "
                f"remove it (`{exe} rm -f {CONTAINER}`) and rerun setup."
            )
        print("    ready")

    if not skip_hooks:
        _step("Installing git hooks (pre-commit)")
        _remove_legacy_hook()
        out = _run(sys.executable, "-m", "pre_commit", "install",
                   "--hook-type", "pre-commit", "--hook-type", "commit-msg")
        if out.returncode != 0:
            print("    skipped: pre-commit is not installed "
                  f"({CMD} setup installs it)")
        else:
            print("    installed")

    _drill_branch()
    return 0


def _drill_branch() -> None:
    """Put a fresh clone on the solver's own drill/<username> branch."""
    from drill.cli import _branch, _git, suggested_branch

    branch = _branch()
    target = suggested_branch()
    _step("Your drill branch")
    if branch.startswith("drill/"):
        print(f"    on {branch}")
        return
    if branch != "main":
        print(f"    staying on {branch}; solve problems on {target}:\n"
              f"    git switch -c {target}")
        return
    exists = bool(_git("rev-parse", "--verify", "--quiet", target))
    args = ["switch", target] if exists else ["switch", "-c", target]
    out = _run("git", *args)
    if out.returncode == 0:
        print(f"    switched to {target}: commit your solutions here and "
              f"push it to share them (git push -u origin {target})")
    else:
        print(f"    couldn't switch: {out.stderr.strip()}\n"
              f"    run: git {' '.join(args)}")
