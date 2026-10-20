"""Settings: `drill.yaml` (local, gitignored) over built-in defaults.

`drill.example.yaml` documents every key; `drill setup` copies it to
`drill.yaml` on first run. `DRILL_*` environment variables override the
file. They are meant for CI, not for day-to-day use. drill never reads
or changes your shell profile or other personal configuration.

The database password is not stored in the file: it lives in the OS
credential store (macOS Keychain, Windows Credential Manager, Linux
Secret Service) via `keyring`. Only machines without such a store
(headless Linux, WSL) keep it in `drill.yaml`.
"""

import os
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import yaml

def _find_root() -> Path:
    """The repo root: the nearest folder with problems/ and pyproject.toml.

    Searched upward from the current directory, so drill works from any
    subfolder; falls back to where this package lives (src/drill).
    """
    if os.environ.get("DRILL_ROOT"):
        return Path(os.environ["DRILL_ROOT"]).resolve()
    here = Path.cwd().resolve()
    for folder in (here, *here.parents):
        if (folder / "problems").is_dir() and (
            folder / "pyproject.toml"
        ).is_file():
            return folder
    return Path(__file__).resolve().parents[2]


ROOT = _find_root()
PROBLEMS_DIR = ROOT / "problems"
SOLUTIONS_DIR = ROOT / "solutions"
CACHE_DIR = ROOT / ".drill"
LOCAL_FILE = ROOT / "drill.yaml"
EXAMPLE_FILE = ROOT / "drill.example.yaml"

DEFAULTS: dict[str, Any] = {
    "database": {
        "host": "localhost",
        "port": 1433,
        "user": "sa",
        "name": "interview_drill",
        "password": None,
    },
    "keychain": {"service": "de-drill", "account": "sa"},
    "java_home": None,
    "default_lang": None,
}

# Environment overrides (CI): variable -> (section, key)
ENV = {
    "DRILL_DB_HOST": ("database", "host"),
    "DRILL_DB_PORT": ("database", "port"),
    "DRILL_DB_USER": ("database", "user"),
    "DRILL_DB_NAME": ("database", "name"),
    "DRILL_DB_PASSWORD": ("database", "password"),
    "DRILL_KEYCHAIN_SERVICE": ("keychain", "service"),
    "DRILL_KEYCHAIN_ACCOUNT": ("keychain", "account"),
    "DRILL_JAVA_HOME": (None, "java_home"),
    "DRILL_LANG": (None, "default_lang"),
}


class ConfigError(Exception):
    pass


def _local() -> dict:
    if not LOCAL_FILE.is_file():
        return {}
    try:
        data = yaml.safe_load(LOCAL_FILE.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"drill.yaml is not valid YAML: {exc}") from exc
    return data or {}


@cache
def settings() -> dict[str, Any]:
    merged = {
        k: dict(v) if isinstance(v, dict) else v for k, v in DEFAULTS.items()
    }
    for key, value in _local().items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key].update(value)
        else:
            merged[key] = value
    for var, (section, key) in ENV.items():
        if var in os.environ:
            target = merged[section] if section else merged
            target[key] = os.environ[var]
    return merged


def reload() -> None:
    settings.cache_clear()
    keychain_password.cache_clear()


@dataclass(frozen=True)
class DbSettings:
    host: str
    port: int
    user: str
    password: str | None
    database: str


def db_settings() -> DbSettings:
    db = settings()["database"]
    return DbSettings(
        host=str(db["host"]),
        port=int(db["port"]),
        user=str(db["user"]),
        password=db.get("password") or keychain_password(),
        database=str(db["name"]),
    )


def keychain_item() -> tuple[str, str]:
    kc = settings()["keychain"]
    return str(kc["service"]), str(kc["account"])


def java_home() -> str | None:
    return settings().get("java_home")


def default_lang() -> str | None:
    return settings().get("default_lang")


@cache
def keychain_password() -> str | None:
    """Read the password from the OS store; None if absent or no store."""
    try:
        import keyring
    except ImportError:
        return None
    service, account = keychain_item()
    try:
        return keyring.get_password(service, account)
    except Exception:  # noqa: BLE001 - no usable backend (headless/WSL)
        return None


def save_local(section: str, key: str, value: Any) -> None:
    """Set one key in drill.yaml (created from the example if missing)."""
    ensure_local_file()
    data = _local()
    data.setdefault(section, {})[key] = value
    LOCAL_FILE.write_text(
        yaml.safe_dump(data, sort_keys=False), encoding="utf-8"
    )
    reload()


def ensure_local_file() -> bool:
    """Create drill.yaml from drill.example.yaml. True if it was created."""
    if LOCAL_FILE.exists():
        return False
    if EXAMPLE_FILE.is_file():
        LOCAL_FILE.write_text(
            EXAMPLE_FILE.read_text(encoding="utf-8"), encoding="utf-8"
        )
    else:
        LOCAL_FILE.write_text(
            yaml.safe_dump(DEFAULTS, sort_keys=False), encoding="utf-8"
        )
    reload()
    return True
