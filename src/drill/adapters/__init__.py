"""Language adapters: load a case's tables, run a solution, return rows.

Every adapter implements `run(problem, solution, cases)` and returns one
`(columns, rows)` tuple or `SolutionError` per case. A missing toolchain
raises `Unavailable` so callers can skip that language with a hint.
"""

from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol

from drill.problems import Case, Problem

Result = tuple[list[str], list[Sequence[Any]]]


class Unavailable(Exception):
    """The toolchain for a language is missing or unreachable."""


class SolutionError(Exception):
    """The solution failed to compile or raised while running."""


class Adapter(Protocol):
    lang: str

    def run(
        self, problem: Problem, solution: Path, cases: list[Case]
    ) -> list[Result | SolutionError]: ...

    def close(self) -> None: ...


_instances: dict[str, Adapter] = {}


def get(lang: str) -> Adapter:
    if lang not in _instances:
        if lang == "tsql":
            from drill.adapters.tsql import TsqlAdapter as cls
        elif lang == "pyspark":
            from drill.adapters.spark import PySparkAdapter as cls
        elif lang == "sparksql":
            from drill.adapters.spark import SparkSqlAdapter as cls
        elif lang == "scala":
            from drill.adapters.scala import ScalaAdapter as cls
        else:
            raise ValueError(f"unknown language {lang}")
        _instances[lang] = cls()
    return _instances[lang]


def close_all() -> None:
    for adapter in _instances.values():
        adapter.close()
    _instances.clear()


def short_error(exc: BaseException, limit: int = 12) -> str:
    text = str(exc).strip() or type(exc).__name__
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) > limit:
        lines = lines[:limit] + ["..."]
    return "\n".join(lines)
