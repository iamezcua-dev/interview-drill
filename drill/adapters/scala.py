"""Scala adapter: compiles and runs the solution with scala-cli.

All cases of a problem run in one JVM through `DrillHarness.scala`.
scala-cli downloads Scala, Spark and a Java 17 runtime on first use, so
the host only needs scala-cli itself.
"""

import json
import re
import shutil
import subprocess
import tempfile
from decimal import Decimal
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from drill import config, types
from drill.adapters import Result, SolutionError, Unavailable
from drill.problems import Case, Problem

HARNESS = Path(__file__).with_name("DrillHarness.scala")
ANSI = re.compile(r"\x1b\[[0-9;]*m")
TIMEOUT = 1800  # first run downloads Spark; later runs take seconds


def _spark_version() -> str:
    try:
        return version("pyspark")
    except PackageNotFoundError:
        return "3.5.8"


def _compile_error(stderr: str, solution: Path) -> str:
    """Keep scala-cli's `[error]` lines (message, code, caret), no noise."""
    text = ANSI.sub("", stderr)
    try:
        shown = solution.relative_to(config.ROOT).as_posix()
    except ValueError:
        shown = solution.name
    text = text.replace(str(solution), shown)
    errors = [
        ln[len("[error] "):] for ln in text.splitlines()
        if ln.startswith("[error]")
    ]
    if not errors:
        errors = [
            ln for ln in text.splitlines()
            if ln.strip() and not ln.startswith(("Warning:", "[hint]", " "))
        ]
    return "\n".join(errors[:20]) or "scala-cli failed without output"


class ScalaAdapter:
    lang = "scala"

    def run(
        self, problem: Problem, solution: Path, cases: list[Case]
    ) -> list[Result | SolutionError]:
        exe = shutil.which("scala-cli")
        if not exe:
            raise Unavailable(
                "scala-cli is not installed; see https://scala-cli.virtuslab"
                ".org/install (Windows: winget install virtuslab.scalacli)"
            )
        spec = {
            "tables": [t.name for t in problem.tables],
            "cases": [
                {
                    "name": case.name,
                    "tables": [
                        {
                            "name": t.name,
                            "ddl": ", ".join(
                                f"`{c.name}` {types.spark(c.type)}"
                                for c in t.columns
                            ),
                            "rows": [
                                [
                                    types.to_json(types.coerce(c.type, v))
                                    for c, v in zip(t.columns, row)
                                ]
                                for row in case.input.get(t.name, [])
                            ],
                        }
                        for t in problem.tables
                    ],
                }
                for case in cases
            ],
        }
        workspace = config.CACHE_DIR / "scala"
        workspace.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory() as tmp:
            spec_path = Path(tmp) / "spec.json"
            out_path = Path(tmp) / "out.json"
            spec_path.write_text(json.dumps(spec), encoding="utf-8")
            cmd = [
                exe, "run", "--power", "-q",
                "--workspace", str(workspace),
                "--dep", f"org.apache.spark::spark-sql:{_spark_version()}",
                str(HARNESS), str(solution),
                "--main-class", "DrillHarness",
                "--", str(spec_path), str(out_path),
            ]
            try:
                proc = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=TIMEOUT
                )
            except subprocess.TimeoutExpired:
                err = SolutionError(f"scala-cli timed out after {TIMEOUT}s")
                return [err] * len(cases)
            if proc.returncode != 0 or not out_path.exists():
                err = SolutionError(_compile_error(proc.stderr, solution))
                return [err] * len(cases)
            raw = json.loads(
                out_path.read_text(encoding="utf-8"), parse_float=Decimal
            )
        out: list[Result | SolutionError] = []
        for item in raw:
            if "error" in item:
                out.append(SolutionError(item["error"]))
            else:
                out.append((item["columns"], item["rows"]))
        return out

    def close(self) -> None:
        pass
