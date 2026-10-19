"""Grade solutions or reference answers and remember the outcome."""

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

from drill import adapters, config
from drill.adapters import SolutionError, Unavailable
from drill.compare import Outcome, compare
from drill.problems import Problem

SOLVED_MARK = "SOLVED"
RESULTS_FILE = config.CACHE_DIR / "results.json"


@dataclass
class Graded:
    problem: Problem
    lang: str | None  # None for theory
    path: Path
    status: str  # pass | fail | skip
    message: str = ""
    cases: list[tuple[str, Outcome]] = field(default_factory=list)

    @property
    def label(self) -> str:
        return self.lang or "theory"

    @property
    def score(self) -> str:
        if not self.cases:
            return ""
        ok = sum(1 for _, o in self.cases if o.passed)
        return f"{ok}/{len(self.cases)}"


def grade_theory(problem: Problem, path: Path) -> Graded:
    lines = [
        ln.strip() for ln in path.read_text(encoding="utf-8").splitlines()
        if ln.strip()
    ]
    if lines and lines[-1] == SOLVED_MARK:
        return Graded(problem, None, path, "pass")
    return Graded(
        problem, None, path, "fail",
        f"self-graded: add a final line '{SOLVED_MARK}' once your answer "
        "is complete",
    )


def grade(problem: Problem, lang: str | None, path: Path) -> Graded:
    if problem.is_theory:
        return grade_theory(problem, path)
    cases = problem.cases()
    try:
        results = adapters.get(lang).run(problem, path, cases)
    except Unavailable as exc:
        return Graded(problem, lang, path, "skip", str(exc))
    graded = Graded(problem, lang, path, "pass")
    for case, result in zip(cases, results):
        if isinstance(result, SolutionError):
            outcome = Outcome(False, str(result))
        else:
            columns, rows = result
            outcome = compare(problem, case, columns, rows)
        graded.cases.append((case.path.stem, outcome))
        if not outcome.passed:
            graded.status = "fail"
    return graded


def load_results() -> dict:
    try:
        return json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def remember(graded: list[Graded]) -> None:
    """Cache solver results locally (never committed) for `drill status`."""
    results = load_results()
    for g in graded:
        if g.status == "skip":
            continue
        entry = results.setdefault(g.problem.id, {})
        entry[g.label] = {
            "status": g.status,
            "score": g.score,
            "at": time.strftime("%Y-%m-%d %H:%M"),
            "mtime": os.path.getmtime(g.path),
        }
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_FILE.write_text(json.dumps(results, indent=2), encoding="utf-8")
