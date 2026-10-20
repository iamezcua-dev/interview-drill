"""Flag every solution commit of a push on GitHub (drill/* branches).

Each commit that changes files under solutions/ gets one commit status:

* success  ✅ its solution(s) pass
* failure  ❌ they don't, or the commit touches more than one problem
* pending  ⚙️ the message contains WIP: not graded

Commits without solution files get no status at all. Each commit is
graded at its own version of the files, not at the tip of the push.
"""

import json
import os
import tempfile
import urllib.request
from pathlib import Path

from drill import adapters, config, runner
from drill import problems as P
from drill.problems import ProblemError

CONTEXT = "drill"


def _git(*args: str) -> str:
    import subprocess

    return subprocess.run(
        ["git", *args], capture_output=True, text=True, cwd=config.ROOT,
        check=True,
    ).stdout


def solution_files(sha: str) -> list[str]:
    out = _git("diff-tree", "--no-commit-id", "--name-only", "-r",
               "--diff-filter=AM", sha, "--", "solutions/")
    return [f for f in out.splitlines() if not f.endswith(".gitkeep")]


def problem_ids(files: list[str]) -> list[str]:
    return sorted({f.split("/")[1] for f in files if f.count("/") >= 2})


def _post_status(sha: str, state: str, description: str) -> None:
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not token or not repo:
        print(f"    (not on GitHub: would set {state} on {sha[:7]})")
        return
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/statuses/{sha}",
        data=json.dumps({
            "state": state, "context": CONTEXT,
            "description": description[:140],
        }).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30):
        pass


def _grade_commit(sha: str, files: list[str]) -> tuple[str, str]:
    """Grade the commit's own version of its solution files."""
    from drill.cli import SOLUTION_FILES, THEORY_FILE

    problem = P.find(files[0].split("/")[1])
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        for rel in files:
            name = rel.rsplit("/", 1)[-1]
            if problem.is_theory:
                if name != THEORY_FILE:
                    continue
                lang = None
            else:
                lang = next(
                    (k for k, v in SOLUTION_FILES.items() if v == name), None
                )
                if lang not in problem.languages:
                    continue
            path = Path(tmp) / name
            path.write_text(_git("show", f"{sha}:{rel}"), encoding="utf-8")
            graded = runner.grade(problem, lang, path)
            label = graded.label
            if graded.status == "skip":
                return "failure", f"{problem.id} {label}: not graded " \
                    f"({graded.message[:60]})"
            results.append((label, graded))
    if not results:
        return "none", ""  # e.g. only notes: not a solution, no flag
    failed = [r for r in results if r[1].status == "fail"]
    summary = ", ".join(
        f"{label} {'passed' if g.status == 'pass' else 'failed'} {g.score}"
        for label, g in results
    )
    state = "failure" if failed else "success"
    return state, f"{problem.id}: {summary}"


def run(base: str, head: str) -> int:
    """Flag each solution commit in base..head; exit code = head's verdict."""
    from drill.cli import is_wip

    commits = _git("rev-list", "--reverse", "--no-merges",
                   f"{base}..{head}").split()
    head_sha = _git("rev-parse", head).strip()
    head_state = None
    try:
        for sha in commits:
            files = solution_files(sha)
            if not files:
                continue  # no solution in this commit: no flag
            ids = problem_ids(files)
            message = _git("log", "-1", "--format=%B", sha)
            subject = message.splitlines()[0] if message else ""
            if len(ids) > 1:
                state = "failure"
                text = f"Refused: one problem per commit (has {', '.join(ids)})"
            elif is_wip(message):
                state, text = "pending", f"{ids[0]}: in progress (WIP)"
            else:
                try:
                    state, text = _grade_commit(sha, files)
                except ProblemError as exc:
                    state, text = "failure", str(exc)
            if state == "none":
                continue
            icon = {"success": "✅", "failure": "❌", "pending": "⚙️"}[state]
            text = f"{icon} {text}"
            print(f"{sha[:7]} {text}   [{subject}]")
            _post_status(sha, state, text)
            if sha == head_sha:
                head_state = state
    finally:
        adapters.close_all()
    if head_state is None:
        print("The last commit has no solution files, so it gets no flag.")
    return 1 if head_state == "failure" else 0
