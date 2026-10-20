"""`drill` command line.

Exit codes are the same locally and in CI:
  0  everything ran; failing *solutions* are reported, never fatal
  1  a reference answer failed, a commit message is invalid, or
     `--strict` and a language had to be skipped
  2  a problem folder is malformed (an author error)
"""

import argparse
import os
import random
import re
import shutil
import subprocess
import sys
from pathlib import Path

from drill import (
    CMD, LANGUAGES, MAINTAINER_EMAIL, SOLUTION_FILES, THEORY_FILE, adapters,
    config,
)
from drill import problems as P
from drill import runner, scaffold
from drill.problems import Problem, ProblemError
from drill.quiz import QUIZ_FILE
from drill.runner import Graded

ICONS = {"pass": "✅", "fail": "❌", "skip": "⏭️ ", "wip": "⚙️ "}
COMMIT_TYPES = (
    "feat", "fix", "docs", "style", "refactor", "perf", "test", "build",
    "ci", "revert", "chore",
)


# --------------------------------------------------------------- helpers

def _git(*args: str) -> str:
    try:
        return subprocess.run(
            ["git", *args], capture_output=True, text=True,
            cwd=config.ROOT, timeout=20,
        ).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


def _branch() -> str:
    return (
        os.environ.get("GITHUB_HEAD_REF")
        or os.environ.get("GITHUB_REF_NAME")
        or _git("rev-parse", "--abbrev-ref", "HEAD")
    )


def is_wip(message: str) -> bool:
    """A commit whose message contains WIP (any case) is in progress."""
    lines = [ln for ln in message.splitlines() if not ln.startswith("#")]
    return "wip" in "\n".join(lines).lower()


def _last_commit_is_wip(path: Path) -> bool:
    message = _git("log", "-1", "--format=%B", "--", str(path))
    return bool(message) and is_wip(message)


def suggested_branch() -> str:
    """drill/<github-login>, falling back to the git user name."""
    login = ""
    if shutil.which("gh"):
        try:
            login = subprocess.run(
                ["gh", "api", "user", "--jq", ".login"],
                capture_output=True, text=True, timeout=10,
            ).stdout.strip()
        except (OSError, subprocess.TimeoutExpired):
            login = ""
    if not login:
        login = _git("config", "user.name")
    slug = re.sub(r"[^a-z0-9]+", "-", login.lower()).strip("-")
    return f"drill/{slug or 'me'}"


def _branch_hint(branch: str) -> str:
    target = suggested_branch()
    exists = bool(_git("rev-parse", "--verify", "--quiet", target))
    switch = f"git switch {target}" if exists else f"git switch -c {target}"
    return (f"Solutions live on your own drill/<username> branch, not on "
            f"'{branch}'.\n  {switch}")


def _langs_for(problem: Problem, lang: str | None) -> list[str | None]:
    if problem.is_theory:
        return [None]
    if lang:
        if lang not in problem.languages:
            raise ProblemError(
                f"{problem.id} is not offered in {lang}; "
                f"choose from {', '.join(problem.languages)}"
            )
        return [lang]
    return list(problem.languages)


def _print_graded(g: Graded, verbose: bool) -> None:
    head = f"{g.problem.id}  {g.label:<9} {ICONS[g.status]} {g.score}"
    print(head.rstrip())
    if g.status == "skip":
        print(f"    skipped: {g.message}")
        return
    if g.message:
        print(f"    {g.message}")
    if verbose:
        for name, outcome in g.cases:
            if outcome.passed:
                print(f"    ok   {name}")
    # Group cases that fail the same way so one bug is reported once.
    groups: dict[str, list[str]] = {}
    for name, outcome in g.cases:
        if not outcome.passed:
            groups.setdefault(outcome.message, []).append(name)
    shown = 0 if verbose else 3
    for index, (message, names) in enumerate(groups.items()):
        if shown and index == shown:
            hidden = sum(len(n) for n in list(groups.values())[shown:])
            print(f"    ... {hidden} more failing cases "
                  f"({CMD} test {g.problem.id} shows all)")
            break
        if len(names) == len(g.cases) > 1:
            label = "all cases"
        elif len(names) > 3:
            label = f"{names[0]} and {len(names) - 1} more cases"
        else:
            label = ", ".join(names)
        print(f"    FAIL {label}")
        for line in message.splitlines():
            print(f"         {line}")


def _github_report(graded: list[Graded], reference: bool) -> None:
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    level = "error" if reference else "warning"
    for g in graded:
        if g.status == "fail":
            first = next(
                (o.message for _, o in g.cases if not o.passed), g.message
            )
            msg = first.replace("\n", "%0A")
            rel = g.path.relative_to(config.ROOT).as_posix()
            print(f"::{level} file={rel},title={g.problem.id} "
                  f"{g.label}::{msg}")
        elif g.status == "skip":
            print(f"::warning title={g.problem.id} {g.label} skipped::"
                  f"{g.message}")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        kind = "Reference answers" if reference else "Solutions"
        lines = [f"### {kind}", "", "| Problem | Language | Result |",
                 "|---|---|---|"]
        for g in graded:
            lines.append(
                f"| {g.problem.id} {g.problem.title} | {g.label} | "
                f"{ICONS[g.status]} {g.score} |"
            )
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n\n")


def _run(
    targets: list[tuple[Problem, str | None, Path]],
    reference: bool, strict: bool, verbose: bool, gate: bool = False,
) -> int:
    if not targets:
        print("Nothing to grade.")
        return 0
    graded = []
    try:
        for problem, lang, path in targets:
            g = runner.grade(problem, lang, path)
            _print_graded(g, verbose)
            graded.append(g)
    finally:
        adapters.close_all()
    if not reference:
        runner.remember(graded)
    _github_report(graded, reference)

    failed = [g for g in graded if g.status == "fail"]
    skipped = [g for g in graded if g.status == "skip"]
    passed = len(graded) - len(failed) - len(skipped)
    print(f"\n{passed} passed, {len(failed)} failed, "
          f"{len(skipped)} skipped")
    if reference and failed:
        return 1
    if (strict or gate) and skipped:
        return 1
    if gate and failed:
        return 1  # CI on drill/* branches: red means not solved
    if failed:
        print("Failures are reported, not blocking: keep iterating and "
              "commit whenever you like.")
    return 0


def _solution_targets(problem: Problem, lang: str | None):
    if lang and problem.is_theory:
        return
    for lg in _langs_for(problem, lang):
        path = problem.solution_path(lg)
        if path.is_file():
            yield problem, lg, path


def _reference_targets(problem: Problem, lang: str | None):
    if lang and problem.is_theory:
        return
    for lg in _langs_for(problem, lang):
        yield problem, lg, problem.reference_path(lg)


def _targets_from_paths(paths: list[str], lang: str | None):
    """Map changed files (from pre-commit) to what needs grading."""
    seen = set()
    for raw in paths:
        parts = Path(raw).as_posix().split("/")
        if len(parts) < 2 or parts[0] not in ("solutions", "problems"):
            continue
        try:
            problem = P.find(parts[1])
        except ProblemError:
            continue
        if parts[0] == "solutions":
            name = parts[-1]
            if problem.is_theory and name in (THEORY_FILE, QUIZ_FILE):
                key = (problem.id, None, "s")
                target = (problem, None, problem.solution_path(None))
            else:
                lg = next(
                    (k for k, v in SOLUTION_FILES.items() if v == name), None
                )
                if lg is None or lg not in problem.languages:
                    continue
                if lang and lg != lang:
                    continue
                key = (problem.id, lg, "s")
                target = (problem, lg, problem.solution_path(lg))
            if key not in seen and target[2].is_file():
                seen.add(key)
                yield "solution", target
        else:
            for target in _reference_targets(problem, lang):
                key = (problem.id, target[1], "r")
                if key not in seen:
                    seen.add(key)
                    yield "reference", target


# --------------------------------------------------------------- commands

def cmd_show(args) -> int:
    problem = P.find(args.problem)
    print(f"{problem.id}: {problem.title}")
    kind = problem.type if not problem.topic else (
        f"{problem.type} ({problem.topic})"
    )
    print(f"[{kind} | {problem.level}]")
    print()
    print(problem.statement)
    if not problem.is_theory:
        from drill import types

        print("\nTables:")
        for table in problem.tables:
            print(f"  {table.name}")
            for c in table.columns:
                null = " NULL" if c.nullable else ""
                print(f"    {c.name:<18} {c.type:<14} "
                      f"T-SQL {types.tsql(c.type)}{null}")
        print(f"\nOutput columns: {', '.join(problem.output)}")
        print(f"Languages: {', '.join(problem.languages)}")
    return 0


def cmd_start(args) -> int:
    problem = P.find(args.problem)
    lang = None
    if not problem.is_theory:
        lang = args.lang or config.default_lang()
        if not lang and len(problem.languages) == 1:
            lang = problem.languages[0]
        if not lang:
            print(f"Pick a language: {CMD} start {problem.id} --lang "
                  f"<{'|'.join(problem.languages)}>  "
                  "(or set default_lang in drill.yaml)")
            return 1
        _langs_for(problem, lang)
    target = problem.solution_path(lang)
    if target.exists():
        print(f"Already started: {target.relative_to(config.ROOT)}")
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        stub = problem.stub_path(lang)
        if stub.is_file():
            shutil.copyfile(stub, target)
        else:
            target.write_text(scaffold.stub(problem, lang), encoding="utf-8")
        print(f"Created {target.relative_to(config.ROOT)}")
    quiz_stub = problem.path / "stubs" / QUIZ_FILE
    quiz_target = target.with_name(QUIZ_FILE)
    if problem.is_theory and quiz_stub.is_file() and not quiz_target.exists():
        shutil.copyfile(quiz_stub, quiz_target)
        print(f"Created {quiz_target.relative_to(config.ROOT)} (answer "
              "every question; it's graded together with your "
              "explanation)")
    print()
    cmd_show(argparse.Namespace(problem=problem.id))
    print(f"\nGrade it with: {CMD} test {problem.id}"
          + (f" --lang {lang}" if lang else ""))
    branch = _branch()
    if not branch.startswith("drill/"):
        print(f"\n⚠️  {_branch_hint(branch)}")
    return 0


def cmd_test(args) -> int:
    problem = P.find(args.problem)
    if args.reference:
        targets = list(_reference_targets(problem, args.lang))
    else:
        targets = list(_solution_targets(problem, args.lang))
        if not targets:
            hint = f" --lang {args.lang}" if args.lang else ""
            print(f"No solution yet. Start one: {CMD} start "
                  f"{problem.id}{hint}")
            return 0
    return _run(targets, args.reference, args.strict, verbose=True)


def cmd_check(args) -> int:
    if args.all:
        problems = P.all_problems()
        if args.reference:
            targets = [
                t for p in problems for t in _reference_targets(p, args.lang)
            ]
        else:
            targets = [
                t for p in problems for t in _solution_targets(p, args.lang)
            ]
        return _run(targets, args.reference, args.strict, args.verbose,
                    gate=args.gate)
    solutions, references = [], []
    for kind, target in _targets_from_paths(args.paths, args.lang):
        (references if kind == "reference" else solutions).append(target)
    code = 0
    if solutions:
        code = max(code, _run(solutions, False, args.strict, args.verbose,
                              gate=args.gate))
    if references:
        code = max(code, _run(references, True, args.strict, args.verbose))
    if not solutions and not references:
        print("Nothing to grade.")
    return code


def _cell(results: dict, problem: Problem, lang: str | None) -> str:
    """✅/❌ from the last grading, ⚙️ while in progress, · not started."""
    path = problem.solution_path(lang)
    if not path.is_file():
        return "·"
    if _last_commit_is_wip(path):
        return ICONS["wip"].strip()
    cached = results.get(problem.id, {}).get(lang or "theory")
    if cached and abs(cached.get("mtime", 0) - path.stat().st_mtime) < 1e-6:
        return ICONS[cached["status"]].strip()
    return ICONS["wip"].strip()  # changed since it was last graded


def cmd_status(args) -> int:
    problems = P.all_problems()
    if args.refresh:
        # Explicit and expensive: re-grade every started solution.
        targets = [t for p in problems for t in _solution_targets(p, None)]
        _run(targets, reference=False, strict=False, verbose=False)
        print()
    results = runner.load_results()
    header = f"{'id':<5} {'title':<46} " + " ".join(
        f"{lg:<8}" for lg in LANGUAGES
    )
    print(header)
    print("-" * len(header))
    done = total = 0
    for p in problems:
        title = (p.title[:43] + "...") if len(p.title) > 46 else p.title
        if p.is_theory:
            cell = _cell(results, p, None)
            cells = f"{'theory: ' + cell:<8}"
            total += 1
            done += cell == "✅"
        else:
            parts = []
            for lg in LANGUAGES:
                if lg in p.languages:
                    cell = _cell(results, p, lg)
                    total += 1
                    done += cell == "✅"
                else:
                    cell = "—"
                parts.append(f"{cell:<8}")
            cells = " ".join(parts)
        print(f"{p.id:<5} {title:<46} {cells}")
    print("\n✅ solved  ❌ not solved  ⚙️  in progress (WIP commit or "
          "changed since last graded)  · not started  — not offered")
    pct = int(done * 100 / total) if total else 0
    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
    print(f"{bar}  {done} / {total} done ({pct}%)")
    return 0


def cmd_next(args) -> int:
    candidates = []
    for p in P.all_problems():
        if args.type and p.type != args.type:
            continue
        if p.is_theory:
            if args.lang:
                continue
            if not p.solution_path(None).exists():
                candidates.append((p, None))
            continue
        langs = [args.lang] if args.lang else list(p.languages)
        if args.lang and args.lang not in p.languages:
            continue
        if args.lang:
            if not p.solution_path(args.lang).exists():
                candidates.append((p, args.lang))
        elif not p.solution_dir().exists() or not any(
            p.solution_path(lg).exists() for lg in langs
        ):
            candidates.append((p, None))
    if not candidates:
        print("You have started every matching problem. Revisit one with "
              f"`{CMD} status`, or add new ones with `{CMD} new`.")
        return 0
    problem, lang = (
        random.choice(candidates) if args.random else candidates[0]
    )
    kind = problem.type if not problem.topic else (
        f"{problem.type} ({problem.topic})"
    )
    print(f"Next: {problem.id} {problem.title} [{kind}]")
    if problem.is_theory:
        print(f"  {CMD} start {problem.id}")
    else:
        for lg in [lang] if lang else problem.languages:
            print(f"  {CMD} start {problem.id} --lang {lg}")
    return 0


def cmd_new(args) -> int:
    existing = [p.name[:4] for p in config.PROBLEMS_DIR.glob("q*-*")]
    number = max((int(e[1:]) for e in existing), default=0) + 1
    pid = f"q{number:03d}"
    slug = args.slug.strip().lower()
    folder = config.PROBLEMS_DIR / f"{pid}-{slug}"
    if not P.FOLDER_RE.match(folder.name):
        print("slug must be kebab-case, e.g. top-three-customers")
        return 2
    languages = args.languages.split(",") if args.type == "coding" else []
    (folder / "stubs").mkdir(parents=True)
    (folder / "reference").mkdir()
    front = [
        "---", f"id: {pid}", f"title: {args.title}", f"type: {args.type}",
        "level: senior",
    ]
    if args.type == "theory":
        front.append("topic: sql  # or spark")
        (folder / "reference" / QUIZ_FILE).write_text(
            "questions:\n  - prompt: \"...\"\n    choices:\n"
            "      a: \"...\"\n      b: \"...\"\n    answer: a\n",
            encoding="utf-8",
        )
    else:
        front += [
            f"languages: [{', '.join(languages)}]",
            "output: [col_a, col_b]",
            "# tolerance: 0.01          # numeric comparison tolerance",
            "# sorted_by: [{col_b: desc}]  # only if order is required",
        ]
    front += ["---", "", "Describe the problem here.", ""]
    (folder / "problem.md").write_text("\n".join(front), encoding="utf-8")
    if args.type == "coding":
        (folder / "cases").mkdir()
        (folder / "tables.yaml").write_text(
            "my_table:\n  columns:\n    - id: int\n    - name: string(50)\n"
            "    - amount: decimal(10,2)\n    - created: date\n",
            encoding="utf-8",
        )
        (folder / "cases" / "01_base_case.yaml").write_text(
            "name: Base case\ninput:\n  my_table:\n"
            "    - [1, \"a\", 10.50, 2024-01-01]\n"
            "expected:\n  rows:\n    - [\"value\", 1]\n",
            encoding="utf-8",
        )
    print(f"Created {folder.relative_to(config.ROOT)}")
    print("Next: fill problem.md, tables.yaml and cases/, then run "
          f"`{CMD} stubs {pid}` to generate stubs and write references.")
    return 0


def cmd_stubs(args) -> int:
    """(Re)generate stubs for a problem from tables.yaml."""
    problem = P.find(args.problem)
    for lang in _langs_for(problem, None):
        path = problem.stub_path(lang)
        if path.exists() and not args.force:
            print(f"keep  {path.relative_to(config.ROOT)}")
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(scaffold.stub(problem, lang), encoding="utf-8")
        print(f"write {path.relative_to(config.ROOT)}")
        ref = problem.reference_path(lang)
        if not ref.exists():
            ref.parent.mkdir(parents=True, exist_ok=True)
            ref.write_text(scaffold.stub(problem, lang), encoding="utf-8")
            print(f"write {ref.relative_to(config.ROOT)}")
    if problem.is_theory:
        ref_quiz = problem.path / "reference" / QUIZ_FILE
        if ref_quiz.is_file():
            stub_quiz = problem.path / "stubs" / QUIZ_FILE
            stub_quiz.write_text(_quiz_stub(problem), encoding="utf-8")
            print(f"write {stub_quiz.relative_to(config.ROOT)} (from "
                  "reference/quiz.yaml, answers removed)")
    return 0


def _quiz_stub(problem: Problem) -> str:
    from drill import quiz

    questions = quiz.load(
        problem.path / "reference" / QUIZ_FILE, require_answers=True
    )
    return quiz.stub_text(problem.id, problem.title, questions)


MIN_CASES = 10


def _similar_titles(problems: list[Problem], threshold: float = 0.8):
    from difflib import SequenceMatcher

    for i, a in enumerate(problems):
        for b in problems[i + 1:]:
            ratio = SequenceMatcher(
                None, a.title.lower(), b.title.lower()
            ).ratio()
            if ratio >= threshold:
                yield a, b


def _lint_quiz(problem: Problem) -> list[str]:
    from drill import quiz

    ref = problem.path / "reference" / QUIZ_FILE
    if not ref.is_file():
        return [f"{problem.id}: missing reference/{QUIZ_FILE}"]
    try:
        questions = quiz.load(ref, require_answers=True)
    except ProblemError as exc:
        return [str(exc)]
    errors = []
    if len(questions) < quiz.MIN_QUESTIONS:
        errors.append(f"{problem.id}: quiz needs at least "
                      f"{quiz.MIN_QUESTIONS} questions, has {len(questions)}")
    stub = problem.path / "stubs" / QUIZ_FILE
    if not stub.is_file() or stub.read_text(encoding="utf-8") != \
            _quiz_stub(problem):
        errors.append(f"{problem.id}: stubs/{QUIZ_FILE} is out of date; "
                      f"run {CMD} stubs {problem.id}")
    return errors


def cmd_lint(args) -> int:
    errors = []
    try:
        problems = P.all_problems()
    except ProblemError as exc:
        print(f"ERROR {exc}")
        return 2
    ids = set()
    for p in problems:
        if p.id in ids:
            errors.append(f"{p.id}: duplicate id")
        ids.add(p.id)
        try:
            if not p.is_theory and len(p.cases()) < MIN_CASES:
                errors.append(
                    f"{p.id}: needs at least {MIN_CASES} cases, has "
                    f"{len(p.cases())}"
                )
        except ProblemError as exc:
            errors.append(str(exc))
        if p.is_theory:
            errors += _lint_quiz(p)
        for lang in _langs_for(p, None):
            for path in (p.stub_path(lang), p.reference_path(lang)):
                if not path.is_file():
                    errors.append(
                        f"{p.id}: missing {path.relative_to(config.ROOT)}"
                    )
    if args.main:
        stray = [
            f for f in config.SOLUTIONS_DIR.rglob("*")
            if f.is_file() and f.name != ".gitkeep"
        ]
        for f in stray:
            errors.append(
                f"solutions must not land on main: "
                f"{f.relative_to(config.ROOT)}"
            )
    for a, b in _similar_titles(problems):
        print(f"WARN  {a.id} and {b.id} have very similar titles; make sure "
              f"one isn't a duplicate:\n      {a.title!r}\n      "
              f"{b.title!r}")
    for e in errors:
        print(f"ERROR {e}")
    print(f"{len(problems)} problems checked, {len(errors)} errors")
    return 2 if errors else 0


def _message_ok(subject: str) -> bool:
    if subject.startswith(("Merge ", "Revert ", "fixup!", "squash!")):
        return True
    pattern = (
        rf"^({'|'.join(COMMIT_TYPES)})(\([a-z0-9][\w./-]*\))?!?: \S.{{2,}}$"
    )
    return bool(re.match(pattern, subject)) and len(subject) <= 72


def cmd_commit_msg(args) -> int:
    """commit-msg hook: lint the message, then grade what this commit adds.

    * drill/* branches: messages are free-form; one problem per commit
      (the only thing that blocks); the changed solution is graded (✅/❌)
      unless the message contains WIP (⚙️). Failing never blocks.
    * other branches: conventional commit messages; changed problems have
      their reference answers graded, and a failing reference blocks.
    """
    branch = _branch()
    on_drill = branch.startswith("drill/")
    text = Path(args.file).read_text(encoding="utf-8")
    subject = next(
        (ln for ln in text.splitlines()
         if ln.strip() and not ln.startswith("#")),
        "",
    )
    if not on_drill and not _message_ok(subject):
        print(f"Commit subject must look like 'feat(q016): Add running "
              f"total problem' (≤72 chars).\nAllowed types: "
              f"{', '.join(COMMIT_TYPES)}\nGot: {subject!r}\n"
              "(Drill branches 'drill/<name>' are exempt.)")
        return 1
    staged = _git("diff", "--cached", "--name-only", "--diff-filter=AM")
    solutions, references = [], []
    for kind, target in _targets_from_paths(staged.splitlines(), None):
        if kind == "solution":
            solutions.append(target)
        elif not on_drill:
            references.append(target)
    ids = sorted({problem.id for problem, _, _ in solutions})
    if solutions and not on_drill:
        print(f"Refused: {_branch_hint(branch)}\nYour staged files stay "
              "staged; switch branches and commit again.")
        return 1
    if on_drill and len(ids) > 1:
        print(f"Refused: one problem per commit, but this commit has "
              f"{', '.join(ids)}.\nCommit them separately, e.g. "
              f"`git restore --staged solutions/{ids[1]}`, then commit "
              "again.")
        return 1
    code = 0
    if solutions and is_wip(text):
        for problem, lang, _ in solutions:
            label = lang or "theory"
            print(f"{problem.id}  {label:<9} {ICONS['wip']} in progress "
                  "(WIP commit, not graded)")
    elif solutions:
        code = _run(solutions, False, False, verbose=False)
    if solutions:
        print(f"\nShare your approach: git push -u origin {branch}\n"
              "No permission to push? Ask the maintainer for access: "
              f"{MAINTAINER_EMAIL}")
    if references:
        code = max(code, _run(references, True, False, verbose=False))
    return code


def cmd_ci(args) -> int:
    from drill import ci

    return ci.run(args.base, args.head)


def cmd_setup(args) -> int:
    from drill.setup import SetupError, setup

    try:
        setup(skip_db=args.skip_db, skip_hooks=args.skip_hooks)
    except SetupError as exc:
        print(f"\nSetup stopped: {exc}", file=sys.stderr)
        return 1
    print("\n==> Checking toolchains")
    code = cmd_doctor(args)
    print(f"\nAll set. Try: {CMD} next")
    return code


def cmd_doctor(args) -> int:
    ok = True

    def line(good: bool | None, what: str, detail: str = "") -> None:
        nonlocal ok
        icon = {True: "✅", False: "❌", None: "⚠️ "}[good]
        ok = ok and good is not False
        print(f"{icon} {what}" + (f": {detail}" if detail else ""))

    line(sys.version_info >= (3, 12), "Python",
         sys.version.split()[0] + " (needs 3.12+)")
    from drill.java import JavaNotFound, find_java_home

    try:
        home, major = find_java_home()
        line(True, "Java for Spark", f"{major} at {home}")
    except JavaNotFound as exc:
        line(False, "Java for Spark (pyspark, sparksql)", str(exc))
    from drill.adapters.tsql import TsqlAdapter
    from drill.adapters import Unavailable

    try:
        conn = TsqlAdapter()._connect("master", autostart=False)
        conn.close()
        s = config.db_settings()
        line(True, "SQL Server (tsql)", f"{s.host}:{s.port}")
    except Unavailable as exc:
        line(None, "SQL Server (tsql)", str(exc))
    scala = shutil.which("scala-cli")
    line(True if scala else None, "scala-cli (scala)",
         scala or "not installed; Scala problems will be skipped")
    hook = config.ROOT / ".git" / "hooks" / "pre-commit"
    installed = hook.is_file() and "pre-commit" in hook.read_text(
        encoding="utf-8", errors="ignore"
    )
    line(True if installed else None, "git hooks",
         "installed" if installed else
         f"run: {CMD} setup")
    return 0 if ok else 1


# --------------------------------------------------------------- parser

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="drill", description="Data engineering interview drills."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def lang_opt(p):
        p.add_argument("--lang", choices=LANGUAGES)

    p = sub.add_parser("show", help="print a problem statement")
    p.add_argument("problem")
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("start", help="copy a stub into solutions/")
    p.add_argument("problem")
    lang_opt(p)
    p.set_defaults(func=cmd_start)

    p = sub.add_parser("test", help="grade one problem")
    p.add_argument("problem")
    lang_opt(p)
    p.add_argument("--reference", action="store_true",
                   help="grade the reference answers instead")
    p.add_argument("--strict", action="store_true",
                   help="fail when a language must be skipped")
    p.set_defaults(func=cmd_test)

    p = sub.add_parser(
        "check", help="grade changed files (hook) or everything (--all)"
    )
    p.add_argument("paths", nargs="*")
    p.add_argument("--all", action="store_true")
    lang_opt(p)
    p.add_argument("--reference", action="store_true")
    p.add_argument("--strict", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--gate", action="store_true",
                   help="exit 1 unless every graded solution passes (CI)")
    p.set_defaults(func=cmd_check)

    p = sub.add_parser("status", help="progress per problem and language")
    p.add_argument("--refresh", action="store_true",
                   help="re-grade every started solution first (slow)")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("next", help="suggest a problem you haven't started")
    p.add_argument("--type", choices=("coding", "theory"))
    lang_opt(p)
    p.add_argument("--random", action="store_true")
    p.set_defaults(func=cmd_next)

    p = sub.add_parser("new", help="scaffold a new problem (authors)")
    p.add_argument("slug", help="kebab-case, e.g. top-three-customers")
    p.add_argument("--title", required=True)
    p.add_argument("--type", choices=("coding", "theory"), default="coding")
    p.add_argument("--languages", default=",".join(LANGUAGES))
    p.set_defaults(func=cmd_new)

    p = sub.add_parser("stubs", help="generate stubs from tables.yaml")
    p.add_argument("problem")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_stubs)

    p = sub.add_parser("lint", help="validate every problem folder")
    p.add_argument("--main", action="store_true",
                   help="also reject solutions/ content (main branch)")
    p.set_defaults(func=cmd_lint)

    p = sub.add_parser("commit-msg", help="(hook) validate a commit message")
    p.add_argument("file")
    p.set_defaults(func=cmd_commit_msg)

    p = sub.add_parser(
        "ci", help="(CI) flag every solution commit in base..head"
    )
    p.add_argument("base")
    p.add_argument("head", nargs="?", default="HEAD")
    p.set_defaults(func=cmd_ci)

    p = sub.add_parser(
        "setup", help="start SQL Server, store its password, install hooks"
    )
    p.add_argument("--skip-db", action="store_true",
                   help="don't touch Docker (use your own SQL Server)")
    p.add_argument("--skip-hooks", action="store_true")
    p.set_defaults(func=cmd_setup)

    p = sub.add_parser("doctor", help="check toolchains and connectivity")
    p.set_defaults(func=cmd_doctor)
    return parser


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (ProblemError, config.ConfigError) as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
