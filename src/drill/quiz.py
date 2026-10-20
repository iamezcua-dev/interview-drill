"""Deterministic quizzes for theory problems.

`reference/quiz.yaml` holds the questions and their answers. The solver's
`quiz.yaml` (generated into `stubs/` by `drill stubs`) has the same
questions with empty answers. A theory problem passes when every quiz
answer is right *and* the free-text `answer.md` is marked SOLVED.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from drill import CMD
from drill.compare import Outcome
from drill.problems import ProblemError

QUIZ_FILE = "quiz.yaml"
MIN_QUESTIONS = 5


@dataclass(frozen=True)
class Question:
    prompt: str
    choices: dict[str, str]
    answer: frozenset[str]


def normalize(raw: Any) -> frozenset[str]:
    """`b`, `[b, d]` or `"b, d"` -> {"b", "d"} (case-insensitive)."""
    if raw is None:
        return frozenset()
    items = raw if isinstance(raw, list) else str(raw).split(",")
    return frozenset(str(i).strip().lower() for i in items if str(i).strip())


def load(path: Path, require_answers: bool) -> list[Question]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ProblemError(f"{path}: {exc}") from exc
    questions = []
    for number, item in enumerate(data.get("questions") or [], 1):
        choices = {
            str(k).lower(): str(v) for k, v in (item.get("choices") or {})
            .items()
        }
        answer = normalize(item.get("answer"))
        if require_answers:
            if len(choices) < 2:
                raise ProblemError(f"{path}: question {number} needs choices")
            if not answer or not answer <= set(choices):
                raise ProblemError(
                    f"{path}: question {number} answer must be one or more "
                    f"of {sorted(choices)}"
                )
        questions.append(Question(str(item.get("prompt", "")), choices,
                                  answer))
    return questions


def stub_text(problem_id: str, title: str, questions: list[Question]) -> str:
    lines = [
        f"# {problem_id}: {title}",
        "# Answer every question with one letter (answer: b) or, when it",
        "# says \"select all that apply\", a list (answer: [a, c]).",
        "questions:",
    ]
    for q in questions:
        lines.append(f"  - prompt: {json.dumps(q.prompt, ensure_ascii=False)}")
        lines.append("    choices:")
        for key, text in q.choices.items():
            lines.append(
                f"      {key}: {json.dumps(text, ensure_ascii=False)}"
            )
        lines.append("    answer:")
    return "\n".join(lines) + "\n"


def grade(reference: list[Question], solver: Path) -> list[tuple[str, Outcome]]:
    """One outcome per question; wrong ones are named, answers not shown."""
    if not solver.is_file():
        return [("quiz", Outcome(False, f"missing {solver.name}: run "
                                        f"{CMD} start again to recreate it"))]
    try:
        answers = load(solver, require_answers=False)
    except ProblemError as exc:
        return [("quiz", Outcome(False, str(exc)))]
    results = []
    for number, question in enumerate(reference, 1):
        given = answers[number - 1].answer if number <= len(answers) \
            else frozenset()
        if not given:
            outcome = Outcome(False, f"question {number} is not answered")
        elif given == question.answer:
            outcome = Outcome(True)
        else:
            outcome = Outcome(False, f"question {number} is not right yet")
        results.append((f"quiz_{number:02d}", outcome))
    return results
