from drill import problems as P
from drill import quiz


def test_normalize_accepts_letters_lists_and_commas():
    assert quiz.normalize("B") == {"b"}
    assert quiz.normalize(["b", "D"]) == {"b", "d"}
    assert quiz.normalize("b, d") == {"b", "d"}
    assert quiz.normalize(None) == frozenset()


def test_stub_keeps_questions_and_drops_answers(tmp_path):
    problem = P.find("q003")
    questions = quiz.load(
        problem.path / "reference" / quiz.QUIZ_FILE, require_answers=True
    )
    stub = tmp_path / "quiz.yaml"
    stub.write_text(quiz.stub_text(problem.id, problem.title, questions),
                    encoding="utf-8")
    reloaded = quiz.load(stub, require_answers=False)
    assert [q.prompt for q in reloaded] == [q.prompt for q in questions]
    assert all(not q.answer for q in reloaded)


def test_wrong_answers_are_named_but_not_revealed(tmp_path):
    reference = [
        quiz.Question("q1", {"a": "x", "b": "y"}, frozenset({"b"})),
        quiz.Question("q2", {"a": "x", "b": "y"}, frozenset({"a"})),
    ]
    solver = tmp_path / "quiz.yaml"
    solver.write_text(
        "questions:\n  - answer: b\n  - answer: b\n", encoding="utf-8"
    )
    outcomes = dict(quiz.grade(reference, solver))
    assert outcomes["quiz_01"].passed
    assert not outcomes["quiz_02"].passed
    assert "not right yet" in outcomes["quiz_02"].message
    assert "a" not in outcomes["quiz_02"].message.split()
