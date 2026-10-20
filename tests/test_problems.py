import pytest

from drill import problems as P
from drill.cli import main


def test_every_problem_loads_and_lints(capsys):
    assert main(["lint"]) == 0
    assert "0 errors" in capsys.readouterr().out


@pytest.mark.parametrize("ref", [
    "7xm8", "problems/7xm8-top-sales-per-department", "top-sales",
])
def test_find_accepts_short_ids(ref):
    assert P.find(ref).id == "7xm8"


def test_new_ids_are_valid_and_unused():
    import random

    taken = {p.id for p in P.all_problems()}
    for seed in range(50):
        pid = P.new_id(random.Random(seed))
        assert P.ID_RE.match(pid) and pid not in taken
        assert not set(pid) & set("01ilo")  # no look-alike characters


def test_coding_cases_match_table_shapes():
    for problem in P.all_problems():
        if not problem.is_theory:
            assert problem.cases(), problem.id
