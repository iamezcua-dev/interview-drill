import pytest

from drill import problems as P
from drill.cli import main


def test_every_problem_loads_and_lints(capsys):
    assert main(["lint"]) == 0
    assert "0 errors" in capsys.readouterr().out


@pytest.mark.parametrize("ref", ["q1", "q001", "1", "problems/q001-x"])
def test_find_accepts_short_ids(ref):
    assert P.find(ref).id == "q001"


def test_coding_cases_match_table_shapes():
    for problem in P.all_problems():
        if not problem.is_theory:
            assert problem.cases(), problem.id
