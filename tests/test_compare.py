from decimal import Decimal
from pathlib import Path

from drill.compare import compare
from drill.problems import Case, Problem


def problem(**kw) -> Problem:
    base = dict(
        id="q999", slug="x", path=Path("."), title="t", type="coding",
        level="senior", topic=None, languages=("pyspark",),
        output=("name", "total"), tolerance=0.005, sorted_by=[],
        statement="",
    )
    base.update(kw)
    return Problem(**base)


def case(rows=None, asserts=None, sorted_by=None) -> Case:
    return Case("c", Path("c.yaml"), {}, rows, asserts or [],
                sorted_by or [])


def test_unordered_match_with_tolerance():
    out = compare(problem(), case([["a", 1.0], ["b", 2.0]]),
                  ["total", "name"], [(2.004, "b"), (Decimal("1"), "a")])
    assert out.passed


def test_missing_and_unexpected_rows_reported():
    out = compare(problem(), case([["a", 1]]), ["name", "total"],
                  [("a", 2)])
    assert not out.passed
    assert "missing: (a, 1)" in out.message
    assert "unexpected: (a, 2)" in out.message


def test_duplicates_count():
    out = compare(problem(), case([["a", 1]]), ["name", "total"],
                  [("a", 1), ("a", 1)])
    assert not out.passed


def test_column_names_are_checked():
    out = compare(problem(), case([["a", 1]]), ["name", "sum"], [("a", 1)])
    assert not out.passed
    assert "missing columns ['total']" in out.message


def test_sorted_by_allows_ties_in_any_order():
    p = problem(sorted_by=[("total", True)])
    rows = [["a", 3], ["b", 2], ["c", 2]]
    ok = compare(p, case(rows, sorted_by=p.sorted_by), ["name", "total"],
                 [("a", 3), ("c", 2), ("b", 2)])
    bad = compare(p, case(rows, sorted_by=p.sorted_by), ["name", "total"],
                  [("b", 2), ("a", 3), ("c", 2)])
    assert ok.passed
    assert not bad.passed and "not sorted" in bad.message


def test_asserts_see_normalized_values():
    asserts = [{"expr": "rows[0]['total'] == 5", "message": "five"}]
    assert compare(problem(), case(None, asserts), ["name", "total"],
                   [("a", 5.0)]).passed
    out = compare(problem(), case(None, asserts), ["name", "total"],
                  [("a", 6)])
    assert out.message == "five"
