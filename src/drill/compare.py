"""One comparator for every language.

Adapters return `(columns, rows)` with native values. Values are
normalized so T-SQL, PySpark, Spark SQL and Scala results compare the
same way:

* numbers -> `Decimal` (compared with the problem's tolerance)
* dates / timestamps -> ISO strings
* everything else -> as is
"""

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Sequence

from drill.problems import Case, Problem

MAX_SHOWN = 5


@dataclass
class Outcome:
    passed: bool
    message: str = ""


def normalize(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


def _equal(a: Any, b: Any, tol: Decimal) -> bool:
    if isinstance(a, Decimal) and isinstance(b, Decimal):
        return abs(a - b) <= tol
    return a == b


def _rows_equal(a: Sequence, b: Sequence, tol: Decimal) -> bool:
    return all(_equal(x, y, tol) for x, y in zip(a, b))


def _show(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, Decimal):
        # 23.990000000000002 (float noise) -> 23.99; 800.00 stays 800.00
        rounded = round(value, 6)
        return str(rounded.normalize() if rounded != value else value)
    return str(value)


def _fmt(row: Sequence) -> str:
    return "(" + ", ".join(_show(v) for v in row) + ")"


def _sort_violation(
    rows: list[tuple], columns: Sequence[str], keys: list[tuple[str, bool]]
) -> str | None:
    idx = [(columns.index(c), desc) for c, desc in keys]

    def ordered(a: tuple, b: tuple) -> bool:
        for i, desc in idx:
            x, y = a[i], b[i]
            if x == y:
                continue
            if x is None or y is None:
                return x is None  # NULLs first, like T-SQL and Spark asc
            return x > y if desc else x < y
        return True

    for pos in range(1, len(rows)):
        if not ordered(rows[pos - 1], rows[pos]):
            wanted = ", ".join(
                f"{c} {'DESC' if d else 'ASC'}" for c, d in keys
            )
            return (
                f"rows are not sorted by {wanted}: "
                f"{_fmt(rows[pos - 1])} comes before {_fmt(rows[pos])}"
            )
    return None


def _run_asserts(
    case: Case, rows: list[tuple], columns: Sequence[str]
) -> str | None:
    records = [dict(zip(columns, r)) for r in rows]
    scope = {
        "__builtins__": {},
        "rows": records,
        "len": len, "set": set, "sum": sum, "min": min, "max": max,
        "sorted": sorted, "any": any, "all": all, "abs": abs,
        "D": Decimal,
    }
    for check in case.asserts:
        try:
            ok = eval(check["expr"], scope)  # noqa: S307 - author-written
        except Exception as exc:  # noqa: BLE001 - report, don't crash
            return f"{check.get('message', check['expr'])} ({exc})"
        if not ok:
            return check.get("message", check["expr"])
    return None


def compare(
    problem: Problem, case: Case, columns: Sequence[str], rows: Sequence
) -> Outcome:
    actual_cols = [c.lower() for c in columns]
    if len(set(actual_cols)) != len(actual_cols):
        return Outcome(False, f"duplicate output columns: {list(columns)}")
    wanted = list(problem.output)
    if set(actual_cols) != set(wanted):
        missing = [c for c in wanted if c not in actual_cols]
        extra = [c for c in actual_cols if c not in wanted]
        parts = []
        if missing:
            parts.append(f"missing columns {missing}")
        if extra:
            parts.append(f"unexpected columns {extra}")
        return Outcome(False, "; ".join(parts))

    order = [actual_cols.index(c) for c in wanted]
    actual = [tuple(normalize(r[i]) for i in order) for r in rows]
    tol = Decimal(str(problem.tolerance))

    if case.rows is not None:
        expected = [tuple(normalize(v) for v in r) for r in case.rows]
        unmatched = list(actual)
        missing = []
        for exp in expected:
            hit = next(
                (i for i, a in enumerate(unmatched)
                 if _rows_equal(exp, a, tol)),
                None,
            )
            if hit is None:
                missing.append(exp)
            else:
                unmatched.pop(hit)
        if missing or unmatched:
            lines = [f"expected {len(expected)} rows, got {len(actual)}"]
            for label, bucket in (("missing", missing),
                                  ("unexpected", unmatched)):
                for row in bucket[:MAX_SHOWN]:
                    lines.append(f"  {label}: {_fmt(row)}")
                if len(bucket) > MAX_SHOWN:
                    lines.append(
                        f"  ... and {len(bucket) - MAX_SHOWN} more {label}"
                    )
            lines.append(f"  columns: {', '.join(wanted)}")
            return Outcome(False, "\n".join(lines))

    if case.sorted_by:
        problem_msg = _sort_violation(actual, wanted, case.sorted_by)
        if problem_msg:
            return Outcome(False, problem_msg)

    if case.asserts:
        failed = _run_asserts(case, actual, wanted)
        if failed:
            return Outcome(False, failed)

    return Outcome(True)
