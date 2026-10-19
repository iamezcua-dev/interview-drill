"""Generate stub files from a problem's tables and output columns."""

import textwrap

from drill import types
from drill.problems import Problem


def _tables_comment(problem: Problem, prefix: str, tsql: bool) -> list[str]:
    to_type = types.tsql if tsql else types.spark
    lines = []
    for table in problem.tables:
        cols = ", ".join(
            f"{c.name} {to_type(c.type)}"
            + (" NULL" if c.nullable else "")
            for c in table.columns
        )
        lines += textwrap.wrap(
            f"{table.name}({cols})", width=74,
            initial_indent=f"{prefix}   ", subsequent_indent=f"{prefix}     ",
        )
    return lines


def _header(problem: Problem, prefix: str, tsql: bool = False) -> str:
    lines = [f"{prefix} {problem.id}: {problem.title}", f"{prefix}"]
    lines += [f"{prefix} Tables:"] + _tables_comment(problem, prefix, tsql)
    lines += [f"{prefix} Output: {', '.join(problem.output)}"]
    lines += [f"{prefix} Full statement: drill show {problem.id}"]
    return "\n".join(lines) + "\n"


def stub(problem: Problem, lang: str | None) -> str:
    if problem.is_theory:
        return (
            f"# {problem.id}: {problem.title}\n\n"
            "<!-- Write your answer below. Add a final line SOLVED when "
            "you are done. -->\n"
        )
    params = [t.name for t in problem.tables]
    if lang in ("tsql", "sparksql"):
        return _header(problem, "--", tsql=lang == "tsql")
    if lang == "pyspark":
        args = ", ".join(f"{p}: DataFrame" for p in params)
        return (
            _header(problem, "#")
            + "\nfrom pyspark.sql import DataFrame\n"
            + "from pyspark.sql import functions as F\n\n\n"
            + f"def solution({args}) -> DataFrame:\n"
            + "    raise NotImplementedError\n"
        )
    if lang == "scala":
        args = ", ".join(f"{p}: DataFrame" for p in params)
        return (
            _header(problem, "//")
            + "\nimport org.apache.spark.sql.DataFrame\n"
            + "import org.apache.spark.sql.functions._\n\n"
            + "object Solution {\n"
            + f"  def solution({args}): DataFrame = ???\n"
            + "}\n"
        )
    raise ValueError(lang)
