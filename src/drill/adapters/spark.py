"""PySpark (DataFrame API) and Spark SQL adapters sharing one session."""

import datetime as dt
import importlib.util
import inspect
import os
import sys
import traceback
from pathlib import Path

from drill import CMD, types
from drill.adapters import Result, SolutionError, Unavailable, short_error
from drill.problems import Case, Problem, Table
from drill.sqltext import spark_statements

LOG4J = Path(__file__).with_name("log4j2.properties")

_session = None


def session():
    global _session
    if _session is not None:
        return _session
    from drill.java import JavaNotFound, configure_env

    try:
        configure_env()
    except JavaNotFound as exc:
        raise Unavailable(str(exc)) from exc
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise Unavailable(f"pyspark is not installed: run {CMD} setup") from exc
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    _session = (
        SparkSession.builder.master("local[2]")
        .appName("drill")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        # If a solver's Python code (e.g. a UDF) crashes a worker, show the
        # Python error instead of a bare EOFException.
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.driver.extraJavaOptions",
                f"-Dlog4j2.configurationFile={LOG4J.as_uri()}")
        .getOrCreate()
    )
    _session.sparkContext.setLogLevel("ERROR")
    return _session


def stop() -> None:
    global _session
    if _session is not None:
        _session.stop()
        _session = None


def _literal(neutral: str, value) -> str:
    """A typed Spark SQL literal for one cell."""
    spark_type = types.spark(neutral)
    value = types.coerce(neutral, value)
    if value is None:
        return f"CAST(NULL AS {spark_type})"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dt.datetime):
        return f"TIMESTAMP'{value.isoformat(sep=' ')}'"
    if isinstance(value, dt.date):
        return f"DATE'{value.isoformat()}'"
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    return f"CAST('{value}' AS {spark_type})"


def _table_sql(table: Table, rows: list) -> str:
    """SELECT over an inline VALUES list, typed exactly as tables.yaml.

    Building inputs in SQL keeps them inside the JVM: unlike
    `createDataFrame(<python list>)` it needs no Python worker processes,
    which are slower to start and crash on some Windows setups.
    """
    names = [f"c{i}" for i in range(len(table.columns))]
    cast = ", ".join(
        f"CAST({n} AS {types.spark(c.type)}) AS `{c.name}`"
        for n, c in zip(names, table.columns)
    )
    if not rows:
        nulls = ", ".join(
            f"CAST(NULL AS {types.spark(c.type)})" for c in table.columns
        )
        return (f"SELECT {cast} FROM VALUES ({nulls}) AS t({', '.join(names)})"
                " WHERE false")
    values = ", ".join(
        "(" + ", ".join(
            _literal(c.type, v) for c, v in zip(table.columns, row)
        ) + ")"
        for row in rows
    )
    return f"SELECT {cast} FROM VALUES {values} AS t({', '.join(names)})"


def frames(problem: Problem, case: Case, spark=None) -> dict:
    spark = spark or session()
    return {
        table.name: spark.sql(
            _table_sql(table, case.input.get(table.name, []))
        )
        for table in problem.tables
    }


def collect(df) -> Result:
    return list(df.columns), [tuple(r) for r in df.collect()]


def _spark_error(exc: Exception) -> str:
    """Keep Spark's message; drop the logical plan and JVM stack."""
    kept = []
    for line in str(exc).splitlines():
        stripped = line.strip()
        if stripped.startswith(("'", "+-", ":-", "== ", "at ", "JVM stack")):
            break
        if stripped:
            kept.append(stripped)
    text = "\n".join(kept).rstrip(";") or type(exc).__name__
    return short_error(Exception(text), limit=6)


class PySparkAdapter:
    lang = "pyspark"

    def _load(self, path: Path):
        name = f"drill_solution_{abs(hash(path))}"
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        # Don't leave __pycache__ folders in the solver's solutions/.
        previous, sys.dont_write_bytecode = sys.dont_write_bytecode, True
        try:
            spec.loader.exec_module(module)
        except Exception as exc:
            raise SolutionError(_user_traceback(exc, path)) from exc
        finally:
            sys.dont_write_bytecode = previous
        fn = getattr(module, "solution", None)
        if not callable(fn):
            raise SolutionError("define a function named solution(...)")
        return fn

    def run(
        self, problem: Problem, solution: Path, cases: list[Case]
    ) -> list[Result | SolutionError]:
        session()
        try:
            fn = self._load(solution)
        except SolutionError as exc:
            return [exc] * len(cases)
        params = list(inspect.signature(fn).parameters)
        names = [t.name for t in problem.tables]
        by_name = set(names) <= set(params)
        out: list[Result | SolutionError] = []
        for case in cases:
            dfs = frames(problem, case)
            try:
                if by_name:
                    result = fn(**dfs)
                else:
                    result = fn(*(dfs[n] for n in names))
            except NotImplementedError:
                out.append(SolutionError("solution() is not implemented"))
                continue
            except Exception as exc:  # noqa: BLE001
                out.append(SolutionError(_user_traceback(exc, solution)))
                continue
            if result is None:
                out.append(SolutionError("solution() returned None"))
                continue
            if not hasattr(result, "collect"):
                out.append(SolutionError(
                    f"solution() must return a DataFrame, got "
                    f"{type(result).__name__}"
                ))
                continue
            try:
                out.append(collect(result))
            except Exception as exc:  # noqa: BLE001
                out.append(SolutionError(_spark_error(exc)))
        return out

    def close(self) -> None:
        stop()


class SparkSqlAdapter:
    lang = "sparksql"

    def run(
        self, problem: Problem, solution: Path, cases: list[Case]
    ) -> list[Result | SolutionError]:
        base = session()
        statements = spark_statements(solution.read_text(encoding="utf-8"))
        if not statements:
            err = SolutionError("solution is empty: write a SELECT query")
            return [err] * len(cases)
        out: list[Result | SolutionError] = []
        for case in cases:
            # A fresh session per case: temp views a previous case created
            # can't leak, and no catalog calls are needed (those require
            # Hadoop's winutils.exe on Windows).
            spark = base.newSession()
            for name, df in frames(problem, case, spark).items():
                df.createOrReplaceTempView(name)
            try:
                for stmt in statements[:-1]:
                    spark.sql(stmt).collect()
                out.append(collect(spark.sql(statements[-1])))
            except Exception as exc:  # noqa: BLE001
                out.append(SolutionError(_spark_error(exc)))
        return out

    def close(self) -> None:
        stop()


def _user_traceback(exc: Exception, path: Path) -> str:
    """Show only frames from the solver's file, plus the error itself."""
    frames_ = [
        f for f in traceback.extract_tb(exc.__traceback__)
        if Path(f.filename) == path
    ]
    lines = [
        f"  line {f.lineno}: {(f.line or '').strip()}" for f in frames_[-3:]
    ]
    message = "".join(traceback.format_exception_only(exc)).strip()
    if type(exc).__module__.startswith(("pyspark", "py4j")):
        message = _spark_error(exc)
    return "\n".join(lines + [message])
