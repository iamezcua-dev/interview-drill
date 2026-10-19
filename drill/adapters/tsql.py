"""T-SQL adapter: runs a plain .sql file against SQL Server via pymssql.

The solution needs no procedure wrapper. `GO` lines are optional; the
last result set the file produces is the answer. Each case gets a fresh
connection so temp tables from one case never leak into the next.
"""

from pathlib import Path

from drill import config, types
from drill.adapters import Result, SolutionError, Unavailable, short_error
from drill.problems import Case, Problem
from drill.sqltext import tsql_batches

HINT = "Run `drill setup` once to create and start SQL Server."


def _message(exc: Exception) -> str:
    args = getattr(exc, "args", ())
    if len(args) >= 2 and isinstance(args[1], bytes):
        text = args[1].decode("utf-8", "replace")
        return text.split("DB-Lib error")[0].strip()
    return short_error(exc)


class TsqlAdapter:
    lang = "tsql"

    def __init__(self) -> None:
        self._ready = False

    def _connect(self, database: str, autostart: bool = True):
        try:
            import pymssql
        except ImportError as exc:
            raise Unavailable("pymssql is not installed: poetry install") \
                from exc
        settings = config.db_settings()
        if not settings.password:
            raise Unavailable(
                "no SQL Server password found in the OS keychain or "
                f"drill.yaml. {HINT}"
            )
        try:
            return pymssql.connect(
                server=settings.host,
                port=settings.port,
                user=settings.user,
                password=settings.password,
                database=database,
                autocommit=True,
                login_timeout=5,
                timeout=60,
            )
        except pymssql.Error as exc:
            from drill.setup import autostart as start_container

            if autostart and start_container():
                return self._connect(database, autostart=False)
            raise Unavailable(
                f"cannot reach SQL Server at {settings.host}:"
                f"{settings.port} ({_message(exc)}). {HINT}"
            ) from exc

    def _ensure_database(self) -> str:
        name = config.db_settings().database
        if not self._ready:
            conn = self._connect("master")
            with conn.cursor() as cur:
                cur.execute(
                    f"IF DB_ID(N'{name}') IS NULL CREATE DATABASE [{name}]"
                )
            conn.close()
            self._ready = True
        return name

    def _load_tables(self, cur, problem: Problem, case: Case) -> None:
        for table in problem.tables:
            cols = ", ".join(
                f"[{c.name}] {types.tsql(c.type)}"
                + (" NULL" if c.nullable else " NOT NULL")
                for c in table.columns
            )
            cur.execute(f"DROP TABLE IF EXISTS [{table.name}]")
            cur.execute(f"CREATE TABLE [{table.name}] ({cols})")
            rows = case.input.get(table.name, [])
            if rows:
                marks = ", ".join(["%s"] * len(table.columns))
                cur.executemany(
                    f"INSERT INTO [{table.name}] VALUES ({marks})",
                    [
                        tuple(
                            types.coerce(c.type, v)
                            for c, v in zip(table.columns, row)
                        )
                        for row in rows
                    ],
                )

    def run(
        self, problem: Problem, solution: Path, cases: list[Case]
    ) -> list[Result | SolutionError]:
        database = self._ensure_database()
        batches = tsql_batches(solution.read_text(encoding="utf-8"))
        if not batches:
            err = SolutionError("solution is empty: write a SELECT query")
            return [err] * len(cases)
        out: list[Result | SolutionError] = []
        for case in cases:
            conn = self._connect(database)
            try:
                with conn.cursor() as cur:
                    cur.execute("SET NOCOUNT ON")
                    self._load_tables(cur, problem, case)
                    last = None
                    for batch in batches:
                        cur.execute(batch)
                        while True:
                            if cur.description:
                                cols = [d[0] for d in cur.description]
                                last = (cols, cur.fetchall())
                            if not cur.nextset():
                                break
                if last is None:
                    out.append(SolutionError(
                        "the solution returned no result set; end it with "
                        "a SELECT"
                    ))
                else:
                    out.append(last)
            except Exception as exc:  # noqa: BLE001
                out.append(SolutionError(_message(exc)))
            finally:
                conn.close()
        return out

    def close(self) -> None:
        pass
