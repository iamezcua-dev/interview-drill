# Interview Drill

Warm-up drills for data engineering interviews. Solve each problem in
**T-SQL, PySpark, Spark SQL or Scala** (or answer theory questions), and
let `drill` grade you instantly.

## Quick start

```bash
poetry install                 # Python deps (needs Python 3.12+)
poetry run drill setup         # SQL Server container, password, git hooks
git switch -c drill/<your-github-handle>
poetry run drill next          # suggests a problem you haven't started
```

`drill setup` is safe to re-run. It creates your local `drill.yaml`, starts
a SQL Server container (`compose.yaml`), stores a generated password in
your OS keychain and installs the git hooks, then runs `drill doctor`.

## The daily loop

```bash
poetry run drill start q001 --lang tsql   # copies a stub to solutions/q001/
# ...write your answer in solutions/q001/solution.tsql.sql...
poetry run drill test q001                # grade it, as often as you like
git commit -am "q001(tsql): Attempt 1"    # failures are reported, never blocking
poetry run drill status                   # your progress
```

- **T-SQL / Spark SQL**: a plain `.sql` file, no procedure wrapper. Clear
  it and start over any time; the last result set is your answer.
- **PySpark / Scala**: implement `solution(...)`, which takes one
  DataFrame per table and returns a DataFrame.
- **Theory**: write your answer in `answer.md`, and add a final line
  `SOLVED` when you're happy with it.
- `drill show q001` prints a statement; `drill next --lang pyspark`
  narrows suggestions.
- Reference answers live in `problems/<id>/reference/`. Peeking is up to
  you.

## Requirements

| Language | Needs | Notes |
| -------- | ----- | ----- |
| T-SQL | Docker (or Podman) | `drill setup` runs SQL Server 2022; it auto-starts when you test |
| PySpark, Spark SQL | Java 17 | Auto-detected; or set `java_home` in `drill.yaml` |
| Scala | [scala-cli](https://scala-cli.virtuslab.org/install) | Fetches Scala, Spark and its own JDK on first run |

A missing toolchain only skips that language, so you can drill the
others. `drill doctor` shows what's available.

On Apple silicon, enable *Use Rosetta for x86/amd64 emulation* in Docker
Desktop (the SQL Server image is x86-64 only).

## Settings

Everything is optional. `drill setup` copies `drill.example.yaml` to
`drill.yaml`, which git ignores, and you edit that copy (port, keychain
item, Java home, default language). The SQL Server password lives in your
OS credential store (macOS Keychain, Windows Credential Manager, Linux
Secret Service), not in files. Machines without a store (headless Linux,
WSL) keep it in `drill.yaml`.

## Branches and contributing

- `main` holds problems, references and tooling. It never contains
  solutions, so `git merge main` into your drill branch never conflicts.
- `drill/<handle>`: your personal solutions. Commit freely.
- `problem/<id>-<slug>`: new problems, via a PR to `main`. Use
  conventional commits (`feat(q016): Add ...`) on these branches.
- CI runs the same `drill` commands as your local hooks. On `drill/*` it
  grades your solutions; elsewhere every reference answer must pass.

Adding a problem: `drill new <slug> --title "..."`, fill the folder, then
`drill stubs <id>` and write the references. See AGENTS.md for the full
checklist.

## Problems

| #    | Type   | Languages             | Problem                                            |
| ---- | ------ | --------------------- | -------------------------------------------------- |
| q001 | Coding | tsql, pyspark, sparksql, scala | Top seller(s) per department              |
| q002 | Coding | tsql, pyspark, sparksql, scala | Running total of sales per department     |
| q003 | Theory | Spark                 | Transformations vs actions in Apache Spark         |
| q004 | Coding | tsql, pyspark, sparksql, scala | Employees earning above their department average |
| q005 | Coding | tsql, pyspark, sparksql, scala | Customers with orders in every month of 2024 |
| q006 | Theory | SQL                   | WHERE vs HAVING                                    |
| q007 | Coding | tsql, pyspark, sparksql, scala | Month-over-month revenue change           |
| q008 | Coding | tsql, pyspark, sparksql, scala | Total spent per user, highest first       |
| q009 | Theory | SQL                   | INNER JOIN vs LEFT JOIN vs CROSS JOIN              |
| q010 | Coding | tsql, pyspark, sparksql, scala | Departments with an average salary above 70,000 |
| q011 | Theory | Spark                 | Lazy evaluation in Spark                           |
| q012 | Coding | tsql, pyspark, sparksql, scala | Top 3 customers by total amount spent     |
| q013 | Coding | tsql, pyspark, sparksql, scala | Most recently visited page per user       |
| q014 | Theory | SQL                   | CTE vs subquery                                    |
| q015 | Coding | tsql, pyspark, sparksql, scala | Employee count per department (including empty) |

## Questions?

Send an email to (iamezcua.dev@gmail.com), but don't expect a quick answer.
