# AGENTS.md

Instructions for any AI coding assistant working in this repository
(Claude Code, Codex, Cursor, Copilot, Gemini, ...). Humans: see
README.md.

## What this repo is

A drill bank for data engineering interviews. Each problem is solvable in
up to four languages (T-SQL, PySpark, Spark SQL, Scala) or is a theory
question. The `drill` CLI (Python, `drill/`) grades solutions against
language-neutral test cases.

## Layout and ownership

```
problems/q001-<slug>/      owned by main: never edit on a drill branch
  problem.md               frontmatter (id, title, type, languages,
                           output, tolerance, sorted_by) + statement
  tables.yaml              input tables with neutral column types
  cases/NN_<name>.yaml     input rows + expected rows / asserts
  stubs/                   starting files copied by `drill start`
  reference/               reference answers (spoilers by design)
solutions/q001/            owned by the solver; empty on main
drill/                     grader CLI and language adapters
drill.example.yaml         documented settings; drill.yaml is local only
```

File names: `solution.tsql.sql`, `solution.sparksql.sql`,
`solution.pyspark.py`, `solution.scala`, `answer.md` (theory). Problem ids
are `q` + 3 digits. Language keys: `tsql`, `pyspark`, `sparksql`,
`scala`.

## Commands

```
poetry run drill setup                 # once: SQL Server, password, hooks
poetry run drill next [--lang L]       # pick an unstarted problem
poetry run drill start q001 --lang L   # copy the stub into solutions/
poetry run drill test q001 [--lang L]  # grade it
poetry run drill status                # progress
poetry run drill check --all --reference   # all reference answers
poetry run drill lint                  # validate problem folders
poetry run pytest                      # tests for drill itself
```

Never run git commands that commit, push or rewrite history unless the
user asks; propose a commit message first.

## Code style

- Max line length 80 for source code, SQL, YAML and Markdown prose.
  Shell commands in docs may exceed it so they stay copy-pasteable.
- Match the surrounding code; Python is checked with ruff.

## Authoring a problem

1. `drill new <kebab-slug> --title "..."` (coding) or add `--type theory`.
2. Fill `problem.md`, `tables.yaml` and at least 10 `cases/*.yaml`
   covering edge cases (ties, empty input, NULLs, precision, ordering).
3. `drill stubs qNNN`, then write a reference in every declared language.
4. `drill test qNNN --reference` must be green for every language.
5. Expected rows must come from the problem author's intent, not be
   derived from the reference you are testing; derive them by hand or
   from an independent source, and say so in the PR.
6. Never change the output contract of a published problem; add a new id.

## Running a drill session (when the user asks to be drilled)

The user practices for interviews and asked for a strict mentor.

**Presenting a problem (every time, in this order):**
1. Show the session stats: score, retry pool, current streak, problem
   id and type (SQL coding / PySpark coding / Theory).
2. Run `drill start <id> --lang <lang>` (coding) or create the answer
   file (theory).
3. Show the stats again, then the table schema, the problem statement and
   any datatype or output notes (`drill show <id>`).
4. Wait for the user's "go" before starting the timer. When they say
   they are done, report the elapsed time and run `drill test` right away,
   without asking them to run it.

**Grading:**
- Be strict: an incomplete answer is a fail; every part of a multi-part
  question must be right.
- Theory: a definition without the *why* / *when it matters* fails.
- On a fail: record it, reset the streak, add the problem to the retry
  pool and teach the concept. Don't give away a coding answer unless
  asked.
- Theory answers live in `solutions/<id>/answer.md`. Append one block per
  attempt so progress stays visible; keep the elaborated answer last:

  ```
  ATTEMPT <N> - PASS|FAIL
  USER ANSWER: <verbatim>
  VERDICT: <what was right / missing>

  ELABORATED CORRECT ANSWER: <full answer>
  ```

  The file's final line is `SOLVED` once the user passes.
- Retry failed problems after a streak of correct answers.

**Commits during a session:** one commit per problem, only that
problem's files, never files of problems still in the retry pool.
Suggested format on drill branches: `q001(tsql): Attempt 2`.
