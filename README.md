# Interview Drill

Warm-up drills for data engineering interviews. Solve each problem in
**T-SQL, PySpark, Spark SQL or Scala**, and
let `drill` grade you instantly.

## Quick start

All you need is **git** (plus Docker for T-SQL problems). No Python,
Poetry or Java install is required.

```bash
git clone https://github.com/iamezcua-dev/interview-drill.git
cd interview-drill
./drill setup        # Windows (cmd or PowerShell): drill setup
./drill next         # suggests a problem you haven't started
```

`./drill` (or `drill.cmd` on Windows) is a small launcher. The first time,
if [uv](https://docs.astral.sh/uv/) is missing, it explains what it is and
**asks before installing it**. uv then downloads the right Python and
drill's dependencies by itself, and your shell profile is never edited.

`drill setup` is safe to re-run. It creates your local `drill.yaml`,
offers to download Java 17 for Spark if you don't have it (into drill's own
cache, nothing system-wide), starts a SQL Server container
(`compose.yaml`), stores a generated password in your OS keychain,
installs the git hooks, puts a fresh clone on your own branch
**`drill/<your-github-login>`**, and finally runs `drill doctor`.

> **Your solutions always live on `drill/<username>`.** Commit there, push
> it here to share your approaches (`git push -u origin drill/<username>`),
> and merge `main` into it whenever you want new problems. It is never
> merged into `main`. The git hook refuses solution commits on any other
> branch and tells you how to switch. No permission to push? Ask for
> access at iamezcua.dev@gmail.com.

## The daily loop

```bash
./drill start 7xm8xp --lang tsql            # copies a stub to solutions/7xm8xp/
# ...write your answer in solutions/7xm8xp/solution.tsql.sql...
./drill test 7xm8xp                         # grade it, as often as you like
git commit -am "7xm8xp(tsql): WIP"          # ⚙️ in progress: saved, not graded
git commit -am "7xm8xp(tsql): Attempt 2"    # graded: ✅ solved or ❌ not solved
./drill status                            # your progress
```

**One problem per commit.** A commit with solutions for two problems is
refused; that is the only thing that ever blocks you. Failing answers
never block.

Every commit that contains a solution gets a flag, in three places: the
git hook prints it when you commit, `drill status` shows it per problem,
and GitHub shows it as the commit's icon after you push.

| Commit | Hook and `drill status` | GitHub icon |
| ------ | ----------------------- | ----------- |
| message contains `WIP` (any case) | ⚙️ in progress, not graded | pending dot, "⚙️ ... in progress (WIP)" |
| the solution passes | ✅ solved | green ✓ |
| the solution fails | ❌ not solved | red ✗ |
| solutions for 2+ problems | refused | red ✗ "one problem per commit" |
| no solution files (e.g. merging `main`) | nothing | no icon |

GitHub can't draw a gear, so in-progress commits use its "pending" dot.
Each pushed commit is graded at its own version of the files. Tip: push
after each solution commit; if a push ends with a commit that has no
solution, GitHub still puts the grading job's ✓ on that last commit.

Only the problem a commit changes is graded. `drill status --refresh`
re-grades every started solution (slow: it runs all their test cases).

- **T-SQL / Spark SQL**: a plain `.sql` file, no procedure wrapper. Clear
  it and start over any time; the last result set is your answer.
- **PySpark / Scala**: implement `solution(...)`, which takes one
  DataFrame per table and returns a DataFrame.
- **Theory** (none published yet; proposals welcome): two parts, both
  needed for ✅. First, a short quiz in `quiz.yaml` (multiple choice,
  graded automatically; wrong questions are named but answers aren't
  revealed). Second, your written explanation in `answer.md`: compare it
  with the reference, then add a final line `SOLVED`.
- `drill show 7xm8xp` (or `drill show top-sales`) prints a statement; `drill next --lang pyspark`
  narrows suggestions.
- Reference answers live in `problems/<id>/reference/`. Peeking is up to
  you.

## Requirements

| Language | Needs | Notes |
| -------- | ----- | ----- |
| All | git | `./drill` brings uv, Python and the dependencies (asks first) |
| T-SQL | Docker (or Podman) | `drill setup` runs SQL Server 2022; it auto-starts when you test |
| PySpark, Spark SQL | Java 17 | Auto-detected, or `drill setup` offers to download it |
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

| Branch | Who | Where | Goes into `main`? |
| ------ | --- | ----- | ----------------- |
| `main` | maintainer | this repo, protected | is `main` |
| `drill/<username>` | every solver | pushed to this repo (ask for collaborator access) | **never**: it only receives `main` |
| `problem/<id>-<slug>` | contributors | **your fork** | via a PR, after review |

- `main` holds problems, reference answers and tooling, never solutions,
  so `git merge main` into your drill branch never conflicts. Pull
  requests need one approval and passing checks.
- On `drill/*` branches, CI flags each solution commit (✅/❌/⚙️). On every
  other branch and on PRs, every reference answer must pass.

### How a problem is verified

Grading is deterministic, with no AI involved. Each problem has at least
10 test cases (`cases/*.yaml`): fixed input rows plus the exact expected
output rows. `drill` runs a solution on the real engine (SQL Server or
Spark) and compares the **output data**: column names, every row, and
the sort order when the problem requires one. Numbers are compared with
a small tolerance. Code structure is never inspected.

A problem is accepted only when its reference answer passes every case
in **every** language it offers. CI enforces this on each PR, and
`drill lint` checks the folder (at least 10 cases, all files present, no
duplicate ids, a warning for similar titles).

### Proposing a problem

1. Fork this repo and create `problem/<id>-<slug>` in your fork.
2. `./drill new <slug> --title "..."`, fill `problem.md`, `tables.yaml`
   and `cases/`, then `./drill stubs <id>` and write the references. For a
   theory problem, write `reference/answer.md` and `reference/quiz.yaml`
   (at least 5 questions); `./drill stubs` derives the answer-free quiz.
3. `./drill test <id> --reference` must pass in every language.
4. Open a PR and complete the checklist. In particular, explain how the
   expected rows were derived: they must come from the problem's intent,
   not from your reference solution's output. See AGENTS.md for details.

## Problems

| Id   | Type   | Languages             | Problem                                            |
| ---- | ------ | --------------------- | -------------------------------------------------- |
| 7xm8xp | Coding | tsql, pyspark, sparksql, scala | Top seller(s) per department              |
| b7x9ye | Coding | tsql, pyspark, sparksql, scala | Running total of sales per department     |
| xf9ca6 | Coding | tsql, pyspark, sparksql, scala | Employees earning above their department average |
| hsnc9v | Coding | tsql, pyspark, sparksql, scala | Customers with orders in every month of 2024 |
| 6acgxy | Coding | tsql, pyspark, sparksql, scala | Month-over-month revenue change           |
| b423cs | Coding | tsql, pyspark, sparksql, scala | Total spent per user, highest first       |
| eraekt | Coding | tsql, pyspark, sparksql, scala | Departments with an average salary above 70,000 |
| t8mugv | Coding | tsql, pyspark, sparksql, scala | Top 3 customers by total amount spent     |
| sknmj3 | Coding | tsql, pyspark, sparksql, scala | Most recently visited page per user       |
| npq6aj | Coding | tsql, pyspark, sparksql, scala | Employee count per department (including empty) |

## Questions?

Send an email to (iamezcua.dev@gmail.com), but don't expect a quick answer.
