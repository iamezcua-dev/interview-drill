"""Data engineering interview drills: problems, solutions and graders."""

import os

# How users run drill: the launcher (./drill, or drill.cmd on Windows).
CMD = "drill" if os.name == "nt" else "./drill"

LANGUAGES = ("tsql", "pyspark", "sparksql", "scala")

SOLUTION_FILES = {
    "tsql": "solution.tsql.sql",
    "pyspark": "solution.pyspark.py",
    "sparksql": "solution.sparksql.sql",
    "scala": "solution.scala",
}

THEORY_FILE = "answer.md"

# Who grants push access for drill/<username> branches.
MAINTAINER_EMAIL = "iamezcua.dev@gmail.com"
