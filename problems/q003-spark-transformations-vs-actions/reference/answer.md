# q003: Transformations vs actions in Apache Spark

**What.** A *transformation* (`select`, `filter`, `withColumn`, `join`,
`groupBy().agg()`, `map`) describes a new DataFrame/RDD derived from an
existing one. It returns immediately and computes nothing; Spark only
records it in the logical plan (the lineage). An *action* (`collect`,
`count`, `show`, `take`, `write`, `foreach`) asks for a result or a side
effect, so Spark must actually run the plan: it optimizes it (Catalyst),
splits it into stages at shuffle boundaries and launches a job.

Transformations come in two kinds:
- *Narrow* (`filter`, `select`, `map`): each output partition depends on
  one input partition, so they run in the same stage without moving data.
- *Wide* (`groupBy`, `join`, `distinct`, `orderBy`): need a shuffle across
  the cluster, which creates a stage boundary and is the expensive part.

**Why it matters in practice.**
- *Performance*: because nothing runs until an action, Catalyst sees the
  whole pipeline and can push filters down, prune columns and pick join
  strategies. Writing "filter early" still helps readability, but the
  optimizer often does it for you.
- *Repeated work*: every action recomputes the lineage from the source.
  Calling `count()` and then `write()` on the same expensive DataFrame
  runs it twice; `cache()`/`persist()` (itself lazy, filled by the next
  action) avoids that.
- *Debugging*: errors in a transformation (bad UDF, bad cast) surface only
  when an action runs, so the stack trace points at the action line.
- *Driver safety*: some actions pull data to the driver (`collect`,
  `toPandas`); on large data they can run the driver out of memory, so
  prefer `take(n)`, `show()` or writing to storage.
- *Fault tolerance*: the recorded lineage is how Spark recomputes lost
  partitions after an executor failure.

SOLVED
