# q011: Lazy evaluation in Spark

**What.** Spark does not execute transformations when you call them. It
builds a plan (a DAG / logical plan) describing how each DataFrame is
derived, and executes it only when an **action** needs a result.

**Why Spark uses it.**
- *Whole-pipeline optimization*: Catalyst sees all steps together and can
  push filters and projections down to the source (read fewer
  partitions/columns), reorder and combine operations, and choose join
  strategies (e.g. broadcast a small table).
- *Pipelining*: consecutive narrow transformations run in one stage, in
  one pass over each partition, without materializing intermediate
  results.
- *Avoid unnecessary work*: data that is never used by an action is never
  computed; `take(5)` may read only a few partitions.
- *Fault tolerance*: the lineage lets Spark recompute lost partitions.

**What triggers computation.** Actions: `count()`, `collect()`, `show()`,
`take()`, `first()`, `foreach()`, writing (`df.write...save()`), and
`toPandas()`. Each action launches a job; the plan is split into stages at
shuffle boundaries and stages into tasks per partition. A few operations
also trigger jobs eagerly, e.g. reading with schema inference.

**Practical consequences.**
- Each action re-runs the whole lineage unless you `cache()`/`persist()`
  (also lazy: filled by the next action, released with `unpersist()`).
- Errors appear at the action, not where the faulty transformation was
  written.
- `explain()` shows the optimized plan without running it, which is how
  you check pushdown and join strategies.

SOLVED
