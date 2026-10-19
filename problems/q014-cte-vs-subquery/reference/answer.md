# q014: CTE vs subquery

**What.** A CTE (Common Table Expression) is a named, temporary result
defined with `WITH name AS (...)` before the main query, and referenced
by name in it. A subquery is a query nested inline: in `FROM` (derived
table), `WHERE ... IN (...)`/`EXISTS`, or the `SELECT` list (scalar or
correlated).

```sql
WITH totals AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders GROUP BY customer_id
)
SELECT * FROM totals WHERE total > (SELECT AVG(total) FROM totals);
```

**Differences.**
- *Readability*: CTEs read top to bottom as named steps; nested
  subqueries are read inside out.
- *Reuse*: a CTE can be referenced several times in the same statement;
  a derived table must be repeated.
- *Recursion*: only CTEs can be recursive (`WITH RECURSIVE` / T-SQL
  recursive CTE), e.g. org charts, hierarchies, date series.
- *Scope*: a CTE lives only for the single statement that follows it; it
  is not a temp table.
- *Performance*: in SQL Server and Spark a CTE is usually inlined, so it
  performs like the equivalent subquery and is **not** materialized;
  referencing it twice may compute it twice. When you need materialization
  use a temp table (SQL Server) or `cache()`/a temp view (Spark).
  (PostgreSQL ≤ 11 always materialized CTEs, a known optimization fence.)

**When to prefer which.**
- CTE: multi-step logic (aggregate → rank → filter), anything reused or
  recursive, and window-function results you need to filter (you can't
  filter a window function in the same `SELECT`'s `WHERE`).
- Subquery: short, single-use filters such as `WHERE x IN (SELECT ...)`,
  `EXISTS` checks or a scalar comparison, where a separate name adds
  nothing.

SOLVED
