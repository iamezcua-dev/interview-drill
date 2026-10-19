# q006: WHERE vs HAVING

**What.** Both filter rows, at different points of query execution:
- `WHERE` filters **individual rows before grouping**. It cannot use
  aggregate functions, because groups don't exist yet.
- `HAVING` filters **groups after `GROUP BY`** (and after aggregates are
  computed). It can use aggregates such as `COUNT(*)`, `SUM(amount)`.

Logical order: `FROM/JOIN → WHERE → GROUP BY → aggregates → HAVING →
SELECT → ORDER BY`.

```sql
SELECT department, AVG(salary) AS avg_salary
FROM employees
WHERE hire_date >= '2020-01-01'      -- row filter: which employees count
GROUP BY department
HAVING AVG(salary) > 70000;          -- group filter: which departments
```

**When you must use one over the other.**
- Condition on an **aggregate** (`COUNT`, `SUM`, `AVG`…) → must be
  `HAVING`; `WHERE AVG(salary) > 70000` is an error.
- Condition on a **plain column** → use `WHERE`. A `HAVING` on a
  grouping column works, but it filters after the expensive grouping,
  while `WHERE` removes rows early (less data to group/shuffle, and it can
  use indexes or partition pruning).
- They answer different questions: `WHERE amount > 100` then `COUNT(*)`
  counts large orders per customer; `HAVING SUM(amount) > 100` keeps
  customers whose total is large. Swapping them changes the result.
- `HAVING` without `GROUP BY` treats the whole table as one group.

In Spark the same distinction applies: `.where()` before `.groupBy()`
vs `.where()` on the aggregated DataFrame.

SOLVED
