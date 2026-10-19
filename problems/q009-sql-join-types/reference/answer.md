# q009: INNER JOIN vs LEFT JOIN vs CROSS JOIN

**INNER JOIN** returns only the row pairs whose join condition is true.
Rows without a match on either side disappear.
*Use case*: orders with their customer details, where every order must
have a valid customer.

**LEFT (OUTER) JOIN** returns every row of the left table; where no right
row matches, the right-side columns are NULL.
*Use case*: all departments with their employee count, including
departments with no employees (`COUNT(e.employee_id)` gives 0), or
finding orphans with `WHERE right.key IS NULL` (an anti-join).

**CROSS JOIN** returns the Cartesian product: every left row paired with
every right row (n × m rows), with no join condition.
*Use case*: building a complete grid, e.g. every store × every date of the
month, then LEFT JOIN actual sales to show zero-sale days.

**Why it matters in practice.**
- Picking INNER where LEFT is needed silently drops rows (the classic
  "missing departments with zero employees" bug).
- In a LEFT JOIN, a filter on the right table in `WHERE`
  (`WHERE e.active = 1`) removes the NULL rows and turns it back into an
  INNER JOIN; put that condition in the `ON` clause instead.
- Join keys with duplicates multiply rows (fan-out), inflating `SUM`s;
  aggregate first or deduplicate.
- CROSS JOIN output grows as n × m; an accidental one (missing condition)
  can explode a query. In Spark, cross joins also cause huge shuffles,
  and joins in general pick a strategy (broadcast vs sort-merge) based on
  table sizes.

SOLVED
