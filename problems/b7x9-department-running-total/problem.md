---
id: b7x9
title: Running total of sales per department
type: coding
level: senior
languages: [tsql, pyspark, sparksql, scala]
output: [department, employee_id, sale_date, amount, running_total]
tolerance: 0.005
---

Return every sale row together with the running total of `amount` for
its department, accumulated in `sale_date` order.

- Each department accumulates independently.
- When two sales in the same department share a `sale_date`, each row
  still gets its own incremental running total (row by row, not one
  combined total for the date). The order between them is up to you.
- Row order of the result does not matter.
