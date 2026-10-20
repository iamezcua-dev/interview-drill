---
id: 7xm8
title: Top seller(s) per department
type: coding
level: senior
languages: [tsql, pyspark, sparksql, scala]
output: [department, employee_id, total_sales]
tolerance: 0.005
---

For each department, return the employee(s) with the highest **total**
sales (the sum of all their sales in that department).

If several employees tie for the top total in a department, return all
of them.

- `total_sales` is the employee's summed `amount`.
- Row order does not matter.
