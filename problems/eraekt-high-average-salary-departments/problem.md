---
id: eraekt
title: Departments with an average salary above 70,000
type: coding
level: senior
languages: [tsql, pyspark, sparksql, scala]
output: [department, avg_salary]
tolerance: 0.01
sorted_by: [{avg_salary: desc}]
---

Return `department` and `avg_salary` (average `salary`) for departments
whose average salary is **strictly greater** than 70,000, ordered by
`avg_salary` descending.

- `hire_date` is not needed.
