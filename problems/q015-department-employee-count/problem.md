---
id: q015
title: Employee count per department (including empty)
type: coding
level: senior
languages: [tsql, pyspark, sparksql, scala]
output: [department_name, employee_count]
---

Return every department with its number of employees, including
departments that have no employees (`employee_count = 0`).

- `emp.department_id` can be NULL (an unassigned employee); such
  employees are not counted in any department.
- Row order does not matter.
