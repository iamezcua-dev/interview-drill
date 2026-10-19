---
id: q012
title: Top 3 customers by total amount spent
type: coding
level: senior
languages: [tsql, pyspark, sparksql, scala]
output: [customer_id, total_spent]
tolerance: 0.005
sorted_by: [{total_spent: desc}]
---

Return the top 3 customers by total amount spent, with `customer_id` and
`total_spent`, ordered by `total_spent` descending.

"Top 3" means the three highest distinct totals: if customers tie, every
customer whose total is among the three highest totals is returned (so
the result can have more than 3 rows).

- Order between customers with the same total does not matter.
