---
id: b423cs
title: Total spent per user, highest first
type: coding
level: senior
languages: [tsql, pyspark, sparksql, scala]
output: [user_id, total_spent]
tolerance: 0.01
sorted_by: [{total_spent: desc}]
---

Return each `user_id` with `total_spent`, the sum of `quantity * price`
over all their transactions, ordered by `total_spent` descending.

- Order between users with the same total does not matter.
- Totals are compared with a 0.01 tolerance (`price` is a floating
  point number).
