---
id: q007
title: Month-over-month revenue change
type: coding
level: senior
languages: [tsql, pyspark, sparksql, scala]
output: [month, revenue, prev_revenue, change]
---

Given monthly revenue, return each month with its revenue, the previous
month's revenue (`prev_revenue`) and the difference
(`change = revenue - prev_revenue`).

- The first month has no previous month: `prev_revenue` and `change`
  are NULL.
- "Previous" follows `month` order, not the order of the input rows.
- Row order of the result does not matter.
