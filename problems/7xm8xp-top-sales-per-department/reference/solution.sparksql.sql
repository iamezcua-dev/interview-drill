WITH totals AS (
    SELECT department, employee_id, SUM(amount) AS total_sales
    FROM employee_sales
    GROUP BY department, employee_id
),
ranked AS (
    SELECT department, employee_id, total_sales,
           RANK() OVER (PARTITION BY department
                        ORDER BY total_sales DESC) AS rnk
    FROM totals
)
SELECT department, employee_id, total_sales
FROM ranked
WHERE rnk = 1;
