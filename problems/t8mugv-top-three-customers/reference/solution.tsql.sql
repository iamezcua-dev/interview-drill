WITH totals AS (
    SELECT customer_id, SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer_id
),
ranked AS (
    SELECT customer_id, total_spent,
           DENSE_RANK() OVER (ORDER BY total_spent DESC) AS rnk
    FROM totals
)
SELECT customer_id, total_spent
FROM ranked
WHERE rnk <= 3
ORDER BY total_spent DESC;
