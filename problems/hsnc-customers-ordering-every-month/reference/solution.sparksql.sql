SELECT customer_id
FROM orders
WHERE order_date >= DATE'2024-01-01' AND order_date < DATE'2025-01-01'
GROUP BY customer_id
HAVING COUNT(DISTINCT month(order_date)) = 12
