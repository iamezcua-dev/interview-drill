SELECT customer_id
FROM orders
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'
GROUP BY customer_id
HAVING COUNT(DISTINCT MONTH(order_date)) = 12;
