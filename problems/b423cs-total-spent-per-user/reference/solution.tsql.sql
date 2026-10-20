SELECT user_id, SUM(quantity * price) AS total_spent
FROM transactions
GROUP BY user_id
ORDER BY total_spent DESC;
