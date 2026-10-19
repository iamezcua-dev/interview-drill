WITH ranked AS (
    SELECT user_id, page, visit_date,
           RANK() OVER (PARTITION BY user_id
                        ORDER BY visit_date DESC) AS rnk
    FROM page_visits
)
SELECT user_id, page, visit_date
FROM ranked
WHERE rnk = 1;
