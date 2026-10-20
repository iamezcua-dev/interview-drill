SELECT department, employee_id, sale_date, amount,
       SUM(amount) OVER (
           PARTITION BY department
           ORDER BY sale_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS running_total
FROM employee_sales;
