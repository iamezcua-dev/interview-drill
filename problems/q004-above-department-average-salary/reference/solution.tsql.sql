-- AVG over an INT column truncates in T-SQL, so average a decimal.
WITH with_avg AS (
    SELECT employee_id, name, department, salary,
           AVG(CAST(salary AS DECIMAL(18,4)))
               OVER (PARTITION BY department) AS dept_avg
    FROM employees
)
SELECT employee_id, name, department, salary
FROM with_avg
WHERE salary > dept_avg;
