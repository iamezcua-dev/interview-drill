WITH with_avg AS (
    SELECT employee_id, name, department, salary,
           AVG(salary) OVER (PARTITION BY department) AS dept_avg
    FROM employees
)
SELECT employee_id, name, department, salary
FROM with_avg
WHERE salary > dept_avg
