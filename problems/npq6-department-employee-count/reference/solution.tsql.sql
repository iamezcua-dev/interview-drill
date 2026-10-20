SELECT d.department_name, COUNT(e.employee_id) AS employee_count
FROM dept AS d
LEFT JOIN emp AS e
    ON e.department_id = d.department_id
GROUP BY d.department_id, d.department_name;
