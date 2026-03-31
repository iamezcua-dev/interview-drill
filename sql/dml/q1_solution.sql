-- Q1 [SQL | Coding | Senior]
-- Per department, return the employee(s) with the highest total sales.
-- Ties must return all tied employees.
--
-- Table : employee_sales
-- Cols  : employee_id  INT
--         department   VARCHAR(5)
--         sale_date    DATE
--         amount       DECIMAL(10,2)
-- Output: department, employee_id, total_sales

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

CREATE OR ALTER PROCEDURE dbo.q1_solution
AS
BEGIN
    WITH cte_ranked_employees AS (
        SELECT
            employee_id,
            department,
            SUM(amount) AS total_sales,
            RANK() OVER (PARTITION BY department ORDER BY SUM(amount) DESC) AS ranked_employees
        FROM employee_sales
        GROUP BY department, employee_id
    )

    SELECT
        department,
        employee_id,
        total_sales
    FROM cte_ranked_employees
    WHERE ranked_employees = 1;

    RETURN

END;
GO
