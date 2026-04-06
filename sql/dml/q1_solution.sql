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

	WITH cte_totals AS (
		SELECT
			department,
			employee_id,
			SUM(amount) AS total_sales
		FROM employee_sales
		GROUP BY department, employee_id
	), cte_total_sales_ranked AS (
		SELECT
			*,
			RANK() OVER(PARTITION BY department ORDER BY total_sales DESC) AS sales_rank_id
		FROM cte_totals
	)

	SELECT department, employee_id, total_sales FROM cte_total_sales_ranked where sales_rank_id = 1;
		
    RETURN

END;
GO
