-- Q2 [SQL | Coding | Senior]
-- Return each individual sale row alongside the running total of
-- sales for that employee's department, ordered by sale_date.
--
-- Table : employee_sales
-- Cols  : employee_id  INT
--         department   VARCHAR(5)
--         sale_date    DATE
--         amount       DECIMAL(10,2)
-- Output: department, employee_id, sale_date, amount, running_total

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

CREATE OR ALTER PROCEDURE dbo.q2_solution
AS
BEGIN

	SELECT
		department,
		employee_id,
		sale_date,
		amount,
		SUM(amount) OVER (PARTITION BY department ORDER BY sale_date) AS running_total
	FROM employee_sales
	ORDER BY sale_date;
	
    RETURN

END;
GO
