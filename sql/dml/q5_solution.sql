-- Q5 [SQL | Coding | Senior]
-- Find customers who placed orders in every month of 2024.
--
-- Table : orders
-- Cols  : order_id    INT
--         customer_id INT
--         order_date  DATE
--         amount      DECIMAL(10,2)
-- Output: customer_id

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

CREATE OR ALTER PROCEDURE dbo.q5_solution
AS
BEGIN

	SELECT customer_id
	FROM orders
	WHERE DATEPART(YEAR, order_date) = 2024
	GROUP BY customer_id
	HAVING count(DISTINCT DATEPART(MONTH, order_date)) = 12;
	
    RETURN

END;
GO
