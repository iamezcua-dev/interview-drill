-- Q12 [SQL | Coding | Senior]
-- Find the top 3 customers by total amount spent.
-- Return customer_id and total_spent, ordered by total_spent desc.
-- If there is a tie for 3rd place, include all tied customers.
--
-- Table : orders
-- Cols  : order_id    INT
--         customer_id INT
--         order_date  DATE
--         amount      DECIMAL(10,2)
-- Output: customer_id, total_spent

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

CREATE OR ALTER PROCEDURE dbo.q12_solution
AS
BEGIN

	WITH cte_totals AS (
		SELECT customer_id, SUM(amount) AS total_spent 
		FROM orders
		GROUP BY customer_id
	), cte_totals_ranked AS (
		SELECT
			customer_id,
			total_spent,
			DENSE_RANK() OVER (ORDER BY total_spent DESC) AS ranked
		FROM cte_totals
	)

	SELECT customer_id, total_spent FROM cte_totals_ranked WHERE ranked <= 3 ORDER BY total_spent DESC;
	
    RETURN

END;
GO
