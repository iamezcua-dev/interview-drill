-- Q15 [SQL | Coding | Senior]
-- Return all departments with their employee count,
-- including departments that have no employees (count = 0).
--
-- Tables: dept, emp
-- dept  : department_id    INT
--         department_name  VARCHAR(50)
-- emp   : employee_id      INT
--         department_id    INT NULL
-- Output: department_name, employee_count

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

CREATE OR ALTER PROCEDURE dbo.q15_solution
AS
BEGIN

	SELECT d.department_name, COUNT(e.employee_id) as employee_count
	FROM dept d LEFT JOIN emp e ON d.department_id = e.department_id
	GROUP BY d.department_name;
	
    RETURN

END;
GO
