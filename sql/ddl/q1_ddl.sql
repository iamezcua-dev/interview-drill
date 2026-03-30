-- Q1 [SQL | Senior]: Per department, return the employee(s) with the highest
--     total sales. Ties must return all tied employees.
-- Output columns: department, employee_id, total_sales

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

PRINT '╔══════════════════════════════════════════════════════════╗';
GO
PRINT '║ Q1 [SQL | Coding | Senior] -- Per department, return the ║';
GO
PRINT '║ employee(s) with the highest total sales. Ties must      ║';
GO
PRINT '║ return all tied employees.                               ║';
GO
PRINT '╠══════════════════════════════════════════════════════════╣';
GO
PRINT '║ Table : employee_sales                                   ║';
GO
PRINT '║ Cols  : employee_id INT, department VARCHAR(5),          ║';
GO
PRINT '║         sale_date DATE, amount DECIMAL(10,2)             ║';
GO
PRINT '║ Output: department, employee_id, total_sales             ║';
GO
PRINT '╚══════════════════════════════════════════════════════════╝';
GO

DROP TABLE IF EXISTS employee_sales;
CREATE TABLE employee_sales (
    employee_id INT           NOT NULL,
    department  VARCHAR(5)    NOT NULL,
    sale_date   DATE          NOT NULL,
    amount      DECIMAL(10,2) NOT NULL
);
GO

INSERT INTO employee_sales (employee_id, department, sale_date, amount) VALUES
    (1, 'APAC', '2024-01-01', 500),
    (1, 'APAC', '2024-01-03', 300),
    (2, 'APAC', '2024-01-02', 700),
    (3, 'EMEA', '2024-01-01', 400),
    (3, 'EMEA', '2024-01-04', 600),
    (4, 'EMEA', '2024-01-02', 200);
GO
