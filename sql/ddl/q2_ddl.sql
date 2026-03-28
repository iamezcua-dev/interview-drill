-- Q2 [SQL | Senior]: Return each individual sale row alongside the
--     running total of sales for that employee's department,
--     ordered by sale_date.
-- Output columns: department, employee_id, sale_date, amount,
--                 running_total

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

PRINT '╔══════════════════════════════════════════════════════════╗';
GO
PRINT '║ Q2 [SQL | Coding | Senior] -- Return each individual    ║';
GO
PRINT '║ sale row alongside the running total of sales for that  ║';
GO
PRINT '║ employee''s department, ordered by sale_date.            ║';
GO
PRINT '╠══════════════════════════════════════════════════════════╣';
GO
PRINT '║ Table : employee_sales                                   ║';
GO
PRINT '║ Cols  : employee_id  INT                                 ║';
GO
PRINT '║         department   VARCHAR(5)                          ║';
GO
PRINT '║         sale_date    DATE                                ║';
GO
PRINT '║         amount       DECIMAL(10,2)                       ║';
GO
PRINT '║ Output: department, employee_id, sale_date,              ║';
GO
PRINT '║         amount, running_total                            ║';
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
