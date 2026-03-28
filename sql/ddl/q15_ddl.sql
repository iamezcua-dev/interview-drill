-- Q15 [SQL | Senior]
-- Return all departments with their employee count,
-- including departments that have no employees (count = 0).
-- Output columns: department_name, employee_count

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

PRINT '╔══════════════════════════════════════════════════════════╗';
GO
PRINT '║ Q15 [SQL | Coding | Senior] -- Return all departments   ║';
GO
PRINT '║ with their employee count, including departments with 0 ║';
GO
PRINT '║ employees.                                              ║';
GO
PRINT '╠══════════════════════════════════════════════════════════╣';
GO
PRINT '║ Tables: dept, emp                                        ║';
GO
PRINT '║ dept  : department_id    INT                             ║';
GO
PRINT '║         department_name  VARCHAR(50)                     ║';
GO
PRINT '║ emp   : employee_id      INT                             ║';
GO
PRINT '║         department_id    INT NULL                        ║';
GO
PRINT '║ Output: department_name, employee_count                  ║';
GO
PRINT '╚══════════════════════════════════════════════════════════╝';
GO

DROP TABLE IF EXISTS emp;
DROP TABLE IF EXISTS dept;

CREATE TABLE dept (
    department_id   INT          NOT NULL,
    department_name VARCHAR(50)  NOT NULL
);

CREATE TABLE emp (
    employee_id   INT         NOT NULL,
    department_id INT             NULL
);
GO

INSERT INTO dept VALUES
    (1, 'Engineering'),
    (2, 'Marketing'),
    (3, 'Sales'),
    (4, 'Legal');

INSERT INTO emp VALUES
    (1, 1),
    (2, 1),
    (3, 2),
    (4, 1),
    (5, 3);
GO
