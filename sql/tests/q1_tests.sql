-- Q1 [SQL | Senior] — TEST CASES
-- Per department, return the employee(s) with the highest total sales.
-- Ties must return all tied employees.
-- Output columns: department, employee_id, total_sales

:r ./sql/dml/q1_solution.sql

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

-- ===================== TEST CASES =====================

-- -------------------------------------------------
-- TEST 1: Base case — no ties
-- APAC: emp1=800, emp2=700 → winner emp1
-- EMEA: emp3=1000, emp4=200 → winner emp3
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',500),(1,'APAC','2024-01-03',300),
    (2,'APAC','2024-01-02',700),
    (3,'EMEA','2024-01-01',400),(3,'EMEA','2024-01-04',600),
    (4,'EMEA','2024-01-02',200);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, total_sales DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q1_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST(800.00  AS DECIMAL(10,2))),
    ('EMEA',3,CAST(1000.00 AS DECIMAL(10,2))))
    t(department,employee_id,total_sales);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 1 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 1 FAIL: Unexpected rows',1;
PRINT 'TEST 1 PASS: Base case, no ties';
GO

-- -------------------------------------------------
-- TEST 2: Two-way tie in one department
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',500),(1,'APAC','2024-01-03',300),
    (2,'APAC','2024-01-02',700),
    (3,'EMEA','2024-01-01',400),(3,'EMEA','2024-01-04',600),
    (5,'EMEA','2024-01-05',600),(5,'EMEA','2024-01-06',400);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, total_sales DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q1_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST(800.00  AS DECIMAL(10,2))),
    ('EMEA',3,CAST(1000.00 AS DECIMAL(10,2))),
    ('EMEA',5,CAST(1000.00 AS DECIMAL(10,2))))
    t(department,employee_id,total_sales);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 2 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 2 FAIL: Unexpected rows',1;
PRINT 'TEST 2 PASS: Two-way tie in EMEA';
GO

-- -------------------------------------------------
-- TEST 3: Three-way tie
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',300),(2,'APAC','2024-01-02',300),(3,'APAC','2024-01-03',300);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, total_sales DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q1_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST(300.00 AS DECIMAL(10,2))),
    ('APAC',2,CAST(300.00 AS DECIMAL(10,2))),
    ('APAC',3,CAST(300.00 AS DECIMAL(10,2))))
    t(department,employee_id,total_sales);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 3 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 3 FAIL: Unexpected rows',1;
PRINT 'TEST 3 PASS: Three-way tie';
GO

-- -------------------------------------------------
-- TEST 4: Single employee per department
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',400),(1,'APAC','2024-01-02',600),
    (2,'EMEA','2024-01-01',900);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, total_sales DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q1_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST(1000.00 AS DECIMAL(10,2))),
    ('EMEA',2,CAST(900.00  AS DECIMAL(10,2))))
    t(department,employee_id,total_sales);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 4 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 4 FAIL: Unexpected rows',1;
PRINT 'TEST 4 PASS: Single employee per department always wins';
GO

-- -------------------------------------------------
-- TEST 5: Single row in entire table
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES (7,'LATAM','2024-06-01',250.50);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, total_sales DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q1_solution;
SELECT * INTO #expected FROM (VALUES('LATAM',7,CAST(250.50 AS DECIMAL(10,2))))
    t(department,employee_id,total_sales);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 5 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 5 FAIL: Unexpected rows',1;
PRINT 'TEST 5 PASS: Single row table';
GO

-- -------------------------------------------------
-- TEST 6: Three departments, clear winners
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',100),(2,'APAC','2024-01-02',900),
    (3,'EMEA','2024-01-01',500),(4,'EMEA','2024-01-02',300),
    (5,'LATAM','2024-01-01',750),(6,'LATAM','2024-01-02',200);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, total_sales DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q1_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC', 2,CAST(900.00 AS DECIMAL(10,2))),
    ('EMEA', 3,CAST(500.00 AS DECIMAL(10,2))),
    ('LATAM',5,CAST(750.00 AS DECIMAL(10,2))))
    t(department,employee_id,total_sales);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 6 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 6 FAIL: Unexpected rows',1;
PRINT 'TEST 6 PASS: Three departments, clear winners';
GO

-- -------------------------------------------------
-- TEST 7: Winner by aggregation beats single large sale
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',600),(1,'APAC','2024-01-02',600),
    (2,'APAC','2024-01-03',1100);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, total_sales DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q1_solution;
SELECT * INTO #expected FROM (VALUES('APAC',1,CAST(1200.00 AS DECIMAL(10,2))))
    t(department,employee_id,total_sales);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 7 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 7 FAIL: Unexpected rows',1;
PRINT 'TEST 7 PASS: Winner by aggregation beats single large sale';
GO

-- -------------------------------------------------
-- TEST 8: Decimal precision
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',333.33),(1,'APAC','2024-01-02',333.33),(1,'APAC','2024-01-03',333.34),
    (2,'APAC','2024-01-04',999.99);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, total_sales DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q1_solution;
SELECT * INTO #expected FROM (VALUES('APAC',1,CAST(1000.00 AS DECIMAL(10,2))))
    t(department,employee_id,total_sales);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 8 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 8 FAIL: Unexpected rows',1;
PRINT 'TEST 8 PASS: Decimal precision aggregation';
GO

-- -------------------------------------------------
-- TEST 9: Second-place is NOT returned
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',1000),
    (2,'APAC','2024-01-02',999),
    (3,'APAC','2024-01-03',998);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, total_sales DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q1_solution;
SELECT * INTO #expected FROM (VALUES('APAC',1,CAST(1000.00 AS DECIMAL(10,2))))
    t(department,employee_id,total_sales);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 9 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 9 FAIL: 2nd/3rd place must not appear',1;
PRINT 'TEST 9 PASS: Only top-ranked returned, no 2nd/3rd place leakage';
GO

-- -------------------------------------------------
-- TEST 10: Tie only at 2nd place — winner is unique
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'EMEA','2024-01-01',500),
    (2,'EMEA','2024-01-02',300),
    (3,'EMEA','2024-01-03',300);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, total_sales DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q1_solution;
SELECT * INTO #expected FROM (VALUES('EMEA',1,CAST(500.00 AS DECIMAL(10,2))))
    t(department,employee_id,total_sales);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 10 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 10 FAIL: 2nd-place tie must not appear',1;
PRINT 'TEST 10 PASS: 2nd-place tie does not pollute results';
GO
