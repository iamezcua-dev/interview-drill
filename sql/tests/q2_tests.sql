-- Q2 [SQL | Senior] — TEST CASES
-- Return each individual sale row alongside the running total of sales
-- for that employee's department, ordered by sale_date.
-- Output columns: department, employee_id, sale_date, amount, running_total
-- NOTE: Tests with two rows sharing the same sale_date within the same department
--       have non-deterministic row order. Those cases verify MAX running_total
--       for that date rather than individual row order.

:r ./sql/dml/q2_solution.sql

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

-- ===================== TEST CASES =====================

-- -------------------------------------------------
-- TEST 1: Base case — 6 rows, two departments
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',500),(1,'APAC','2024-01-03',300),(2,'APAC','2024-01-02',700),
    (3,'EMEA','2024-01-01',400),(3,'EMEA','2024-01-04',600),(4,'EMEA','2024-01-02',200);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, sale_date DATE, amount DECIMAL(10,2), running_total DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q2_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST('2024-01-01' AS DATE),CAST(500.00 AS DECIMAL(10,2)),CAST(500.00  AS DECIMAL(10,2))),
    ('APAC',2,CAST('2024-01-02' AS DATE),CAST(700.00 AS DECIMAL(10,2)),CAST(1200.00 AS DECIMAL(10,2))),
    ('APAC',1,CAST('2024-01-03' AS DATE),CAST(300.00 AS DECIMAL(10,2)),CAST(1500.00 AS DECIMAL(10,2))),
    ('EMEA',3,CAST('2024-01-01' AS DATE),CAST(400.00 AS DECIMAL(10,2)),CAST(400.00  AS DECIMAL(10,2))),
    ('EMEA',4,CAST('2024-01-02' AS DATE),CAST(200.00 AS DECIMAL(10,2)),CAST(600.00  AS DECIMAL(10,2))),
    ('EMEA',3,CAST('2024-01-04' AS DATE),CAST(600.00 AS DECIMAL(10,2)),CAST(1200.00 AS DECIMAL(10,2))))
    t(department,employee_id,sale_date,amount,running_total);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 1 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 1 FAIL: Unexpected rows',1;
PRINT 'TEST 1 PASS: Base case';
GO

-- -------------------------------------------------
-- TEST 2: Single row — running_total equals amount
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES (1,'APAC','2024-03-01',750.00);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, sale_date DATE, amount DECIMAL(10,2), running_total DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q2_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST('2024-03-01' AS DATE),CAST(750.00 AS DECIMAL(10,2)),CAST(750.00 AS DECIMAL(10,2))))
    t(department,employee_id,sale_date,amount,running_total);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 2 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 2 FAIL: Unexpected rows',1;
PRINT 'TEST 2 PASS: Single row, running_total = amount';
GO

-- -------------------------------------------------
-- TEST 3: Single employee, strictly cumulative
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',100),(1,'APAC','2024-01-02',200),(1,'APAC','2024-01-03',300);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, sale_date DATE, amount DECIMAL(10,2), running_total DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q2_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST('2024-01-01' AS DATE),CAST(100.00 AS DECIMAL(10,2)),CAST(100.00 AS DECIMAL(10,2))),
    ('APAC',1,CAST('2024-01-02' AS DATE),CAST(200.00 AS DECIMAL(10,2)),CAST(300.00 AS DECIMAL(10,2))),
    ('APAC',1,CAST('2024-01-03' AS DATE),CAST(300.00 AS DECIMAL(10,2)),CAST(600.00 AS DECIMAL(10,2))))
    t(department,employee_id,sale_date,amount,running_total);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 3 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 3 FAIL: Unexpected rows',1;
PRINT 'TEST 3 PASS: Single employee running total accumulates correctly';
GO

-- -------------------------------------------------
-- TEST 4: Departments partitioned independently
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',300),(2,'APAC','2024-01-02',400),
    (3,'EMEA','2024-01-01',100),(4,'EMEA','2024-01-02',200);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, sale_date DATE, amount DECIMAL(10,2), running_total DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q2_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST('2024-01-01' AS DATE),CAST(300.00 AS DECIMAL(10,2)),CAST(300.00 AS DECIMAL(10,2))),
    ('APAC',2,CAST('2024-01-02' AS DATE),CAST(400.00 AS DECIMAL(10,2)),CAST(700.00 AS DECIMAL(10,2))),
    ('EMEA',3,CAST('2024-01-01' AS DATE),CAST(100.00 AS DECIMAL(10,2)),CAST(100.00 AS DECIMAL(10,2))),
    ('EMEA',4,CAST('2024-01-02' AS DATE),CAST(200.00 AS DECIMAL(10,2)),CAST(300.00 AS DECIMAL(10,2))))
    t(department,employee_id,sale_date,amount,running_total);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 4 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 4 FAIL: Unexpected rows',1;
PRINT 'TEST 4 PASS: Partitions are independent, no cross-dept accumulation';
GO

-- -------------------------------------------------
-- TEST 5: Three departments
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC', '2024-01-01',200),(1,'APAC', '2024-01-02',300),
    (2,'EMEA', '2024-01-01',500),(2,'EMEA', '2024-01-02',100),
    (3,'LATAM','2024-01-01',150),(3,'LATAM','2024-01-02',250);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, sale_date DATE, amount DECIMAL(10,2), running_total DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q2_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC', 1,CAST('2024-01-01' AS DATE),CAST(200.00 AS DECIMAL(10,2)),CAST(200.00 AS DECIMAL(10,2))),
    ('APAC', 1,CAST('2024-01-02' AS DATE),CAST(300.00 AS DECIMAL(10,2)),CAST(500.00 AS DECIMAL(10,2))),
    ('EMEA', 2,CAST('2024-01-01' AS DATE),CAST(500.00 AS DECIMAL(10,2)),CAST(500.00 AS DECIMAL(10,2))),
    ('EMEA', 2,CAST('2024-01-02' AS DATE),CAST(100.00 AS DECIMAL(10,2)),CAST(600.00 AS DECIMAL(10,2))),
    ('LATAM',3,CAST('2024-01-01' AS DATE),CAST(150.00 AS DECIMAL(10,2)),CAST(150.00 AS DECIMAL(10,2))),
    ('LATAM',3,CAST('2024-01-02' AS DATE),CAST(250.00 AS DECIMAL(10,2)),CAST(400.00 AS DECIMAL(10,2))))
    t(department,employee_id,sale_date,amount,running_total);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 5 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 5 FAIL: Unexpected rows',1;
PRINT 'TEST 5 PASS: Three departments partitioned correctly';
GO

-- -------------------------------------------------
-- TEST 6: Decimal precision
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',333.33),
    (1,'APAC','2024-01-02',333.33),
    (1,'APAC','2024-01-03',333.34);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, sale_date DATE, amount DECIMAL(10,2), running_total DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q2_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST('2024-01-01' AS DATE),CAST(333.33 AS DECIMAL(10,2)),CAST(333.33  AS DECIMAL(10,2))),
    ('APAC',1,CAST('2024-01-02' AS DATE),CAST(333.33 AS DECIMAL(10,2)),CAST(666.66  AS DECIMAL(10,2))),
    ('APAC',1,CAST('2024-01-03' AS DATE),CAST(333.34 AS DECIMAL(10,2)),CAST(1000.00 AS DECIMAL(10,2))))
    t(department,employee_id,sale_date,amount,running_total);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 6 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 6 FAIL: Unexpected rows',1;
PRINT 'TEST 6 PASS: Decimal precision maintained in running total';
GO

-- -------------------------------------------------
-- TEST 7: Out-of-order insertion — ordering follows sale_date, not insert order
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-06-15',400),
    (1,'APAC','2024-01-10',100),
    (1,'APAC','2024-03-22',250);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, sale_date DATE, amount DECIMAL(10,2), running_total DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q2_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST('2024-01-10' AS DATE),CAST(100.00 AS DECIMAL(10,2)),CAST(100.00 AS DECIMAL(10,2))),
    ('APAC',1,CAST('2024-03-22' AS DATE),CAST(250.00 AS DECIMAL(10,2)),CAST(350.00 AS DECIMAL(10,2))),
    ('APAC',1,CAST('2024-06-15' AS DATE),CAST(400.00 AS DECIMAL(10,2)),CAST(750.00 AS DECIMAL(10,2))))
    t(department,employee_id,sale_date,amount,running_total);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 7 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 7 FAIL: Unexpected rows',1;
PRINT 'TEST 7 PASS: Ordering follows sale_date, not insertion order';
GO

-- -------------------------------------------------
-- TEST 8: Same date across departments — no cross-contamination
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',1000),
    (2,'EMEA','2024-01-01',2000);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, sale_date DATE, amount DECIMAL(10,2), running_total DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q2_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST('2024-01-01' AS DATE),CAST(1000.00 AS DECIMAL(10,2)),CAST(1000.00 AS DECIMAL(10,2))),
    ('EMEA',2,CAST('2024-01-01' AS DATE),CAST(2000.00 AS DECIMAL(10,2)),CAST(2000.00 AS DECIMAL(10,2))))
    t(department,employee_id,sale_date,amount,running_total);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 8 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 8 FAIL: Unexpected rows',1;
PRINT 'TEST 8 PASS: Same sale_date across departments, partitions remain isolated';
GO

-- -------------------------------------------------
-- TEST 9: Large amounts
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',9999999.99),
    (1,'APAC','2024-01-02',1.00);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department VARCHAR(5), employee_id INT, sale_date DATE, amount DECIMAL(12,2), running_total DECIMAL(12,2));
INSERT INTO #actual EXEC dbo.q2_solution;
SELECT * INTO #expected FROM (VALUES
    ('APAC',1,CAST('2024-01-01' AS DATE),CAST(9999999.99 AS DECIMAL(12,2)),CAST(9999999.99  AS DECIMAL(12,2))),
    ('APAC',1,CAST('2024-01-02' AS DATE),CAST(1.00       AS DECIMAL(12,2)),CAST(10000000.99 AS DECIMAL(12,2))))
    t(department,employee_id,sale_date,amount,running_total);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 9 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 9 FAIL: Unexpected rows',1;
PRINT 'TEST 9 PASS: Large amounts handled without overflow';
GO

-- -------------------------------------------------
-- TEST 10: Two rows with same sale_date in same dept
--          RANGE: both rows see running_total = 1000 (combined)
--          ROWS:  one row sees 300, other sees 1000 (incremental)
--          We enforce ROWS behavior: both individual totals must exist
-- -------------------------------------------------
TRUNCATE TABLE employee_sales;
INSERT INTO employee_sales VALUES
    (1,'APAC','2024-01-01',300),
    (2,'APAC','2024-01-01',700),
    (3,'APAC','2024-01-02',500);

DROP TABLE IF EXISTS #actual;
CREATE TABLE #actual (
    department   VARCHAR(5),
    employee_id  INT,
    sale_date    DATE,
    amount       DECIMAL(10,2),
    running_total DECIMAL(10,2)
);
INSERT INTO #actual EXEC dbo.q2_solution;
-- With ROWS: running totals on 2024-01-01 must be 300 and 1000
-- (one per physical row). With RANGE both would be 1000 — fail.
IF (SELECT COUNT(DISTINCT running_total)
    FROM #actual WHERE sale_date = '2024-01-01') <> 2
    THROW 50001,
        'TEST 10 FAIL: tied-date rows must have distinct running_totals (use ROWS not RANGE)',1;
IF (SELECT MAX(running_total)
    FROM #actual WHERE sale_date = '2024-01-02') <> 1500.00
    THROW 50002,
        'TEST 10 FAIL: running_total at 2024-01-02 should be 1500',1;
IF (SELECT COUNT(*) FROM #actual) <> 3
    THROW 50003,'TEST 10 FAIL: Expected 3 rows',1;
PRINT 'TEST 10 PASS: ROWS frame — tied dates get distinct running totals';
GO
