-- Q12 [SQL | Senior] — TEST CASES
-- Find the top 3 customers by total amount spent.
-- Return customer_id and total_spent, ordered by total_spent descending.
-- If there is a tie for 3rd place, include all tied customers.
-- Output columns: customer_id, total_spent

:r ./sql/dml/q12_solution.sql

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

-- -------------------------------------------------
-- TEST 1: Base case — 5 customers, top 3 distinct
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',500.00),(2,1,'2024-01-05',300.00),
    (3,2,'2024-01-02',1200.00),
    (4,3,'2024-01-03',400.00),(5,3,'2024-01-04',200.00),
    (6,4,'2024-01-06',150.00),
    (7,5,'2024-01-07',900.00);
-- customer totals: 1=800, 2=1200, 3=600, 4=150, 5=900
-- top 3: 2(1200), 5(900), 1(800)

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT, total_spent DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q12_solution;
SELECT * INTO #expected FROM (VALUES
    (2,CAST(1200.00 AS DECIMAL(10,2))),
    (5,CAST( 900.00 AS DECIMAL(10,2))),
    (1,CAST( 800.00 AS DECIMAL(10,2))))
    t(customer_id,total_spent);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 1 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 1 FAIL: Unexpected rows',1;
PRINT 'TEST 1 PASS: Base case top 3';
GO

-- -------------------------------------------------
-- TEST 2: Tie for 3rd — all tied customers returned
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',1000.00),
    (2,2,'2024-01-01', 900.00),
    (3,3,'2024-01-01', 800.00),
    (4,4,'2024-01-01', 800.00),
    (5,5,'2024-01-01', 500.00);
-- top 3 by DENSE_RANK: 1(1000), 2(900), 3&4(800) — 4 rows returned

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT, total_spent DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q12_solution;
SELECT * INTO #expected FROM (VALUES
    (1,CAST(1000.00 AS DECIMAL(10,2))),
    (2,CAST( 900.00 AS DECIMAL(10,2))),
    (3,CAST( 800.00 AS DECIMAL(10,2))),
    (4,CAST( 800.00 AS DECIMAL(10,2))))
    t(customer_id,total_spent);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 2 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 2 FAIL: Unexpected rows',1;
PRINT 'TEST 2 PASS: Tie for 3rd — all tied customers included';
GO

-- -------------------------------------------------
-- TEST 3: Exactly 3 customers — all returned
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',300.00),
    (2,2,'2024-01-01',200.00),
    (3,3,'2024-01-01',100.00);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT, total_spent DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q12_solution;
SELECT * INTO #expected FROM (VALUES
    (1,CAST(300.00 AS DECIMAL(10,2))),
    (2,CAST(200.00 AS DECIMAL(10,2))),
    (3,CAST(100.00 AS DECIMAL(10,2))))
    t(customer_id,total_spent);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 3 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 3 FAIL: Unexpected rows',1;
PRINT 'TEST 3 PASS: Exactly 3 customers, all returned';
GO

-- -------------------------------------------------
-- TEST 4: Fewer than 3 customers — return all
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',500.00),
    (2,2,'2024-01-01',300.00);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT, total_spent DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q12_solution;
SELECT * INTO #expected FROM (VALUES
    (1,CAST(500.00 AS DECIMAL(10,2))),
    (2,CAST(300.00 AS DECIMAL(10,2))))
    t(customer_id,total_spent);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 4 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 4 FAIL: Unexpected rows',1;
PRINT 'TEST 4 PASS: Fewer than 3 customers, all returned';
GO

-- -------------------------------------------------
-- TEST 5: Multiple orders per customer aggregated correctly
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',100.00),(2,1,'2024-01-02',200.00),(3,1,'2024-01-03',300.00),
    (4,2,'2024-01-01',800.00),
    (5,3,'2024-01-01',50.00),(6,3,'2024-01-02',50.00),
    (7,4,'2024-01-01',999.00);
-- totals: 1=600, 2=800, 3=100, 4=999 → top3: 4(999),2(800),1(600)

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT, total_spent DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q12_solution;
SELECT * INTO #expected FROM (VALUES
    (4,CAST(999.00 AS DECIMAL(10,2))),
    (2,CAST(800.00 AS DECIMAL(10,2))),
    (1,CAST(600.00 AS DECIMAL(10,2))))
    t(customer_id,total_spent);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 5 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 5 FAIL: Unexpected rows',1;
PRINT 'TEST 5 PASS: Multiple orders aggregated correctly';
GO

-- -------------------------------------------------
-- TEST 6: 4th-place customer excluded
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',1000.00),
    (2,2,'2024-01-01', 900.00),
    (3,3,'2024-01-01', 800.00),
    (4,4,'2024-01-01', 700.00);

DROP TABLE IF EXISTS #actual;
CREATE TABLE #actual (customer_id INT, total_spent DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q12_solution;
IF EXISTS(SELECT * FROM #actual WHERE customer_id = 4)
    THROW 50001,'TEST 6 FAIL: 4th-place customer should not appear',1;
IF (SELECT COUNT(*) FROM #actual) <> 3
    THROW 50002,'TEST 6 FAIL: Expected exactly 3 rows',1;
PRINT 'TEST 6 PASS: 4th-place customer excluded';
GO

-- -------------------------------------------------
-- TEST 7: All customers tied — only rank 1 exists, all returned
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',500.00),
    (2,2,'2024-01-01',500.00),
    (3,3,'2024-01-01',500.00),
    (4,4,'2024-01-01',500.00),
    (5,5,'2024-01-01',500.00);

DROP TABLE IF EXISTS #actual;
CREATE TABLE #actual (customer_id INT, total_spent DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q12_solution;
IF (SELECT COUNT(*) FROM #actual) <> 5
    THROW 50001,'TEST 7 FAIL: All 5 customers should be returned when all tied at rank 1',1;
PRINT 'TEST 7 PASS: All customers tied, all returned';
GO

-- -------------------------------------------------
-- TEST 8: Tie for 1st — both rank-1 customers returned plus next
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',1000.00),
    (2,2,'2024-01-01',1000.00),
    (3,3,'2024-01-01', 800.00),
    (4,4,'2024-01-01', 700.00);
-- DENSE_RANK: 1&2 → rank1, 3 → rank2, 4 → rank3 — all 4 within top 3 ranks? No: rank1,2,3 → customers 1,2,3,4

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT, total_spent DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q12_solution;
SELECT * INTO #expected FROM (VALUES
    (1,CAST(1000.00 AS DECIMAL(10,2))),
    (2,CAST(1000.00 AS DECIMAL(10,2))),
    (3,CAST( 800.00 AS DECIMAL(10,2))),
    (4,CAST( 700.00 AS DECIMAL(10,2))))
    t(customer_id,total_spent);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 8 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 8 FAIL: Unexpected rows',1;
PRINT 'TEST 8 PASS: Tie for 1st handled correctly';
GO

-- -------------------------------------------------
-- TEST 9: Decimal precision in totals
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',333.33),(2,1,'2024-01-02',333.33),(3,1,'2024-01-03',333.34),
    (4,2,'2024-01-01',999.99),
    (5,3,'2024-01-01',500.00);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT, total_spent DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q12_solution;
SELECT * INTO #expected FROM (VALUES
    (2,CAST( 999.99 AS DECIMAL(10,2))),
    (1,CAST(1000.00 AS DECIMAL(10,2))),
    (3,CAST( 500.00 AS DECIMAL(10,2))))
    t(customer_id,total_spent);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 9 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 9 FAIL: Unexpected rows',1;
PRINT 'TEST 9 PASS: Decimal totals precise';
GO

-- -------------------------------------------------
-- TEST 10: Single customer
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES (1,1,'2024-01-01',250.00);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT, total_spent DECIMAL(10,2));
INSERT INTO #actual EXEC dbo.q12_solution;
SELECT * INTO #expected FROM (VALUES (1,CAST(250.00 AS DECIMAL(10,2)))) t(customer_id,total_spent);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 10 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 10 FAIL: Unexpected rows',1;
PRINT 'TEST 10 PASS: Single customer returned';
GO
