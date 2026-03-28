-- Q5 [SQL | Senior] — TEST CASES
-- Find customers who placed orders in every month of 2024.
-- Output columns: customer_id

:r ./sql/dml/q5_solution.sql

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

-- -------------------------------------------------
-- TEST 1: Base case — only customer 1 has all 12 months
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,  1, '2024-01-15', 100),(2,  1, '2024-02-10', 200),(3,  1, '2024-03-05', 150),
    (4,  1, '2024-04-20', 300),(5,  1, '2024-05-11', 250),(6,  1, '2024-06-30', 175),
    (7,  1, '2024-07-14', 400),(8,  1, '2024-08-22', 320),(9,  1, '2024-09-09', 210),
    (10, 1, '2024-10-18', 190),(11, 1, '2024-11-25', 280),(12, 1, '2024-12-31', 350),
    (13, 2, '2024-01-05', 100),(14, 2, '2024-02-14', 200),(15, 2, '2024-03-22', 150),
    (16, 3, '2024-01-10', 500),(17, 3, '2024-01-28', 600);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT);
INSERT INTO #actual EXEC dbo.q5_solution;
SELECT * INTO #expected FROM (VALUES (1)) t(customer_id);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 1 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 1 FAIL: Unexpected rows',1;
PRINT 'TEST 1 PASS: Only customer 1 qualifies';
GO

-- -------------------------------------------------
-- TEST 2: Two customers with all 12 months
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,  1,'2024-01-01',100),(2,  1,'2024-02-01',100),(3,  1,'2024-03-01',100),
    (4,  1,'2024-04-01',100),(5,  1,'2024-05-01',100),(6,  1,'2024-06-01',100),
    (7,  1,'2024-07-01',100),(8,  1,'2024-08-01',100),(9,  1,'2024-09-01',100),
    (10, 1,'2024-10-01',100),(11, 1,'2024-11-01',100),(12, 1,'2024-12-01',100),
    (13, 2,'2024-01-01',100),(14, 2,'2024-02-01',100),(15, 2,'2024-03-01',100),
    (16, 2,'2024-04-01',100),(17, 2,'2024-05-01',100),(18, 2,'2024-06-01',100),
    (19, 2,'2024-07-01',100),(20, 2,'2024-08-01',100),(21, 2,'2024-09-01',100),
    (22, 2,'2024-10-01',100),(23, 2,'2024-11-01',100),(24, 2,'2024-12-01',100);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT);
INSERT INTO #actual EXEC dbo.q5_solution;
SELECT * INTO #expected FROM (VALUES (1),(2)) t(customer_id);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 2 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 2 FAIL: Unexpected rows',1;
PRINT 'TEST 2 PASS: Both customers with 12 months qualify';
GO

-- -------------------------------------------------
-- TEST 3: Customer with 11 months does NOT qualify (missing December)
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',100),(2,1,'2024-02-01',100),(3,1,'2024-03-01',100),
    (4,1,'2024-04-01',100),(5,1,'2024-05-01',100),(6,1,'2024-06-01',100),
    (7,1,'2024-07-01',100),(8,1,'2024-08-01',100),(9,1,'2024-09-01',100),
    (10,1,'2024-10-01',100),(11,1,'2024-11-01',100);

DROP TABLE IF EXISTS #actual;
CREATE TABLE #actual (customer_id INT);
INSERT INTO #actual EXEC dbo.q5_solution;
IF EXISTS(SELECT * FROM #actual) THROW 50001,'TEST 3 FAIL: Customer with 11 months must not qualify',1;
PRINT 'TEST 3 PASS: 11-month customer correctly excluded';
GO

-- -------------------------------------------------
-- TEST 4: Multiple orders in same month count as ONE month
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',100),(2,1,'2024-01-15',100),(3,1,'2024-01-28',100),
    (4,1,'2024-02-01',100),(5,1,'2024-03-01',100),(6,1,'2024-04-01',100),
    (7,1,'2024-05-01',100),(8,1,'2024-06-01',100),(9,1,'2024-07-01',100),
    (10,1,'2024-08-01',100),(11,1,'2024-09-01',100),(12,1,'2024-10-01',100),
    (13,1,'2024-11-01',100),(14,1,'2024-12-01',100);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT);
INSERT INTO #actual EXEC dbo.q5_solution;
SELECT * INTO #expected FROM (VALUES (1)) t(customer_id);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 4 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 4 FAIL: Unexpected rows',1;
PRINT 'TEST 4 PASS: Multiple orders in same month treated as one distinct month';
GO

-- -------------------------------------------------
-- TEST 5: No customers qualify — all have fewer than 12 months
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',100),(2,2,'2024-06-01',100),(3,3,'2024-12-01',100);

DROP TABLE IF EXISTS #actual;
CREATE TABLE #actual (customer_id INT);
INSERT INTO #actual EXEC dbo.q5_solution;
IF EXISTS(SELECT * FROM #actual) THROW 50001,'TEST 5 FAIL: No customer should qualify',1;
PRINT 'TEST 5 PASS: Empty result when no customer has all 12 months';
GO

-- -------------------------------------------------
-- TEST 6: Orders outside 2024 are ignored
-- Customer 1 has all 12 months in 2024 + some in 2023 → still qualifies
-- Customer 2 needs 2023 orders to reach 12 → must NOT qualify
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',100),(2,1,'2024-02-01',100),(3,1,'2024-03-01',100),
    (4,1,'2024-04-01',100),(5,1,'2024-05-01',100),(6,1,'2024-06-01',100),
    (7,1,'2024-07-01',100),(8,1,'2024-08-01',100),(9,1,'2024-09-01',100),
    (10,1,'2024-10-01',100),(11,1,'2024-11-01',100),(12,1,'2024-12-01',100),
    (13,1,'2023-06-01',100),
    (14,2,'2024-01-01',100),(15,2,'2024-02-01',100),(16,2,'2023-03-01',100),
    (17,2,'2023-04-01',100),(18,2,'2023-05-01',100),(19,2,'2023-06-01',100),
    (20,2,'2023-07-01',100),(21,2,'2023-08-01',100),(22,2,'2023-09-01',100),
    (23,2,'2023-10-01',100),(24,2,'2023-11-01',100),(25,2,'2023-12-01',100);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT);
INSERT INTO #actual EXEC dbo.q5_solution;
SELECT * INTO #expected FROM (VALUES (1)) t(customer_id);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 6 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 6 FAIL: Non-2024 orders must not count',1;
PRINT 'TEST 6 PASS: Orders outside 2024 correctly ignored';
GO

-- -------------------------------------------------
-- TEST 7: Customer with exactly one order per month qualifies
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-31',100),(2,1,'2024-02-29',100),(3,1,'2024-03-31',100),
    (4,1,'2024-04-30',100),(5,1,'2024-05-31',100),(6,1,'2024-06-30',100),
    (7,1,'2024-07-31',100),(8,1,'2024-08-31',100),(9,1,'2024-09-30',100),
    (10,1,'2024-10-31',100),(11,1,'2024-11-30',100),(12,1,'2024-12-31',100);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT);
INSERT INTO #actual EXEC dbo.q5_solution;
SELECT * INTO #expected FROM (VALUES (1)) t(customer_id);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 7 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 7 FAIL: Unexpected rows',1;
PRINT 'TEST 7 PASS: Exactly one order per month (last days) qualifies';
GO

-- -------------------------------------------------
-- TEST 8: Large number of customers, only one qualifies
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-01',100),(2,1,'2024-02-01',100),(3,1,'2024-03-01',100),
    (4,1,'2024-04-01',100),(5,1,'2024-05-01',100),(6,1,'2024-06-01',100),
    (7,1,'2024-07-01',100),(8,1,'2024-08-01',100),(9,1,'2024-09-01',100),
    (10,1,'2024-10-01',100),(11,1,'2024-11-01',100),(12,1,'2024-12-01',100),
    (13,2,'2024-01-01',100),(14,3,'2024-02-01',100),(15,4,'2024-03-01',100),
    (16,5,'2024-04-01',100),(17,6,'2024-05-01',100),(18,7,'2024-06-01',100);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT);
INSERT INTO #actual EXEC dbo.q5_solution;
SELECT * INTO #expected FROM (VALUES (1)) t(customer_id);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 8 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 8 FAIL: Unexpected rows',1;
PRINT 'TEST 8 PASS: Single qualifier among many customers';
GO

-- -------------------------------------------------
-- TEST 9: Duplicate orders on same date still count as one month
-- -------------------------------------------------
TRUNCATE TABLE orders;
INSERT INTO orders VALUES
    (1,1,'2024-01-05',100),(2,1,'2024-01-05',200),(3,1,'2024-02-01',100),
    (4,1,'2024-03-01',100),(5,1,'2024-04-01',100),(6,1,'2024-05-01',100),
    (7,1,'2024-06-01',100),(8,1,'2024-07-01',100),(9,1,'2024-08-01',100),
    (10,1,'2024-09-01',100),(11,1,'2024-10-01',100),(12,1,'2024-11-01',100),
    (13,1,'2024-12-01',100);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (customer_id INT);
INSERT INTO #actual EXEC dbo.q5_solution;
SELECT * INTO #expected FROM (VALUES (1)) t(customer_id);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 9 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 9 FAIL: Unexpected rows',1;
PRINT 'TEST 9 PASS: Duplicate same-date orders correctly treated as one month';
GO

-- -------------------------------------------------
-- TEST 10: No orders at all — empty result
-- -------------------------------------------------
TRUNCATE TABLE orders;

DROP TABLE IF EXISTS #actual;
CREATE TABLE #actual (customer_id INT);
INSERT INTO #actual EXEC dbo.q5_solution;
IF EXISTS(SELECT * FROM #actual) THROW 50001,'TEST 10 FAIL: Empty table must return no rows',1;
PRINT 'TEST 10 PASS: Empty table returns empty result';
GO
