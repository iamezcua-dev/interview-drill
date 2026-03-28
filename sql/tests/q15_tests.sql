-- Q15 [SQL | Senior] — TEST CASES
-- Return all departments with employee_count, including depts with 0 employees.
-- Output columns: department_name VARCHAR, employee_count INT

:r ./sql/dml/q15_solution.sql

USE UnoSquareDrill;
GO

SET NOCOUNT ON;
GO

-- -------------------------------------------------
-- TEST 1: Base case — 4 depts, one empty (Legal)
-- -------------------------------------------------
DELETE FROM emp; DELETE FROM dept;
INSERT INTO dept VALUES (1,'Engineering'),(2,'Marketing'),(3,'Sales'),(4,'Legal');
INSERT INTO emp VALUES (1,1),(2,1),(3,2),(4,1),(5,3);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department_name VARCHAR(50), employee_count INT);
INSERT INTO #actual EXEC dbo.q15_solution;
SELECT * INTO #expected FROM (VALUES
    ('Engineering',3),('Marketing',1),('Sales',1),('Legal',0))
    t(department_name,employee_count);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 1 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 1 FAIL: Unexpected rows',1;
PRINT 'TEST 1 PASS: Base case';
GO

-- -------------------------------------------------
-- TEST 2: All departments empty
-- -------------------------------------------------
DELETE FROM emp; DELETE FROM dept;
INSERT INTO dept VALUES (1,'A'),(2,'B'),(3,'C');

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department_name VARCHAR(50), employee_count INT);
INSERT INTO #actual EXEC dbo.q15_solution;
SELECT * INTO #expected FROM (VALUES ('A',0),('B',0),('C',0))
    t(department_name,employee_count);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 2 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 2 FAIL: Unexpected rows',1;
PRINT 'TEST 2 PASS: All departments empty';
GO

-- -------------------------------------------------
-- TEST 3: All employees in one department
-- -------------------------------------------------
DELETE FROM emp; DELETE FROM dept;
INSERT INTO dept VALUES (1,'Only'),(2,'Empty');
INSERT INTO emp VALUES (1,1),(2,1),(3,1);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department_name VARCHAR(50), employee_count INT);
INSERT INTO #actual EXEC dbo.q15_solution;
SELECT * INTO #expected FROM (VALUES ('Only',3),('Empty',0))
    t(department_name,employee_count);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 3 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 3 FAIL: Unexpected rows',1;
PRINT 'TEST 3 PASS: All employees in one dept, other is empty';
GO

-- -------------------------------------------------
-- TEST 4: Single department with employees
-- -------------------------------------------------
DELETE FROM emp; DELETE FROM dept;
INSERT INTO dept VALUES (1,'Solo');
INSERT INTO emp VALUES (1,1),(2,1);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department_name VARCHAR(50), employee_count INT);
INSERT INTO #actual EXEC dbo.q15_solution;
SELECT * INTO #expected FROM (VALUES ('Solo',2)) t(department_name,employee_count);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 4 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 4 FAIL: Unexpected rows',1;
PRINT 'TEST 4 PASS: Single department with employees';
GO

-- -------------------------------------------------
-- TEST 5: Single empty department
-- -------------------------------------------------
DELETE FROM emp; DELETE FROM dept;
INSERT INTO dept VALUES (1,'Empty');

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department_name VARCHAR(50), employee_count INT);
INSERT INTO #actual EXEC dbo.q15_solution;
SELECT * INTO #expected FROM (VALUES ('Empty',0)) t(department_name,employee_count);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 5 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 5 FAIL: Unexpected rows',1;
PRINT 'TEST 5 PASS: Single empty department';
GO

-- -------------------------------------------------
-- TEST 6: Multiple empty departments
-- -------------------------------------------------
DELETE FROM emp; DELETE FROM dept;
INSERT INTO dept VALUES (1,'X'),(2,'Y'),(3,'Z');
INSERT INTO emp VALUES (1,1);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department_name VARCHAR(50), employee_count INT);
INSERT INTO #actual EXEC dbo.q15_solution;
SELECT * INTO #expected FROM (VALUES ('X',1),('Y',0),('Z',0))
    t(department_name,employee_count);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 6 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 6 FAIL: Unexpected rows',1;
PRINT 'TEST 6 PASS: Multiple empty departments included';
GO

-- -------------------------------------------------
-- TEST 7: Large employee count in one dept
-- -------------------------------------------------
DELETE FROM emp; DELETE FROM dept;
INSERT INTO dept VALUES (1,'Big'),(2,'Small');
INSERT INTO emp
    SELECT TOP 50 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 1
    FROM sys.objects;
INSERT INTO emp VALUES (51,2);

DROP TABLE IF EXISTS #actual;
CREATE TABLE #actual (department_name VARCHAR(50), employee_count INT);
INSERT INTO #actual EXEC dbo.q15_solution;
IF (SELECT employee_count FROM #actual WHERE department_name = 'Big') <> 50
    THROW 50001,'TEST 7 FAIL: Big dept should have 50 employees',1;
IF (SELECT employee_count FROM #actual WHERE department_name = 'Small') <> 1
    THROW 50002,'TEST 7 FAIL: Small dept should have 1 employee',1;
PRINT 'TEST 7 PASS: Large employee count correct';
GO

-- -------------------------------------------------
-- TEST 8: No duplicate department rows
-- -------------------------------------------------
DELETE FROM emp; DELETE FROM dept;
INSERT INTO dept VALUES (1,'Eng'),(2,'HR');
INSERT INTO emp VALUES (1,1),(2,1),(3,2);

DROP TABLE IF EXISTS #actual;
CREATE TABLE #actual (department_name VARCHAR(50), employee_count INT);
INSERT INTO #actual EXEC dbo.q15_solution;
IF (SELECT COUNT(*) FROM #actual) <> 2
    THROW 50001,'TEST 8 FAIL: Expected exactly 2 rows, one per department',1;
PRINT 'TEST 8 PASS: No duplicate department rows';
GO

-- -------------------------------------------------
-- TEST 9: Five departments, two empty
-- -------------------------------------------------
DELETE FROM emp; DELETE FROM dept;
INSERT INTO dept VALUES (1,'A'),(2,'B'),(3,'C'),(4,'D'),(5,'E');
INSERT INTO emp VALUES (1,1),(2,2),(3,3);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department_name VARCHAR(50), employee_count INT);
INSERT INTO #actual EXEC dbo.q15_solution;
SELECT * INTO #expected FROM (VALUES
    ('A',1),('B',1),('C',1),('D',0),('E',0))
    t(department_name,employee_count);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 9 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 9 FAIL: Unexpected rows',1;
PRINT 'TEST 9 PASS: Five departments, two empty';
GO

-- -------------------------------------------------
-- TEST 10: Employee count is exactly 1 per dept
-- -------------------------------------------------
DELETE FROM emp; DELETE FROM dept;
INSERT INTO dept VALUES (1,'P'),(2,'Q'),(3,'R');
INSERT INTO emp VALUES (1,1),(2,2),(3,3);

DROP TABLE IF EXISTS #actual; DROP TABLE IF EXISTS #expected;
CREATE TABLE #actual (department_name VARCHAR(50), employee_count INT);
INSERT INTO #actual EXEC dbo.q15_solution;
SELECT * INTO #expected FROM (VALUES ('P',1),('Q',1),('R',1))
    t(department_name,employee_count);
IF EXISTS(SELECT * FROM #expected EXCEPT SELECT * FROM #actual) THROW 50001,'TEST 10 FAIL: Missing/wrong rows',1;
IF EXISTS(SELECT * FROM #actual EXCEPT SELECT * FROM #expected) THROW 50002,'TEST 10 FAIL: Unexpected rows',1;
PRINT 'TEST 10 PASS: One employee per department';
GO
