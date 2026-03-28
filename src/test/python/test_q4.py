"""
Q4 [PySpark | Senior] — TEST CASES
Problem: Return employees whose salary is strictly above their department average.
Output columns: employee_id, name, department, salary
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from q4_solution import solution


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("test_q4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


SCHEMA = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("name",        StringType(),  False),
    StructField("department",  StringType(),  False),
    StructField("salary",      IntegerType(), False),
])


def to_tuples(df):
    return set(df.select("employee_id", "name", "department", "salary").collect())


# -------------------------------------------------
# TEST 1: Base case — standard 6 rows, two departments
# APAC avg = 76666.67 → Alice (80000) and Carol (90000) qualify
# EMEA avg = 70000.00 → Eve (75000) qualifies
# -------------------------------------------------
def test_base_case(spark):
    data = [
        (1, "Alice", "APAC", 80000),
        (2, "Bob",   "APAC", 60000),
        (3, "Carol", "APAC", 90000),
        (4, "Dave",  "EMEA", 70000),
        (5, "Eve",   "EMEA", 75000),
        (6, "Frank", "EMEA", 65000),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    result = to_tuples(solution(df))
    assert len(result) == 3
    ids = {r["employee_id"] for r in result}
    assert ids == {1, 3, 5}


# -------------------------------------------------
# TEST 2: Employee exactly at department average is NOT returned
# APAC: emp1=100, emp2=100 → avg=100, nobody strictly above
# -------------------------------------------------
def test_exactly_at_avg_excluded(spark):
    data = [
        (1, "Alice", "APAC", 100),
        (2, "Bob",   "APAC", 100),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    result = to_tuples(solution(df))
    assert len(result) == 0


# -------------------------------------------------
# TEST 3: Single employee per department — always at avg, never above
# -------------------------------------------------
def test_single_employee_per_dept(spark):
    data = [
        (1, "Alice", "APAC", 90000),
        (2, "Bob",   "EMEA", 80000),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    result = to_tuples(solution(df))
    assert len(result) == 0


# -------------------------------------------------
# TEST 4: Multiple employees above avg in one department
# APAC: avg = 60000, emp2=80000 and emp3=90000 both qualify
# -------------------------------------------------
def test_multiple_above_avg(spark):
    data = [
        (1, "Alice", "APAC", 10000),
        (2, "Bob",   "APAC", 80000),
        (3, "Carol", "APAC", 90000),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    result = to_tuples(solution(df))
    ids = {r["employee_id"] for r in result}
    assert ids == {2, 3}


# -------------------------------------------------
# TEST 5: Single department only
# avg = (50000+70000+90000) / 3 = 70000 → only emp3 qualifies
# -------------------------------------------------
def test_single_department(spark):
    data = [
        (1, "Alice", "APAC", 50000),
        (2, "Bob",   "APAC", 70000),
        (3, "Carol", "APAC", 90000),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    result = to_tuples(solution(df))
    ids = {r["employee_id"] for r in result}
    assert ids == {3}


# -------------------------------------------------
# TEST 6: Three departments with independent averages
# -------------------------------------------------
def test_three_departments(spark):
    data = [
        (1,  "A", "APAC",  100),
        (2,  "B", "APAC",  200),   # above APAC avg 150
        (3,  "C", "EMEA",  300),
        (4,  "D", "EMEA",  500),   # above EMEA avg 400
        (5,  "E", "LATAM", 600),
        (6,  "F", "LATAM", 1000),  # above LATAM avg 800
        (7,  "G", "LATAM", 800),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    result = to_tuples(solution(df))
    ids = {r["employee_id"] for r in result}
    assert ids == {2, 4, 6}


# -------------------------------------------------
# TEST 7: Output columns are exactly the required ones
# -------------------------------------------------
def test_output_columns(spark):
    data = [
        (1, "Alice", "APAC", 80000),
        (2, "Bob",   "APAC", 60000),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    result_df = solution(df)
    assert set(result_df.columns) == {"employee_id", "name", "department", "salary"}


# -------------------------------------------------
# TEST 8: No extra columns leaked (e.g. avg_salary should not appear)
# -------------------------------------------------
def test_no_extra_columns(spark):
    data = [
        (1, "Alice", "APAC", 80000),
        (2, "Bob",   "APAC", 60000),
        (3, "Carol", "APAC", 90000),
    ]
    df = spark.createDataFrame(data, SCHEMA)
    result_df = solution(df)
    assert len(result_df.columns) == 4


# -------------------------------------------------
# TEST 9: Result is filtered correctly when one dept has all below avg (impossible)
# and another has qualifiers — only qualifying dept rows returned
# APAC: all same → nobody above; EMEA: one above avg
# -------------------------------------------------
def test_one_dept_no_qualifiers(spark):
    data = [
        (1, "Alice", "APAC", 50000),
        (2, "Bob",   "APAC", 50000),
        (3, "Carol", "EMEA", 40000),
        (4, "Dave",  "EMEA", 80000),  # above EMEA avg 60000
    ]
    df = spark.createDataFrame(data, SCHEMA)
    result = to_tuples(solution(df))
    ids = {r["employee_id"] for r in result}
    assert ids == {4}


# -------------------------------------------------
# TEST 10: Large salary values — no overflow
# -------------------------------------------------
def test_large_salaries(spark):
    data = [
        (1, "Alice", "APAC", 1_000_000),
        (2, "Bob",   "APAC", 2_000_000),  # above avg 1_500_000
        (3, "Carol", "APAC", 2_500_000),  # above avg 1_500_000
    ]
    df = spark.createDataFrame(data, SCHEMA)
    result = to_tuples(solution(df))
    ids = {r["employee_id"] for r in result}
    assert ids == {2, 3}
