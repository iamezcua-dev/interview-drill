"""
Q7 [PySpark | Senior] — TEST CASES
Problem: Compute month-over-month revenue change for each month.
Output columns: month, revenue, prev_revenue, change
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType
from q7_solution import solution


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("test_q7")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


SCHEMA = StructType([
    StructField("month",   IntegerType(), False),
    StructField("revenue", IntegerType(), False),
])


def to_sorted_rows(df):
    return df.orderBy("month").collect()


# -------------------------------------------------
# TEST 1: Base case — 6 months, verify all values including nulls
# -------------------------------------------------
def test_base_case(spark):
    data = [(1,10000),(2,12000),(3,9500),(4,11000),(5,13500),(6,12800)]
    df = spark.createDataFrame(data, SCHEMA)
    rows = to_sorted_rows(solution(df))
    assert len(rows) == 6
    assert rows[0]["prev_revenue"] is None
    assert rows[0]["change"]       is None
    assert rows[1]["prev_revenue"] == 10000
    assert rows[1]["change"]       == 2000
    assert rows[2]["prev_revenue"] == 12000
    assert rows[2]["change"]       == -2500


# -------------------------------------------------
# TEST 2: Single row — prev_revenue and change are both null
# -------------------------------------------------
def test_single_row(spark):
    data = [(1, 5000)]
    df = spark.createDataFrame(data, SCHEMA)
    rows = to_sorted_rows(solution(df))
    assert len(rows) == 1
    assert rows[0]["revenue"]      == 5000
    assert rows[0]["prev_revenue"] is None
    assert rows[0]["change"]       is None


# -------------------------------------------------
# TEST 3: Two rows — first has nulls, second has correct diff
# -------------------------------------------------
def test_two_rows(spark):
    data = [(1, 1000), (2, 1500)]
    df = spark.createDataFrame(data, SCHEMA)
    rows = to_sorted_rows(solution(df))
    assert rows[0]["prev_revenue"] is None
    assert rows[0]["change"]       is None
    assert rows[1]["prev_revenue"] == 1000
    assert rows[1]["change"]       == 500


# -------------------------------------------------
# TEST 4: Negative change (revenue drops)
# -------------------------------------------------
def test_negative_change(spark):
    data = [(1, 2000), (2, 1000)]
    df = spark.createDataFrame(data, SCHEMA)
    rows = to_sorted_rows(solution(df))
    assert rows[1]["change"] == -1000


# -------------------------------------------------
# TEST 5: Zero change (same revenue two months in a row)
# -------------------------------------------------
def test_zero_change(spark):
    data = [(1, 5000), (2, 5000)]
    df = spark.createDataFrame(data, SCHEMA)
    rows = to_sorted_rows(solution(df))
    assert rows[1]["change"] == 0


# -------------------------------------------------
# TEST 6: Output has exactly the required 4 columns
# -------------------------------------------------
def test_output_columns(spark):
    data = [(1, 1000), (2, 2000)]
    df = spark.createDataFrame(data, SCHEMA)
    result = solution(df)
    assert set(result.columns) == {"month", "revenue", "prev_revenue", "change"}


# -------------------------------------------------
# TEST 7: Output has no extra columns
# -------------------------------------------------
def test_no_extra_columns(spark):
    data = [(1, 1000), (2, 2000)]
    df = spark.createDataFrame(data, SCHEMA)
    assert len(solution(df).columns) == 4


# -------------------------------------------------
# TEST 8: Months not in order in input — output still ordered correctly
# -------------------------------------------------
def test_unordered_input(spark):
    data = [(3, 9000), (1, 5000), (2, 7000)]
    df = spark.createDataFrame(data, SCHEMA)
    rows = to_sorted_rows(solution(df))
    assert rows[0]["month"] == 1
    assert rows[0]["prev_revenue"] is None
    assert rows[1]["month"] == 2
    assert rows[1]["prev_revenue"] == 5000
    assert rows[1]["change"] == 2000
    assert rows[2]["month"] == 3
    assert rows[2]["prev_revenue"] == 7000
    assert rows[2]["change"] == 2000


# -------------------------------------------------
# TEST 9: Large revenue values — no overflow
# -------------------------------------------------
def test_large_values(spark):
    data = [(1, 1_000_000), (2, 2_000_000)]
    df = spark.createDataFrame(data, SCHEMA)
    rows = to_sorted_rows(solution(df))
    assert rows[1]["change"] == 1_000_000


# -------------------------------------------------
# TEST 10: All months with alternating increase/decrease
# -------------------------------------------------
def test_alternating_changes(spark):
    data = [(1,100),(2,200),(3,150),(4,300),(5,250),(6,400)]
    df = spark.createDataFrame(data, SCHEMA)
    rows = to_sorted_rows(solution(df))
    changes = [r["change"] for r in rows]
    assert changes[0] is None
    assert changes[1] == 100
    assert changes[2] == -50
    assert changes[3] == 150
    assert changes[4] == -50
    assert changes[5] == 150
