# Tests for Q10 — avg salary per department, filtered > 70000, ordered desc

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from q10_solution import solution


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("test_q10")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


SCHEMA = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("department",  StringType(),  False),
    StructField("salary",      DoubleType(),  False),
    StructField("hire_date",   DateType(),    False),
])


def make_df(spark, data):
    return spark.createDataFrame(data, SCHEMA)


def test_base_case(spark):
    data = [
        (1, "Engineering", 95000.0, date(2020, 1, 15)),
        (2, "Engineering", 85000.0, date(2019, 6, 1)),
        (3, "Marketing",   60000.0, date(2021, 3, 10)),
        (4, "Marketing",   65000.0, date(2022, 7, 20)),
        (5, "Sales",       72000.0, date(2020, 11, 5)),
        (6, "Sales",       68000.0, date(2021, 9, 14)),
    ]
    # Engineering avg: 90000, Sales avg: 70000 (not > 70000), Marketing avg: 62500
    result = solution(make_df(spark, data))
    rows = result.collect()
    depts = [r.department for r in rows]
    assert "Engineering" in depts
    assert "Marketing" not in depts
    assert "Sales" not in depts


def test_exact_threshold_excluded(spark):
    # avg exactly 70000 should NOT be included (> not >=)
    data = [
        (1, "A", 70000.0, date(2020, 1, 1)),
        (2, "B", 80000.0, date(2020, 1, 1)),
    ]
    result = solution(make_df(spark, data))
    rows = result.collect()
    depts = [r.department for r in rows]
    assert "A" not in depts
    assert "B" in depts


def test_all_below_threshold(spark):
    data = [
        (1, "A", 50000.0, date(2020, 1, 1)),
        (2, "B", 60000.0, date(2020, 1, 1)),
    ]
    result = solution(make_df(spark, data))
    assert result.count() == 0


def test_all_above_threshold(spark):
    data = [
        (1, "A", 80000.0, date(2020, 1, 1)),
        (2, "B", 90000.0, date(2020, 1, 1)),
    ]
    result = solution(make_df(spark, data))
    assert result.count() == 2


def test_descending_order(spark):
    data = [
        (1, "A", 75000.0, date(2020, 1, 1)),
        (2, "B", 95000.0, date(2020, 1, 1)),
        (3, "C", 85000.0, date(2020, 1, 1)),
    ]
    result = solution(make_df(spark, data))
    rows = result.collect()
    avgs = [r.avg_salary for r in rows]
    assert avgs == sorted(avgs, reverse=True)


def test_output_columns(spark):
    data = [(1, "Engineering", 80000.0, date(2020, 1, 1))]
    result = solution(make_df(spark, data))
    assert set(result.columns) == {"department", "avg_salary"}


def test_single_employee_dept(spark):
    data = [(1, "Solo", 75000.0, date(2020, 1, 1))]
    result = solution(make_df(spark, data))
    rows = result.collect()
    assert len(rows) == 1
    assert abs(rows[0].avg_salary - 75000.0) < 0.01


def test_avg_computed_correctly(spark):
    data = [
        (1, "A", 71000.0, date(2020, 1, 1)),
        (2, "A", 79000.0, date(2020, 1, 1)),
    ]
    # avg = 75000
    result = solution(make_df(spark, data))
    rows = result.collect()
    assert len(rows) == 1
    assert abs(rows[0].avg_salary - 75000.0) < 0.01


def test_hire_date_not_used(spark):
    # hire_date should have no effect on the result
    data = [
        (1, "A", 80000.0, date(1990, 1, 1)),
        (2, "A", 90000.0, date(2099, 12, 31)),
    ]
    result = solution(make_df(spark, data))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0].department == "A"


def test_multiple_employees_per_dept(spark):
    data = [
        (1, "A", 100000.0, date(2020, 1, 1)),
        (2, "A",  80000.0, date(2020, 1, 1)),
        (3, "A",  60000.0, date(2020, 1, 1)),
        (4, "B",  50000.0, date(2020, 1, 1)),
    ]
    # A avg: 80000, B avg: 50000
    result = solution(make_df(spark, data))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0].department == "A"
    assert abs(rows[0].avg_salary - 80000.0) < 0.01


def test_three_qualifying_depts_ordered(spark):
    data = [
        (1, "X", 72000.0, date(2020, 1, 1)),
        (2, "Y", 85000.0, date(2020, 1, 1)),
        (3, "Z", 91000.0, date(2020, 1, 1)),
    ]
    result = solution(make_df(spark, data))
    rows = result.collect()
    assert [r.department for r in rows] == ["Z", "Y", "X"]
