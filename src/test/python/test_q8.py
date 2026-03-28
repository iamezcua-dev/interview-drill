# Tests for Q8 — total_spent per user ordered by total_spent desc

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType
from q8_solution import solution


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("test_q8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


SCHEMA = StructType([
    StructField("user_id",    IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity",   IntegerType(), False),
    StructField("price",      DoubleType(),  False),
])


def make_df(spark, data):
    return spark.createDataFrame(data, SCHEMA)


def to_rows(df):
    return df.collect()


def test_base_case(spark):
    data = [
        (1, 101, 2, 9.99),
        (2, 102, 1, 49.99),
        (1, 103, 3, 5.00),
        (3, 101, 1, 9.99),
        (2, 104, 2, 15.00),
        (3, 105, 4, 3.50),
    ]
    # user1: 2*9.99 + 3*5.00 = 19.98 + 15.00 = 34.98
    # user2: 1*49.99 + 2*15.00 = 49.99 + 30.00 = 79.99
    # user3: 1*9.99 + 4*3.50 = 9.99 + 14.00 = 23.99
    # order: user2, user1, user3
    result = solution(make_df(spark, data))
    rows = to_rows(result)
    assert [r.user_id for r in rows] == [2, 1, 3]
    assert abs(rows[0].total_spent - 79.99) < 0.01
    assert abs(rows[1].total_spent - 34.98) < 0.01
    assert abs(rows[2].total_spent - 23.99) < 0.01


def test_single_user(spark):
    data = [(1, 101, 5, 2.00)]
    result = solution(make_df(spark, data))
    rows = to_rows(result)
    assert len(rows) == 1
    assert rows[0].user_id == 1
    assert abs(rows[0].total_spent - 10.0) < 0.01


def test_single_row_per_user(spark):
    data = [
        (1, 101, 1, 100.0),
        (2, 102, 1, 50.0),
    ]
    result = solution(make_df(spark, data))
    rows = to_rows(result)
    assert rows[0].user_id == 1
    assert rows[1].user_id == 2


def test_all_same_user(spark):
    data = [
        (1, 101, 2, 10.0),
        (1, 102, 3, 5.0),
        (1, 103, 1, 20.0),
    ]
    # total = 20 + 15 + 20 = 55
    result = solution(make_df(spark, data))
    rows = to_rows(result)
    assert len(rows) == 1
    assert rows[0].user_id == 1
    assert abs(rows[0].total_spent - 55.0) < 0.01


def test_descending_order(spark):
    data = [
        (1, 101, 1, 1.0),
        (2, 102, 1, 5.0),
        (3, 103, 1, 3.0),
        (4, 104, 1, 10.0),
        (5, 105, 1, 2.0),
    ]
    result = solution(make_df(spark, data))
    rows = to_rows(result)
    spent = [r.total_spent for r in rows]
    assert spent == sorted(spent, reverse=True)


def test_output_columns(spark):
    data = [(1, 101, 1, 5.0)]
    result = solution(make_df(spark, data))
    assert set(result.columns) == {"user_id", "total_spent"}


def test_quantity_multiplied(spark):
    data = [(1, 101, 10, 3.0)]
    result = solution(make_df(spark, data))
    rows = to_rows(result)
    assert abs(rows[0].total_spent - 30.0) < 0.01


def test_decimal_precision(spark):
    data = [
        (1, 101, 3, 1.11),
        (2, 102, 2, 2.22),
    ]
    # user1: 3.33, user2: 4.44
    result = solution(make_df(spark, data))
    rows = to_rows(result)
    assert rows[0].user_id == 2
    assert abs(rows[0].total_spent - 4.44) < 0.01


def test_many_transactions_per_user(spark):
    data = [(1, i, 1, 1.0) for i in range(100)] + [(2, 200, 1, 150.0)]
    # user1: 100.0, user2: 150.0
    result = solution(make_df(spark, data))
    rows = to_rows(result)
    assert rows[0].user_id == 2


def test_three_users_correct_totals(spark):
    data = [
        (1, 101, 2, 5.0),   # 10.0
        (2, 102, 4, 3.0),   # 12.0
        (3, 103, 1, 8.0),   # 8.0
        (1, 104, 1, 2.0),   # +2.0 → user1 total: 12.0
    ]
    result = solution(make_df(spark, data))
    rows = to_rows(result)
    totals = {r.user_id: r.total_spent for r in rows}
    assert abs(totals[1] - 12.0) < 0.01
    assert abs(totals[2] - 12.0) < 0.01
    assert abs(totals[3] - 8.0) < 0.01
    assert rows[2].user_id == 3  # last is user3


def test_large_values(spark):
    data = [
        (1, 101, 1000, 999.99),
        (2, 102, 500, 1000.0),
    ]
    # user1: 999990.0, user2: 500000.0
    result = solution(make_df(spark, data))
    rows = to_rows(result)
    assert rows[0].user_id == 1
    assert abs(rows[0].total_spent - 999990.0) < 0.01
