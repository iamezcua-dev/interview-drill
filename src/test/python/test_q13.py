# Tests for Q13 — most recently visited page per user

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from q13_solution import solution


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("test_q13")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


SCHEMA = StructType([
    StructField("user_id",    IntegerType(), False),
    StructField("page",       StringType(),  False),
    StructField("visit_date", DateType(),    False),
])


def make_df(spark, data):
    return spark.createDataFrame(data, SCHEMA)


def test_base_case(spark):
    data = [
        (1, "home",    date(2024, 1, 5)),
        (1, "about",   date(2024, 1, 3)),
        (1, "contact", date(2024, 1, 5)),
        (2, "home",    date(2024, 1, 7)),
        (2, "pricing", date(2024, 1, 2)),
        (3, "blog",    date(2024, 1, 1)),
    ]
    # user1: home+contact on Jan5, user2: home on Jan7, user3: blog on Jan1
    result = solution(make_df(spark, data))
    rows = {(r.user_id, r.page) for r in result.collect()}
    assert (1, "home")    in rows
    assert (1, "contact") in rows
    assert (1, "about")   not in rows
    assert (2, "home")    in rows
    assert (2, "pricing") not in rows
    assert (3, "blog")    in rows


def test_single_visit_per_user(spark):
    data = [
        (1, "home",    date(2024, 1, 1)),
        (2, "pricing", date(2024, 1, 2)),
    ]
    result = solution(make_df(spark, data))
    rows = {(r.user_id, r.page) for r in result.collect()}
    assert rows == {(1, "home"), (2, "pricing")}


def test_tie_on_latest_date_both_returned(spark):
    data = [
        (1, "A", date(2024, 3, 1)),
        (1, "B", date(2024, 3, 1)),
        (1, "C", date(2024, 1, 1)),
    ]
    result = solution(make_df(spark, data))
    pages = {r.page for r in result.collect()}
    assert pages == {"A", "B"}


def test_output_columns(spark):
    data = [(1, "home", date(2024, 1, 1))]
    result = solution(make_df(spark, data))
    assert set(result.columns) == {"user_id", "page", "visit_date"}


def test_older_visits_excluded(spark):
    data = [
        (1, "old",    date(2023, 6, 1)),
        (1, "newest", date(2024, 6, 1)),
    ]
    result = solution(make_df(spark, data))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0].page == "newest"


def test_multiple_users_independent(spark):
    data = [
        (1, "A", date(2024, 1, 10)),
        (1, "B", date(2024, 1, 5)),
        (2, "C", date(2024, 1, 3)),
        (2, "D", date(2024, 1, 8)),
    ]
    result = solution(make_df(spark, data))
    rows = {(r.user_id, r.page) for r in result.collect()}
    assert rows == {(1, "A"), (2, "D")}


def test_visit_date_in_output(spark):
    data = [(1, "home", date(2024, 5, 20))]
    result = solution(make_df(spark, data))
    rows = result.collect()
    assert rows[0].visit_date == date(2024, 5, 20)


def test_single_user_many_visits(spark):
    data = [(1, f"page{i}", date(2024, 1, i)) for i in range(1, 11)]
    # latest is page10 on Jan 10
    result = solution(make_df(spark, data))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0].page == "page10"


def test_three_way_tie(spark):
    data = [
        (1, "X", date(2024, 2, 1)),
        (1, "Y", date(2024, 2, 1)),
        (1, "Z", date(2024, 2, 1)),
    ]
    result = solution(make_df(spark, data))
    pages = {r.page for r in result.collect()}
    assert pages == {"X", "Y", "Z"}


def test_many_users_each_one_result(spark):
    data = [
        (i, "latest", date(2024, 6, 1)) for i in range(1, 6)
    ] + [
        (i, "older",  date(2024, 1, 1)) for i in range(1, 6)
    ]
    result = solution(make_df(spark, data))
    rows = result.collect()
    assert all(r.page == "latest" for r in rows)
    assert len(rows) == 5
