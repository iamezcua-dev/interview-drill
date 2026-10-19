from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def solution(page_visits: DataFrame) -> DataFrame:
    by_user = Window.partitionBy("user_id")
    return (
        page_visits.withColumn("latest", F.max("visit_date").over(by_user))
        .where(F.col("visit_date") == F.col("latest"))
        .select("user_id", "page", "visit_date")
    )
