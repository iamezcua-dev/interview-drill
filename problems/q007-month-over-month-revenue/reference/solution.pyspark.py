from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def solution(monthly_revenue: DataFrame) -> DataFrame:
    by_month = Window.orderBy("month")
    return (
        monthly_revenue.withColumn(
            "prev_revenue", F.lag("revenue").over(by_month)
        )
        .withColumn("change", F.col("revenue") - F.col("prev_revenue"))
        .select("month", "revenue", "prev_revenue", "change")
    )
