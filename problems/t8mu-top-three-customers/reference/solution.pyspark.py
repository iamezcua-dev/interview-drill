from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def solution(orders: DataFrame) -> DataFrame:
    totals = orders.groupBy("customer_id").agg(
        F.sum("amount").alias("total_spent")
    )
    by_total = Window.orderBy(F.desc("total_spent"))
    return (
        totals.withColumn("rnk", F.dense_rank().over(by_total))
        .where(F.col("rnk") <= 3)
        .orderBy(F.desc("total_spent"))
        .select("customer_id", "total_spent")
    )
