from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def solution(transactions: DataFrame) -> DataFrame:
    return (
        transactions.groupBy("user_id")
        .agg(F.sum(F.col("quantity") * F.col("price")).alias("total_spent"))
        .orderBy(F.desc("total_spent"))
    )
