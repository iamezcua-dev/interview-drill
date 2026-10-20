from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def solution(orders: DataFrame) -> DataFrame:
    return (
        orders.where(F.year("order_date") == 2024)
        .groupBy("customer_id")
        .agg(F.countDistinct(F.month("order_date")).alias("months"))
        .where(F.col("months") == 12)
        .select("customer_id")
    )
