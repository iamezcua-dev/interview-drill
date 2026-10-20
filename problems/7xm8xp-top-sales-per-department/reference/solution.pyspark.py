from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def solution(employee_sales: DataFrame) -> DataFrame:
    totals = employee_sales.groupBy("department", "employee_id").agg(
        F.sum("amount").alias("total_sales")
    )
    by_dept = Window.partitionBy("department").orderBy(F.desc("total_sales"))
    return (
        totals.withColumn("rnk", F.rank().over(by_dept))
        .where(F.col("rnk") == 1)
        .select("department", "employee_id", "total_sales")
    )
