from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def solution(employees: DataFrame) -> DataFrame:
    return (
        employees.groupBy("department")
        .agg(F.avg("salary").alias("avg_salary"))
        .where(F.col("avg_salary") > 70000)
        .orderBy(F.desc("avg_salary"))
    )
