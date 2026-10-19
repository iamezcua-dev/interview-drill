from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def solution(employees: DataFrame) -> DataFrame:
    by_dept = Window.partitionBy("department")
    return (
        employees.withColumn("dept_avg", F.avg("salary").over(by_dept))
        .where(F.col("salary") > F.col("dept_avg"))
        .select("employee_id", "name", "department", "salary")
    )
