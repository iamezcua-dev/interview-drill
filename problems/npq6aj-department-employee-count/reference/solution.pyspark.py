from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def solution(dept: DataFrame, emp: DataFrame) -> DataFrame:
    return (
        dept.join(emp, on="department_id", how="left")
        .groupBy("department_id", "department_name")
        .agg(F.count("employee_id").alias("employee_count"))
        .select("department_name", "employee_count")
    )
