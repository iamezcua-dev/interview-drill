from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def solution(employee_sales: DataFrame) -> DataFrame:
    running = (
        Window.partitionBy("department")
        .orderBy("sale_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return employee_sales.select(
        "department", "employee_id", "sale_date", "amount",
        F.sum("amount").over(running).alias("running_total"),
    )
