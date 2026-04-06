# Q7 [PySpark | Coding | Senior] — Given a DataFrame of monthly revenue, compute the month-over-month revenue change.
# Output columns: month, revenue, prev_revenue, change.

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import lag


def solution(df: DataFrame) -> DataFrame:
    window_spec = Window.orderBy("month")
    result = df.withColumn("prev_revenue", lag("revenue", 1).over(window_spec))
    return result.withColumn("change",result.revenue - result.prev_revenue)