# Q10 [PySpark | Coding | Senior] — Given a DataFrame of employees, return department and avg_salary
# per department, only for departments where avg_salary > 70000, ordered by avg_salary descending.
# Output columns: department, avg_salary.

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col


def solution(df: DataFrame) -> DataFrame:
    return df.groupBy("department").agg(avg("salary").alias("avg_salary")).filter(col("avg_salary") > 70000).orderBy(col("avg_salary").desc())