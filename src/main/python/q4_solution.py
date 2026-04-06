# Q4 [PySpark | Coding | Senior] — Return employees whose salary is strictly above their department average.
# Output columns: employee_id, name, department, salary.

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg

def solution(df: DataFrame) -> DataFrame:
    averages_df = (
        df.groupBy("department")
        .agg(
            avg("salary").alias("avg_dept_salary")
        )
    )
    result = df.join(averages_df, "department").filter(df.salary > averages_df.avg_dept_salary)
    return result.select(result.employee_id, result.name, result.department, result.salary)