# Q8 [PySpark | Coding | Senior] — Given a DataFrame of transactions, return user_id and total_spent
# (sum of quantity * price per user), ordered by total_spent descending.
# Output columns: user_id, total_spent.

from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, col

def solution(df: DataFrame) -> DataFrame:
    return df.groupBy("user_id").agg(sum(df.quantity * df.price).alias("total_spent")).orderBy(col("total_spent").desc())