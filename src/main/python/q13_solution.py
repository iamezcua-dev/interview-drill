# Q13 [PySpark | Coding | Senior] — For each user, return the most recently visited page.
# If a user visited multiple pages on the same latest date, return all of them.
# Output columns: user_id, page, visit_date.

from pyspark.sql import DataFrame
from pyspark.sql.functions import max


def solution(df: DataFrame) -> DataFrame:
    latest_df = df.groupBy("user_id").agg(max("visit_date").alias("visit_date"))
    return df.join(latest_df, ["user_id","visit_date"]).select("user_id", "page", "visit_date")