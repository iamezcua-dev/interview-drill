# q007: Month-over-month revenue change
#
# Tables:
#   monthly_revenue(month INT, revenue INT)
# Output: month, revenue, prev_revenue, change
# Full statement: drill show q007

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def solution(monthly_revenue: DataFrame) -> DataFrame:
    raise NotImplementedError
