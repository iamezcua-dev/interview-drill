# q008: Total spent per user, highest first
#
# Tables:
#   transactions(user_id INT, product_id INT, quantity INT, price DOUBLE)
# Output: user_id, total_spent
# Full statement: drill show q008

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def solution(transactions: DataFrame) -> DataFrame:
    raise NotImplementedError
