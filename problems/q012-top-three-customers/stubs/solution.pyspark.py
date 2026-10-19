# q012: Top 3 customers by total amount spent
#
# Tables:
#   orders(order_id INT, customer_id INT, order_date DATE, amount
#     DECIMAL(10,2))
# Output: customer_id, total_spent
# Full statement: drill show q012

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def solution(orders: DataFrame) -> DataFrame:
    raise NotImplementedError
