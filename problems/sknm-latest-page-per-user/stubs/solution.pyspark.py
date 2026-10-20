# sknm: Most recently visited page per user
#
# Tables:
#   page_visits(user_id INT, page STRING, visit_date DATE)
# Output: user_id, page, visit_date
# Full statement: drill show sknm

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def solution(page_visits: DataFrame) -> DataFrame:
    raise NotImplementedError
