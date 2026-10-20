# 7xm8: Top seller(s) per department
#
# Tables:
#   employee_sales(employee_id INT, department STRING, sale_date DATE,
#     amount DECIMAL(10,2))
# Output: department, employee_id, total_sales
# Full statement: drill show 7xm8

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def solution(employee_sales: DataFrame) -> DataFrame:
    raise NotImplementedError
