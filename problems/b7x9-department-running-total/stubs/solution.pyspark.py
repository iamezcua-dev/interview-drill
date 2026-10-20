# b7x9: Running total of sales per department
#
# Tables:
#   employee_sales(employee_id INT, department STRING, sale_date DATE,
#     amount DECIMAL(10,2))
# Output: department, employee_id, sale_date, amount, running_total
# Full statement: drill show b7x9

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def solution(employee_sales: DataFrame) -> DataFrame:
    raise NotImplementedError
