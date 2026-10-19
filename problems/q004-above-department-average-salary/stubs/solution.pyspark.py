# q004: Employees earning above their department average
#
# Tables:
#   employees(employee_id INT, name STRING, department STRING, salary INT)
# Output: employee_id, name, department, salary
# Full statement: drill show q004

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def solution(employees: DataFrame) -> DataFrame:
    raise NotImplementedError
