# xf9ca6: Employees earning above their department average
#
# Tables:
#   employees(employee_id INT, name STRING, department STRING, salary INT)
# Output: employee_id, name, department, salary
# Full statement: drill show xf9ca6

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def solution(employees: DataFrame) -> DataFrame:
    raise NotImplementedError
