# q010: Departments with an average salary above 70,000
#
# Tables:
#   employees(employee_id INT, department STRING, salary DOUBLE, hire_date
#     DATE)
# Output: department, avg_salary
# Full statement: drill show q010

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def solution(employees: DataFrame) -> DataFrame:
    raise NotImplementedError
