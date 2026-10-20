// xf9ca6: Employees earning above their department average
//
// Tables:
//   employees(employee_id INT, name STRING, department STRING, salary
//     INT)
// Output: employee_id, name, department, salary
// Full statement: drill show xf9ca6

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Solution {
  def solution(employees: DataFrame): DataFrame = ???
}
