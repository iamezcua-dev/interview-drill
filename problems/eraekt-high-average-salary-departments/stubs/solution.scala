// eraekt: Departments with an average salary above 70,000
//
// Tables:
//   employees(employee_id INT, department STRING, salary DOUBLE,
//     hire_date DATE)
// Output: department, avg_salary
// Full statement: drill show eraekt

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Solution {
  def solution(employees: DataFrame): DataFrame = ???
}
