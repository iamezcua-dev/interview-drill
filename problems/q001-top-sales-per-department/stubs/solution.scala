// q001: Top seller(s) per department
//
// Tables:
//   employee_sales(employee_id INT, department STRING, sale_date DATE,
//     amount DECIMAL(10,2))
// Output: department, employee_id, total_sales
// Full statement: drill show q001

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Solution {
  def solution(employee_sales: DataFrame): DataFrame = ???
}
