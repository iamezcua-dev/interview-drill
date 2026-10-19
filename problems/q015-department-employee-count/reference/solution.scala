import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Solution {
  def solution(dept: DataFrame, emp: DataFrame): DataFrame =
    dept
      .join(emp, Seq("department_id"), "left")
      .groupBy("department_id", "department_name")
      .agg(count("employee_id").as("employee_count"))
      .select("department_name", "employee_count")
}
