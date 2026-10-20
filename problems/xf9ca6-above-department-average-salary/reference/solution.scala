import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Solution {
  def solution(employees: DataFrame): DataFrame = {
    val byDept = Window.partitionBy("department")
    employees
      .withColumn("dept_avg", avg("salary").over(byDept))
      .where(col("salary") > col("dept_avg"))
      .select("employee_id", "name", "department", "salary")
  }
}
