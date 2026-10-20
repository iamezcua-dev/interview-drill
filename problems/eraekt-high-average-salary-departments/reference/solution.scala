import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Solution {
  def solution(employees: DataFrame): DataFrame =
    employees
      .groupBy("department")
      .agg(avg("salary").as("avg_salary"))
      .where(col("avg_salary") > 70000)
      .orderBy(desc("avg_salary"))
}
