import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Solution {
  def solution(employee_sales: DataFrame): DataFrame = {
    val totals = employee_sales
      .groupBy("department", "employee_id")
      .agg(sum("amount").as("total_sales"))
    val byDept = Window.partitionBy("department").orderBy(desc("total_sales"))
    totals
      .withColumn("rnk", rank().over(byDept))
      .where(col("rnk") === 1)
      .select("department", "employee_id", "total_sales")
  }
}
