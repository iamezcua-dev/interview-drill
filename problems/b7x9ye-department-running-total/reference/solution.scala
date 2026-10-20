import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Solution {
  def solution(employee_sales: DataFrame): DataFrame = {
    val running = Window
      .partitionBy("department")
      .orderBy("sale_date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    employee_sales.select(
      col("department"), col("employee_id"), col("sale_date"),
      col("amount"), sum("amount").over(running).as("running_total")
    )
  }
}
