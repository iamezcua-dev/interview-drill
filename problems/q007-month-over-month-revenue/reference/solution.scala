import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Solution {
  def solution(monthly_revenue: DataFrame): DataFrame = {
    val byMonth = Window.orderBy("month")
    monthly_revenue
      .withColumn("prev_revenue", lag("revenue", 1).over(byMonth))
      .withColumn("change", col("revenue") - col("prev_revenue"))
      .select("month", "revenue", "prev_revenue", "change")
  }
}
