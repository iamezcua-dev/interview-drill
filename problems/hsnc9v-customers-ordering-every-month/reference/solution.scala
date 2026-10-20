import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Solution {
  def solution(orders: DataFrame): DataFrame =
    orders
      .where(year(col("order_date")) === 2024)
      .groupBy("customer_id")
      .agg(countDistinct(month(col("order_date"))).as("months"))
      .where(col("months") === 12)
      .select("customer_id")
}
