import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Solution {
  def solution(orders: DataFrame): DataFrame = {
    val totals = orders
      .groupBy("customer_id")
      .agg(sum("amount").as("total_spent"))
    val byTotal = Window.orderBy(desc("total_spent"))
    totals
      .withColumn("rnk", dense_rank().over(byTotal))
      .where(col("rnk") <= 3)
      .orderBy(desc("total_spent"))
      .select("customer_id", "total_spent")
  }
}
