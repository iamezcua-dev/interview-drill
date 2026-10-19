import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Solution {
  def solution(transactions: DataFrame): DataFrame =
    transactions
      .groupBy("user_id")
      .agg(sum(col("quantity") * col("price")).as("total_spent"))
      .orderBy(desc("total_spent"))
}
