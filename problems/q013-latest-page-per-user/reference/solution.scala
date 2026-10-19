import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Solution {
  def solution(page_visits: DataFrame): DataFrame = {
    val byUser = Window.partitionBy("user_id")
    page_visits
      .withColumn("latest", max("visit_date").over(byUser))
      .where(col("visit_date") === col("latest"))
      .select("user_id", "page", "visit_date")
  }
}
