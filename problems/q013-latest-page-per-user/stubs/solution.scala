// q013: Most recently visited page per user
//
// Tables:
//   page_visits(user_id INT, page STRING, visit_date DATE)
// Output: user_id, page, visit_date
// Full statement: drill show q013

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Solution {
  def solution(page_visits: DataFrame): DataFrame = ???
}
