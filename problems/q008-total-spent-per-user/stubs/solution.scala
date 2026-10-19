// q008: Total spent per user, highest first
//
// Tables:
//   transactions(user_id INT, product_id INT, quantity INT, price DOUBLE)
// Output: user_id, total_spent
// Full statement: drill show q008

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Solution {
  def solution(transactions: DataFrame): DataFrame = ???
}
