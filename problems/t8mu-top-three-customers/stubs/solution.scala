// t8mu: Top 3 customers by total amount spent
//
// Tables:
//   orders(order_id INT, customer_id INT, order_date DATE, amount
//     DECIMAL(10,2))
// Output: customer_id, total_spent
// Full statement: drill show t8mu

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Solution {
  def solution(orders: DataFrame): DataFrame = ???
}
