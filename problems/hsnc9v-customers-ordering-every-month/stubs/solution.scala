// hsnc9v: Customers with orders in every month of 2024
//
// Tables:
//   orders(order_id INT, customer_id INT, order_date DATE, amount
//     DECIMAL(10,2))
// Output: customer_id
// Full statement: drill show hsnc9v

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Solution {
  def solution(orders: DataFrame): DataFrame = ???
}
