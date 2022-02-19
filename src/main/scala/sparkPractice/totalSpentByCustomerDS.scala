package sparkPractice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object totalSpentByCustomerDS extends App{
  case class CustomerOrder(cust_id:Int, item_id:Int, amount_spent:Double)
  Logger.getLogger("org").setLevel(Level.ERROR)

  /*val spark = SparkSession
    .builder()
    .appName("TotalSpentByCustomer")
    .master("Local[*]")
    .getOrCreate()

   */
  val spark = SparkSession
    .builder
    .appName("TotalSpentByCustomer")
    .master("local[*]")
    //.config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  val customerOrderSchema = new StructType()
    .add("cust_id",IntegerType,true)
    .add("item_id",IntegerType, true)
    .add("amount_spent",DoubleType, true)

  import spark.implicits._
  val ds = spark.read
    .schema(customerOrderSchema)
    .csv("data/customer-orders.csv")
    .as[CustomerOrder]

  val totalByCustomer = ds
    .groupBy("cust_id")
    .agg(round(sum("amount_spent"),2)
      .alias("total_spent"))

 totalByCustomer.show(totalByCustomer.count.toInt)
//spark.stop()




}
