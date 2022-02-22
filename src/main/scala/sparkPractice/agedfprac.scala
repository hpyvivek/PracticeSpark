package sparkPractice
// to find age grp people from csv file
import org.apache.log4j._
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object agedfprac extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("AgeFilter")
    .master("local[*]")
    .getOrCreate()

  val df = spark. read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/fakefriends.csv")

  df.show(30)

  /* val friendsByAge = df.select("age", "friends")
  friendsByAge.show(30)
  friendsByAge.groupBy("age").avg("friends").show()
  friendsByAge.groupBy("age").avg("friends").sort("age").show()
  friendsByAge.groupBy("age").agg(round(avg("friends"), 2))
    .sort("age").show()
   */
  val friendsByAge:DataFrame = df.select("age", "friends")
  val abc =  friendsByAge.show(30)
  friendsByAge.groupBy("age").avg("friends").show(30)
  friendsByAge.groupBy("age").agg(round(avg("friends"),2)).sort("age").show(30)
  friendsByAge.groupBy("age").agg(round(avg("friends"),2)).alias("friends_avg").sort("age").show(30)

























  spark.stop()

}
