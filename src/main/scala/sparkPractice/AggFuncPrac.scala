package sparkPractice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._


object AggFuncPrac extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("Agg")
    .master("local[*]")
    .getOrCreate()

 import spark.implicits._

  val sampleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )

  val df = sampleData.toDF("employee_name","department","salary")

  //df.show()

  val count = df.select(approx_count_distinct("salary")).collect()(0)(0)
  println("Distinct_count_salary := " + count )

  val average = df.select(avg("salary")).collect()(0)(0)
  println(" Average : = " + average )

  df.select(collect_list("department")).show(false)
  df.select(collect_list("salary")).show(false)
  df.select(collect_list("employee_name")).show(false)
  df.select(countDistinct("department","salary")).collect()(0)(0)
  println("Distinct Count := "+ df.select(countDistinct("department")).collect()(0)(0))

  df.select(first("salary")).show(false)
  df.select(sum("salary")).show(false)






}
