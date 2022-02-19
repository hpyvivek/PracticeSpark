package sparkPractice


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object minTempSelf extends App{
  case class Temperature(stationID:String, date:Int, measure_type:String,temperature:Float)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("MinimumTemp")
    .master("local[*]")
    .getOrCreate()

  val tempSchema = new StructType()
    .add("stationId", StringType, true)
    .add("date",IntegerType, true)
    .add("measure_type",StringType, true)
    .add("temperature",FloatType, true)

  import spark.implicits._
  val ds = spark.read
    .schema(tempSchema)
    .csv("data/1800.csv")
    .as[Temperature]

  val minTemp = ds.filter($"measure_Type" === "TMIN")   //filter out everything except TMIN
  val stationTemp = minTemp.select("stationId", "temperature") //select only stationId and Temperature
  val minTempByStation = stationTemp.groupBy("stationId").min("temperature") //agg to find min temp of every ID
  val minTempByStationF = minTempByStation
    .withColumn("temperature", round($"min(temperature)" * 0.1f * (9.0f/5.0f)+ 32.0f,2))
    .select("stationID","temperature")
    .sort("temperature")

  val results = minTempByStationF.collect()

  for(result<- results){
    val station = result(0)
    val temp = result(1).asInstanceOf[Float]
    val formattedTemp = f"$temp%.2f F"
    println(s"$station min Temperature = $formattedTemp")

  }






}
