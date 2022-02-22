package sparkPractice
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object mostPopularMovie extends App{
  //case class PopularMovie(movie_Id:Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("MovieID")
    .master("local[*]")
    .getOrCreate()


  val movieSchema = new StructType()
    .add("user_id",IntegerType, true)
    .add("movie_ID",IntegerType,true)
    .add("rating",IntegerType,true)
    .add("timeStamp",LongType,true)

  import spark.implicits._
  val ds = spark.read
    .option("sep","\t")
    .schema(movieSchema)
    .csv("data/ml-100k/u.data")
    //.as[PopularMovie]

   val topMovie = ds.groupBy("movie_id").count().orderBy(desc("count")).show(30)

   // ds.show(50)

  //val topMoviesID = ds.groupBy("movie_Id")//.show()
  //topMoviesID.show()
  //val countMovie = topMoviesID.agg(count("user_Id")).show()
  //val orderMovie = countMovie.orderBy(("count")).show()
  //val orderMovieDesc = countMovie.orderBy(desc("count"))
  //orderMovieDesc.show()


  spark.stop()


}
