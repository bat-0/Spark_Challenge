import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.control.Exception

object Part2 {

  case class Application(
                          App: String,
                          Category: String,
                          Rating: Double,
                          Reviews: String,
                          Size: String,
                          Installs: String,
                          Type: String,
                          Price: String,
                          ContentRating: String,
                          Genres: String,
                          LastUpdated: String,
                          CurrentVer: String,
                          AndroidVer: String
                        )

  def main(args: Array[String]): Unit = {
    // logger settings
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkSession settings, running on 1 core with the name "Part2"
    val spark = SparkSession
      .builder
      .appName("Part2")
      .master("local")
      .getOrCreate()

    //to force cast string => double
    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[Application].schema

    //using the header provided in the csv to replicate the schema used
    val Applications = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("input/googleplaystore.csv")


    //filter applications by Ratings >= 4.0
    val df_4rating = Applications.filter(Applications("Rating")>=4.0)

    //drop all NaN values
    val df_filtered = df_4rating.na.drop()

    //sort the table by Ratings in Descending order
    import org.apache.spark.sql.functions._
    val df_2 = df_filtered.orderBy(desc("Rating"))

    //df_2.show()

    val path = "output/best_apps"
    //save as unique csv  ('coalesce' has better performance & uses less resources than 'repartition')
    //maintaining the header line and setting the delimiter to '§'
    try{
      df_2.coalesce(1).write
        .option("header", value = true)
        .option("delimiter","§")
        .mode("overwrite")
        .csv(path)

      println("File saved successfully at: " + path)
    }catch {
      case e: Exception => println(Exception)
    }

    //stop the spark session
    spark.stop()
  }
}
