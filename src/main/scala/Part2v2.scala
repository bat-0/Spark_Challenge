import com.carrotsearch.hppc.Intrinsics.cast
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.isnan

import java.sql.Date

object Part2v2 {

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

    //to cast string => double
    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[Application].schema

    //using the header provided in the csv to replicate the schema used
    val Applications = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("input/googleplaystore.csv")

    //temporary view named "applications"
    Applications.createOrReplaceTempView("applications")
    Applications.printSchema()

    //filter applications by Ratings >= 4.0
    val df_4rating = Applications.filter(Applications("Rating")>=4.0)

    //drop all NaN values
    val df_filtered = df_4rating.na.drop()

    //sort the table by Ratings in Descending order
    import org.apache.spark.sql.functions._
    val df_2 = df_filtered.orderBy(desc("Rating"))

    df_2.show()

    spark.stop()
  }
}
