import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.functions.regexp_replace

object Part1 {

  def main(args: Array[String]): Unit = {
    // logger settings
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkSession settings, running on 1 core with the name "Part1"
    val spark = SparkSession
      .builder
      .appName("Part1")
      .master("local")
      .getOrCreate()

    //using the header provided in the csv to replicate the schema used
    val UserReviews = spark.read
      .option("null", 0)
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("input/googleplaystore_user_reviews.csv")

    //temporary view named "reviews"
    UserReviews.createOrReplaceTempView("reviews")

    //grouping the lines by the 'App' column and calculating the average value of the 'Sentiment_Polarity' column
    val df_avg = spark.sql("SELECT App, AVG(Sentiment_Polarity) FROM reviews GROUP BY App")

    //change column names to match the requested
    val df_1 = df_avg.withColumnRenamed("avg(CAST(Sentiment_Polarity AS DOUBLE))", "Average_Sentiment_Polarity")

    //remove null values after calculation and set them to zero
    //val df_1_noNulls = df_1.withColumn("Average_Sentiment_Polarity", regexp_replace(col("Average_Sentiment_Polarity"), null, 0.0)

    //show dataframe
    df_1.show()

    //save as csv
    //df_1.write.option("header" -> true, "delimiter"->"§").format("com.databricks.spark.csv").save("output/df_1.csv")


    //stopping the spark session
    spark.stop()
  }


}