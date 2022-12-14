import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, regexp_replace, to_timestamp}
import org.apache.spark.sql.types.{DoubleType, LongType}
import scala.util.control.Exception


object Part4 {

  case class Review(
                     App: String,
                     Translated_Review: String,
                     Sentiment: String,
                     Sentiment_Polarity: Double,
                     Sentiment_Subjectivity: Double,
                   )


  case class Application(
                          App: String,
                          Category: String,
                          Rating: String,
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

    // SparkSession settings, running on 1 core with the name "Part3"
    val spark = SparkSession
      .builder
      .appName("Part4")
      .master("local")
      .getOrCreate()

    //to force type casting
    import org.apache.spark.sql.Encoders
    val appSchema = Encoders.product[Application].schema
    val reviewSchema = Encoders.product[Review].schema

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////

    val Reviews = spark.read
      .option("header", "true")
      .schema(reviewSchema)
      .csv("input/googleplaystore_user_reviews.csv")



    //removed all non-entries (entire rows with nan/null values)
    val df_removeNulls = Reviews.na.drop("all")
    //new dataframe grouped by 'App' with the average rating given in 'Sentiment_Polarity'
    val df_avg = df_removeNulls.groupBy("App").avg("Sentiment_Polarity")
    //renamed column to 'Average_Sentiment_Polarity'
    val df_1 = df_avg.withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")

    //create temporary view
    df_1.createOrReplaceTempView("reviews")

    //df_1.printSchema()
    //df_1.show()

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////

    //using the header provided in the csv to replicate the schema used
    val Applications = spark.read
      .option("header", "true")
      .schema(appSchema)
      .csv("input/googleplaystore.csv")

    //removing NaN values
    val df = Applications.na.drop()

    //removing the unit type nomenclature from the Size column
    //note: Size is set to zero when no value is given ('Varies with device' input is shown)
    import org.apache.spark.sql.functions.when
    import spark.implicits._
    val df_noLetters = df.withColumn("Size",
      when($"Size".endsWith("M"),regexp_replace($"Size","M",""))
        .when($"Size".endsWith("G"),regexp_replace($"Size","G",""))
        .when($"Size".endsWith("K"),regexp_replace($"Size","K",""))
        .otherwise("0"))


    //formatting the Date string for DateType casting
    val df_dateTransformation = df_noLetters.withColumn("LastUpdated",
      when($"LastUpdated".startsWith("January"),regexp_replace($"LastUpdated","January","01"))
        .when($"LastUpdated".startsWith("February"),regexp_replace($"LastUpdated","February","02"))
        .when($"LastUpdated".startsWith("March"),regexp_replace($"LastUpdated","March","03"))
        .when($"LastUpdated".startsWith("April"),regexp_replace($"LastUpdated","April","04"))
        .when($"LastUpdated".startsWith("May"),regexp_replace($"LastUpdated","May","05"))
        .when($"LastUpdated".startsWith("June"),regexp_replace($"LastUpdated","June","06"))
        .when($"LastUpdated".startsWith("July"),regexp_replace($"LastUpdated","July","07"))
        .when($"LastUpdated".startsWith("August"),regexp_replace($"LastUpdated","August","08"))
        .when($"LastUpdated".startsWith("September"),regexp_replace($"LastUpdated","September","09"))
        .when($"LastUpdated".startsWith("October"),regexp_replace($"LastUpdated","October","10"))
        .when($"LastUpdated".startsWith("November"),regexp_replace($"LastUpdated","November","11"))
        .when($"LastUpdated".startsWith("December"),regexp_replace($"LastUpdated","December","12"))
    )
    val df_dateTransformation2 = df_dateTransformation.withColumn("LastUpdated",
      when($"LastUpdated".contains(" "),regexp_replace($"LastUpdated", " ", "-"))
    )
    val df_dateTransformation3 = df_dateTransformation2.withColumn("LastUpdated",
      when($"LastUpdated".contains(","),regexp_replace($"LastUpdated", ",", ""))
    )

    //converting the LastUpdated(string) column to DateType
    val df_stringToDateType = df_dateTransformation3.withColumn("LastUpdated", to_timestamp(col("LastUpdated"), "MM-dd-yyyy"))

    //removing the $ sign out of the strings in the 'Price' column
    val df_priceTransformation = df_stringToDateType.withColumn("Price",
      when($"Price".contains("$"),
      expr("substring(Price, 2, length(Price))")))

    //casting string columns to their respective types
    import org.apache.spark.sql.functions._
    val df_typeChange = df_priceTransformation.withColumn("Size",col("Size").cast(DoubleType))
      .withColumn("Rating",col("Rating").cast(DoubleType))
      //.withColumn("Price",col("Price").cast(DoubleType))
      .withColumn("Reviews", col("Reviews").cast(LongType))


    //converting the Price column from USD => EUR with 0.9 rate
    //leaving Free applications with a 'null' entry on the Price column
    val df_dollarToEur = df_typeChange.withColumn("Price",
      when($"Price".isNotNull,
      round(col("Price") * lit(0.9),2)))


    //renaming columns
    val df_renamed = df_dollarToEur.withColumnRenamed("ContentRating", "Content_Rating")
      .withColumnRenamed("LastUpdated", "Last_Updated")
      .withColumnRenamed("CurrentVer", "Current_Version")
      .withColumnRenamed("AndroidVer", "Minimum_Android_Version")

    //changing Genres & Category columns to transform String to Array[String]
    val df_3 = df_renamed.withColumn("Genres", split(col("Genres"), ";").cast("array<String>"))
      .withColumn("Category", split(col("Category"), ";").cast("array<String>"))

    //temporary view named "applications"
    df_3.createOrReplaceTempView("applications")
    //df_3.show(false)
    //df_3.printSchema()


    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////

    //using sql native syntax to write join operation on temporary tables from df_1 & df_3
    val df_join = spark.sql("select * from applications left join reviews using(App)")

    val path = "output/googleplaystore_cleaned"
    //saving the dataframe to a parquet file, using Gzip compression
    try{
      df_join.coalesce(1).write
        //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .option("compression", "gzip")
        .mode("overwrite")
        .parquet(path)

      println("File saved successfully at: " + path)
    }catch{
      case e: Exception => println(Exception)
    }

    //terminate spark session
    spark.stop()
  }
}
