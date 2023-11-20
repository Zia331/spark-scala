import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Main extends App {
  Logger.getLogger("org").setLevel(Level.FATAL)

  val spark = SparkSession
    .builder()
    .appName("hw0")
    .master("spark://192.168.56.104:7077")
    // .master("local[4]") //run locally with 4 cores
    .getOrCreate()

  val dataPath = "spacenews-202309.csv"
  
  val rawData: DataFrame = spark.read
    .option("sep", ",")
    .option("quote","\"")
    .option("header", "true")
    .option("multiLine","true")
    .csv(dataPath)
    // .na.drop("any")

  val columnsToAnalyze = Seq("title","content","date", "author", "postexcerpt")

  val selectedDF = rawData.select(columnsToAnalyze.map(col): _*)

  // Q1
  val df1a = countWord("title",selectedDF)
  exportDataFrameAsCSV(df1a, "Q1_a").show()
  val df1b = countWordFrequency("title",selectedDF)
  exportDataFrameAsCSV(df1b, "Q1_b").show()

  // Q2
  val df2a = countWord("title",selectedDF)
  exportDataFrameAsCSV(df2a, "Q2_a").show()
  val df2b = countWordFrequency("title",selectedDF)
  exportDataFrameAsCSV(df2b, "Q2_b").show()

  // Q3-1
  val total_count_distinct_date = selectedDF
    .select(countDistinct("date")).first().getLong(0)

  val df3a = selectedDF
    .groupBy("date")
    .count()
    .withColumn("avg_articles_per_day", col("count") / lit(total_count_distinct_date))
    .drop("count")
    // lit(): to convert totalCount into a constant value so that it can be used in the division operation
  exportDataFrameAsCSV(df3a, "Q3_a").show()

  // Q3-2
  val groupedByDate = selectedDF
    .groupBy("date")
    .agg(count("*").alias("total_articles_per_day"))
    .select("date", "total_articles_per_day")

  val groupedByDateAuthor = selectedDF
    .groupBy("date", "author")
    .agg(count("*").alias("total_articles_each_author_per_day"))
    .select("date", "author", "total_articles_each_author_per_day")
    
  val df3b = groupedByDate
    .join(groupedByDateAuthor, "date")
    .withColumn("avg_articles_each_author_per_day", col("total_articles_each_author_per_day")/ col("total_articles_per_day"))
    .select("date", "author", "avg_articles_each_author_per_day")
  exportDataFrameAsCSV(df3b, "Q3_b").show()

  // Q4
  val wordsDF = selectedDF
    .withColumn("words_title", split(col("title"), " "))
    .withColumn("words_postexcerpt", split(col("postexcerpt"), " "))
    .select("words_title", "words_postexcerpt")

  val df4 = wordsDF.withColumn("words_postexcerpt", coalesce(col("words_postexcerpt"), array())) // fill na with empty array
    .filter(
      (array_contains(col("words_title"), "space") ||
      array_contains(col("words_title"), "Space")) &&
      (array_contains(col("words_postexcerpt"), "space") ||
      array_contains(col("words_postexcerpt"), "Space")))
    // concat back to sentence
    .withColumn("title", concat_ws(" ", col("words_title")))
    .withColumn("postexcerpt", concat_ws(" ", col("words_postexcerpt")))
    .drop("words_postexcerpt")
    .drop("words_title")
  exportDataFrameAsCSV(df4, "Q4").show()

  def countWord(columnName: String, dataFrame: DataFrame): DataFrame =
  {
    val newColumnName = "word_" + columnName

    // word count in total
    // Create a new column named "words" in the DataFrame, // and each row is an array of words splited by space
    dataFrame.withColumn("words", split(col(columnName), " ")) 
      // Explode the array into individual words // use select to get the target column // use explode to get a new row for each element in the array
      .select(explode(col("words")).as(newColumnName))
      .groupBy(newColumnName)
      .count()
      .orderBy(col("count").desc)
  }

  def countWordFrequency(columnName: String, dataFrame: DataFrame): DataFrame =
  {
    val newColumnName = "word_" + columnName
    // word count per day  
    dataFrame.withColumn("words_array", split(col(columnName), " ")) 
      .withColumn(newColumnName, explode(col("words_array")))
      // .orderBy(col("date"))
      .select(newColumnName, "date")
      .groupBy(newColumnName, "date")
      .count()
      .orderBy(col("count").desc, col("date"))
  }

  def exportDataFrameAsCSV( dataFrame: DataFrame, outputPath: String): DataFrame =
  {
    dataFrame.write
      .format("csv")
      .option("header", "true") // Include header row with column names
      .save(outputPath)
    
    dataFrame // return data frame to show in console
  }

  spark.stop()
}
