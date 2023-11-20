import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.Map
import java.io.FileWriter
import org.apache.commons.csv.CSVFormat

object Main extends App {
  Logger.getLogger("org").setLevel(Level.FATAL)

  val spark = SparkSession
    .builder()
    .appName("hw2")
    .master("spark://192.168.56.104:7077")
    // .master("local[4]") //run locally with 4 cores
    .getOrCreate()

  // ---------------------------------- Q1 ----------------------------------------- //
  val NewsDataDF = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("escape", "\"")
      // .option("multiLine","true")
      .csv("Data/News_Final.csv")

  countWord("title",  NewsDataDF).show()
  countWord("headline",  NewsDataDF).show()

  countWordBy("title", NewsDataDF, "publishdate").show()
  val TitleTopicDF = countWordBy("title", NewsDataDF, "topic")
  TitleTopicDF.select("word_title","count","topic").show()

  countWordBy("headline", NewsDataDF, "publishdate").show()
  val HeadlineTopicDF = countWordBy("headline", NewsDataDF, "topic")
  HeadlineTopicDF.select("word_headline","count","topic").show()
  // ---------------------------------- end Q1 ----------------------------------------- //

  // ----------------------------------Q2/hour----------------------------------------- //
  // filename: platfotrm_topic.csv
  val platforms = Seq("Facebook", "LinkedIn", "GooglePlus")
  val topics = Seq("Microsoft", "Obama", "Palestine")

  for (platform <- platforms) 
  {
    // build dataframe with topic "Economy"
    var platformDataFrame: DataFrame = spark.read.option("header", "true").csv(s"Data/${platform}_Economy.csv")

    for(topic <- topics)
    {
      val filepath = s"Data/${platform}_${topic}.csv"

      val topicDataFrame = spark.read.option("header", "true").csv(filepath)

      platformDataFrame = platformDataFrame.union(topicDataFrame)
    }

    val tsColumns = platformDataFrame.columns.filter(_.startsWith("TS"))

    val resultDF = tsColumns.foldLeft(platformDataFrame)((df, colName) => df.withColumn(colName, col(colName).cast("double")))
      .withColumn(s"sum_${platform}_result", tsColumns.map(col).reduce(_ + _))
      .withColumn(s"avg_${platform}_by_hour", col(s"sum_${platform}_result")/48)
      .select(s"sum_${platform}_result", s"avg_${platform}_by_hour")

    // resultDF.show()
    exportDataFrameAsCSV(resultDF, s"Q2_${platform}_by_hour.csv")
  }

  // ----------------------------------Q2/day----------------------------------------- //
  for (platform <- platforms) 
  {
    // build dataframe with topic "Economy"
    var platformDataFrame: DataFrame = spark.read.option("header", "true").csv(s"Data/${platform}_Economy.csv")

    for(topic <- topics)
    {
      val filepath = s"Data/${platform}_${topic}.csv"

      val topicDataFrame = spark.read.option("header", "true").csv(filepath)

      platformDataFrame = platformDataFrame.union(topicDataFrame)
    }

    val tsColumns = platformDataFrame.columns.filter(_.startsWith("TS"))

    val resultDF = tsColumns.foldLeft(platformDataFrame)((df, colName) => df.withColumn(colName, col(colName).cast("double")))
      .withColumn(s"sum_${platform}_result", tsColumns.map(col).reduce(_ + _))
      .withColumn(s"avg_${platform}_by_day", col(s"sum_${platform}_result")/2)
      .select(s"avg_${platform}_by_day")

    // resultDF.show()
    exportDataFrameAsCSV(resultDF, s"Q2_${platform}_by_day.csv")
  }
  // ----------------------------------end Q2----------------------------------------- //

  // ----------------------------------Q3----------------------------------------- //
  val df3a = NewsDataDF
    .groupBy("topic")
    .agg(sum("SentimentTitle").alias("total_title_score"))

  val df3b = NewsDataDF
    .groupBy("topic")
    .agg(sum("SentimentHeadline").alias("total_headline_score"))

  val df3c = NewsDataDF
    .groupBy("topic")
    .agg(avg("SentimentTitle").alias("avg_title_score"))

  val df3d = NewsDataDF
    .groupBy("topic")
    .agg(avg("SentimentHeadline").alias("avg_headline_score"))

  df3a.join(df3b, "topic").join(df3c, "topic").join(df3d, "topic").show()  
  // ----------------------------------end Q3----------------------------------------- //

  // ---------------------------------- Q4 ----------------------------------------- //
  // Extract the top-100 words for each topic
  val all_topics = Seq("economy", "microsoft", "obama", "palestine")
  all_topics.foreach( topic => {
    val topHeadlinesWords = HeadlineTopicDF.filter(col("topic")===topic).limit(100).select("word_headline").collect().map(_.getString(0))
    val topTopicWords = TitleTopicDF.filter(col("topic")===topic).limit(100).select("word_title").collect().map(_.getString(0))
    val matrix_headline = calculateCooccurrenceMatrix(topHeadlinesWords, "Headline")
    val matrix_title = calculateCooccurrenceMatrix(topTopicWords, "Title")
    export2DArrayToCsv(matrix_headline, topHeadlinesWords, s"${topic}_headline.csv")
    export2DArrayToCsv(matrix_title, topTopicWords, s"${topic}_title.csv")
  })

  // ----------------------------------end Q4----------------------------------------- //
  def calculateCooccurrenceMatrix(topWords: Array[String], content: String): Array[Array[Int]] =
  {
    // Initialize a 100x100 matrix with zeros
    val cooccurrenceMatrix = Array.ofDim[Int](100, 100)

    val TopWordMapIndex: Map[String, Int] = Map()
    for ((word, index) <- topWords.zipWithIndex) {
      TopWordMapIndex(word) = index
    }
    // println("Initialized Map:")
    // TopWordMapIndex.foreach { case (word, index) =>
    //   println(s"$word -> $index")
    // }

    val contents = NewsDataDF.filter(col("topic")==="economy").select(content).collect().map(_.getString(0))
    for(c<-0 until contents.length){
      // Check if "Headline" column is not null
      val contentOption: Option[String] = Option(contents(c))
      val words: Array[String] = contentOption.map(_.split(" ")).getOrElse(Array.empty)

      for(i<-0 until words.length){
        if(TopWordMapIndex.contains(words(i)))
        {
          for(j<-0 until words.length){
            if(TopWordMapIndex.contains(words(j)))
            {
              cooccurrenceMatrix(TopWordMapIndex(words(i)))(TopWordMapIndex(words(j))) += 1
            }
          }
        }
      }
    }

    cooccurrenceMatrix
  }
  
  def countWord(columnName: String, dataFrame: DataFrame): DataFrame =
  {
    // word count in total
    val newColumnName = "word_" + columnName

    // Create a new column named "words" in the DataFrame, 
    // and each row is an array of words splited by space
    dataFrame.withColumn("words", split(col(columnName), " ")) 
      // Explode the array into individual words 
      // use select to get the target column 
      // use explode to get a new row for each element in the array
      .select(explode(col("words")).as(newColumnName))
      .groupBy(newColumnName)
      .count()
      .orderBy(col("count").desc)
  }

  def countWordBy(columnName: String, dataFrame: DataFrame, byCondition: String): DataFrame =
  {
    val newColumnName = "word_" + columnName
    // word count per day  
    dataFrame.withColumn("words_array", split(col(columnName), " ")) 
      .withColumn(newColumnName, explode(col("words_array")))
      // .select(newColumnName, byCondition)
      .groupBy(newColumnName, byCondition)
      .count()
      .orderBy(col("count").desc, col(byCondition))
  }

  def exportDataFrameAsCSV( dataFrame: DataFrame, outputPath: String): DataFrame =
  {
    dataFrame.write
      .format("csv")
      .option("header", "true") // Include header row with column names
      .save(outputPath)
    
    dataFrame // return data frame to show in console
  }

  def export2DArrayToCsv(matrix: Array[Array[Int]], headers: Seq[String], filePath: String): Unit = {
    val writer = new FileWriter(filePath)
    val csvPrinter = CSVFormat.DEFAULT.withHeader((Seq("") ++ headers): _*).print(writer)

    try {
      for ((row, word) <- matrix.zip(headers)) {
        val values = row.map(_.toString)
        csvPrinter.printRecord((word +: values): _*)
      }
    } finally {
      writer.close()
      csvPrinter.close()
    }
  }
  
  spark.stop()
}
