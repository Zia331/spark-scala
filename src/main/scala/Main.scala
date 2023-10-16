import org.apache.spark.sql.{SparkSession, Row, types => T}
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

import T.DoubleType

object Main extends App {
  Logger.getLogger("org").setLevel(Level.FATAL)

  val spark = SparkSession
    .builder()
    .appName("hw0")
    .master("local[2]") //run locally with 2 cores
    .getOrCreate()

  // val data = List("Hello, world", "I'm running Spark!")
  // val msgDF = spark.createDataFrame(
  //   spark.sparkContext.makeRDD(data.map(x => Row(x))),
  //   schema = T.StructType(Array(T.StructField("msg", T.StringType)))
  // )
  // msgDF.foreach((row: Row) => println(row.getAs("msg")))

  val dataPath = "household_power_consumption.txt" // Replace with your actual file path
    val rawData: DataFrame = spark.read
      .option("delimiter", ";")
      .option("header", "true")
      .csv(dataPath)

      
    val castedDF = rawData
      .withColumn("Global_active_power", col("Global_active_power").cast(DoubleType))
      .withColumn("Global_reactive_power", col("Global_reactive_power").cast(DoubleType))
      .withColumn("Voltage", col("Voltage").cast(DoubleType))
      .withColumn("Global_intensity", col("Global_intensity").cast(DoubleType))
      .na.fill(0)


    val columnsToAnalyze = Seq("Global_active_power","Global_reactive_power","Voltage","Global_intensity")
    
    // Task 1: Output the minimum, maximum, and count of specific columns
    val minMaxCountData = calculateMinMaxCount(castedDF, columnsToAnalyze)
    minMaxCountData.show()

    // // Task 2: Output the mean and standard deviation of specific columns
    // val meanStdData = calculateMeanStdDev(castedDF, columnsToAnalyze)
    // meanStdData.show()

    // // Task 3: Perform min-max normalization on the specified columns
    // val normalizedData = performMinMaxNormalization(castedDF, columnsToAnalyze)
    // normalizedData.show()

    spark.stop()

  def calculateMinMaxCount(data: DataFrame, columns: Seq[String]): DataFrame = {
    val selectExprs = columns.flatMap { colName =>
      Seq(
        expr(s"min($colName) as min_$colName"),
        expr(s"max($colName) as max_$colName"),
        expr(s"sum($colName) as sum_$colName")
      )
    }

    data.select(selectExprs: _*)
  }

  // def calculateMeanStdDev(data: DataFrame, columns: Seq[String]): DataFrame = {
  //   // mean and standard deviation
    
  //   val summary: MultivariateStatisticalSummary = Statistics.colStats(data)
  // }

  // def performMinMaxNormalization(data: DataFrame, columns: Seq[String]): DataFrame = {
  //   // Implement logic for min-max normalization
  //   // Return a DataFrame with normalized output
  // }
}
