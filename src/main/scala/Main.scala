import org.apache.spark.sql.{SparkSession, Row, types => T}
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.linalg.DenseVector

import java.io.{BufferedWriter, FileWriter, File}

import T.DoubleType
import T.LongType
import T.StructType
import T.StringType

object Main extends App {
  Logger.getLogger("org").setLevel(Level.FATAL)

  val spark = SparkSession
    .builder()
    .appName("hw0")
    .master("local[2]") //run locally with 2 cores
    .getOrCreate()

  val dataPath = "household_power_consumption.txt" // Replace with your actual file path
  val rawData: DataFrame = spark.read
    .option("delimiter", ";")
    .option("header", "true")
    .csv(dataPath)

  val columnsToAnalyze = Seq("Global_active_power","Global_reactive_power","Voltage","Global_intensity")

  val selectedDF = rawData.select(columnsToAnalyze.map(col): _*)
  val processedDF = selectedDF.select(selectedDF.columns.map(col(_).cast(DoubleType)): _*).na.fill(0)

  val outputPath = "output.txt"

  val outputFile = new File(outputPath)
  if (outputFile.exists()) {
    outputFile.delete()
  }

  val fileWriter = new FileWriter(outputPath, true) // `true`: append mode
  val bufferedWriter = new BufferedWriter(fileWriter)

  val listBuffer1: Seq[String] = performMinMaxNormalization(processedDF, columnsToAnalyze(0))
  val listBuffer2: Seq[String] = performMinMaxNormalization(processedDF, columnsToAnalyze(1))
  val listBuffer3: Seq[String] = performMinMaxNormalization(processedDF, columnsToAnalyze(2))
  val listBuffer4: Seq[String] = performMinMaxNormalization(processedDF, columnsToAnalyze(3))

  // sub-task 1&2
  columnsToAnalyze.foreach(col => {
    processedDF.describe(col).show()
  })

  val combinedList = listBuffer1 ++ listBuffer2 ++ listBuffer3 ++ listBuffer4

  // Write output file
  val dataCount = processedDF.count().toInt
  val len = columnsToAnalyze.length-1
  val range = dataCount-1
  for(i <- 0 to range){
    for(j <- 0 to len){
      val index = i+(dataCount*j)
      bufferedWriter.write(combinedList(index))
      if(j<len)
        bufferedWriter.write(",")
      else
        bufferedWriter.write("\n")
    }
  }

  bufferedWriter.close()

  spark.stop()

  def performMinMaxNormalization (dataFrame: DataFrame, colName: String): Seq[String] = {
    val assembler = new VectorAssembler()
      .setInputCols(Array(colName))
      .setOutputCol("vector_"+colName)

    val assembledData = assembler.transform(dataFrame.select(colName))
    val scaledColumn : String = "scaled_" + colName

    val scaler = new MinMaxScaler()
      .setInputCol("vector_" + colName)
      .setOutputCol(scaledColumn)

    val scaledData = scaler.fit(assembledData).transform(assembledData) //.show() //adding this cause error on using "select()" 
    
    val scaledDataSeq = scaledData.select(scaledColumn).collect().map(_.toString)

    scaledDataSeq.map(value => value.substring(2, value.length - 2))
  }
}
