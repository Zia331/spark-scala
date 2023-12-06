import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.Map
import java.io.FileWriter
import java.io.PrintWriter
import org.apache.commons.csv.CSVFormat

import breeze.linalg.{DenseMatrix, DenseVector}
import scala.collection.mutable
import scala.util.Random

import scala.io.Source
import java.io.InputStream
import java.nio.charset.MalformedInputException

object Main extends App {
  Logger.getLogger("org").setLevel(Level.FATAL)

  val spark = SparkSession
    .builder()
    .appName("hw3")
    // .master("spark://192.168.56.104:7077")
    // .config("spark.executor.memory", "30g")  // Set executor memory
    // .config("spark.driver.memory", "16g")  // Set driver memory
    .master("local[8]") //run locally with 4 cores
    .getOrCreate()

  val baseName = "reut2-"
  val fileExtension = ".sgm"
  val numFiles = 3

  // Generate file paths and store in an array
  val filenames = Array.tabulate(numFiles)(i => s"$baseName${"%03d".format(i)}$fileExtension")

  val documents: List[String] = getBodyText(filenames)

  val k: Int = 2

  // Initialize a mutable map to store the index of each shingle
  val shingleIndexMap: Map[String, Int] = Map.empty

  // Initialize a mutable list to store the binary representation of each document
  val documentBinaryList: mutable.ListBuffer[DenseVector[Int]] = mutable.ListBuffer.empty

  // Initialize a set to store unique shingles across all documents
  val shingleSet: Set[String] = generateKShingles(documents.toArray, k)

  for ((shingle, index) <- shingleSet.zipWithIndex) {
    shingleIndexMap.put(shingle, index)
  }

  // Iterate over each document to generate shingles and construct the feature matrix
  for ((document, docIndex) <- documents.zipWithIndex) {
    val shingles = generateShingles(document, k)

    // Initialize a binary vector for the current document
    val binaryVector = DenseVector.zeros[Int](shingleSet.size)

    // Update binary vector based on shingle presence
    for (shingle <- shingles) {
      binaryVector(shingleIndexMap(shingle)) = 1
    }

    // Add the binary vector to the list
    documentBinaryList.append(binaryVector)
  }

  // Convert the list of binary vectors to a dense matrix
  val denseMatrixList: Seq[DenseMatrix[Int]] = documentBinaryList.map(_.toDenseMatrix)
  val featureMatrix: DenseMatrix[Int] = DenseMatrix.vertcat(denseMatrixList: _*).t // Transpose the matrix

  // Print the resulting feature matrix
  println(featureMatrix)
  val shingleArray = shingleSet.toArray
  // shingleArray.foreach(println)
  // println(shingleArray.size)
  exportDenseMatrixToCsv(featureMatrix, shingleArray, "output1.csv")
  
/*
  // val hashedValueList: mutable.ListBuffer[DenseVector[Int]] = mutable.ListBuffer.empty
  // val numberHashFunctions = 5
  // val hashFunctions = generateHashFunctions( numberHashFunctions, modulus)
  // val modulus = shingleArray.size-1
  // for( i <- 0 until featureMatrix.rows){
  //   val hashedVal = hashFunctions.map( hashfunc => hashfunc(i))
  //   val hashedVector = DenseVector.zeros[Int](numberHashFunctions)
  //   for ((value, index) <- hashedVal.zipWithIndex){
  //     hashedVector(index) = value
  //   }
  //   hashedValueList.append(hashedVector)
  // }

  // val hashedMatrixList: Seq[DenseMatrix[Int]] = hashedValueList.map(_.toDenseMatrix)
  // val hashedMatrix: DenseMatrix[Int] = DenseMatrix.vertcat(hashedMatrixList: _*).t
  // println(hashedMatrix)
*/

  val numberHashFunctions = 5
  val modulus = shingleArray.size-1
  val hashFunctions = generateHashFunctions( numberHashFunctions, modulus)
  val minHashedMatrix = DenseMatrix.fill[Int](numberHashFunctions,featureMatrix.cols)(Int.MaxValue)
  for( i <- 0 until featureMatrix.rows){
    val hashedVal = hashFunctions.map( hashfunc => hashfunc(i))
    for( j <- 0 until featureMatrix.cols){
      if(featureMatrix(i,j) == 1){
        for ((value, index) <- hashedVal.zipWithIndex){
          if(value < minHashedMatrix( index, j)){
            minHashedMatrix( index, j) = value
          }
        }
      }
    }
  }
  
  val hashArray: Array[Int] = Array.range(1, numberHashFunctions+1)
  val hashSeq: Seq[String] = hashArray.map( n => n.toString).toSeq
  // exportDenseMatrixToCsv(minHashedMatrix, hashSeq, "output2.csv")
  println(minHashedMatrix)
  println(s"Rows: ${minHashedMatrix.rows}, Cols: ${minHashedMatrix.cols}")

  val numRowsPerBand = 200
  val bands = minHashedMatrix.reshape(numRowsPerBand, minHashedMatrix.size/numRowsPerBand) //2d array: band*doc
  val bandHashes = (0 until bands.rows).map(rowIdx => hashBand(bands(rowIdx, ::).t))
  val bandGroups: Map[Int, Seq[Int]] =
    mutable.Map() ++ bandHashes.zipWithIndex.toIndexedSeq
    .groupBy { case (hash, docIndex) => hash }
    .mapValues(_.map(_._2).toSeq)

  // bandGroups.foreach { case (hash, docIndices) =>
  //   println(s"Hash: $hash, Document Id (${docIndices.size}): ${docIndices.map(_.toInt + 1).mkString(", ")}")
  // }
  val writer = new PrintWriter("output3.txt")
  try {
    for ((hash, docIndices) <- bandGroups) {
      val line = s"Hash: $hash, Document Id (${docIndices.size}): ${docIndices.map(_.toInt + 1).mkString(", ")}"
      writer.println(line)
    }
  } finally {
    writer.close()
  }

  def hashBand(band: DenseVector[Int]): Int = {
    val hashFunction = HashFunction(31,55,5)

    // Sum the hashed values
    val hashedSum = band.map { value =>
      hashFunction(value)
    }.sum

    hashedSum
  }

  case class HashFunction(a: Int, b: Int, modulus: Int) {
    def apply(value: Int): Int = (value * a + b) % modulus
  }

  def generateHashFunctions(numFunctions: Int, modulus: Int): Seq[HashFunction] = {
    val random = new Random()
    
    // Generate 'numFunctions' random hash functions
    (1 to numFunctions).map { _ =>
      val a = random.nextInt(20)
      val b = random.nextInt(20)
      HashFunction(a, b, modulus)
    }
  }

  def getBodyText(filenames: Seq[String]): List[String] = {
    var combinedData: List[String] = List.empty[String]
    combinedData = filenames.flatMap { filename =>
      val path = s"Data/${filename}"
      // val path = s"Data/reut2-000.sgm"
      try {
        val fileContent: String = Source.fromFile(path, "UTF-8").getLines().mkString("\n")

        val ReutersPattern = """(?s)<REUTERS[^>]*LEWISSPLIT="TRAIN"[^>]*>(.*?)<\/REUTERS>""".r
        val BodyPattern = """(?s)<BODY>(.*?)<\/BODY>""".r

        val reutersMatches = ReutersPattern.findAllMatchIn(fileContent).map { reutersMatch =>
          val reutersContent = reutersMatch.group(1)
          val bodyContent = BodyPattern.findFirstMatchIn(reutersContent).map(_.group(1)).getOrElse("No BODY found")
          bodyContent
        }.toList

        // println(s"Number of Matches: ${reutersMatches.size}")
        reutersMatches
      }catch{
        case ex: MalformedInputException =>
          // Handle the exception (e.g., print an error message)
          println(s"Error reading file: ${ex.getMessage}")
        List.empty[String]
      }
    }.toList
    combinedData
  }

  def generateKShingles(input: Array[String], k: Int): Set[String] = {
    input.flatMap(passage => passage.split("\\s+").sliding(k).map(_.mkString(" "))).toSet
  }

  def generateShingles(input: String, k: Int): Set[String] = {
    input.split("\\s+").sliding(k).map(_.mkString(" ")).toSet
  }

  def exportDenseMatrixToCsv(matrix: DenseMatrix[Int], index: Seq[String], filePath: String): Unit = {
    val writer = new FileWriter(filePath)
    val headers = (1 to matrix.cols).map(_.toString)
    val csvPrinter = CSVFormat.DEFAULT.withHeader((Seq("") ++ headers): _*).print(writer)

    try {
      val matrixArray = matrix.toArray.grouped(matrix.cols).toArray
      for ((row, word) <- matrixArray.zip(index)) {
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