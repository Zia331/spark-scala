import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

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
    .master("local[8]") //run locally
    .getOrCreate()

  val baseName = "reut2-"
  val fileExtension = ".sgm"
  val numFiles = 1

  // Generate file paths and store in an array
  val filenames = Array.tabulate(numFiles)(i => s"$baseName${"%03d".format(i)}$fileExtension")

  val sc = spark.sparkContext
  val documentsRDD: RDD[String] = sc.parallelize(getBodyText(filenames))

  val k: Int = 1

  // Initialize a mutable map to store the index of each shingle
  val shingleIndexMap: Map[String, Int] = Map.empty

  // Initialize a mutable list to store the binary representation of each document
  // val documentBinaryList: mutable.ListBuffer[DenseVector[Int]] = mutable.ListBuffer.empty

  // Initialize a set to store unique shingles across all documents
  val shingleSet: Set[String] = generateKShingles(documentsRDD.collect(), k)
  println("shingleSet size:", shingleSet.size)
  shingleSet.zipWithIndex.map{ case(shingle, index) => 
    shingleIndexMap.put(shingle, index)
  }
  println("shingleIndexMap is out!")

  // Iterate over each document to generate shingles and construct the feature matrix
  val binaryVectorRDD: RDD[DenseVector[Int]] = documentsRDD.map { document =>
    val shingles = generateShingles(document, k)

    // Initialize a binary vector for the current document
    val binaryVector = DenseVector.zeros[Int](shingleSet.size)

    // Update binary vector based on shingle presence
    for (shingle <- shingles) {
      binaryVector(shingleIndexMap(shingle)) = 1
    }

    binaryVector
  }

  val denseMatrixRDD: RDD[DenseMatrix[Int]] = binaryVectorRDD.map(_.toDenseMatrix)
  println("denseMatrix is out!")

  // Transpose the matrices to have rows represent shingles and columns represent documents
  // val denseMatrixRDD: RDD[DenseMatrix[Int]] = denseMatrixRDD.map(_.t)

  // Concatenate the matrices
  // val featureMatrix: DenseMatrix[Int] = DenseMatrix.vertcat(denseMatrixRDD.collect(): _*).t
  // println("featureMatrix is out!")
  // Print the resulting feature matrix
  // println(featureMatrix)
  
  // exportDenseMatrixToCsv(featureMatrix, shingleArray, "output1.csv")
  // val featureMatrixRDD = sc.parallelize(featureMatrix.toArray.grouped(featureMatrix.cols).toArray.zip(shingleSet.toSeq))

  // Use the exportDenseMatrixToCsv function
  // exportDenseMatrixToCsv(featureMatrixRDD, "output1.csv")
  // Combine the matrices into a single RDD
  val combinedMatrixRDD: RDD[String] = denseMatrixRDD.flatMap { matrix =>
    matrix.toArray.map(_.toString)
  }

  // Zip the combinedMatrixRDD with shingleSet
  val numDocuments = denseMatrixRDD.count().toInt
  println("NumDoc", numDocuments)
  // val outputRDD: RDD[String] = combinedMatrixRDD.zipWithIndex.map { case (value, index) =>
  //   val col = index % numDocuments
  //   val row = index / numDocuments
  //   // println(index, row, col, value)
  //   s"${shingleSet.toArray.apply(row.toInt)},$col,$value"
  // }
  val outputRDD: RDD[String] = combinedMatrixRDD
    .zipWithIndex
    .groupBy { case (_, index) => index % numDocuments }
    .map { case (_, values) =>
      val firstElement = shingleSet.toArray.apply((values.head._2 / numDocuments).toInt)
      val columnValues = values.map { case (_, value) => value }.mkString(", ")
      s"$firstElement, $columnValues"
  }
  outputRDD.foreach(println)

  // Save the outputRDD to a CSV file
  // outputRDD.saveAsTextFile("output1")

  val numberHashFunctions = 5
  val modulus = shingleSet.size-1
  val hashFunctions = generateHashFunctions( numberHashFunctions, modulus)
  
  // val minHashedMatrix = DenseMatrix.fill[Int](numberHashFunctions,featureMatrix.cols)(Int.MaxValue)
  
  // for( i <- 0 until featureMatrix.rows){
  //   val hashedVal = hashFunctions.map( hashfunc => hashfunc(i))
  //   for( j <- 0 until featureMatrix.cols){
  //     if(featureMatrix(i,j) == 1){
  //       for ((value, index) <- hashedVal.zipWithIndex){
  //         if(value < minHashedMatrix( index, j)){
  //           minHashedMatrix( index, j) = value
  //         }
  //       }
  //     }
  //   }
  // }

  // Initialize the minHashedMatrix RDD with Int.MaxValue
  var minHashedMatrixRDD = sc.parallelize(Seq.fill(numberHashFunctions, denseMatrixRDD.count().toInt)(Int.MaxValue))

  // Broadcast hashFunctions to all worker nodes
  val broadcastHashFunctions = sc.broadcast(hashFunctions)
  println("broadcastHashFunctions")
  // Perform the computation using RDD transformations
  denseMatrixRDD.zipWithIndex.foreach { case (matrix, i) =>
    val hashedVal = broadcastHashFunctions.value.map(hashfunc => hashfunc(i.toInt))
    println(i)
    // Update minHashedMatrixRDD based on the condition
    minHashedMatrixRDD = minHashedMatrixRDD.zipWithIndex.mapPartitions { iter =>
      iter.map { case (row, j) =>
        val newRow = if (matrix(0, j.toInt) == 1) {
          hashedVal.zip(row).map { case (value, currentMin) =>
            math.min(value, currentMin)
          }
        } else {
          row
        }
        newRow
      }
    }
  }

  // Save the minHashedMatrixRDD to a CSV file
  val minHashedOutputRDD: RDD[String] = minHashedMatrixRDD.zipWithIndex.map { case (row, j) =>
    val col = j.toInt
    val values = row.map(_.toString).mkString(",")
    s"$col,$values"
  }
  println("Minhashed")

  minHashedOutputRDD.saveAsTextFile("output2")
  
  // val hashArray: Array[Int] = Array.range(1, numberHashFunctions+1)
  // val hashSeq: Seq[String] = hashArray.map( n => n.toString).toSeq
  // exportDenseMatrixToCsv(minHashedMatrix, hashSeq, "output2.csv")
  // println(minHashedMatrix)
  // println(s"Rows: ${minHashedMatrix.rows}, Cols: ${minHashedMatrix.cols}")

  val numRowsPerBand = 200
  val numCols = minHashedMatrixRDD.count().toInt
  val bands = minHashedMatrixRDD
    .zipWithIndex()
    .map { case (row, idx) =>
      (idx / numRowsPerBand, (idx % numRowsPerBand, row))
    }
    .groupByKey()
    .sortByKey()
    .map { case (_, indexedRows) =>
      indexedRows.toSeq.sortBy(_._1).map(_._2)
    }

  // val bands = minHashedMatrixRDD.reshape(numRowsPerBand, numCols/numRowsPerBand) //2d array: band*doc
  // val bandHashes = (0 until bands.rows).map(rowIdx => hashBand(bands(rowIdx, ::).t))
  // val bandGroups: Map[Int, Seq[Int]] =
  //   mutable.Map() ++ bandHashes.zipWithIndex.toIndexedSeq
  //   .groupBy { case (hash, docIndex) => hash }
  //   .mapValues(_.map(_._2).toSeq)

  // // bandGroups.foreach { case (hash, docIndices) =>
  // //   println(s"Hash: $hash, Document Id (${docIndices.size}): ${docIndices.map(_.toInt + 1).mkString(", ")}")
  // // }

  // Use the hashBand function on each band to get bandHashes
  // val bandHashes = bands.mapPartitions { bandIterator =>
  //   bandIterator.map { band =>
  //     val transposedBand = band.transpose
  //     hashBand(transposedBand)
  //   }
  // }.collect()
  val bandHashes = bands.mapPartitions { bandIterator =>
    bandIterator.map(band => hashBand(DenseVector(band.flatten.toArray)))
  }.collect()

  // Create bandGroups using bandHashes and document indices
  val bandGroups: Map[Int, Seq[Int]] = mutable.Map() ++
    bandHashes.zipWithIndex.toIndexedSeq
      .groupBy { case (hash, docIndex) => hash }
      .mapValues(_.map(_._2).toSeq)

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

        // val ReutersPattern = """(?s)<REUTERS[^>]*LEWISSPLIT="TRAIN"[^>]*>(.*?)<\/REUTERS>""".r
        val ReutersPattern = """(?s)<REUTERS[^>]*>(.*?)<\/REUTERS>""".r
        val BodyPattern = """(?s)<BODY>(.*?)<\/BODY>""".r

        val reutersMatches = ReutersPattern.findAllMatchIn(fileContent).map { reutersMatch =>
          val reutersContent = reutersMatch.group(1)
          val bodyContent = BodyPattern.findFirstMatchIn(reutersContent).map(_.group(1)).getOrElse("No BODY found")
          bodyContent
        }.toList

        // val reutersMatches = BodyPattern.findAllMatchIn(fileContent).map(_.group(1)).toList

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

  // def exportDenseMatrixToCsv(matrix: DenseMatrix[Int], index: Seq[String], filePath: String): Unit = {
  //   val writer = new FileWriter(filePath)
  //   val headers = (1 to matrix.cols).map(_.toString)
  //   val csvPrinter = CSVFormat.DEFAULT.withHeader((Seq("") ++ headers): _*).print(writer)

  //   try {
  //     val matrixArray = matrix.toArray.grouped(matrix.cols).toArray
  //     for ((row, word) <- matrixArray.zip(index)) {
  //       val values = row.map(_.toString)
  //       csvPrinter.printRecord((word +: values): _*)
  //     }
  //   } finally {
  //     writer.close()
  //     csvPrinter.close()
  //   }
  // }
  // def exportDenseMatrixToCsv(matrixRDD: RDD[(Array[Int], String)], filePath: String): Unit = {
  //   matrixRDD.foreachPartition { partition =>
  //     println("Writing", filePath)
  //     val writer = new FileWriter(filePath, true)  // Use "true" to append to the file
  //     val csvPrinter = CSVFormat.DEFAULT.print(writer)

  //     try {
  //       for ((row, word) <- partition) {
  //         val values = row.map(_.toString)
  //         csvPrinter.printRecord((word +: values): _*)
  //       }
  //     } finally {
  //       writer.close()
  //       csvPrinter.close()
  //     }
  //   }
  // }
  spark.stop()
}