file://<WORKSPACE>/src/main/scala/Main.scala
### java.lang.IndexOutOfBoundsException: 0

occurred in the presentation compiler.

action parameters:
offset: 3078
uri: file://<WORKSPACE>/src/main/scala/Main.scala
text:
```scala
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import java.io.FileWriter
import java.io.PrintWriter
import org.apache.commons.csv.CSVFormat

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors, DenseMatrix, DenseVector}
import org.apache.spark.ml.stat.Correlation

object Main extends App {
  Logger.getLogger("org").setLevel(Level.FATAL)

  val spark = SparkSession
    .builder()
    .appName("hw4")
    // .master("spark://192.168.56.104:7077")
    // .config("spark.executor.memory", "30g")  // Set executor memory
    // .config("spark.driver.memory", "16g")  // Set driver memory
    .master("local[8]") //run locally
    .getOrCreate()

import spark.implicits._

  val movieDF = readDatFile("Data/movies.dat", Seq("movie_id", "title", "genre"))
  val ratingDF = readDatFile("Data/ratings.dat",Seq("user_id","movie_id","rating","timestamp"))
  val userDF = readDatFile("Data/users.dat",Seq("user_id","gender","age","occupation","zipcode"))

  // ratingDF
  //   .groupBy("movie_id")
  //   .agg(avg("rating").alias("avg_rating"))
  //   .orderBy(col("avg_rating").desc)
  //   .show()

  // ratingDF
  //   .join(userDF, "user_id")
  //   .groupBy("movie_id","gender")
  //   .agg(avg("rating").alias("avg_rating_gender"))
  //   .orderBy(col("avg_rating_gender").desc)
  //   .show()

  // ratingDF
  //   .join(userDF, "user_id")
  //   .groupBy("movie_id","age")
  //   .agg(avg("rating").alias("avg_rating_age"))
  //   .orderBy(col("avg_rating_age").desc)
  //   .show()

  // ratingDF
  //   .join(userDF, "user_id")
  //   .groupBy("movie_id","occupation")
  //   .agg(avg("rating").alias("avg_rating_occupation"))
  //   .orderBy(col("avg_rating_occupation").desc)
  //   .show()

  // ratingDF
  //   .join(movieDF, "movie_id")
  //   .groupBy("user_id")
  //   .agg(avg("rating").alias("avg_rating_user"))
  //   .orderBy(col("avg_rating_user").desc)
  //   .show()

  // ratingDF
  //   .join(movieDF, "movie_id")
  //   .groupBy("user_id","genre")
  //   .agg(avg("rating").alias("avg_rating_genre"))
  //   .orderBy(col("avg_rating_genre").desc)
  //   .show()

  val userItemMatrix = ratingDF
    .groupBy("user_id", "movie_id")
    .agg(avg("rating").alias("rating"))
    .withColumn("movie_id", col("movie_id").cast(IntegerType))

  // userItemMatrix.show()

  val assembler = new VectorAssembler()
    .setInputCols(Array("movie_id", "rating"))
    .setOutputCol("features")

  val indexedUserItemMatrix = new StringIndexer()
    .setInputCol("user_id")
    .setOutputCol("user_id_indexed")
    .fit(userItemMatrix)
    .transform(userItemMatrix)

  val assembledData = assembler
    .transform(indexedUserItemMatrix)
    .select("user_id_indexed", "features")

  // assembledData.show()

  val correlationMatrix: Vector = Correlation.corr(assembledData, "features").first()[@@1)
  // val correlationMatrixRow: Row = correlationMatrix.getAs[Row](0)
  // val similarityMatrix: Vector = correlationMatrixRow.getAs[Vector](0)
  // val similarityMatrix: Array = correlationMatrix.toArray
  // println(similarityMatrix)
  println(correlationMatrix)

  // Example usage
  // val topSimilarUsers = findTopSimilarUsers(1, correlationMatrix, 5)
  // println(s"Top Similar Users for user_id 1:")
  // topSimilarUsers.foreach(println)

  // def findTopSimilarUsers(userID: Int, numSimilarUsers: Int): Array[(Int, Double)] = {
  //   val userIndex = indexedUserItemMatrix.select("user_id", "user_id_indexed")
  //     .distinct().filter($"user_id" === userID).first().getInt(1)
  //   val userSimilarities = correlationMatrix.toArray.zipWithIndex.filter(_._2 != userIndex)
  //   val topSimilarUsers = userSimilarities.sortBy(-_._1).take(numSimilarUsers)
  //   val indexedUsers = indexedUserItemMatrix.select("user_id", "user_id_indexed")
  //     .distinct().collect().map(row => (row.getInt(1), row.getInt(0))).toMap

  //   topSimilarUsers.map { case (similarity, index) =>
  //     (indexedUsers(index), similarity)
  //   }
  // }

  def readDatFile(path: String, columnNames: Seq[String]): DataFrame = {
    val textData = spark.sparkContext.textFile(path)
    
    val rowsRDD = textData.map(line => {
      val columns = line.split("::")
      Row(columns: _*)
    })

    val schema = StructType(columnNames.map(colName => StructField(colName, StringType, true)))

    spark.createDataFrame(rowsRDD, schema)
  }
  
  spark.stop()
}
```



#### Error stacktrace:

```
scala.collection.LinearSeqOps.apply(LinearSeq.scala:131)
	scala.collection.LinearSeqOps.apply$(LinearSeq.scala:128)
	scala.collection.immutable.List.apply(List.scala:79)
	dotty.tools.dotc.util.Signatures$.countParams(Signatures.scala:501)
	dotty.tools.dotc.util.Signatures$.applyCallInfo(Signatures.scala:186)
	dotty.tools.dotc.util.Signatures$.computeSignatureHelp(Signatures.scala:97)
	dotty.tools.dotc.util.Signatures$.signatureHelp(Signatures.scala:63)
	scala.meta.internal.pc.MetalsSignatures$.signatures(MetalsSignatures.scala:17)
	scala.meta.internal.pc.SignatureHelpProvider$.signatureHelp(SignatureHelpProvider.scala:51)
	scala.meta.internal.pc.ScalaPresentationCompiler.signatureHelp$$anonfun$1(ScalaPresentationCompiler.scala:388)
```
#### Short summary: 

java.lang.IndexOutOfBoundsException: 0