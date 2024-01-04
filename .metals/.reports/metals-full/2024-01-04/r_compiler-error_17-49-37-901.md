file://<WORKSPACE>/src/main/scala/Main.scala
### java.lang.IndexOutOfBoundsException: 0

occurred in the presentation compiler.

action parameters:
offset: 3539
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
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

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
    .withColumn("rating", col("rating").cast("double"))
  val userDF = readDatFile("Data/users.dat",Seq("user_id","gender","age","occupation","zipcode"))

  // Q1
  ratingDF
    .groupBy("movie_id")
    .agg(avg("rating").alias("avg_rating"))
    .orderBy(col("avg_rating").desc)
    .show()

  // Q2
  ratingDF
    .join(userDF, "user_id")
    .groupBy("movie_id","gender")
    .agg(avg("rating").alias("avg_rating_gender"))
    .orderBy(col("avg_rating_gender").desc)
    .show()

  ratingDF
    .join(userDF, "user_id")
    .groupBy("movie_id","age")
    .agg(avg("rating").alias("avg_rating_age"))
    .orderBy(col("avg_rating_age").desc)
    .show()

  ratingDF
    .join(userDF, "user_id")
    .groupBy("movie_id","occupation")
    .agg(avg("rating").alias("avg_rating_occupation"))
    .orderBy(col("avg_rating_occupation").desc)
    .show()

  // Q3
  ratingDF
    .join(movieDF, "movie_id")
    .groupBy("user_id")
    .agg(avg("rating").alias("avg_rating_user"))
    .orderBy(col("avg_rating_user").desc)
    .show()

  ratingDF
    .join(movieDF, "movie_id")
    .groupBy("user_id","genre")
    .agg(avg("rating").alias("avg_rating_genre"))
    .orderBy(col("avg_rating_genre").desc)
    .show()

  // Q4
  val targetUser = 123
  val targetUserRatings = ratingDF.filter($"user_id" === targetUser).drop("timestamp")
    .withColumnRenamed("user_id","target_user_id")
    .withColumnRenamed("rating","target_user_rating")
  val otherUserRatings = ratingDF.filter($"user_id" !== targetUser).drop("timestamp")

  val joinedDF = otherUserRatings.join(targetUserRatings, "movie_id")
  // joinedDF.show()

  case class UserScore(id: String, score:Double)
  val result: Array[UserScore] =  userDF.rdd.map(row => {
    val user = row.getAs[String]("user_id")
    if(user != targetUser){
      val user_rdd = joinedDF
        .filter($"user_id" === user)
        .rdd
        .map(row2=>Vectors.dense(Array(row2.getAs[Double]("rating"),row2.getAs[Double]("target_user_rating"))))
      
      if (user_rdd.count() > 0) {
        val rowMatrix = new RowMatrix(user_rdd)
        val similarities = rowMatrix.columnSimilarities()
        UserScore(user, similarities.entries.take(1)(0).value)
      }else{
        UserScore("invalid", 0)
      }
    }else{
      UserScore("invalid", 0)
    }
  }).sortBy(_.score)(Ordering.Double.reverse)

  val sortedUserScores = result.collect().sortBy(_.score)(Ordering.Double.reverse,@@)
  sortedUserScores.take(20).foreach(println)

  // Q5
  val targetMovie = "10"
  val targetMovieRatings = ratingDF.filter($"movie_id" === targetMovie).drop("timestamp")
    .withColumnRenamed("rating","target_movie_rating")
    .withColumnRenamed("movie_id","target_movie_id")
  val otherMovieRatings = ratingDF.filter($"movie_id" !== targetMovie).drop("timestamp")
  val joinedMovieDF = otherMovieRatings.join(targetMovieRatings, "user_id")
  // joinedMovieDF.show()

  case class MovieScore(id: String, score:Double)
  val result2: Array[MovieScore] =  movieDF.rdd.map(row => {
    val movie = row.getAs[String]("movie_id")
    if(movie != targetMovie){
      val movie_rdd = joinedMovieDF
        .filter($"movie_id" === movie)
        .rdd
        .map(row2=>Vectors.dense(Array(row2.getAs[Double]("rating"),row2.getAs[Double]("target_movie_rating"))))
      
      if (movie_rdd.count() > 0) {
        val rowMatrix = new RowMatrix(movie_rdd)
        val similarities = rowMatrix.columnSimilarities()
        MovieScore(movie, similarities.entries.take(1)(0).value)
      }else{
        MovieScore("invalid", 0)
      }
    }else{
      MovieScore("invalid", 0)
    }
  })

  val sortedMovieScores = result2.collect().sortBy(_.score)(Ordering.Double.reverse, scala.reflect.ClassTag[Double])
  sortedMovieScores.take(20).foreach(println)

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
	dotty.tools.dotc.util.Signatures$.computeSignatureHelp(Signatures.scala:94)
	dotty.tools.dotc.util.Signatures$.signatureHelp(Signatures.scala:63)
	scala.meta.internal.pc.MetalsSignatures$.signatures(MetalsSignatures.scala:17)
	scala.meta.internal.pc.SignatureHelpProvider$.signatureHelp(SignatureHelpProvider.scala:51)
	scala.meta.internal.pc.ScalaPresentationCompiler.signatureHelp$$anonfun$1(ScalaPresentationCompiler.scala:388)
```
#### Short summary: 

java.lang.IndexOutOfBoundsException: 0