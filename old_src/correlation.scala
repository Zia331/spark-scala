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

  val correlationMatrix: Array[Row] = Correlation.corr(assembledData, "features").head(0)
  // val correlationMatrixRow: Row = correlationMatrix.getAs[Row](0)
  // val similarityMatrix: Vector = correlationMatrix(0)
  // println(similarityMatrix)
  println(correlationMatrix)

  // Example usage
  val topSimilarUsers = findTopSimilarUsers(1, 5)
  println(s"Top Similar Users for user_id 1:")
  topSimilarUsers.foreach(println)

  def findTopSimilarUsers(userID: Int, numSimilarUsers: Int): Array[(Int, Double)] = {
    val userIndex = indexedUserItemMatrix.select("user_id", "user_id_indexed")
      .distinct().filter($"user_id" === userID).first().getInt(1)

    val userSimilarities = correlationMatrix.map(row => 
      row.getDouble(userIndex.toInt)).zipWithIndex.filter(_._2 != userIndex)

    val topSimilarUsers = userSimilarities.sortBy(-_._1).take(numSimilarUsers)

    val indexedUsers = indexedUserItemMatrix.select("user_id", "user_id_indexed")
      .distinct().collect().map(row => (row.getInt(1), row.getInt(0))).toMap

    topSimilarUsers.map { case (similarity, index) =>
      (indexedUsers(index), similarity)
    }
  }