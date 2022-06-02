import TransformUtil.{epoc_to_timestamp, explode_genre, split_column }
import Main.{read_format, write_format }
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

object LoadUtil {

  val logger = LoggerFactory.getLogger(LoadUtil.getClass)

  def updateRating(filePath: String, savePath: String, spark: SparkSession): Unit = {
    val customSchema = StructType(Array(
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("rating", DoubleType, true),
      StructField("timestamp", LongType, true)))

    val new_ratings = spark
      .read
      .format(read_format)
      .option("header", "true")
      .schema(customSchema)
      .load(filePath)
      .transform(epoc_to_timestamp)

    if (!DeltaTable.isDeltaTable(spark, savePath)) {
      logger.info("No Table found Creating new table at "+savePath)

      new_ratings.write
        .format(write_format)
        .partitionBy("movieId")
        .mode("overwrite")
        .save(savePath)

      logger.info("Rating table successfully created at path " + savePath )
    }else {
      logger.info("Table already exists at path " + savePath + ", merging to it")

      val ratingTable = DeltaTable.forPath(spark, savePath)

      ratingTable
        .as("ratings")
        .merge(
          new_ratings.as("updates"),
          "ratings.userId = updates.userId AND ratings.movieId = updates.movieId ")
        .whenMatched
        .updateExpr(
          Map(
            "userId" -> "updates.userId",
            "movieId" -> "updates.movieId",
            "rating" -> "updates.rating",
            "timestamp" -> "updates.timestamp"
          ))
        .whenNotMatched
        .insertExpr(
          Map(
            "userId" -> "updates.userId",
            "movieId" -> "updates.movieId",
            "rating" -> "updates.rating",
            "timestamp" -> "updates.timestamp"
          ))
        .execute()

      logger.info("Rating table successfully updated at path " + savePath )
    }
  }

  def updateMovies(filePath: String, savePath: String, spark: SparkSession): Unit = {
    val customSchema = StructType(Array(
      StructField("movieId", IntegerType, true),
      StructField("title", StringType, true),
      StructField("genres", StringType, true)))

    val new_movies = spark
      .read
      .format(read_format)
      .option("header", "true")
      .schema(customSchema)
      .load(filePath)
//      .transform(split_column)

    // Exception: Cannot perform Merge as multiple source rows matched and attempted to modify the same
    // update can not be done on movieId after splitting

    if (!DeltaTable.isDeltaTable(spark, savePath)) {
      logger.info("No Table found Creating new table at "+savePath)

      new_movies.write
        .format(write_format)
        .partitionBy("movieId")
        .mode("overwrite")
        .save(savePath)

      logger.info("Movies table successfully created at path " + savePath )
    }else {
      logger.info("Table already exists at path " + savePath + ", merging to it")

      val moviesTable = DeltaTable.forPath(spark, savePath)

      moviesTable
        .as("movies")
        .merge(
          new_movies.as("updates"),
          "movies.movieId = updates.movieId ")
        .whenMatched
        .updateExpr(
          Map(
            "movieId" -> "updates.movieId",
            "title" -> "updates.title",
            "genres" -> "updates.genres"
          ))
        .whenNotMatched
        .insertExpr(
          Map(
            "movieId" -> "updates.movieId",
            "title" -> "updates.title",
            "genres" -> "updates.genres"
          ))
        .execute()

      logger.info("Movies table successfully updated at path " + savePath )
    }
  }

  def addTags(filePath: String, savePath: String, spark: SparkSession): Unit = {
    val customSchema = StructType(Array(
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("tag", StringType, true),
      StructField("timestamp", LongType, true)))


    val new_tags = spark
      .read
      .format(read_format)
      .option("header", "true")
      .schema(customSchema)
      .load(filePath)
      .transform(epoc_to_timestamp)

    new_tags.write
      .format(write_format)
      .partitionBy("movieId")
      .mode("append")
      .save(savePath)

    logger.info("Tags table successfully updated at path " + savePath )
  }
}
