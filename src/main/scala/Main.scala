import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import LoadUtil._
import TransformUtil.{explode_genre, topRated}

import java.util.TimeZone
import java.nio.file.Files
import java.nio.file.Paths

//import java.time.Instant

object Main {

  // Define the input and output formats
  val read_format = "csv"
  val write_format = "delta"

  val logger = LoggerFactory.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("MovieLens")
      .master("local[*]")
      .getOrCreate()
    try {
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

      var load_path = ""
      var save_path = ""
      if (args.length >= 2) {
        load_path = args(0)
        save_path = args(1)
      } else {
        logger.error("Load Path and Save Path are mandatory arguments please add them in order")
        System.exit(1)
      }

      val ratings_load_path = load_path + "/ratings.csv"
      val ratings_save_path = save_path + "/ratings"
      val top_movies_save_path = save_path + "/top_movies"

      val movies_load_path = load_path + "/movies.csv"
      val movies_save_path = save_path + "/movies"
      val trf_movies_save_path = save_path + "/trf_movies"


      val tags_load_path = load_path + "/tags.csv"
      val tags_save_path = save_path + "/tags"

      if (Files.notExists(Paths.get(save_path))){
        logger.error("Load path directory not found")
        System.exit(1)
      }
      if (Files.exists(Paths.get(ratings_load_path)))
        updateRating(ratings_load_path, ratings_save_path, spark)
      else
        logger.warn("Rating file not found in path. Skipping")
      if (Files.exists(Paths.get(movies_load_path)))
        updateMovies(movies_load_path, movies_save_path, spark)
      else
        logger.warn("Movies file not found in path. Skipping")
      if (Files.exists(Paths.get(tags_load_path)))
        addTags(tags_load_path, tags_save_path, spark)
      else
        logger.warn("Tags file not found in path. Skipping")


      val moviesDF = DeltaTable.forPath(spark, movies_save_path).toDF

      logger.info("Transforming Movies table")

      val transformedMovies = moviesDF.transform(explode_genre)
      transformedMovies.show(false)
      transformedMovies.write
        .format(write_format)
        .partitionBy("movieId")
        .mode("overwrite")
        .save(trf_movies_save_path)

      logger.info("Movies transformation completed")

      logger.info("Generating top movies file")

      val ratingDF = DeltaTable.forPath(spark, ratings_save_path).toDF

      topRated(ratingDF, moviesDF, top_movies_save_path)

      logger.info("Top 10 movies Generated at path " + top_movies_save_path)
    }
    catch{
      case ex: Exception => {
        logger.error(ex.getMessage)
        ex.printStackTrace()
      }
    }finally {
      spark.close()
    }
  }
}