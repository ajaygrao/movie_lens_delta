import LoadUtil.{addTags, updateMovies, updateRating}
import TransformUtil.{epoc_to_timestamp, explode_genre, topRated}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.reflect.io.Directory
import java.io.File
import java.util.TimeZone

class MainTest extends FunSuite {

  val spark: SparkSession = SparkSession.builder
    .appName("MovieLens")
    .master("local[2]")
    .getOrCreate()

  val testDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/test/resources/test_df.csv")

  val ratings_load_path = "src/test/resources/ratings_sample.csv"
  val ratings_update_load_path = "src/test/resources/ratings_sample2.csv"
  val ratings_table_name = "default.ratings"
  val top_movies_save_path = "default.top_movies"

  val movies_load_path = "src/test/resources/movies_sample.csv"
  val movies_update_load_path = "src/test/resources/movies_sample2.csv"
  val movies_table_name = "default.movies"
  val trf_movies_save_path = "default.trf_movies"


  val tags_load_path = "src/test/resources/tags_sample.csv"
  val tags_update_load_path = "src/test/resources/tags_sample2.csv"
  val tags_table_name = "default.tags"

  test("Load Ratings") {
    val directory = new Directory(new File(ratings_table_name))
    directory.deleteRecursively()
    updateRating(ratings_load_path, ratings_table_name, spark)
    val ratingDF = DeltaTable.forPath(spark, ratings_table_name).toDF.count()
    assert(ratingDF == 15)
  }

  test("Update Ratings") {
    updateRating(ratings_update_load_path, ratings_table_name, spark)
    val ratingUpdateDF = DeltaTable.forPath(spark, ratings_table_name).toDF
    ratingUpdateDF.createOrReplaceTempView("ratings")
    val rate = spark.sql("select rating from ratings where movieId=151 and userId=4").first()
    assert(ratingUpdateDF.count() == 16)
    assert(rate.getDouble(0) == 1.0)
  }

  test("Load Tags") {
    val directory = new Directory(new File(tags_table_name))
    directory.deleteRecursively()
    addTags(tags_load_path, tags_table_name, spark)
    val tagsDF = DeltaTable.forPath(spark, tags_table_name).toDF.count()
    assert(tagsDF == 9)

  }

  test("Update Tags") {
    addTags(tags_update_load_path, tags_table_name, spark)
    val tagsUpdateDF = DeltaTable.forPath(spark, tags_table_name).toDF
    assert(tagsUpdateDF.count() == 13)
  }

  test("Load Movies") {
    val directory = new Directory(new File(movies_table_name))
    directory.deleteRecursively()
    updateMovies(movies_load_path, movies_table_name, spark)
    val moviesDF = DeltaTable.forPath(spark, movies_table_name).toDF.count()
    assert(moviesDF == 11)

  }

  test("Update Movies") {
    updateMovies(movies_update_load_path, movies_table_name, spark)
    val moviesUpdateDF = DeltaTable.forPath(spark, movies_table_name).toDF
    assert(moviesUpdateDF.count() == 12)
    moviesUpdateDF.createOrReplaceTempView("movies")
    val movie = spark.sql("select title from movies where movieId=151").first()
    assert(movie.getString(0) == "Avengers 99")
  }

  test("epoc to timestamp") {
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    testDF.show
    val trasformedDF = epoc_to_timestamp(testDF)
    trasformedDF.show(false)
    trasformedDF.createOrReplaceTempView("test")
    val movie = spark.sql("select timestamp from test where id=1").first()
    import java.sql.Timestamp
    val timestamp=Timestamp.valueOf("2000-07-30 19:08:00")
    print(timestamp)
    assert(movie.getTimestamp(0) == timestamp)
    spark.conf.unset("spark.sql.session.timeZone")
  }

  test("explode genre") {
    val trasformedDF = explode_genre(testDF)
    assert(trasformedDF.count() == 5)
  }

  test("top ratings") {
    val moviesDF = DeltaTable.forPath(spark, movies_table_name).toDF
    val ratingDF = DeltaTable.forPath(spark, ratings_table_name).toDF
    topRated(ratingDF, moviesDF, top_movies_save_path)
    val top = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(top_movies_save_path)
    top.createOrReplaceTempView("top")
    val movie = spark.sql("select movieId, avg_rating from top").first()
    assert(movie.getInt(0) == 1)
    assert(movie.getDouble(1) == 2.29)
    var directory = new Directory(new File(movies_table_name))
    directory.deleteRecursively()
    directory = new Directory(new File(tags_table_name))
    directory.deleteRecursively()
    directory = new Directory(new File(ratings_table_name))
    directory.deleteRecursively()
    directory = new Directory(new File(top_movies_save_path))
    directory.deleteRecursively()
  }
}