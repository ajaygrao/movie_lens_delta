import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{avg, col, count, desc, explode, from_unixtime, from_utc_timestamp, round, split, to_date, to_timestamp, to_utc_timestamp}
import org.slf4j.LoggerFactory

object TransformUtil {

  val logger = LoggerFactory.getLogger(TransformUtil.getClass)

  def split_column(df: DataFrame): DataFrame = {
    df.withColumn("genres", split(col("genres"), "\\|"))
  }

  def epoc_to_timestamp(df: DataFrame): DataFrame = {
    df.withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp"))))
  }

  def explode_genre(df: DataFrame): DataFrame = {
    df.withColumn("genres", explode(split(col("genres"), "\\|")))
  }

  def topRated(ratingDF: DataFrame, moviesDF: DataFrame, csv_path: String): Unit = {
    val rating_filtered = ratingDF.groupBy("movieId").agg(
      count("rating").as("cnt_rating"),
      round(avg("rating"),2).as("avg_rating")
    )
      .where(col("cnt_rating") >= 5)

    rating_filtered.show(false)
    val top_movies = moviesDF.join(rating_filtered, Seq("movieId"))
      .select("movieId","title", "avg_rating")
      .orderBy(desc("avg_rating"))
      .limit(10)
    top_movies.show(false)
    top_movies.repartition(1)
      .write.option("header",true)
      .mode(SaveMode.Overwrite)
      .csv(csv_path)
  }
}
