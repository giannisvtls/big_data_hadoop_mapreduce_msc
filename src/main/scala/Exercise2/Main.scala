package Exercise2
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.{PrintWriter, File}

object Main {
  case class Movie(
                    imdbID: String,
                    title: String,
                    year: Int,
                    runtime: Int,          // Will store just the number of minutes
                    genre: List[String],
                    released: String,
                    imdbRating: Double,
                    imdbVotes: Double,
                    countries: List[String]
                  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IMDB Analysis")
      .config("spark.master", "local[*]")
      .getOrCreate()

    import spark.implicits._

    val writer = new PrintWriter(new File("src/main/scala/Exercise2/movie_analysis_results.txt"))

    // Define a UDF to parse runtime string
    val parseRuntime = udf((runtimeStr: String) => {
      if (runtimeStr == null || runtimeStr.trim.isEmpty) {
        0 // default value for missing runtime
      } else {
        runtimeStr.replace(",", "").split(" ")(0).trim.toInt // extract just the number
      }
    })

    val moviesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/scala/Exercise2/movies.csv")
      .select(
        $"imdbID",
        $"title",
        $"year".cast("int"),
        parseRuntime($"runtime").as("runtime"),  // Parse runtime string to Int
        split($"genre", ",").as("genre"),        // Split on comma for genres
        $"released",
        $"imdbRating".cast("double"),
        $"imdbVotes".cast("double"),
        split($"country", ",").as("countries")   // Split on comma for countries
      )
      .distinct()  // Remove duplicates based on all columns
      .as[Movie]

    // Task 1: Total duration per country
    val durationByCountryDF = moviesDF
      .select(explode($"countries").as("country"), $"runtime")
      .groupBy("country")
      .agg(sum("runtime").as("total_duration"))
      .orderBy("country")

    writer.println("Total Duration by Country:")
    writer.println("========================")

    durationByCountryDF.collect().foreach { row =>
      writer.println(f"${row.getString(0).trim}%-20s ${row.getLong(1)}%d minutes")
    }

    // Task 2: Count by year and genre for high rated movies
    val highRatedByYearGenreDF = moviesDF
      .filter($"imdbRating" > 8.0)
      .select($"year", explode($"genre").as("genre"))
      .groupBy($"year", $"genre")
      .count()
      .select(
        concat_ws("_", $"year", $"genre").as("year_genre"),
        $"count"
      )
      .orderBy("year_genre")

    writer.println("\nHigh Rated Movies (Score > 8) Count by Year and Genre:")
    writer.println("================================================")

    highRatedByYearGenreDF.collect().foreach { row =>
      val yearGenre = row.getString(0).split("_")
      writer.println(f"Year: ${yearGenre(0)}%-6s Genre: ${yearGenre(1).trim}%-15s Count: ${row.getLong(1)}%d")
    }

    writer.close()
    println("Results have been written to movie_analysis_results.txt")

    spark.stop()
  }
}