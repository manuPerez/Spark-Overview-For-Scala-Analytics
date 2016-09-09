import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

case class Rating(userId: Int, movieId: Int, rating: Double)

object Rating {
  //Format is: userId, movieId, rating, timestamp
  def parseRating(str: String): Rating = {
    val fields = str.split("::")

    new Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }
}

case class Movie(movieId: Int, title: String, genres: Seq[String])

object Movie {
  //Format is: movieId, title, genre1|genre2
  def parseMovie(str: String): Movie = {
    val fields = str.split("::")

    new Movie(fields(0).toInt, fields(1), fields(2).split("|"))
  }
}

// We will take a Collaborative filtering example to ratering movies.
// The data is from (http://grouplens.org/datasets/movielens/) MovieLens is a
//     non-comemrcial movie recommendation website
// The example is inspired from the MovielLensALS.scala example from Spark distribution.
object SupervisedLearningExample {
  val numIterations = 10
  val rank = 10 // number of features to consider when training the model
  //this file is in UserID::MovieID::Rating::Timestamp format
  val ratingsFile = "data/als/sample_ratings.txt"
  val moviesFile = "data/als/sample_movies.txt"
  val testFile = "data/als/test.data"

  def main(args: Array[String]) = run()

  def run(): Unit = {
    val conf = new SparkConf()
        .setAppName("SupervisedLearning")
        .setMaster("local[*]")
        .set("spark.app.id", "ALS")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    Logger.getRootLogger.setLevel(Level.ERROR)

    // This will be our training dataset
    val ratingsData: RDD[Rating] =
      sc.textFile(ratingsFile).map(Rating.parseRating).cache()

    // This will be our test dataset to verify the model
    val testData: RDD[Rating] = sc.textFile(testFile).map(Rating.parseRating).cache()

    val numRatings = ratingsData.count()
    val numUsers = ratingsData.map(_.userId).distinct().count()
    val numMovies = ratingsData.map(_.movieId).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    // This is nuch more simplified version, in real world we try different rank,
    //   iterations and other parameters to find best model.
    // Tipically ALS model looks for two properties, usercol for user info
    //   and itemcol for items that we are recommending
    val als = new ALS()
                  .setUserCol("userId")
                  .setItemCol("movieId")
                  .setRank(rank)
                  .setMaxIter(numIterations)

    // Training the model, converting rdd to dataframe for spark.ml
    val model: ALSModel = als.fit(ratingsData.toDF())

    // Now trying the model on our testdata
    val predictions: DataFrame = model.transform(testData.toDF()).cache()

    // Metadata about the movies
    val movies = sc.textFile(moviesFile).map(Movie.parseMovie).toDF()

    // Try to find out if our model has any falsePositives
    //   and joining with movies so that we can print the movie names
    val falsePositives = predictions.join(movies)
            .where((predictions("movieId") === movies("movieId")) && ($"rating" <= 1) && ($"prediction" >= 4))
            .select($"userId", predictions("movieId"), $"title", $"rating", $"prediction")
    val numFalsePositives = falsePositives.count()
    println(s"Found $numFalsePositives false positives")
    if (numFalsePositives > 0){
      println(s"Example of false positives:")
      falsePositives.limit(100).collect().foreach(println)
    }

    // Show first 20 predictions
    predictions.show()

    // Running predictions for user 4 as an example to find
    //   out whether user likes some movies
    println(">>> Find out predictions where user 26 likes movies 10, 15, 20 & 25")
    val df26 = sc.makeRDD(Seq(4 -> 10, 4 -> 15, 4 -> 20, 4 -> 25)).toDF("userId", "movieId")
    model.transform(df26).show()

    sc.stop()
  }

}