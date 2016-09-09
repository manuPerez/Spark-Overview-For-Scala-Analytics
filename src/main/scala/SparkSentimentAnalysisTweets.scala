import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.{Status, Twitter}

import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

//Generally, regression refers to predicting a
// numeric quantity like size or income or temperature,
// while classification refers to predicting a label
// or cate‐ gory, like “spam” or “picture of a cat.”
//Naive bayes is a classification algorithm
object SparkSentimentAnalysisTweets {


  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: SparkSentimentAnalysisTweets <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }
    //read twitter tokens from arguments
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    val conf = new SparkConf()
      .setAppName("SentimentAnalysis")
      .setMaster("local[*]")
      .set("spark.app.id", "Senti")   // To silence Metrics warning.

    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    try {
      val model: NaiveBayesModel = SparkSentimentAnalysis.createModel(sc)

      val stream: DStream[Status] = TwitterUtils.createStream(ssc, None, filters) //filter criteria

      stream
        .filter(s => s.getUser.getLang == "en")
        .filter(s => !s.isPossiblySensitive)
        .map(s => (s.getUser.getName, s.getText))
        .foreachRDD { rdd =>
          val htf: HashingTF = new HashingTF()
          //this is the power of Spark where we are able to use algo. written for batch job for streaming almost without change
          //addresses the Lambda architecture problems
          rdd.map {
            case (username, text) => (model.predict(htf.transform(text.split(" "))), text)
          }.foreach(println)
        }

      ssc.start()
      ssc.awaitTerminationOrTimeout(1000 * 60) //run for 60 seconds

    } finally {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }
}