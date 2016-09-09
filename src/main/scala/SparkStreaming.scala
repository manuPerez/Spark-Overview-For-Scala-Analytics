import java.io.PrintWriter

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds

object SparkStreaming {
  val port = 9000
  val interval = Seconds(2)
  val pause = 10 // milliseconds
  val server = "127.0.0.1"
  val checkPointDir = "output/checkpoint_dir"
  val runtime = 30 * 1000 // run for N*1000 milliseconds
  val numIterations = 100000

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark Streaming")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "SparkStreaming")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // is a convention to use a function to construct the streaming context
    def createContext(): StreamingContext = {
      val ssc = new StreamingContext(sc, interval)  // minibatch interval (2 seconds here)
      ssc.checkpoint(checkPointDir)

      val dStream = ssc.socketTextStream(server, port)  // listen to a socket of text data
      val numbers = for {  // for each line, split on whitespace, convert to an int
        line <- dStream
        number <- line.trim.split("\\s+")
      } yield number.toInt

      numbers.foreachRDD(rdd => rdd.countByValue().foreach(println))  // for each minibatch RDD, after construction,
                                                                      // run this code

                                                                      // countByValues is like reduceByKey, where our
                                                                      // single ints are "keys" and "values"

      ssc
    }

    var ssc: StreamingContext = null  // hack: use vars and nulls here so we can init these inside the "try" clause,
    // but see them in the "finally" clause
    var dataThread: Thread = null

    try{
//      dataThread = startDataSocketThread(port) // start the data source socket in a separate thread

      ssc = StreamingContext.getOrCreate(checkPointDir, createContext _)
      ssc.start()

      ssc.awaitTerminationOrTimeout(runtime)
    } finally {
      if (dataThread != null) dataThread.interrupt()
      if (ssc != null) ssc.stop(stopSparkContext = true, stopGracefully = true)
    }

    // to set up the data source socket, first create a Runnable
    def makeRunnable(port: Int) = new Runnable {
        def run() = {
          val listener = new java.net.ServerSocket(port)
          var socket: java.net.Socket = null
          try{
            val socket = listener.accept()
            val out = new PrintWriter(socket.getOutputStream, true)
            (1 to numIterations).foreach {
              i => val number = (100 * math.random).toInt
                out.println(number)
                Thread.sleep(pause)
            }
          } finally {
            listener.close()
            if (socket != null) socket.close()
          }
        }
    }
  }


}
