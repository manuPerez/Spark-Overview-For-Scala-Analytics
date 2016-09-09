import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.hashing.MurmurHash3

object GraphingFlights {

  var quiet = false

  def main(args: Array[String]) {
//    val input = "data/superheroMovies.csv"
    val inputFlights = "data/alaska-airlines/2008.csv"
    val conf = new SparkConf()
      .setAppName("GraphX")
      .setMaster("local[*]")
      .set("spark.app.id", "GraphX") // To silence Metrics warning.

    val sc = new SparkContext(conf)
//
//    val csv = sc.textFile(input)
//
//    val all = csv.map(lines => lines.split(",").map(_.trim))
//
//    val header = all.first
//
//    val data = all.filter(_(0) != header(0))
//
//    val superheroes = data.map(
//      t => {
//
//        val s = new Superhero()
//
//        s.year = t(0).toInt
//        s.title = t(1)
//        s.marvelDC = t(2)
//        s.IMDB = t(3).isEmpty match {
//          case true => 0.0
//          case false => t(3).toDouble
//        }
//        s.rottenTomatoes = t(4).toInt
//        s.IMDBRottenTomatoesComposite = t(5).isEmpty match {
//          case true => 0.0
//          case false => t(5).toDouble
//        }
//        s.openingWeekendBoxOffice = t(6).isEmpty match {
//          case true => 0.0
//          case false => t(6).toDouble
//        }
//        s.avgMovieTicketPriceForYear = t(7).isEmpty match {
//          case true => 0.0
//          case false => t(7).toDouble
//        }
//        s.estOpeningWeekendAttendance = t(8).isEmpty match {
//          case true => 0.0
//          case false => t(8).toDouble
//        }
//        s.USPopulationYearOfOpening = t(9).isEmpty match {
//          case true => 0.0
//          case false => t(9).toDouble
//        }
//        s.percentOfPopulationAttendingOpeningWeekend = t(10).isEmpty match {
//          case true => 0.0
//          case false => t(10).toDouble
//        }
//
//        s
//      }
//    )

        val csv = sc.textFile(inputFlights)

        val all = csv.map(lines => lines.split(",").map(_.trim))

        val header = all.first

        val data = all.filter(_(0) != header(0))

        val flights = data.map(
          t => {

            val s = new Flight()

            s.Year = t(0)
            s.Month = t(1)
            s.DayofMonth = t(2)
            s.DayOfWeek = t(3)
            s.DepTime = t(4)
            s.CRSDepTime = t(5)
            s.ArrTime = t(6)
            s.CRSArrTime = t(7)
            s.UniqueCarrier = t(8)
            s.FlightNum = t(9)
            s.TailNum = t(10)
            s.ActualElapsedTime = t(11)
            s.CRSElapsedTime = t(12)
            s.AirTime = t(13)
            s.ArrDelay = t(14)
            s.DepDelay = t(15)
            s.Origin = t(16)
            s.Dest = t(17)
            s.Distance = t(18)
            s.TaxiIn = t(19)
            s.TaxiOut = t(20)
            s.Cancelled = t(21)
            s.CancellationCode = t(22)
            s.Diverted = t(23)
            s.CarrierDelay = t(24)
            s.WeatherDelay = t(25)
            s.NASDelay = t(26)
            s.SecurityDelay = t(27)
            s.LateAircraftDelay = t(28)

            s
          }
        )

    try{
      //create vertices out of airport codes for both origin and dest
      val airportCodes = flights.flatMap { f => Seq(f.Origin, f.Dest) }
      val airportVertices: RDD[(VertexId, String)] =
        airportCodes.distinct().map(x => (MurmurHash3.stringHash(x).toLong, x))

      //create edges between origin -> dest pair and the set the edge attribute
      //to count of number of flights between given pair of origin and dest
      val flightEdges = flights.map(f =>
        ((stringHash(f.Origin), stringHash(f.Dest)), 1))
        .reduceByKey(_+_)
        .map {
          case ((src1, src2), attr) => Edge(src1, src2, attr)
        }

      val graph = Graph(airportVertices, flightEdges)
      if (!quiet) {
        println("\nNumber of airports in the graph:")
        println(graph.numVertices)
        println("\nNumber of flights in the graph:")
        println(graph.numEdges)
      }

      //top 10 flights between two airports
      //graph.triplets returns RDD of EdgeTriplet that has src airport, desc airport and
      //attribute. This
      println("\nFinding the most frequent flights between airports:")
      val triplets: RDD[EdgeTriplet[String, PartitionID]] = graph.triplets

      triplets.sortBy(_.attr, ascending=false)
        .map(triplet =>
          s"${triplet.srcAttr} -> ${triplet.dstAttr}: ${triplet.attr}")
        .take(10).foreach(println)


      println("\nBusiest airport:")
      val by =
        triplets.map { triplet =>
          (triplet.srcAttr, triplet.attr)
        }.reduceByKey(_ + _)
      by.sortBy(-_._2).take(1).foreach(println)

      //what airport has the most in degrees or unique flights into it?
      //vertices with no in-degree are ignore here
      val incoming: RDD[(VertexId, (PartitionID, String))] = graph.inDegrees.join(airportVertices)

      println("\nAirports with least number of distinct incoming flights:")
      incoming.map {
        case (_, (count, airport)) => (count, airport)
      }.sortByKey().take(10).foreach(println)

      println("\nAirports with most number of distinct outgoing flights:")
      val outgoing: RDD[(VertexId, (PartitionID, String))] = graph.outDegrees.join(airportVertices)

      outgoing.map {
        case (_, (count, airport)) => (count, airport)
      }.sortByKey(ascending = false).take(10).foreach(println)

    } finally {
      sc.stop()
    }
  }

  def stringHash(str: String): Int = MurmurHash3.stringHash(str)


}