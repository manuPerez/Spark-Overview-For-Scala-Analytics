import breeze.linalg.max
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.csv._

/**
  * Created by manu on 7/06/16.
  */
object SparkDataFrames {
  val quiet = false

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark DataFrames")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.app.id", "SparkDataFrames")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._    // Need for column idioms like $"foo".desc

    try{
      val heroPath = "data/superheroMovies.csv"
      val allHeroPath = "data/allsuperheroMovies.csv"

      val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(heroPath)
      val dfAll = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(allHeroPath)
//      dfAll.printSchema()
//      dfAll.show(192)

//      df.select($"title", $"IMDB",$"year")
//        .groupBy($"year")
//        .agg(org.apache.spark.sql.functions.max($"IMDB"))
//        .orderBy($"year").show()

//      val pelis = dfAll.select("movie", "yearOfMovie")


//      pelis_por_anio.show()

//      pelis_por_anio.cache()

      val dfPart = df.select("year", "title", "marvelDC", "IMDB")

      val joined = dfAll.join(dfPart, dfAll("yearOfMovie").equalTo(df("year")) && dfAll("movie").equalTo(df("title")), "left_outer")

      val pelis_por_anio = dfAll.groupBy("yearOfMovie").count().toDF("year","numMoviesPerYear").orderBy("numMoviesPerYear", "year")

      val joined2 = joined.join(pelis_por_anio, joined("yearOfMovie").equalTo(pelis_por_anio("year")))
                        .select("yearOfMovie", "movie", "marvelDC", "IMDB", "numMoviesPerYear")
                        .orderBy("yearOfMovie", "IMDB")
                        .toDF("yearOfMovie", "movie", "marvelDC", "IMDB", "numMoviesPerYear")

      joined2.select("yearOfMovie", "movie", "marvelDC", "IMDB").show(joined.count().toInt)

    } finally {
      sc.stop()
    }
  }
}
