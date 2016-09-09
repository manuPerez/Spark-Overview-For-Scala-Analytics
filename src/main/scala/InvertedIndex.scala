import java.nio.file.{Files, Paths}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext

import scala.reflect.io.Path
import scala.util.Try

object InvertedIndex {
  def main(args: Array[String]) {
    val inpath = "output/crawl"
    val outpath = "output/inverted-index"

    if (Files.exists(Paths.get(outpath))) {
      val path: Path = Path (outpath)
      Try(path.deleteRecursively())
    }

    val sc = new SparkContext("local[*]", "Inverted Index")

    val stopWords: Broadcast[Set[String]] = sc.broadcast(StopWords.words)

    try {
      val lineRe = """^\(([^,]+),(.*)\)\s*$""".r
      val input = sc.textFile(inpath).map {
        case lineRe(name, text) => (name.trim, text.toLowerCase)
        case badLine => Console.err.println(s"Unexpected line: $badLine")
          ("", "")
      }
      input.flatMap {
        case (path, text) =>
          text.trim.split("""[^\w']""").map(word => ((word, path), 1))
      }
      .filter {
        case ((word, _), _) => stopWords.value.contains(word) == false
      }
      .reduceByKey {
        (count1, count2) => count1 + count2
      }
      .map {
        case ((word, path), n) => (word, (path, n))
      }
      .groupByKey
      .sortByKey(ascending = true)
//      .map {
//        //      case (word, iterable) => (word, iterable.mkString(", "))
//        case (word, iterable) =>
//          val vect = iterable.to[Vector].sortBy {
//            case (path, n) => (-n, path)
//          }
//          (word, vect.mkString(", "))
//      }
      .saveAsTextFile(outpath)

//      Console.in.read()
    } finally {
      sc.stop()
    }
  }
}
