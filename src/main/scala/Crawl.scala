
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkContext

import scala.reflect.io.Path
import scala.util.Try

object Crawl {
  def main(args: Array[String]) {
    val separator = java.io.File.separator
    val inpath = "data/*"
    val outpath = "output/crawl"

    if (Files.exists(Paths.get(outpath))) {
      val path: Path = Path (outpath)
      Try(path.deleteRecursively())
    }

    val sc = new SparkContext("local[*]", "Crawl")  //clean up old output and setup the SparkContext


    try{
      //                      |------------!----------|---!--|
      //                       read a directory
      //                       (or more) return       map over the
      //                       records:               records and
      //                       file_name, contents    transform them
      val files_contents = sc.wholeTextFiles(inpath).map {
        case (id, text) =>
          val lastSep = id.lastIndexOf(separator)          // --|  we're going remove
        val id2 = if (lastSep < 0) id.trim                 //   |  the leading path part,
          else id.substring(lastSep+1, id.length).trim     // --|  because it's not useful
        //                                  |-------!-------|
        //                                   remove embedded
        //                                   new lines in the
        //                                   text, so it's
        //                                   all on one line
        val text2 = text.trim.replaceAll("""\s*\n\s*""", " ")
          //       |--!--|
          //        return
          //        the cleaned
          //        up data
          (id2, text2)
      }
      println(s"Writing output to: $outpath")
      files_contents.saveAsTextFile(outpath)
      //                           |---¡---|
      //                            write out
      //                            the results
      //                            and quit
    } finally {
      sc.stop()
    }
  }
}
