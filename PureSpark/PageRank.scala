import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

import scala.collection.mutable.ListBuffer

object PageRank {
  def main(args: Array[String]) {
    val File = args(0) 
    val conf = new SparkConf().setAppName("PageRank Pure sPARK")
    val sc = new SparkContext(conf)

    val PageData = sc.textFile(File)
    val lines = PageData.flatMap(file => file.split("\n"))
    
    val links = PageData.map{line =>
          val filefields = line.split("\t")
          val xml = Jsoup.parse(filefields(3).replace("\\n", "\n"))
          val targets = xml.getElementsByTag("target")
          var result = new ListBuffer[String]()
          for(target <- 0 to targets.size()-1){
            result += targets.get(target).text()
          }

      (filefields(1),result.toList)
    }.cache()
    var finalranks = links.mapValues(v => 1.0)

    val iters = args(2).toInt

    for (i <- 1 to iters) {
      val contribs = links.join(finalranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      finalranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
      val output = finalranks.map(item =>item.swap).sortByKey(false,1).take(100)
      val temp = sc.parallelize(output)
      temp.coalesce(1).saveAsTextFile(args(1))
	sc.stop()
  }
}
