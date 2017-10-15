import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.jsoup.Jsoup

import scala.collection.mutable.ListBuffer

object PageRank {

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println(
        "Usage: PageRank <inputFile> <outputFile> <Iterations>")
      System.exit(-1)
    }

    case class Artset(val title: String, val links: List[String])

    val File = args(0)
    val conf = new SparkConf().setAppName("Application")
    val sc = new SparkContext(conf)

    val finalData = sc.textFile(File)
    val lines = finalData.flatMap(file => file.split("\n"))

    val links = finalData.map{line =>
      val finalfields = line.split("\t")
      val xml = Jsoup.parse(finalfields(3).replace("\\n", "\n"))
      val targets = xml.getElementsByTag("target")
      var result = new ListBuffer[String]()
      for(target <- 0 to targets.size()-1){
        result += targets.get(target).text()

      }
      new Artset (finalfields(1),result.toList)
    }.cache()

    val vertices = links.map(a => (Hash(a.title), a.title)).cache

    val edgesList:RDD[Edge[Double]] = links.flatMap{a =>
      val src = Hash(a.title)
      a.links.map{t => Edge(src,Hash(t), 1.0)
      }
    }

    val graph = Graph(vertices,edgesList)
    val pgRank = graph.staticPageRank(2).cache()
    val tG = graph.outerJoinVertices(pgRank.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    val output = tG.vertices.top(100)//.map(item =>item.swap).sortByKey(false,1).take(100)
    val temp = sc.parallelize(output)
    temp.coalesce(1).saveAsTextFile(args(1))
    sc.stop()
  }

  def Hash(title: String): VertexId = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }
}
