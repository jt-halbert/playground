import org.apache.hadoop.examples.terasort.TeraInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.hadoop.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object wikiGraph {

  val conf = new SparkConf().setAppName("simple graph app")
  conf.setMaster("localhost")
  val sc = new SparkContext(conf)

  val titles = sc.textFile("file:///data/datasets/wikipedia/titles-sorted.txt")
  val titlesWithIndex: RDD[(VertexId, String)] = titles.zipWithIndex().map(t => (t._2 + 1L, t._1))
  val links = sc.textFile("file:///data/datasets/wikipedia/links-simple-sorted.txt")
  val adjacencyList = links.flatMap(s => {
    val pieces = s.split(":").map(_.trim())
    pieces(1).split("\\s+").map(t => (pieces(0).toLong, t.toLong))
  })
  val test = sc.newAPIHadoopFile[Text, Text, TeraInputFormat]("")
  val graph = Graph(titlesWithIndex, adjacencyList.map(t => Edge(t._1, t._2, "")))
  val inDegrees: VertexRDD[Int] = graph.inDegrees
  val outDegrees: VertexRDD[Int] = graph.outDegrees

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

}
