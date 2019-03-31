
package scala.graph

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge

object GraphXReader {
  val sc = SparkContext.getOrCreate()
  def read(fileName: String): Unit = {
    val graph1 = sc.textFile(fileName).map(x => splitNode(x))
    val vertices1 = graph1.map(x => (x.dstId, false)).distinct()
    val vertices = (vertices1 ++ graph1.map(x => (x.srcId, false))).distinct()

  }
  def splitNode(line: String): Edge[String] = {
    val splitted = line.split(" ")
    Edge(splitted(0).toLong, splitted(1).toLong, "Friend")
  }
}
