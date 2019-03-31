package scala.graph

import org.apache.spark.rdd.RDD

class Graph(graph: RDD[(KCoreVertex, KCoreVertex)]) {
  val distributedGraph = graph

  def getDegree(node: KCoreVertex): Long = {
    distributedGraph.countByKey().getOrElse(node, -1)
  }

  def getDegrees() = distributedGraph.countByKey()

}
