package scala.graph

import org.apache.spark.rdd.RDD

class Graph(graph: RDD[(Node, Node)]) {
  val distributedGraph = graph

  def getDegree(node: Node): Long = {
    distributedGraph.countByKey().getOrElse(node, -1)
  }

  def getDegrees() = distributedGraph.countByKey()
  //def getDegree(node: Int): Long = {
  //  distributedGraph.countByKey().getOrElse(new T(node), 0)
  //}
}
