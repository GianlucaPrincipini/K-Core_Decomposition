package graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx._


object GraphReader {
  val sc = SparkContext.getOrCreate()

  /**
    * Nel file le associazioni sono monodirezionali, per rendere bidirezionale ogni arco abbiamo ripetuto la procedura su
    * @param fileName
    * @return
    */
  def readFile(fileName: String): Graph[VertexId, Int] = {
    val graph1 = sc.textFile(fileName).map(x => split(x, false))
    val undirectedGraph = graph1 ++ sc.textFile(fileName).map(x => split(x, true))
    val keys = undirectedGraph.map(x => (x.srcId, x.srcId)).distinct()
    Graph(keys, undirectedGraph, -1)
  }

  /**
    * Da riga del file a Edge di interi
    * @param line riga del file
    * @param inverted inverti la tupla
    * @return tupla di interi
    */
  def split(line: String, inverted: Boolean): Edge[Int] = {
    val splitted = line.split(" ")
    if (inverted)
      Edge(splitted(1).toLong, splitted(0).toLong, 0)
    else
      Edge(splitted(0).toLong, splitted(1).toLong, 0)
  }

}
