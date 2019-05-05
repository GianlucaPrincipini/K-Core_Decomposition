package graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

import scala.graph.KCoreVertex
import scala.collection.Map
import scala.collection.immutable.HashMap

class GraphReader extends Serializable {

  /**
    * Nel file le associazioni sono monodirezionali, per rendere bidirezionale ogni arco abbiamo ripetuto la procedura su
    * @param fileName
    * @return
    */
  def readFile(fileName: String, sc: SparkContext): Graph[KCoreVertex, String] = {
    println("Loading: " + fileName)
    val file = sc.textFile(fileName).cache()
    val graph1 = file.map(x => split(x, false))
    val undirectedGraph = graph1.union(sc.textFile(fileName).map(x => split(x, true)))
    val keys: RDD[(VertexId, KCoreVertex)] = undirectedGraph.map(x => (x.srcId, new KCoreVertex(x.srcId)))
    val graph = Graph[KCoreVertex, String](keys, undirectedGraph)
    val degrees = graph.inDegrees
    val output = graph.joinVertices(degrees)((id, vertex, degree) => {
      vertex.coreness = degree;
      vertex
    })
    println("File loaded, initializing vertices")
    output
    // println(joined)
  }


  /**
    * Da riga del file a Edge di interi
    * @param line riga del file
    * @param inverted inverti la tupla
    * @return tupla di interi
    */
  def split(line: String, inverted: Boolean): Edge[String] = {
    val splitted = line.split("[ \\t]")
    if (inverted){
      new Edge(splitted(1).toLong, splitted(0).toLong, splitted(1) + "->" + Int.MaxValue)
    }
    else {
      new Edge(splitted(0).toLong, splitted(1).toLong, splitted(0) + "->" + Int.MaxValue)
    }
  }
}
