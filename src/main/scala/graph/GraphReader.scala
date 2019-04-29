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
    * Inizializzazione del KCoreVertex, la coreness iniziale è il suo grado.
    * la stima della coreness dei vicini viene inizializzata al valore massimo
    * @param vertex
    * @param degreesMap
    * @return
    */
  def initializeKCoreVertex(vertex: KCoreVertex, neighbors: Map[VertexId, Array[VertexId]]) = {
    vertex.est = new HashMap[VertexId, Int]()
    val vertexNeighbors = neighbors.getOrElse(vertex.nodeId, null)
    vertexNeighbors.foreach(neighborId => vertex.est ++= Map(neighborId -> Int.MaxValue))
    vertex.coreness = vertex.est.keys.size
    vertex
  }

  /**
    * Nel file le associazioni sono monodirezionali, per rendere bidirezionale ogni arco abbiamo ripetuto la procedura su
    * @param fileName
    * @return
    */
  def readFile(fileName: String, sc: SparkContext): Graph[KCoreVertex, Map[VertexId, Int]] = {
    println("Loading: " + fileName)
    val file = sc.textFile(fileName).cache()
    val graph1 = file.map(x => split(x, false))
    val undirectedGraph = graph1.union(sc.textFile(fileName).map(x => split(x, true)))
    val keys: RDD[(VertexId, KCoreVertex)] = undirectedGraph.map(x => (x.srcId, new KCoreVertex(x.srcId))).distinct()
    val graph = Graph[KCoreVertex, Map[VertexId, Int]](keys, undirectedGraph)
    val neighbors = graph.collectNeighborIds(EdgeDirection.Out).collectAsMap()
    val retGraph = graph.mapVertices((vertexId, vertexStruct) => initializeKCoreVertex(vertexStruct, neighbors))
    retGraph
  }


  /**
    * Da riga del file a Edge di interi
    * @param line riga del file
    * @param inverted inverti la tupla
    * @return tupla di interi
    */
  def split(line: String, inverted: Boolean): Edge[Map[VertexId, Int]] = {
    val splitted = line.split(" ")
    if (inverted){
      new Edge(splitted(1).toLong, splitted(0).toLong, Map(splitted(1).toLong -> Int.MaxValue))
    }
    else {
      new Edge(splitted(0).toLong, splitted(1).toLong, Map(splitted(0).toLong -> Int.MaxValue))
    }
  }
}
