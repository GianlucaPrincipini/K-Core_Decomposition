
import graph.GraphReader
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import scala.collection.Map
import scala.collection.immutable.HashMap
import scala.graph.KCoreVertex



object Main {
  val dummyMessage: Map[VertexId, Int] = Map(Long.MinValue -> -1)

  def vertexProgram(id: VertexId, attr: KCoreVertex, msg: Map[VertexId, Int]) = {

    if (msg != dummyMessage) {
      attr.updated = false
      msg.foreach(tuple => {
        if (tuple._2 <= attr.est.get(tuple._1).get) {
          attr.est = attr.est + (tuple._1 -> tuple._2)
          val computedCoreness = attr.computeIndex()
          if (computedCoreness < attr.coreness) {
            attr.coreness = computedCoreness
            attr.updated = true
          }
        }
      })
    } else {
      attr.updated = true
    }
    attr
  }


  def sendMessage(triplet: EdgeTriplet[KCoreVertex, Map[VertexId, Int]]) = {
    if (triplet.srcAttr.updated || triplet.attr == dummyMessage) {
      Iterator((triplet.dstId, Map(triplet.srcAttr.nodeId -> triplet.srcAttr.coreness)))
    } else {
      Iterator.empty
    }
  }

  def mergeMessages(msg1: Map[VertexId, Int], msg2: Map[VertexId, Int]) = {
    msg1 ++ msg2
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val k = 1
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "GraphX")
    val fileName = "resources/kcoreTestDataset.txt"
    val graph = GraphReader.readFile(fileName)
    val kcore = graph.pregel(dummyMessage, maxIterations = k)(
      (id, attr, msg) => vertexProgram(id, attr, msg),
      triplet => sendMessage(triplet),
      (coreness1, coreness2) => mergeMessages(coreness1, coreness2)
    )
    val alpha = GraphReader.readAlpha("resources/mappingAlphaIndex.txt")

    kcore.vertices.join(alpha).collect().take(15).sortBy(_._1).foreach(x => println(x._2._2, x._2._1.coreness))
  }
}
