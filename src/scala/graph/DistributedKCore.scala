package graph

import org.apache.spark.graphx.{EdgeTriplet, VertexId}


import org.apache.spark.graphx._
import scala.collection.Map
import scala.graph.KCoreVertex



object DistributedKCore {
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


  def decomposeGraph(graph: Graph[KCoreVertex, Map[VertexId, Int]], maxIterations: Int) = {
    graph.pregel(dummyMessage, maxIterations = maxIterations)(
      (id, attr, msg) => vertexProgram(id, attr, msg),
      triplet => sendMessage(triplet),
      (coreness1, coreness2) => mergeMessages(coreness1, coreness2)
    )
  }

}
