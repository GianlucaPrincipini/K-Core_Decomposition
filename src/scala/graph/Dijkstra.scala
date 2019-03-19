package graph

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


object Dijkstra {
  type VertexId = Long

  case class City(
                 name: String,
                 id: VertexId
                 )

  case class VertexAttribute(
                            cityName: String,
                            distance: Double,
                            path: List[City]
                            )

  def vProg = (
    vertexId: VertexId,
    currentVertexAttr: VertexAttribute,
    newVertexAttribute: VertexAttribute
  ) => if (currentVertexAttr.distance <= newVertexAttribute.distance) {
    currentVertexAttr
  } else newVertexAttribute

  def sendMsg = (edgeTriplet: EdgeTriplet[VertexAttribute, Double]) => {
    if (edgeTriplet.srcAttr.distance < (edgeTriplet.dstAttr.distance - edgeTriplet.attr)) {
      Iterator((
        edgeTriplet.dstId,
        new VertexAttribute(
          edgeTriplet.dstAttr.cityName,
          edgeTriplet.srcAttr.distance + edgeTriplet.attr,
          edgeTriplet.srcAttr.path :+ new City(
            edgeTriplet.dstAttr.cityName,
            edgeTriplet.dstId
          )
        )
      ))
    } else Iterator.empty
  }

  def mergeMsg = (
    attribute1: VertexAttribute,
    attribute2: VertexAttribute
  ) => if (attribute1.distance < attribute2.distance) attribute1 else attribute2

  def dijkstra(initialGraph: Graph[VertexAttribute, Double]): Unit = {
    initialGraph.pregel(
      initialMsg = VertexAttribute(
        "",
        Double.PositiveInfinity,
        List[City]()
      ),
      maxIterations = Int.MaxValue,
      activeDirection = EdgeDirection.Out
    )(
      vProg,
      sendMsg,
      mergeMsg
    )
  }


}
