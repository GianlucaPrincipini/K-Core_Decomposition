package scala.graph

import org.apache.spark.graphx.VertexId

import scala.collection.Map

class KCoreVertex(id: VertexId) extends Serializable {
  val nodeId = id
  var updated = false
  var est: Map[VertexId, Int] = null
  var coreness = 0

  override def hashCode(): Int = nodeId.toInt

  def canEqual(a: Any): Boolean = a.isInstanceOf[KCoreVertex]
  override def equals(that: Any): Boolean =
    that match {
      case that: KCoreVertex  => that.canEqual(this) && this.nodeId == that.nodeId
      case _ => false
    }

  def computeIndex() = {
    val count = Array.fill[Int](this.coreness)(0)
    est.foreach(tuple => {
      val j = Math.min(coreness, tuple._2) - 1
      count(j) = count(j) + 1
    })
    var i = coreness
    while (i > 2) {
      count(i - 2) = count(i - 2) + count(i-1)
      i = i - 1
    }
    i = coreness
    while (i > 1 && count(i-1) < i-1) {
      i = i - 2
    }
    i
  }


  override def toString: String = nodeId + ": " + coreness


}
