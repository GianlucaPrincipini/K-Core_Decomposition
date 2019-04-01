package scala.graph

import org.apache.spark.graphx.VertexId

class KCoreVertex(id: VertexId) extends Serializable {
  val nodeId = id
  var updated = true
  var est: Map[VertexId, Int] = null
  var coreness = 0

  override def hashCode(): Int = nodeId.toInt

  def canEqual(a: Any): Boolean = a.isInstanceOf[KCoreVertex]
  override def equals(that: Any): Boolean =
    that match {
      case that: KCoreVertex  => that.canEqual(this) && this.nodeId == that.nodeId
      case _ => false
    }

  override def toString: String = nodeId + ": " + coreness


}
