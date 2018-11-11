package scala.graph

class Node(id: Int) extends Serializable {
  val nodeId = id
  val updated = false

  override def hashCode(): Int = nodeId

  def canEqual(a: Any): Boolean = a.isInstanceOf[Node]
  override def equals(that: Any): Boolean =
    that match {
      case that: Node  => that.canEqual(this) && this.nodeId == that.nodeId
      case _ => false
    }

  override def toString: String = nodeId + ": " + updated
}
