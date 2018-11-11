package graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.graph.{Graph, Node}

object GraphReader {
  val sc = SparkContext.getOrCreate()

  /**
    * Nel file le associazioni sono monodirezionali, per rendere bidirezionale ogni arco abbiamo ripetuto la procedura su
    * @param fileName
    * @return
    */
  def readFile(fileName: String): Graph= {
    val graph1 = sc.textFile(fileName).map(x => splitNode(x, false))
    new Graph((graph1 ++ sc.textFile(fileName).map(x => splitNode(x, true))).distinct())
  }

  /**
    * Da riga del file a tupla di interi
    * @param line riga del file
    * @param inverted inverti la tupla
    * @return tupla di interi
    */
  def split(line: String, inverted: Boolean): (Int, Int) = {
    val splitted = line.split(" ")
    if (inverted)
      (splitted(1).toInt, splitted(0).toInt)
    else
      (splitted(0).toInt, splitted(1).toInt)
  }

  def splitNode(line: String, inverted: Boolean): (Node, Node) = {
    val splitted = line.split(" ")
    if (inverted)
      (new Node(splitted(1).toInt), new Node(splitted(0).toInt))
    else
      (new Node(splitted(0).toInt), new Node(splitted(1).toInt))
  }
}
