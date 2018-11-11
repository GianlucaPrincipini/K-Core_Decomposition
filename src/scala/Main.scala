import graph.GraphReader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.graph.Node


object Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext(new SparkConf()
      .setMaster("local[*]")
      .setAppName("Main"))
    val graph = GraphReader.readFile("resources/facebook_combined.txt")
    println(graph.getDegree(new Node(0)))
  }
}
