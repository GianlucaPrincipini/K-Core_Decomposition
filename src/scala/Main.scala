
import graph.{DistributedKCore, GraphReader}
import org.apache.spark._
import org.apache.log4j._

object Main {

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val maxIterations = 10
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "GraphX")
    val fileName = "resources/kcoreTestDataset.txt"
    val graph = GraphReader.readFile(fileName)
    val kCore = DistributedKCore.decomposeGraph(graph, maxIterations);
    val alpha = GraphReader.readAlpha("resources/mappingAlphaIndex.txt")

    kCore.vertices.join(alpha).collect().take(15).sortBy(_._1).foreach(x => println(x._2._2, x._2._1.coreness))
  }
}
