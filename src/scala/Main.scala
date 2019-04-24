
import graph.{DistributedKCore, GraphReader}
import org.apache.spark._
import org.apache.log4j._

object Main {

  def main(args: Array[String]) {
    var fileName = "resources/kcoreTestDataset.txt"
    if (args.size > 0) {
      fileName = args(0)
    }

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sparkConf = new SparkConf()
      .setAppName("KCoreDecomposition")
      .setMaster("local[*]");
    val sc = new SparkContext(sparkConf)
    val maxIterations = 10
    val graph = GraphReader.readFile(fileName)
    val kCore = DistributedKCore.decomposeGraph(graph, maxIterations)
    kCore.vertices.collect().take(15).sortBy(_._1).foreach(x => println(x._2))
  }
}
