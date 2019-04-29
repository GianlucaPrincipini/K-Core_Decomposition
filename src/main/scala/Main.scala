
import graph.{DistributedKCore, GraphReader}
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Main {

  val files = Map("facebook" -> "s3n://scpproject/facebook.txt", "pokec" -> "s3n://scpproject/pokec.txt", "livejournal" -> "s3n://scpproject/livejournal.txt")


  def main(args: Array[String]) {
    var currentFile = "facebook"
    // var fileName = "resources/facebook_combined.txt"
    if (args.size > 0) {
      currentFile = args(0)
    }
    var fileName = files.get(currentFile).get
    // Set the log level to only print errors
    // Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    var sessionBuilder = SparkSession.builder().appName("KCoreDecomposition")


    if (args.size > 1) {
      sessionBuilder = sessionBuilder.master(args(1))
    }
    val session = sessionBuilder.getOrCreate()
    val maxIterations = 10

    val graph = new GraphReader().readFile(fileName, session.sparkContext)
    val kCore = DistributedKCore.decomposeGraph(graph, maxIterations)
    kCore.vertices.sortBy(_._2.coreness).saveAsTextFile("s3n://scpproject/output_" + currentFile + ".txt")
    session.close()
  }
}
