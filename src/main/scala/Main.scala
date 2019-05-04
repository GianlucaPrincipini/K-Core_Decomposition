
import graph.{DistributedKCore, GraphReader}
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import java.util.{Calendar, Date}
import java.io.PrintWriter

object Main {
  val files = Map("facebook" -> "s3n://scpproject/input/facebook.txt", "pokec" -> "s3n://scpproject/input/pokec.txt", "livejournal" -> "s3n://scpproject/input/livejournal.txt")
  // val files = Map("facebook" -> "resources/facebook.txt", "pokec" -> "resources/pokec.txt", "livejournal" -> "resources/livejournal.txt", "test" -> "resources/test.txt")

  def main(args: Array[String]) {
    val startTimeStamp = new Date().getTime
    var currentFile = "test"
    val appName = "KCoreDecomposition"
    val sparkConf = new SparkConf().setAppName(appName)
    if (args.size > 0) {
      currentFile = args(0)
    }

    val fileName = files.get(currentFile).get
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine


    var sessionBuilder = SparkSession.builder().config(sparkConf)
    if (args.size > 1) {
      sessionBuilder = sessionBuilder.master(args(1))
    }
    val session = sessionBuilder.getOrCreate()

    val maxIterations = 10

    val graph = new GraphReader().readFile(fileName, session.sparkContext)

    val kCore = DistributedKCore.decomposeGraph(graph, maxIterations)
    val outputDestination = "s3n://scpproject/output/" + appName + "/" + currentFile + "/" + startTimeStamp
    kCore.vertices.sortBy(_._2.coreness).saveAsTextFile(outputDestination)
    // kCore.vertices.sortBy(_._2.coreness).saveAsTextFile("resources/output/" + appName + "/" + currentFile + "/" + startTimeStamp)
    val totmsg = kCore.vertices.map(x => x._2.receivedMsg).sum()
    val outputMsg = "Total messages in this execution = " + totmsg
    println(outputMsg)
    session.close()
  }
}