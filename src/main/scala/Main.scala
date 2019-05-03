
import graph.{DistributedKCore, GraphReader}
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import java.util.{Calendar, Date}


object Main {
  var totmsg: BigInt = 0

  //val files = Map("facebook" -> "s3n://scpproject/facebook.txt", "pokec" -> "s3n://scpproject/pokec.txt", "livejournal" -> "s3n://scpproject/livejournal.txt")
  val files = Map("facebook" -> "resources/facebook.txt", "pokec" -> "resources/pokec.txt", "livejournal" -> "resources/livejournal.txt")

  def main(args: Array[String]) {
    val startTimeStamp = new Date().getTime
    var currentFile = "facebook"
    val appName = "KCoreDecomposition"
    val sparkConf = new SparkConf().setAppName(appName)
    var numberOfExecutors = 1
    println(args.size)
    if (args.size > 0) {
      currentFile = args(0)
    }
    if (args.size == 2) {
      numberOfExecutors = args(1).toInt
      sparkConf.setAppName(appName + "_" + numberOfExecutors)
    }  else {
      sparkConf.setMaster("local[*]")
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
    //kCore.vertices.sortBy(_._2.coreness).saveAsTextFile("s3n://scpproject/output_" + currentFile + ".txt")
    kCore.vertices.sortBy(_._2.coreness).saveAsTextFile("resources/output/" + appName + "/" + numberOfExecutors + "/" + startTimeStamp)
    kCore.vertices.foreach(x => totmsg = totmsg + x._2.receivedMsg);
    println("Total messages in this execution = " + totmsg)
    session.close()
  }
}