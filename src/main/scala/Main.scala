
import graph.{DistributedKCore, GraphReader}
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import java.util.{Calendar, Date}


object Main {
  var totmsg: BigInt = 0

  //val files = Map("facebook" -> "s3n://scpproject/facebook.txt", "pokec" -> "s3n://scpproject/pokec.txt", "livejournal" -> "s3n://scpproject/livejournal.txt")

  def main(args: Array[String]) {
    var currentFile = "facebook"
    if (args.size > 0) {
      currentFile = args(0)
    }
    val fileName = "resources/test.txt"

    //var fileName = files.get(currentFile).get
    // Set the log level to only print errors
     Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    var sessionBuilder = SparkSession.builder().appName("KCoreDecomposition").master("local[*]")


    if (args.size > 1) {
      sessionBuilder = sessionBuilder.master(args(1))
    }
    val session = sessionBuilder.getOrCreate()
    val maxIterations = 50

    val graph = new GraphReader().readFile(fileName, session.sparkContext)
    val kCore = DistributedKCore.decomposeGraph(graph, maxIterations)
    //kCore.vertices.sortBy(_._2.coreness).saveAsTextFile("s3n://scpproject/output_" + currentFile + ".txt")
    kCore.vertices.sortBy(_._2.coreness).saveAsTextFile("resources/output"+ new Date().getTime)
    graph.vertices.foreach(x => totmsg = totmsg + x._2.receivedMsg);
    print("Total messages in this execution = " + totmsg)
    session.close()
  }
}