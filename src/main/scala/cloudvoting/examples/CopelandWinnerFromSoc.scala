package cloudvoting.examples

import cloudvoting.methods.scoringrules.{Copeland, Winner}
import cloudvoting.utils.importpreflib.readSoc
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CopelandWinnerFromSoc {

  /**
    * Calculates the Copeland Winner from a SOC file. Only use PrefLib format as input!
    *
    * @param args: filepath of the input file
    */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    if(args.length == 0){
      println("Please provide the filepath of the input file as argument")
    }else {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("CloudVoting Example: CopelandWinnerFromSoc")
      val sc = new SparkContext(conf)

      println("CloudVoting: Reading File from "+ args(0))

      val graph = readSoc(args(0), sc)
      val VerticesRDD = Copeland(graph, sc)

      val CopelandWinner = Winner(VerticesRDD)

      println("CloudVoting: The Copeland winner is " + CopelandWinner._2._1 + " (VertexId = "+CopelandWinner._1+") with score " + CopelandWinner._2._2)
    }
  }
}
