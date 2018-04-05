package cloudvoting.examples

import cloudvoting.methods.scoringrules.{Plurality, Winner}
import cloudvoting.utils.importpreflib.readPwg
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object PluralityWinnerFromPwg {

  /**
    * Calculates the Plurality Winner from a PWG file. Only use PrefLib format as input!
    *
    * @param args: filepath of the input file
    */
  def main(args: Array[String]): Unit = {
    if(args.length == 0){
      println("Please provide the filepath of the input file as argument")
    }else {
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)

      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("CloudVoting Example: PluralityWinnerFromPwg")
      val sc = new SparkContext(conf)

      println("CloudVoting: Reading File from " + args(0))

      val graph = readPwg(args(0), sc)
      val PluralityVerticesRDD = Plurality(graph, sc)

      val PluralityWinner = Winner(PluralityVerticesRDD)

      println("CloudVoting: The Plurality winner is " + PluralityWinner._2._1 + " (VertexId = "+PluralityWinner._1+") with score " + PluralityWinner._2._2)
    }
  }
}
