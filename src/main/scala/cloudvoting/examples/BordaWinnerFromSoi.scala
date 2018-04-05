package cloudvoting.examples

import cloudvoting.methods.scoringrules.{BordaFromSoi, Winner}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object BordaWinnerFromSoi {

  /**
    * Calculates the Borda Winner from a SOI file. Only use PrefLib format as input!
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
      conf.setAppName("CloudVoting Example: BordaWinnerFromSoi")
      val sc = new SparkContext(conf)

      println("CloudVoting: Reading File from " + args(0))

      val BordaVerticesRDD = BordaFromSoi(args(0), sc)

      val BordaWinner = Winner(BordaVerticesRDD)

      println("CloudVoting: The Borda winner is " + BordaWinner._2._1 + " (VertexId = "+BordaWinner._1+") with score " + BordaWinner._2._2)
    }
  }
}
