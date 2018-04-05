package cloudvoting.examples

import cloudvoting.methods.scoringrules.Simpson
import cloudvoting.utils.importpreflib.readPwg
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SimpsonWinnerFromPwg {

  /**
    * Calculates the Simpson Winner from a PWG file. Only use PrefLib format as input!
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
      conf.setAppName("CloudVoting Example: SimpsonWinnerFromPwg")
      val sc = new SparkContext(conf)

      println("CloudVoting: Reading File from "+ args(0))

      val graph = readPwg(args(0), sc)
      val Winner = Simpson(graph, sc)


      println("CloudVoting: The Simpson winner has ID "+ Winner)
    }
  }
}
