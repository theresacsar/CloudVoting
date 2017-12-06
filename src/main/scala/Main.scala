/**
  * Created by theresa csar on 04.08.2017.
  */
package main.scala

import DataTypes.GraphDataTypes
import DataTypes.GraphDataTypes.PairwiseWeightedGraph
import ReadPrefLib._
import WinnerDetermination._
import org.apache.spark.sql.SparkSession

object Main {


  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("CloudVoting")
      .getOrCreate()


    //  val filename = "C:/Users/there/IdeaProjects/CloudVoting/src/main/resources/ED-00011-00000003.pwg"
    val filename = "C:/Users/there/IdeaProjects/CloudVoting/src/main/resources/ED-00011-00000003.soc"
    //val filename = "C:/Users/there/IdeaProjects/CloudVoting/src/main/resources/spotify.soi"

    // ReadPrefLib.readpwg(filename, sparkSession.sparkContext)

    val bordavertices = ReadPrefLib.BordaFromSoc(filename, sparkSession.sparkContext)
    val bordawinner = WinnerDetermination.Winner(bordavertices)
    println("CloudVoting: the borda winner: " + bordawinner)


    /*

      val pluralityvertices = ReadPrefLib.PluralityFromSoc(filename, sparkSession.sparkContext)

      val pluralwinner = WinnerDetermination.Winner(pluralityvertices)
      println("CloudVoting: the plurality winner: " + pluralwinner)
      val pluralwinnerset = pluralityvertices.filter(vertex => vertex._2._2==pluralwinner._2._2)
      println("CloudVoting: the plurality winners: ")
      pluralwinnerset.foreach(entry => println("CloudVoting: plural " + entry ))

      val graph: PairwiseWeightedGraph = ReadPrefLib.readSoc(filename, sparkSession.sparkContext)
      val copelandscores = WinnerDetermination.CopelandScores(GraphDataTypes.toWeak(graph),sparkSession.sparkContext)
      val winner = WinnerDetermination.Winner(copelandscores)
      println("CloudVoting: the copeland winner: " + winner)

      val winnerset = graph.vertices.filter(vertex => vertex._1==winner._2._2)
      println("CloudVoting: the copeland winners: ")
      winnerset.foreach(entry => println("CloudVoting: copeland " + entry ))


      val otherscores = WinnerDetermination.Scores(GraphDataTypes.toWeak(graph),sparkSession.sparkContext)
      println("CloudVoting: the other winner: " + WinnerDetermination.Winner(otherscores)._2)

      println("CloudVoting: transform to strict dominance graph")
      val strict = GraphDataTypes.toStrict(graph)
      System.out.println("CloudVoting: The strict dominance graph has " + strict.numVertices + " vertices end " + strict.numEdges + " edges")


      val sccs = strict.stronglyConnectedComponents(10)
      System.out.println("CloudVoting: There are "+sccs.vertices.collect().distinct.length+" sccs in the input graph")


     /* DG stands for dominance graph
    *   - if one alternative is dominated by another than there is only one edge connecting them (a dom b : a -> b)
    *   - the weight of the edge is the difference in the votes
    *   - if there is a tie, than there are two edges with weight 0 in both directions (a -> b, b -> a)
    * */
    val DG = GraphDataTypes.toWeak(ReadPrefLib.readpwg("src/main/resources/ED-00011-00000009.pwg", sc))

    System.out.println("Result Graph with "DG.numVertices + " vertices end " + DG.numEdges + " edges")

    val DG2 = ReadPrefLib.fromsoctoDG("src/main/resources/ED-00011-00000001.soc", sc)

   System.out.println("Result Graph 2 with " + DG2.numVertices + " vertices end " + DG2.numEdges + " edges")



    System.out.println("The graph has " + DG.numVertices + " vertices end " + DG.numEdges + " edges")

    val DG2 = GraphDataTypes.toWeakScored(ReadPrefLib.readSocWithScore("src/main/resources/ED-00011-00000009.soi", sc))
    System.out.println("The graph has " + DG2.numVertices + " vertices end " + DG2.numEdges + " edges")

    System.out.println("Get vertex with highest borda score:")

    val BordaWinner = DG2.vertices.collect().reduce((v1,v2) => if(v1._2._2 >= v2._2._2){v1}else{v2})

    System.out.println("The borda winner is: "+ BordaWinner.toString())

    val maximum =  DG2.outDegrees.reduce((v1,v2) => if(v1._2>v2._2) {v1 } else {v2})


    System.out.println("The vertex with the maximum outdegree is "+ maximum.toString())

    System.out.println("are there more vertices with this outdegree?")
    DG2.outDegrees.filter(v => (v._2 == maximum._2)).foreach(System.out.println(_))

    val copelandscores = WinnerDetermination.CopelandScores(DG, sc)

    System.out.println("Copeland Graph with " + copelandscores.numVertices + " vertices end " + copelandscores.numEdges + " edges")

    val winner = WinnerDetermination.Winner(copelandscores)

    System.out.println("and the copeland winner in graph 1 is: "+ winner)


    val copelandscores2 = WinnerDetermination.CopelandScores(GraphDataTypes.removeScores(DG2), sc)

    System.out.println("Copeland Graph with " + copelandscores2.numVertices + " vertices end " + copelandscores2.numEdges + " edges")

    val winner2 = WinnerDetermination.Winner(copelandscores2)

    System.out.println("and the copeland winner in graph 2 is: "+ winner2)
  }*/
  }
}