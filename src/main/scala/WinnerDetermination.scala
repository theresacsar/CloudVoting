/**
  * Created by there on 14.08.2017.
  */
package WinnerDetermination

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD

import DataTypes.GraphDataTypes._


object WinnerDetermination {

  /* returns the graph with the number of votes preferring a over b - votes preferring b over a  as second vertex attribute */
  def Scores(graph: Graph[String, Long], sc: SparkContext): ScoredGraph = {

    val outgoing = graph.triplets.map(triplet => (triplet.srcId, triplet.attr)).reduceByKey((a, b) => a + b)
    val incoming = graph.triplets.map(triplet => (triplet.dstId, triplet.attr)).reduceByKey((a, b) => 0 - (a + b))
    val scores: RDD[(VertexId, Long)] = outgoing.union(incoming).reduceByKey((a, b) => a + b)

    var resultgraph: ScoredGraph = graph.outerJoinVertices(scores) {
      case (id, name, Some(score)) => (name, score)
      case (id, name, None) => (name, 0)
    }

    resultgraph
  }

  /* input is a graph as returned by the method Scores or CopelandScores and returns the Candidate with the highest Score*/
  def Winner(scores: ScoredGraph): (VertexId, (String, VertexId)) = {
    val res = scores.vertices.reduce((vertex1, vertex2) =>
      if (vertex1._2._2 > vertex2._2._2) {
        vertex1
      } else vertex2)
    res
  }

  /* input are scoredvertices */
  def Winner(scores: ScoredVertices): (VertexId, (String, VertexId)) = {
    val res: (VertexId, (String, VertexId)) = scores.reduce((vertex1, vertex2) =>
      if (vertex1._2._2 > vertex2._2._2) {
        vertex1
      } else vertex2)

    res
  }


  /* returns the graph with the number of votes preferring a over b - votes preferring b over a  as second vertex attribute */
  def CopelandScores(graph: Graph[String, Long], sc: SparkContext): ScoredGraph = {

    val out = graph.outDegrees
    val in = graph.inDegrees
    val scores: RDD[(VertexId, Long)] = out.join(in).map(degrees => (degrees._1, degrees._2._1-degrees._2._2))
    //  sfg.join(graph.inDegrees).reduceByKey((out, in) => out - in)

  //  val scores : RDD[(VertexId, Long)] = graph.triplets.flatMap(triplet => List((triplet.srcId, 1L),(triplet.dstId, -1L))).reduceByKey((a, b) => a + b)

    var resultgraph: Graph[(String, Long), VertexId] = graph.outerJoinVertices(scores) {
      case (id, name, Some(score)) => (name, score)
      case (id, name, None) => (name, 0)
    }

    resultgraph
  }


  def min(a: Long, b: Long): Long = {if (a>b) b else a}
  def max(a: Long, b: Long): Long = {if (a>b) a else b}




}