package cloudvoting.utils

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


package object datatypes {
  type WeightedGraph = Graph[String,Long]
  type WeakDominanceGraph = Graph[String,Long]
  type StrictDominanceGraph = Graph[String,Long]
  type ScoredWeightedGraph = Graph[(String,Long), Long]
  type ScoredWeakDominanceGraph = Graph[(String,Long),Long]
  type ScoredStrictDominanceGraph = Graph[(String,Long),Long]
  type ScoredGraph = Graph[(String,Long),Long]
  type ScoredVertices = VertexRDD[(String,Long)]


  /**
    * Removing all edges with a weight equal to 0
    *
    * @param WeakGraph
    * @return StrictDominanceGraph
    */
  implicit def toStrict(WeakGraph: WeakDominanceGraph): StrictDominanceGraph = {
    Graph(vertices = WeakGraph.vertices, edges = WeakGraph.edges.filter(edge => edge.attr > 0))
  }

  /**
    * Removing all edges with a weight equal to 0
    *
    * @param WeakGraph
    * @return
    */
  implicit def toStrictScored(WeakGraph: ScoredWeakDominanceGraph): ScoredStrictDominanceGraph = {
    Graph(vertices = WeakGraph.vertices, edges = WeakGraph.edges.filter(edge => edge.attr > 0))
  }

  /**
    * Convert a WeightedGraph to a WeakDominanceGraph
    *
    * @param pwg
    * @return
    */
  implicit def toWeak(pwg: WeightedGraph): StrictDominanceGraph = {
    var revedges = pwg.reverse.mapEdges(edge => (-1) * edge.attr).edges
    /* create graph with old and reverse (negative) edges. Calculate the sum of the edges between each (src, dst) edge and only keep edges with a positive value --> dominance graph with weights) */
    Graph(vertices = pwg.vertices, edges = Graph(pwg.vertices, pwg.edges.union(revedges)).groupEdges((i, j) => i + j).edges.filter(e => e.attr >= 0))
  }


  /**
    * Convert a ScoredWeightedGraph to a ScoredWeakDominanceGraph
    *
    * @param pwg
    * @return
    */
  implicit def toWeakScored(pwg: ScoredWeightedGraph): ScoredWeakDominanceGraph = {
    var revedges = pwg.reverse.mapEdges(edge => (-1) * edge.attr).edges
    /* create graph with old and reverse (negative) edges.
    Calculate the sum of the edges between each (src, dst) edge
    and only keep edges with a positive value --> dominance graph with weights) */
    Graph(vertices = pwg.vertices, edges = Graph(pwg.vertices, pwg.edges.union(revedges)).groupEdges((i, j) => i + j).edges.filter(e => e.attr >= 0))
  }

  /**
    * Remove the scores from a ScoredWeightedGraph and return a WeightedGraph
    * @param spwg
    * @return
    */
  implicit def removeScores(spwg: ScoredWeightedGraph): WeightedGraph = {
    val vertices: RDD[(VertexId, String)] = spwg.vertices.map(vertex => (vertex._1, vertex._2._1))
    Graph(vertices = vertices,edges = spwg.edges)
  }

}
