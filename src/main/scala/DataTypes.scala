/**
  * Created by theresa csar on 08.09.2017.
  */

package DataTypes

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphDataTypes {
  type PairwiseWeightedGraph = Graph[String,Long]
  type WeakDominanceGraph = Graph[String,Long]
  type StrictDominanceGraph = Graph[String,Long]
  type ScoredPairwiseWeightedGraph = Graph[(String,Long), Long]
  type ScoredWeakDominanceGraph = Graph[(String,Long),Long]
  type ScoredStrictDominanceGraph = Graph[(String,Long),Long]
  type ScoredGraph = Graph[(String,Long),Long]
  type ScoredVertices = VertexRDD[(String,Long)]
  type SchwartzVertex = (Long,Long, String, Long, Boolean)
  type SchwartzMaxVertex = (Long, Long, Long, Long, String, Long, Boolean)
  type SchwartzSet = VertexRDD[SchwartzVertex]


  /* The weak dominance graph has to have weights */
  implicit def toStrict(WeakGraph: WeakDominanceGraph): StrictDominanceGraph = {
    Graph(vertices = WeakGraph.vertices, edges = WeakGraph.edges.filter(edge => edge.attr > 0))
  }

  implicit def toStrictScored(WeakGraph: ScoredWeakDominanceGraph): ScoredStrictDominanceGraph = {
    Graph(vertices = WeakGraph.vertices, edges = WeakGraph.edges.filter(edge => edge.attr > 0))
  }

  implicit def toWeak(pwg: PairwiseWeightedGraph): WeakDominanceGraph = {
    var revedges = pwg.reverse.mapEdges(edge => (-1) * edge.attr).edges
    /* create graph with old and reverse (negative) edges. Calculate the sum of the edges between each (src, dst) edge and only keep edges with a positive value --> dominance graph with weights) */
    Graph(vertices = pwg.vertices, edges = Graph(pwg.vertices, pwg.edges.union(revedges)).groupEdges((i, j) => i + j).edges.filter(e => e.attr >= 0))
  }

  implicit def toWeakScored(pwg: ScoredPairwiseWeightedGraph): ScoredWeakDominanceGraph = {
    var revedges = pwg.reverse.mapEdges(edge => (-1) * edge.attr).edges
    /* create graph with old and reverse (negative) edges. Calculate the sum of the edges between each (src, dst) edge and only keep edges with a positive value --> dominance graph with weights) */
    Graph(vertices = pwg.vertices, edges = Graph(pwg.vertices, pwg.edges.union(revedges)).groupEdges((i, j) => i + j).edges.filter(e => e.attr >= 0))
  }

  implicit def removeScores(spwg: ScoredPairwiseWeightedGraph): PairwiseWeightedGraph = {
    val vertices: RDD[(VertexId, String)] = spwg.vertices.map(vertex => (vertex._1, vertex._2._1))
    Graph(vertices = vertices,edges = spwg.edges)
  }


}

