package cloudvoting.methods

import cloudvoting.utils.datatypes._
import cloudvoting.utils.importpreflib.VerticesFromPreflib

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

package object scoringrules {

  /**
    * returns the scored graph with the plurality scores for a weighted graph (weighted by votes)
    *
    * @param graph : Graph[String, Long]
    * @param sc : SparkContext
    * @return
    */
  def Plurality(graph: Graph[String, Long], sc: SparkContext): ScoredGraph = {

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


  /**
    * returns the graph with the Copeland Score as second vertex attribute.
    * CopelandScore = number of votes preferring a over b - votes preferring b over a
    *
    * @param graph : Graph[String, Long]
    * @param sc : SparkContext
    * @return ScoredGraph
    */
  def Copeland(graph: Graph[String, Long], sc: SparkContext): ScoredGraph = {

    val out = graph.outDegrees
    val in = graph.inDegrees
    val scores: RDD[(VertexId, Long)] = out.join(in).map(degrees => (degrees._1, degrees._2._1-degrees._2._2))

    var resultgraph: Graph[(String, Long), VertexId] = graph.outerJoinVertices(scores) {
      case (id, name, Some(score)) => (name, score)
      case (id, name, None) => (name, 0)
    }

    resultgraph
  }


  /**
    * Returns the Simpson Winner of the Graph.
    *
    * @param graph : Graph[String, Long]
    * @param sc : SparkContext
    * @return ScoredGraph
    */
  def Simpson(graph: Graph[String, Long], sc: SparkContext): VertexId = {
    val MinGraph = graph.aggregateMessages[Long](triplet => triplet.sendToSrc(triplet.attr), (a, b) => Math.min(a, b))
    val x = MinGraph.max()
    x._1
  }


  /**
    * Reads from a .soc or .soi file (preflib format) and returns Vertices with Bordascores
    * @param filename : String = (hdfs) filepath to the input file
    * @param sc : SparkContext
    * @return : ScoredVertices with Bordascores
    */
  def BordaFromSoc(filename: String, sc: SparkContext): ScoredVertices = {

    // make file available to the SparkContext as an RDD with one entry per line
    // zipWithIndex() adds the line numbers to the RDD
    val file: RDD[(String, VertexId)] = sc.textFile(filename).zipWithIndex() //file with line numbers

    // the number of candidates is stored in the first line of the pwg file
    val n = file.first()._1.toLong

    val vertices: RDD[(VertexId, String)] = VerticesFromPreflib(file)


    //RDD with arrays of strings containing the preferences
    //first entry is the number of voters casting this vote - i.e. the weight
    val votes: RDD[((Long, Array[String]), Long)] = file.filter(line => (line._2 > (n + 1)))
      .map(entry => entry._1.split(","))
      .map(entry => (entry(0).toLong, entry.drop(1)))
      .zipWithIndex()

    val helper = votes.map(vote =>
      vote._1._2.zipWithIndex.transform(entry =>
        (entry._1, ((vote._1._2.length - entry._2) * vote._1._1.toInt))))

    val scores = helper.flatMap(vote => vote.toList)
      .reduceByKey((s1, s2) => s1 + s2)
      .map(entry => (entry._1.toLong, entry._2.toLong))

    val tmp = vertices.join(scores)

    val scoredvertices = VertexRDD(tmp)

    scoredvertices
  }

  /**
    * alias for BordaFromSoc
    */
  def BordaFromSoi(filename: String, sc: SparkContext): ScoredVertices = {
    BordaFromSoc(filename, sc)
  }



  /* calculates plurality score while reading the votes from a .soc or .soi file */
  def PluralityFromSoc(filename: String, sc: SparkContext): ScoredVertices = {
    /* returns the vertexRDD with plurality scores */
    val file: RDD[(String, VertexId)] = sc.textFile(filename).zipWithIndex() //file with line numbers

    //number of candidates is in the first row
    val n = file.first()._1.toLong

    val vertices: RDD[(VertexId, String)] = VerticesFromPreflib(file)

    val scores =  file.filter(line => (line._2 > (n + 1))).map(entry => entry._1.split(",")).map(entry => (entry(1).toLong, entry(0).toLong)).reduceByKey(_+_)

    val tmp = vertices.join(scores)

    val scoredvertices = VertexRDD(tmp)

    scoredvertices
  }

  /*alias for PluralityFromSoc */
  def PluralityFromSoi(filename: String, sc: SparkContext): ScoredVertices = {
    PluralityFromSoc(filename, sc)
  }


  /* calculates anit plurality score while reading the votes from a .soc or .soi file */
  def AntiPluralityFromSoc(filename: String, sc: SparkContext): ScoredVertices = {
    /* returns the vertexRDD with anti plurality scores */
    val file: RDD[(String, VertexId)] = sc.textFile(filename).zipWithIndex() //file with line numbers

    //number of candidates is in the first row
    val n = file.first()._1.toLong

    val vertices: RDD[(VertexId, String)] = VerticesFromPreflib(file)

    //RDD with arrays of strings containing the preferences
    //first entry is the number of voters casting this vote - i.e. the weight
    val votes: RDD[((VertexId, Array[String]), VertexId)] = file.filter(line => (line._2 > (n + 1))).map(entry => entry._1.split(",")).map(entry => (entry(0).toLong, entry.drop(2))).zipWithIndex()

    val helper: RDD[mutable.WrappedArray[(String, Int)]] = votes.map(vote => vote._1._2.zipWithIndex.transform(entry => (entry._1, vote._1._1.toInt)))

    val scores: RDD[(VertexId, Long)] = helper.flatMap(vote => vote.toList).reduceByKey((s1, s2) => s1 + s2).map(entry => (entry._1.toLong, entry._2.toLong))

    val tmp = vertices.join(scores)

    val scoredvertices = VertexRDD(tmp)

    scoredvertices
  }

  /*alias for PluralityFromSoc */
  def AntiPluralityFromSoi(filename: String, sc: SparkContext): ScoredVertices = {
    PluralityFromSoc(filename, sc)
  }
}
