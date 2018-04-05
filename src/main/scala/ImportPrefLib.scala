/**
  * Created by theresa csar on 04.08.2017.
  */

package ReadPrefLib

import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.collection.mutable
import DataTypes.GraphDataTypes._
import org.apache.spark.rdd.RDD


object ReadPrefLib {

  /* reads the graph from a .pwg file */
  def readpwg(filename: String, sc: SparkContext): PairwiseWeightedGraph = {
    val file = sc.textFile(filename).zipWithIndex() //file with line numbers

    //number of candidates is in the first row
    val n = file.first()._1.toLong

    val vertices: RDD[(VertexId, String)] = file.filter(line => (line._2 <= n && line._2 > 0)).map(entry => entry._1).map(entry => (entry.split(","))).map(entry => (entry(0).toLong, entry(1)))

    val edges: EdgeRDD[Long] = EdgeRDD.fromEdges(file.filter(line => (line._2 > (n + 1))).map(entry => entry._1).map(entry => entry.split(",")).map(entry => Edge(entry(1).toLong, entry(2).toLong, entry(0).toLong)))

    return Graph(vertices, edges)
  }

  /* calculates borda score while reading the votes from a .soc or .soi file */
  def BordaFromSoc(filename: String, sc: SparkContext): ScoredVertices = {
    /* returns a graph with bordascores at each vertex */
    val file: RDD[(String, VertexId)] = sc.textFile(filename).zipWithIndex() //file with line numbers

    //number of candidates is in the first row
    val n = file.first()._1.toLong

    val vertices: RDD[(VertexId, String)] = VerticesFromPreflib(file)


    //RDD with arrays of strings containing the preferences
    //first entry is the number of voters casting this vote - i.e. the weight
    val votes: RDD[((Long, Array[String]), Long)] = file.filter(line => (line._2 > (n + 1))).map(entry => entry._1.split(",")).map(entry => (entry(0).toLong, entry.drop(1))).zipWithIndex()

    val helper = votes.map(vote => vote._1._2.zipWithIndex.transform(entry => (entry._1, ((vote._1._2.length - entry._2) * vote._1._1.toInt))))

    val scores = helper.flatMap(vote => vote.toList).reduceByKey((s1, s2) => s1 + s2).map(entry => (entry._1.toLong, entry._2.toLong))

    val tmp = vertices.join(scores)

    val scoredvertices = VertexRDD(tmp)

    scoredvertices
  }

  /*alias for BordaFromSoc */
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

  def VerticesFromPreflib(file: RDD[(String, VertexId)]): RDD[(VertexId, String)] = {
    val n = file.first()._1.toLong
    file.filter(line => (line._2 <= n && line._2 > 0)).map(entry => entry._1).map(entry => (entry.split(","))).map(entry => (entry(0).toLong, entry(1)))
  }

  /* get the lines containing the votes from a soc or soi file */
  /* the RDD is of the form: ((Weight = first entry in the line, remaining entries in the line (preferences) as Array[String]), VoteID) */
  def VotesFromSoc(file: RDD[(String, VertexId)]): RDD[((VertexId, Array[String]), VertexId)]   = {
    val n = file.first()._1.toLong
    file.filter(line => (line._2 > (n + 1))).map(entry => entry._1.split(",")).map(entry => (entry(0).toLong, entry.drop(1))).zipWithIndex()
  }

  def min(v1: Int, v2: Int): Int = {
    if(v1 < v2){
      return v1
    }else{
      return v2
    }
  }

  def PrefToRDD(array : Array[String], sc: SparkContext): RDD[(String, Int)] ={
    /* transforms array of preferences to an RDD. Only the first occurence is kept */
    sc.parallelize(array.zipWithIndex).reduceByKey((v1,v2) => min(v1,v2))
  }

  def readSoc(filename: String, sc: SparkContext): PairwiseWeightedGraph = {
    /* reads a soc file and transforms it into a pairwise weighted graph */
    val file: RDD[(String, VertexId)] = sc.textFile(filename).zipWithIndex() //file with line numbers

    //number of candidates is in the first row
    val n = file.first()._1.toLong

    val vertices: RDD[(VertexId, String)] = VerticesFromPreflib(file)

    /* RDD[((Weight, Preferences),VoteID)] */
    val votes: RDD[((Long, Array[String]), Long)] = VotesFromSoc(file)

    println("CloudVotingSoc: transform to edges: ")

    var fulledges = votes.flatMap(value => edgewithweight(value._1._2, value._1._1)).reduceByKey(_+_)

    println("CloudVotingSoc: transform to edges done")

    fulledges.map(edge => Edge(edge._1._1, edge._1._2, edge._2))

    Graph(vertices, EdgeRDD.fromEdges(fulledges.map(edge => Edge(edge._1._1, edge._1._2, edge._2))))
  }


  def edgewithweight(array: Array[String], weight: Long): List[((VertexId, VertexId),Long)] ={
    var prefs = array.clone()
    var edgelist :mutable.ListBuffer[((VertexId, VertexId),Long)] = mutable.ListBuffer()
    while(prefs.length > 1){
      val src = prefs(0)
      prefs = prefs.drop(1)
      prefs.foreach(entry => edgelist.append(((src.toLong, entry.toLong),weight)))
    }
    edgelist.toList
  }


}
