package cloudvoting.utils

import cloudvoting.utils.datatypes._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.mutable

/**
  * Provides methods for importing files in PrefLib format to Spark and converting them to one of the Graph datatypes.
  *
  * In particular use
  *   readSoc(filename: String, sc: SparkContext): WeightedGraph
  *   readSoi(filename: String, sc: SparkContext): WeightedGraph
  *   readPwg(filename: String, sc: SparkContext): WeightedGraph
  * to read a WeightedGraph from .soc, .soi or .pwg files.
  *
  */
package object importpreflib {


  /**
    * Read .soc file and return a WeightedGraph.
    *
    * @param filename : String = (hdfs) filepath
    * @param sc : SparkContext
    * @return : WeightedGraph
    */
  def readSoc(filename: String, sc: SparkContext): WeightedGraph = {
    /* reads a soc file and transforms it into a pairwise weighted graph */
    val fileRDD: RDD[(String, Long)] = sc.textFile(filename).zipWithIndex() //file with line numbers

    //number of candidates is in the first row
    val n = fileRDD.first()._1.toLong

    val verticesRDD: RDD[(VertexId, String)] = VerticesFromPreflib(fileRDD)

    val votesRDD: RDD[(Long, Array[String])] = VotesFromSoc(fileRDD)

    var fulledges: RDD[((VertexId, VertexId), Long)] = votesRDD.flatMap(value =>
      edgewithweight(value._2, value._1, n)).reduceByKey(_+_, Math.round(n)).cache()

    Graph(verticesRDD, EdgeRDD.fromEdges(fulledges.map(edge => Edge(edge._1._1, edge._1._2, edge._2))))
  }

  /**
    * Read .soi file and return a WeightedGraph.
    *
    * @param filename : String = (hdfs) filepath
    * @param sc : SparkContext
    * @return : WeightedGraph
    */
  def readSoi(filename: String, sc: SparkContext): WeightedGraph = {
    readSoc(filename,sc)
  }

  /**
    * Reads from a .pwg-file and returns a WeightedGraph. The edges have the weight
    * corresponding to the number of voters supporting this edge.
    *
    * @param filename : String = (hdfs) filepath of the input file
    * @param sc : SparkContext
    * @return : WeightedGraph
    *
    */
  def readPwg(filename: String, sc: SparkContext): WeightedGraph = {

    // make file available to the SparkContext as an RDD with one entry per line
    // zipWithIndex() adds the line numbers to the RDD
    val fileRDD = sc.textFile(filename).zipWithIndex()

    // the number of candidates is stored in the first line of the pwg file
    val n = fileRDD.first()._1.toLong

    //Reading the vertices from the following n lines
    val vertices: RDD[(VertexId, String)] = VerticesFromPreflib(fileRDD)

    //Reading edges from the remaining lines of the file
    val edges: EdgeRDD[Long] =
      EdgeRDD.fromEdges(fileRDD.filter(line => (line._2 > (n + 1)))
        .map(entry => entry._1)
        .map(entry => entry.split(","))
        .map(entry => Edge(entry(1).toLong, entry(2).toLong, entry(0).toLong)))

    return Graph(vertices, edges)
  }

  /**
    * Reading the vertices together with their names from a PrefLib file.
    *
    * @param fileRDD :  RDD[(String, Long)])
    * @return : RDD containing VertexIDs and Names (String)
    */
  def VerticesFromPreflib(fileRDD: RDD[(String, VertexId)]): RDD[(VertexId, String)] = {
    val n = fileRDD.first()._1.toLong
    fileRDD.filter(line => (line._2 <= n && line._2 > 0)).map(entry => entry._1).map(entry => (entry.split(","))).map(entry => (entry(0).toLong, entry(1)))
  }


  /**
    * Reads Preferences (Votes) from a soc or soi file.
    *
    * Input is an RDD containing the lines of the inputfile with the preferences from a soc or soi file
    * Returns an RDD containing the preferences of the form: (weight, preferencelist)
    * ((Weight (number of votes supporting this preferencelist) = first entry in the line,
    * preferencelist = remaining entries in this line of the file as Array[String]))
    *
    *
    * @param fileRDD: RDD[(String, Long)])
    * @return RDD[(Long, Array[String])]
    *
    *
    **/
  def VotesFromSoc(fileRDD: RDD[(String, Long)]): RDD[(Long, Array[String])] = {
    val n = fileRDD.first()._1.toLong
    var votes: RDD[(String, Long)] = fileRDD.filter(line => (line._2 > (n + 1))).persist()
    var m: Long = votes.count()
    votes = votes.repartition(Math.max(Math.round(m/100),1))
    votes.map(entry => entry._1.split(",")).map(entry => (entry(0).toLong, entry.drop(1))).cache()
  }

  /**
    * Transforms an array of preferences to an RDD. Only the first occurrence is kept to eliminate possible repetitions of candidates.
    *
    * @param array : Array[String]
    * @param sc : SparkContex
    * @return RDD[(String, Int)]
    */
  def PrefToRDD(array : Array[String], sc: SparkContext): RDD[(String, Int)] ={
    sc.parallelize(array.zipWithIndex).reduceByKey((v1,v2) => Math.min(v1,v2))
  }




  /**
    * Reads Edges with weights from array with preferences read from .soc and .soi files
    *
    * @param array : array containing a preferencelist
    * @param weight : number of voters supporting this preferencelist
    * @param n : number of candidates
    * @return : List[((VertexId, VertexId),Long)] = Edges with weights
    */
  def edgewithweight(array: Array[String], weight: Long, n: Long): List[((VertexId, VertexId),Long)] ={
    var prefs: Array[VertexId] = array.map(entry => entry.toLong)
    var edgelist :mutable.ListBuffer[((VertexId, VertexId),Long)] = mutable.ListBuffer()
    var  notvoted: Set[VertexId] =  (1 to n.toInt).map(_.toLong).toSet.diff(prefs.toSet)

    //For each candidate not included in this vote create an edge from each candidate in the preferencelist with the corresponding weight
    //(equivalent of attaching all not included candidates at the last position of the preferencelist)
    for (elem <- prefs) {
      notvoted.foreach(entry => edgelist.append(((elem.toLong, entry.toLong), weight)))
    }

    // Create all weighted edges from the Preferencelist
    while(prefs.length > 1){
      val src = prefs(0)
      prefs = prefs.drop(1).distinct
      prefs.foreach(entry => edgelist.append(((src.toLong, entry.toLong),weight)))
    }

    edgelist.toList
  }

}
