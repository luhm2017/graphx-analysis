package com.lakala.datacenter.faund

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.lib.{ConnectedComponents, PageRank, StronglyConnectedComponents}
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/28 0028.
  * http://www.cnblogs.com/yueyebigdata/p/5893454.html
  */
object ApplyRandomForest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ApplyPageRank")
    val sc = new SparkContext(conf)
    // Control the log level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    if (sc.isLocal) FileUtils.deleteDirectory(new File(args(1)))
    val runFull = args(2)
    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)
    val graph = applyGraphAlgorithm(generateGraph(sc, args(0)))

  }

  /**
    *
    * @return
    */
  def applyGraphAlgorithm = (graph: Graph[Int, Int]) => {
    val pageRankGraph = graph.pageRank(0.00015)
    // Page Rank join with graph object
    val graphWithPageRank = graph.outerJoinVertices(pageRankGraph.vertices) {
      case (id, attr, Some(pr)) => (pr, attr)
      case (id, attr, None) => (0.0, attr)
    }

    //Triangle Count Algorithm
    val triangleComponent = graph.partitionBy(PartitionStrategy.RandomVertexCut)
      .triangleCount().vertices

    //Triangle Count join with page rank graph object
    val triByGraph = graphWithPageRank.outerJoinVertices(triangleComponent) {
      case (id, (rank, attr), Some(tri)) => (rank, tri, attr)
      case (id, (rank, attr), None) => (rank, 0, attr)
    }

    //Connected Component Algorithm
    val cComponent = ConnectedComponents.run(graph).vertices
    //Connected Component join with triangle component graph object
    val ccByGraph = triByGraph.outerJoinVertices(cComponent) {
      case (id, (rank, tri, attr), Some(cc)) => (rank, tri, cc, attr)
      case (id, (rank, tri, attr), None) => (rank, tri, -1L, attr)
    }

    //Strongly Connected Component Algorithm
    val stronglyConnected = StronglyConnectedComponents.run(graph, 3).vertices
    //Strongly Connected Component join with connected component object
    val stByGraph = ccByGraph.outerJoinVertices(stronglyConnected) {
      case (id, (rank, tri, cc, attr), Some(st)) => (rank, tri, cc, st, attr)
      case (id, (rank, tri, cc, attr), None) => (rank, tri, cc, id.toLong, attr)
    }
    stByGraph
  }

  /**
    * Return a RDD of (1-id, 2-id, similarity) where
    * 1-id < 2-id to avoid duplications
    *
    * @param graph
    * @return
    */

  def jaccardSimilarityAllApplys(graph: Graph[Int, Int]): RDD[(Long, Long, Double)] = {
    val neighbors = graph.collectNeighborIds(EdgeDirection.Either).map(x => (x._1, x._2))
    val combinations = neighbors.cartesian(neighbors)
    val SimilarityAll = combinations.map { x => (x._1._1, x._2._1, jaccard(x._1._2.toSet, x._2._2.toSet)) }
    val result = SimilarityAll.map(x => (x._3, (x._1, x._2))).sortByKey(false, 1).map(x => (x._2._1, x._2._2, x._1))
    result
  }

  /**
    * Helper function
    * Jaccard 系数定义为A与B交集的大小与A与B并集的大小的比值
    * Given two sets, compute its Jaccard similarity and return its result.
    * If the union part is zero, then return 0.
    * @param a
    * @param b
    * @tparam A
    * @return
    */
  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    val union: Double = (a ++ b).size
    val intersect: Double = a.intersect(b).size
    return (if (union == 0) 0.0 else (intersect / union))
  }

  /**
    *
    * @return
    */
  def generateGraph = (sc: SparkContext, input: String) => {
    val edges = sc.textFile(input).filter(filterLine).mapPartitions(lines =>
      lines.map { line =>
        val arr = line.replaceAll("&", "->").replaceAll("-", "").split(">")
        Edge(arr(0).toLong, arr(3).toLong, 1)
      })

    println("size++" + edges.count())
    Graph.fromEdges(edges, 1, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
      .partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((a, b) => a + b)
  }

  /**
    *
    * @param line
    * @return
    */
  def filterLine(line: String): Boolean = {

    val arr = line.replaceAll("&", "->").replaceAll("-", "").split(">")
    (arr(1) match {
      //      case s if (s.contains("&7") || s.contains("&13") || s.contains("&15") || s.contains("&16") || s.contains("&17") || s.contains("&19") || s.contains("&20")) => true
      case s if (s.contains("7") || s.contains("13") || s.contains("15") || s.contains("16")
        || s.contains("17") || s.contains("19") || s.contains("20")) => true
      case _ => false
    }) && (arr(4) match {
      case s if (s.contains("7") || s.contains("13") || s.contains("15") || s.contains("16")
        || s.contains("17") || s.contains("19") || s.contains("20")) => true
      case _ => false
    })
  }
}
