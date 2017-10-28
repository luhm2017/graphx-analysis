package com.lakala.datacenter.main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linyanshi on 2017/9/19 0019.
  */
object CallHistoryPageRank {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("CallHistoryPageRank")
    val sc = new SparkContext(conf)
    val edgeRdd = sc.textFile(args(0)).mapPartitions(lines => lines.map { line =>
      //      val arr = line.split("\t")
      //      Edge(arr(0).toLong, arr(1).toLong, 1)
      val arr = line.split(",")
                  Edge(arr(1).toLong, arr(2).toLong, 1)
//      Edge(arr(2).toLong, arr(1).toLong, arr(3).toInt)
    })
    //    val graph = GraphLoader.edgeListFile(sc, args(0), numEdgePartitions = 4)

    val graph = Graph.fromEdges(edgeRdd, 1, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
    //参数：图，迭代次数
    val pageRankGraph = graph.pageRank(0.0001)

    pageRankGraph.vertices.sortBy(x => x._2).mapPartitions(ls => ls.map(k => s"${k._1},${k._2}")).repartition(1).saveAsTextFile(args(1))
    sc.stop()
  }
}
