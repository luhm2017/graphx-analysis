package com.lakala.datacenter.main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linyanshi on 2017/9/20 0020.
  * http://blog.sina.com.cn/s/blog_482da2d20102drpt.html
  */
object PICCallAlgorithm {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("PICCallAlgorithm")

    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0), 200)
    val edges = data.map(line => {
      val items = line.split(",")
      Edge(items(1).toLong, items(2).toLong, 1)
      //            val items = line.split("\t")
      //                  Edge(items(0).toLong, items(1).toLong, 1)
    })
    val graph = Graph.fromEdges(edges, 1, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
    //参数：图，迭代次数
    val pageRankGraph = graph.pageRank(0.0001)
    val pic = new PowerIterationClustering().setK(args(2).toInt).setMaxIterations(args(3).toInt).setInitializationMode("degree")
    val model = pic.run(pageRankGraph)
    val result = model.assignments.map(a => (a.id, a.cluster))
    result.mapPartitions(ves => ves.map(ve => s"${ve._1},${ve._2}")).repartition(1).saveAsTextFile(args(1))
    //        val landmarks = sc.textFile("/user/guozhijie/explortoutput/louvainout4")
    //          .mapPartitions(lines=>lines.map(line=>{val arr =line.split(",")
    //            arr(1).toLong})).distinct().top(args(2).toInt)
    //    val landmarks = data.map(line => {
    //      val items = line.split(",")
    //      items(1).toLong
    //    }).distinct().top(args(2).toInt)
    //    val landmarksBR = sc.broadcast(landmarks)
    //    val shortPathGraph = ShortestPaths.run(graph, landmarksBR.value)
    //    graph.unpersist()
    //
    //    implicit def iterebleWithAvg[T: Numeric](data: Iterable[T]) = new {
    //      def avg = average(data)
    //    }
    //
    //    def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    //      num.toDouble(ts.sum) / ts.size
    //    }
    //
    //    shortPathGraph.vertices.map {
    //      vx =>
    //        (vx._1, {
    //          val dx = 1.0 / vx._2.map {
    //            sx => sx._2
    //          }.seq.avg
    //          val d = if (dx.isNaN | dx.isNegInfinity | dx.isPosInfinity) 0.0 else dx
    //          d
    //        })
    //    }.sortBy({ vx => vx._1 }, ascending = true)
    //      .mapPartitions(rows => rows.filter(k => k._2 > 0d).map(row => s"${row._1},${row._2}")).repartition(1).saveAsTextFile(args(1))
    //        val similarities = Jaccard.jaccardSimilarityAllMobiles(graph)
    //        val centralityGraph: Graph[(Double,Double),Int] = graph.hits(VertexMeasureConfiguration(treatAsUndirected=true))
    //    val picLabels = PowerIterationClustering.runPIC(similarities)
    //    picLabels.mapPartitions(lca => lca.map(l => s"${l._1},${l._2}")).repartition(1).saveAsTextFile(args(1))

    //        val vertexembeddedness = graph.closenessCentrality(VertexMeasureConfiguration(treatAsUndirected = true))
    //        vertexembeddedness.vertices.mapPartitions(ves=>ves.map(ve=>s"${ve._1},${ve._2}")).repartition(1).saveAsTextFile(args(1))
    sc.stop()
  }
}
