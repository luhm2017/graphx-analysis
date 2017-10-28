package com.lakala.datacenter.main

import com.lakala.datacenter.louvain.LouvainCore
import org.apache.spark.graphx.lib.{LabelPropagation, PageRank}
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkConf, SparkContext}
import ml.sparkling.graph.operators.OperatorsDSL._
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN

import scala.collection.mutable

/**
  * Created by linyanshi on 2017/9/25 0025.
  */
object Analytics extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: Analytics <taskType> <file> --numEPart=<num_edge_partitions> [other options]")
      System.err.println("Supported 'taskType' as follows:")
      System.err.println("  pagerank    Compute PageRank")
      System.err.println("  pic         Compute the graph of vertices distince")
      System.err.println("  lpa         Compute the graph of vertices lable")
      System.err.println("  pscan         Compute the graph of vertices lable")
      System.err.println("  louvain         Compute the graph of vertices communid")
      System.exit(1)
    }

    val taskType = args(0)
    val fname = args(1)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString(_))
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_AND_DISK_SER)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_AND_DISK_SER)

    taskType match {
      case "pagerank" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)
        val numRepartition = options.remove("numRep").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("PageRankAlgorithm"))

        val edgeRdd = sc.textFile(fname).mapPartitions(lines => lines.map { line =>
          val arr = line.split(",")
          Edge(arr(1).toLong, arr(2).toLong, arr(3).toInt)
        })

        val unpartitionedGraph = Graph.fromEdges(edgeRdd, 1, edgeStorageLevel, vertexStorageLevel)
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val pr = (numIterOpt match {
          case Some(numIter) => PageRank.run(graph, numIter)
          case None => PageRank.runUntilConvergence(graph, tol)
        }).vertices.cache()

        println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_ + _))

        if (!outFname.isEmpty) {
          logWarning("Saving pageranks of pages to " + outFname)
          pr.map { case (id, r) => s"$id,$r" }.repartition(numRepartition.getOrElse(10)).saveAsTextFile(outFname)
        }

        sc.stop()

      case "lap" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)
        val numRepartition = options.remove("numRep").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("=============================================")
        println("|             LabelPropagation               |")
        println("=============================================")

        val sc = new SparkContext(conf.setAppName("LabelPropagationAlgorithm"))

        val edgeRdd = sc.textFile(fname).mapPartitions(lines => lines.map { line =>
          val arr = line.split(",")
          Edge(arr(1).toLong, arr(2).toLong, arr(3).toInt)
        })

        val unpartitionedGraph = Graph.fromEdges(edgeRdd, 1, edgeStorageLevel, vertexStorageLevel)

        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val lpaGraph = (numIterOpt match {
          case Some(numIter) => LabelPropagation.run(graph, numIter)
        }).cache()
        val modularity = lpaGraph.modularity()
        println(s"GRAPHX: modularity:   ${modularity}")

        if (!outFname.isEmpty) {
          logWarning("Saving LabelPropagation graph with vertex attributes containing the label of community affiliation " + outFname)
          lpaGraph.vertices.filter(k => k._1 != k._2).sortBy(x => x._2)
            .mapPartitions(ls => ls.map(k => s"${k._1},${k._2}")).repartition(numRepartition.getOrElse(10)).saveAsTextFile(outFname)
        }
        sc.stop()

      case "pscan" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)
        val numRepartition = options.remove("numRep").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("=============================================")
        println("|             PSCAN               |")
        println("=============================================")

        val sc = new SparkContext(conf.setAppName("PSCANAlgorithm"))

        val edgeRdd = sc.textFile(fname).mapPartitions(lines => lines.map { line =>
          val arr = line.split(",")
          Edge(arr(1).toLong, arr(2).toLong, arr(3).toInt)
        })

        val unpartitionedGraph = Graph.fromEdges(edgeRdd, 1, edgeStorageLevel, vertexStorageLevel)

        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val pscanGraph = (numIterOpt match {
          case Some(numIter) => PSCAN.computeConnectedComponents(graph, 0.000001)
        }).cache()
        val modularity = pscanGraph.modularity()
        println(s"GRAPHX: modularity:   ${modularity}")

        if (!outFname.isEmpty) {
          logWarning("Saving PSCAN graph with vertex " + outFname)
          pscanGraph.vertices.filter(k => k._1 != k._2).sortBy(x => x._2)
            .mapPartitions(ls => ls.map(k => s"${k._1},${k._2}")).repartition(numRepartition.getOrElse(10)).saveAsTextFile(outFname)
        }
        sc.stop()

      case "louvain" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)
        val numRepartition = options.remove("numRep").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("=============================================")
        println("|             LouvainHarness               |")
        println("=============================================")

        val sc = new SparkContext(conf.setAppName("LouvainHarnessAlgorithm"))

        val edgeRdd = sc.textFile(fname).mapPartitions(lines => lines.map { line =>
          val arr = line.split(",")
          Edge(arr(1).toLong, arr(2).toLong, arr(3).toDouble)
        })

        val unpartitionedGraph = Graph.fromEdges(edgeRdd, 1, edgeStorageLevel, vertexStorageLevel)

        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        var louvainGraph = LouvainCore.createLouvainGraph(graph)

        var level = -1 // number of times the graph has been compressed 统计图按社区压缩的次数
      var q = -1.0 // current modularity value  // 当前模块度的值
      var halt = false
        do {
          level += 1
          println(s"\nStarting Louvain level $level")

          // 调用louvain算法计算每个节点所属的当前最佳社区 label each vertex with its best community choice at this level of compression
          val (currentQ, currentGraph, passes) = LouvainCore.louvain(sc, louvainGraph, numIterOpt.getOrElse(100), 1)
          louvainGraph.unpersistVertices(blocking = false)
          louvainGraph = currentGraph

          //      saveLevel(sc,level,currentQ,louvainGraph)

          // 如果模块度增加量超过0.001，说明当前划分比前一次好，继续迭代 If modularity was increased by at least 0.001 compress the graph and repeat
          // 如果计算当前社区的迭代次数少于3次，就停止循环 halt immediately if the community labeling took less than 3 passes
          println(s"if ($passes > 2 && $currentQ > $q + 0.001 )")
          if (passes > 2 && currentQ > q + 0.001) {
            q = currentQ
            louvainGraph = LouvainCore.compressGraph(louvainGraph)
          }
          else {
            halt = true
          }

        } while (!halt)

        louvainGraph.vertices.filter(k=>k._1 != k._2.community).sortBy(k=>k._2.community)
          .map { x => x._1 + "," + x._2 }.repartition(numRepartition.getOrElse(10)).saveAsTextFile(outFname)

        sc.stop()
    }
  }
}
