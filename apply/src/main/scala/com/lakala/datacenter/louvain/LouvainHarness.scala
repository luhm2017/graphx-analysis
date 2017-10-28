package com.lakala.datacenter.louvain

/**
  * Created by chenqingqing on 2017/4/4.
  */

import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Coordinates execution of the louvain distributed community detection process on a graph.
  *
  * The input Graph must have an edge type of Long.
  *
  * All lower level algorithm functions are in LouvainCore, this class acts to
  * coordinate calls into LouvainCore and check for convergence criteria
  *
  * Two hooks are provided to allow custom behavior
  * -saveLevel  override to save the graph (vertcies/edges) after each phase of the process
  * -finalSave  override to specify a final action / save when the algorithm has completed. (not nessicary if saving at each level)
  *
  * High Level algorithm description.
  *
  * Set up - Each vertex in the graph is assigned its own community.
  *  1.  Each vertex attempts to increase graph modularity by changing to a neighboring community, or reamining in its current community.
  *  2.  Repeat step 1 until progress is no longer made
  *         - progress is measured by looking at the decrease in the number of vertices that change their community on each pass.
  * If the change in progress is < minProgress more than progressCounter times we exit this level.
  *  3. -saveLevel, each vertex is now labeled with a community.
  *  4. Compress the graph representing each community as a single node.
  *  5. repeat steps 1-4 on the compressed graph.
  *  6. repeat until modularity is no longer improved
  *
  * For details see:  Fast unfolding of communities in large networks, Blondel 2008
  *
  *
  */
class LouvainHarness(minProgress: Int, progressCounter: Int) {


  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Double]) = {

    var louvainGraph = LouvainCore.createLouvainGraph(graph)

    var level = -1 // number of times the graph has been compressed 统计图按社区压缩的次数
    var q = -1.0 // current modularity value  // 当前模块度的值
    var halt = false
    do {
      level += 1
      println(s"\nStarting Louvain level $level")

      // 调用louvain算法计算每个节点所属的当前最佳社区 label each vertex with its best community choice at this level of compression
      val (currentQ, currentGraph, passes) = LouvainCore.louvain(sc, louvainGraph, minProgress, progressCounter)
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
    finalSave(sc, level, q, louvainGraph)
  }

  /**
    * Save the graph at the given level of compression with community labels
    * level 0 = no compression
    *
    * override to specify save behavior
    */
  def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Double]) = {

  }

  /**
    * Complete any final save actions required
    *
    * override to specify save behavior
    */
  def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Double]) = {


  }

  /**
    * 返回用户对的形式，表示两个用户位于同一个社区
    *
    * @param sc
    * @param level
    * @param q
    * @param graph
    */
  def getPairs(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Double]) = {

  }


}
