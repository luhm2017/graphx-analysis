package com.lakala.datacenter.jaccard

import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.rdd.RDD

/**
  * Created by linyanshi on 2017/9/20 0020.
  */
object Jaccard {
  /**
    * Return a RDD of (1-id, 2-id, similarity) where
    * 1-id < 2-id to avoid duplications
    *
    * @param graph
    * @return
    */

  def jaccardSimilarityAllMobiles(graph: Graph[Int, Int]): RDD[(Long, Long, Double)] = {
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
}
