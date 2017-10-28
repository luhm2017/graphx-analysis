/**
  * *
  * @author: Ting Pan <tpan35@gatech.edu>
  **/
package edu.gatech.cse8803.jaccard

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /**
      * Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients.
      * Return a List of patient IDs ordered by the highest to the lowest similarity.
      * For ties, random order is okay
      */


    val neighbors = graph.collectNeighborIds(EdgeDirection.Either).map(x => (x._1, x._2.filter(p => p > 1000))).filter(_._1 <= 1000)
    val neighbors_wo_patient = neighbors.filter(_._1 != patientID)
    val source = neighbors.filter(_._1 == patientID).map(_._2).collect.flatten.toSet
    val SimilarityOneVsAll = neighbors_wo_patient.map { case (vid, nbrs) => (vid, jaccard(source, nbrs.toSet)) }
    val result = SimilarityOneVsAll.sortBy(_._2, false).map(_._1).take(10).toList
    result
  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
      * Given a patient, med, diag, lab graph, calculate pairwise similarity between all
      *patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where
      * patient-1-id < patient-2-id to avoid duplications
      */
    val neighbors = graph.collectNeighborIds(EdgeDirection.Either).map(x => (x._1, x._2.filter(p => p > 1000))).filter(_._1 <= 1000)
    val combinations = neighbors.cartesian(neighbors).filter { case (a, b) => a._1 < b._1 }
    val SimilarityAll = combinations.map { x => (x._1._1, x._2._1, jaccard(x._1._2.toSet, x._2._2.toSet)) }
    val result = SimilarityAll.map(x => (x._3, (x._1, x._2))).sortByKey(false, 1).map(x => (x._2._1, x._2._2, x._1))
    result
  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /**
      * Helper function
      * *
      * Given two sets, compute its Jaccard similarity and return its result.
      * If the union part is zero, then return 0.
      */


    val union: Double = (a ++ b).size
    val intersect: Double = a.intersect(b).size
    return (if (union == 0) 0.0 else (intersect / union))
  }

}
