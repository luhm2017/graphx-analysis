/**
  * @author Ting Pan <tpan35@gatech.edu>.
  */

package edu.gatech.cse8803.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{PowerIterationClustering => PIC}
import org.apache.spark.mllib.clustering.PowerIterationClustering


/**
  * Power Iteration Clustering (PIC), a scalable graph clustering algorithm developed by
  * [[http://www.icml2010.org/papers/387.pdf Lin and Cohen]]. From the abstract: PIC finds a very
  * low-dimensional embedding of a dataset using truncated power iteration on a normalized pair-wise
  * similarity matrix of the data.
  *
  * @see [[http://en.wikipedia.org/wiki/Spectral_clustering Spectral clustering (Wikipedia)]]
  */

object PowerIterationClustering {

  /** run PIC using Spark's PowerIterationClustering implementation
    *
    * @input: All pair similarities in the shape of RDD[(patientID1, patientID2, similarity)]
    * @return: Cluster assignment for each patient in the shape of RDD[(PatientID, Cluster)]
    *
    * */

    def runPIC(similarities: RDD[(Long, Long, Double)]): RDD[(Long, Int)] = {
    val sc = similarities.sparkContext


    /** Remove placeholder code below and run Spark's PIC implementation */
    similarities.cache().count()
    val pic = new PowerIterationClustering().setK(3).setMaxIterations(100)
    val model=pic.run(similarities)
    val result = model.assignments.map(a => (a.id,a.cluster))
    //val check = result.map(x=>x.swap).groupByKey().map(x=>(x._1,x._2.size))

    //println("PIC: ")
    //println(check.foreach(println))

    result
  }
}
