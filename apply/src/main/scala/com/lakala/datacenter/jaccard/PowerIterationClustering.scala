package com.lakala.datacenter.jaccard

import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD

/**
  * Created by linyanshi on 2017/9/20 0020.
  */
object PowerIterationClustering {

  /**
    * run PIC using Spark's PowerIterationClustering implementation
    * @param similarities All pair similarities in the shape of RDD[(selfmobile, caller, similarity)]
    * @return Cluster assignment for each patient in the shape of RDD[(mobile, Cluster)]
    */
  def runPIC(similarities: RDD[(Long, Long, Double)]): RDD[(Long, Int)] = {
    val sc = similarities.sparkContext


    /** Remove placeholder code below and run Spark's PIC implementation */
    similarities.cache().count()
    val pic = new PowerIterationClustering().setK(3).setMaxIterations(100)
    val model=pic.run(similarities)
    val result = model.assignments.map(a => (a.id,a.cluster))
    val check = result.map(x=>x.swap).groupByKey().map(x=>(x._1,x._2.size))

    println("PIC: ")
    println(check.foreach(println))

    result
  }
}
