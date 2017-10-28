package com.lakala.datacenter.louvain

/**
  * Created by chenqingqing on 2017/4/4.
  */


import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.Array.canBuildFrom

/**
  * Execute the louvain algorithim and save the vertices and edges in hdfs at each level.
  * Can also save locally if in local mode.
  *
  * See LouvainHarness for algorithm details
  */
class HDFSLouvainRunner(minProgress: Int, progressCounter: Int, outputdir: String) extends LouvainHarness(minProgress: Int, progressCounter: Int) {

  var qValues = Array[(Int, Double)]()

  override def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Double]) = {
    graph.vertices.saveAsTextFile(outputdir + "/level_" + level + "_vertices")
    graph.edges.saveAsTextFile(outputdir + "/level_" + level + "_edges")
    qValues = qValues :+ ((level, q))
    println(s"qValue: $q")

    // overwrite the q values at each level
    sc.parallelize(qValues, 1).saveAsTextFile(outputdir + "/qvalues")
  }

  override def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Double]) = {
    graph.vertices.filter(k=>k._1 != k._2.community).sortBy(k=>k._2.community).map { x => x._1 + "," + x._2 }.repartition(10).saveAsTextFile(outputdir)
    //graph.edges.saveAsTextFile(outputdir+"/final_edges")

    println(s"qValue: $q")
  }


}
