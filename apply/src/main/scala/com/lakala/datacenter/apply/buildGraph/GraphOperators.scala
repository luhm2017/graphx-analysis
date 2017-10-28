package com.lakala.datacenter.apply.buildGraph

import com.lakala.datacenter.apply.model.{ApplyInfo, CallHistoryEntity}
import com.lakala.datacenter.utils.UtilsToos._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
  * Created by ASUS-PC on 2017/4/18.
  */
object GraphOperators {
  def getApplyGraph(sc: SparkContext, path: String): Graph[ApplyInfo, String] = {
    val loadApply: RDD[String] = sc.textFile(path)
    val vertexsAndEdgesRdd: RDD[((Long, ApplyInfo), (Long, ApplyInfo), (Long, ApplyInfo), (Long, ApplyInfo),
      (Edge[String]), (Edge[String]), (Edge[String]))] = loadApply.mapPartitions(lines =>
      lines.map { line =>
        var arr = line.split(",")

        val term_id = if (StringUtils.isNotBlank(arr(7)) && !"null".equals(arr(7).toLowerCase)) arr(7) else "OL"
        val return_pan = if (StringUtils.isNotBlank(arr(16)) && !"null".equals(arr(16).toLowerCase)) arr(16) else "0L"
        val empmobile = if (StringUtils.isNotBlank(arr(41)) && !"null".equals(arr(41).toLowerCase)) arr(41) else "0L"

        ((hashId(arr(1)), new ApplyInfo(arr(1), arr(2), arr(4), arr(7), arr(10), arr(16), arr(41))),
          (hashId(term_id), new ApplyInfo("", "", "", term_id, "", "", "")),
          (hashId(return_pan), new ApplyInfo("", "", "", "", "", return_pan, "")),
          (empmobile.toLong, new ApplyInfo("", "", "", "", "", "", empmobile)),
          //创建枚举类
          (Edge(hashId(arr(1)), hashId(term_id), "term_id")),
          (Edge(hashId(arr(1)), hashId(return_pan), "return_pan")),
          (Edge(hashId(arr(1)), empmobile.toLong, "empmobile"))
          )
      }
    )

    //rout table ver -edge
    val vertex1: RDD[(Long, ApplyInfo)] = vertexsAndEdgesRdd.map(ve => ve._1)
    val vertex2: RDD[(Long, ApplyInfo)] = vertexsAndEdgesRdd.map(ve => ve._2).filter(k => k._1 != hashId("0L"))
    val vertex3: RDD[(Long, ApplyInfo)] = vertexsAndEdgesRdd.map(ve => ve._3).filter(k => k._1 != hashId("0L"))
    val vertex4: RDD[(Long, ApplyInfo)] = vertexsAndEdgesRdd.map(ve => ve._4).filter(k => k._1 != 0L)
    //
    val edge1: RDD[Edge[String]] = vertexsAndEdgesRdd.map(ve => ve._5).filter(k => k.dstId != hashId("0L"))
    val edge2: RDD[Edge[String]] = vertexsAndEdgesRdd.map(ve => ve._6).filter(k => k.dstId != hashId("0L"))
    val edge3: RDD[Edge[String]] = vertexsAndEdgesRdd.map(ve => ve._7).filter(k => k.dstId != 0L)

    val disVertex = vertex1.union(vertex2).union(vertex3).union(vertex4).reduceByKey((x, y) => x)
    val distEdges = edge1.union(edge2).union(edge3).distinct()

    Graph(disVertex, distEdges)
  }

  def getCallHistoryGraph(sc: SparkContext, path: String): Graph[CallHistoryEntity, String] = {
    val lineRdd: RDD[String] = sc.textFile(path)
    val vertexsAndEdgesRdd: RDD[((Long, CallHistoryEntity),
      (Long, CallHistoryEntity), (Edge[String]))] =
      lineRdd.mapPartitions { lines =>
        lines.map { line =>
          var arr: Array[String] = line.split("\u0001")
          val loginname = if (StringUtils.isNotBlank(arr(4))) arr(4).toLong else 0L
          val caller_phone = if (StringUtils.isNotBlank(arr(6))) arr(6).toLong else 0L
          (
            (loginname, new CallHistoryEntity(loginname, caller_phone)),
            (caller_phone, new CallHistoryEntity(caller_phone, loginname)),
            //创建枚举类
            (Edge(loginname, caller_phone, "call"))
            )
        }
      }
    //rout table ver -edge
    val vertex1: RDD[(Long, CallHistoryEntity)] = vertexsAndEdgesRdd.map(ve => ve._1).filter(k => k._1 != 0L)
    val vertex2: RDD[(Long, CallHistoryEntity)] = vertexsAndEdgesRdd.map(ve => ve._2).filter(k => k._1 != 0L)

    val disVertex = vertex1.union(vertex2).reduceByKey((x, y) => x)
    val distEdges: RDD[Edge[String]] = vertexsAndEdgesRdd.map(ve => ve._3).filter(k => k.srcId != 0L).distinct()
    Graph(disVertex, distEdges)
  }
}
