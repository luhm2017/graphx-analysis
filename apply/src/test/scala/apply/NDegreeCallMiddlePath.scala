package apply

import entity.TwoDegree
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ASUS-PC on 2017/4/23.
  */
object NDegreeCallMiddlePath {
  def main(args: Array[String]): Unit = {
    val degree = 3 //这里是二跳邻居 所以只需要定义为2即可
    val conf = new SparkConf().setMaster("local[2]").setAppName("TwoDegreeRel2")
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    var veLineRdd = sc.textFile("file:///C:/Users/peter/lakalaFinance_workspaces/graphx-analysis/apply/data3/part-00003")
    val ves = veLineRdd.mapPartitions { lines =>
      lines.map { line =>
        var arr = line.split(" ")
        //        (Iterable((arr(0).toLong, arr(0)), (arr(1).toLong, arr(1))), (Edge(arr(0).toLong, arr(1).toLong, 0)))
        (Iterable((arr(0).toLong, arr(0)), (arr(1).toLong, arr(1))), (Edge(arr(0).toLong, arr(1).toLong, 0)))
      }
    }

    var vertexRDD: RDD[(Long, String)] = ves.flatMap(k => k._1).distinct()
    var edgeRdd = ves.filter(k => k._2.srcId != 0L && k._2.dstId != 0L).map(k => k._2)

    val g: Graph[String, Int] = Graph(vertexRDD, edgeRdd)

    type VMap = Map[VertexId, TwoDegree]

    /**
      * 节点数据的更新 就是集合的union
      */
    def vprog(vid: VertexId, vdata: VMap, message: VMap): Map[VertexId, TwoDegree] = addMaps(vdata, message)

    /**
      * 发送消息
      */
    def sendMsg(e: EdgeTriplet[VMap, _]) = {
      //取出源节点差集key值 发送(e.srcId->目标节点)
      val dstMap = (e.srcAttr.keySet -- e.dstAttr.keySet).map { k =>
        val e2 = e.srcAttr(k)
        k -> e2.copy(s"${e2.attr}->${e.dstId}", loop = e2.loop - 1)
      }.toMap
//      //目标节点差集key值 发送(e.srcId->目标节点)
//      val dstMap2 = (e.dstAttr.keySet -- dstMap.keySet).map { k =>
//        val e2 = e.dstAttr(k)
//        k -> e2.copy(s"${e.srcId}->${e2.attr}", loop = e2.loop - 1)
//      }.toMap

      //      if (srcMap.size == 0 && dstMap.size == 0)
      //        Iterator.empty
      //      else
      Iterator((e.dstId, dstMap) /*, (e.srcId, srcMap)*/)
    }

    /**
      *
      * @param msgA spmap1的节点属性值
      * @param msgB spmap2 接收到的消息
      * @return
      */
    def mergeMsg(msgA: String, msgB: String): String = {
      if (!msgA.contains("->") && !msgB.contains("->")) {
        s"$msgB->$msgA"
      } else if (msgA.equals(msgB)) {
        msgA
      }
      else if (msgA.contains(msgB)) msgA
      else if (msgB.contains(msgA)) msgB
      else {
        msgB + "-->" + msgA
      }
    }

    /**
      * 消息的合并 (第一次迭代 spmap1的节点属性值是 ("自身id",迭代次数))，spmap2 消息也是 ("自身id",迭代次数))
      *
      * @param spmap1 节点属性
      * @param spmap2 消息
      * @return
      */
    def addMaps(spmap1: VMap, spmap2: VMap): VMap =
      (spmap1.keySet ++ spmap2.keySet).map {
        k =>
          var result: TwoDegree = null
          if (spmap1.get(k).isDefined && spmap2.get(k).isDefined) {
            var spmap11 = spmap1.getOrElse(k, new TwoDegree(s"$k->$k", Int.MaxValue))
            var spmap12 = spmap2.getOrElse(k, new TwoDegree(s"$k->$k", Int.MaxValue))
            val loop = math.min(spmap11.loop, spmap12.loop)
            result = spmap11.copy(mergeMsg(spmap11.attr, spmap12.attr), loop)

          } else {
            result = if (spmap1.get(k).isDefined) spmap1.getOrElse(k, new TwoDegree(s"$k->$k", Int.MaxValue))
            else spmap2.getOrElse(k, new TwoDegree(s"$k->$k", Int.MaxValue))
          }
          k -> result
      }.toMap



    val newG = g.mapVertices((vid, attr) => Map[VertexId, TwoDegree](vid -> new TwoDegree(attr, loop = degree)))
      .pregel(Map[VertexId, TwoDegree](), degree, EdgeDirection.Out)(vprog, sendMsg, addMaps)

    val degreeJumpRdd = newG.vertices.mapValues(_.filter(_._2.loop == 0)).mapPartitions { vs =>
      vs.map { v =>
        val values = v._2.filter(_._2.loop == 0)
        val forRs = for(v <- values ;if(v._2.loop==0)) yield (v._2.attr)
        forRs
      }
    }.flatMap(k=>k)
    println("============================")
    degreeJumpRdd.sortBy(s=>s).foreach(println)
  }
}
