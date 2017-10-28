import com.lakala.datacenter.apply.model.NDegreeEntity
import com.lakala.datacenter.core.utils.UtilsToos._
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ASUS-PC on 2017/4/18.
  */
object RunLoadApplyGraphx {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("RunLoadApplyGraphx")
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val degree = 3 //这里是二跳邻居 所以只需要定义为2即可
    val applyPath = "file:///F:/lakalaFinance_workspaces/applogs/query_result.csv"
    val callPath = "file:///F:/lakalaFinance_workspaces/graphx-analysis/apply/data3/part-00003"

    val applyVertexEdge = sc.textFile(applyPath).mapPartitions(lines =>
      lines.map { line =>
        var arr = line.split(",")
        val term_id = if (StringUtils.isNotBlank(arr(7)) && !"null".equals(arr(7).toLowerCase)) arr(7) else "OL"
        val return_pan = if (StringUtils.isNotBlank(arr(16)) && !"null".equals(arr(16).toLowerCase)) arr(16) else "0L"
        val empmobile = if (StringUtils.isNotBlank(arr(41)) && !"null".equals(arr(41).toLowerCase)) arr(41) else "0L"

        (Iterator((hashId(arr(1)), arr(1)),
          //          (hashId(term_id), term_id),
          //          (hashId(return_pan), return_pan),
          (empmobile.toLong, empmobile)),
          //创建枚举类
          Iterator(
            //            (Edge(hashId(arr(1)), hashId(term_id), 0)),
            //            (Edge(hashId(term_id), hashId(arr(1)), 0)),
            //            (Edge(hashId(arr(1)), hashId(arr(1)), 0)),
            //            (Edge(hashId(return_pan), hashId(return_pan), 0)),
            (Edge(hashId(arr(1)), empmobile.toLong, 0)),
            (Edge(empmobile.toLong, hashId(arr(1)), 0))
          )
        )
      }
    )

    val callVertexEdge = sc.textFile(callPath).mapPartitions { lines =>
      lines.map { line =>
        var arr = line.split(" ")
        (Iterable((arr(0).toLong, arr(0)), (arr(1).toLong, arr(1))), (Edge(arr(0).toLong, arr(1).toLong, 0)))
      }
    }

    var applyVertexRDD: RDD[(Long, String)] = applyVertexEdge.flatMap(k => k._1).distinct()
    var applyEdgeRdd = applyVertexEdge.flatMap(k => k._2).filter(k => k.srcId != 0L && k.dstId != 0L)

    var callVertexRDD: RDD[(Long, String)] = callVertexEdge.flatMap(k => k._1).filter(k => k._1 != 0L).distinct()
    var callEdgeRdd = callVertexEdge.filter(k => k._2.srcId != 0L && k._2.dstId != 0L).map(k => k._2)

    //    val sgv = new SimpleGraphViewer("", "", false)
    //    sgv.runIteratro(callVertexRDD.filter(k => k._1 != 0L).map(k => k._1 + " " + k._2).toLocalIterator, callEdgeRdd.map(k => k.srcId + " " + k.dstId + " " + k.attr).toLocalIterator, true)
    //    sgv.run()

    println("**********************************************************")

        val g: Graph[String, Int] = Graph(applyVertexRDD ++ callVertexRDD, applyEdgeRdd ++ callEdgeRdd)
//    val g: Graph[String, Int] = Graph(applyVertexRDD, applyEdgeRdd)

    type VMap = Map[VertexId, NDegreeEntity]

    /**
      * 节点数据的更新 就是集合的union
      */
    def vprog(vid: VertexId, vdata: VMap, message: VMap): Map[VertexId, NDegreeEntity] = addMaps(vdata, message)

    /**
      * 发送消息
      */
    def sendMsg(e: EdgeTriplet[VMap, _]) = {
      //取出源节点差集key值 发送(e.srcId->目标节点)
      val dstMap = (e.srcAttr.keySet -- e.dstAttr.keySet).map { k =>
        val e2 = e.srcAttr(k)
        k -> e2.copy(s"${e2.attr}->${e.dstId}", loop = e2.loop - 1)
      }.toMap

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
          var result: NDegreeEntity = null
          if (spmap1.get(k).isDefined && spmap2.get(k).isDefined) {
            var spmap11 = spmap1.getOrElse(k, new NDegreeEntity(s"$k->$k", Int.MaxValue))
            var spmap12 = spmap2.getOrElse(k, new NDegreeEntity(s"$k->$k", Int.MaxValue))
            val loop = math.min(spmap11.loop, spmap12.loop)
            result = spmap11.copy(mergeMsg(spmap11.attr, spmap12.attr), loop)

          } else {
            result = if (spmap1.get(k).isDefined) spmap1.getOrElse(k, new NDegreeEntity(s"$k->$k", Int.MaxValue))
            else spmap2.getOrElse(k, new NDegreeEntity(s"$k->$k", Int.MaxValue))
          }
          k -> result
      }.toMap


    val newG = g.mapVertices((vid, _) => Map[VertexId, NDegreeEntity](vid -> new NDegreeEntity(s"$vid", loop = degree)))
      .pregel(Map[VertexId, NDegreeEntity](), degree, EdgeDirection.Out)(vprog, sendMsg, addMaps)

    val degreeJumpRdd = newG.vertices.mapValues(_.filter(_._2.loop == 0)).mapPartitions { vs =>
      vs.map { v =>
        val values = v._2.filter(_._2.loop == 0)
        val forRs = for (v <- values; if (v._2.loop == 0)) yield (v._2.attr)
        forRs
      }
    }.flatMap(k => k)
    println("============================")
    val s = degreeJumpRdd.sortBy(s => s)
    println("total num " + s.count())
    s.foreach(println)
  }

}
