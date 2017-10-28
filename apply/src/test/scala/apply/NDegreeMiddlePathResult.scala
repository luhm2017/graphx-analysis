package apply

import entity.TwoDegree
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ASUS-PC on 2017/4/23.
  * 生成N度的中间路径结果
  */
object NDegreeMiddlePathResult {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TwoDegreeRel2")
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val vertexRDD: RDD[(Long, String)] = sc.parallelize(Array((1.toLong, "1"), (2.toLong, "2")
      , (3.toLong, "3"), (4.toLong, "4"), (5.toLong, "5"), (6.toLong, "6"), (7.toLong, "7"), (8.toLong, "8"), (9.toLong, "9")))

    val edgeRdd: RDD[Edge[Int]] = sc.parallelize(Array(//边的信息
      Edge(1.toLong, 2.toLong, 0), Edge(1.toLong, 3.toLong, 0), Edge(3.toLong, 2.toLong, 0), Edge(2.toLong, 3.toLong, 0), Edge(3.toLong, 4.toLong, 0), Edge(4.toLong, 3.toLong, 0),Edge(3.toLong, 5.toLong, 0), Edge(3.toLong, 6.toLong, 0),
      Edge(4.toLong, 5.toLong, 0), Edge(5.toLong, 6.toLong, 0), Edge(7.toLong, 8.toLong, 0), Edge(7.toLong, 9.toLong, 0), Edge(8.toLong, 9.toLong, 0), Edge(2.toLong, 11.toLong, 0),
      Edge(6.toLong, 11.toLong, 0), Edge(2.toLong, 12.toLong, 0), Edge(6.toLong, 12.toLong, 0))
    )

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
      //取两个集合的差集  然后将生命值减1
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


    val degree = 2 //这里是二跳邻居 所以只需要定义为2即可
    val newG = g.mapVertices((vid, attr) => Map[VertexId, TwoDegree](vid -> new TwoDegree(s"${vid}", loop = degree)))
      .pregel(Map[VertexId, TwoDegree](), degree, EdgeDirection.Out)(vprog, sendMsg, addMaps)
    //
    newG.vertices.collect().foreach(println(_))
    //    Map中的key表示周边的顶点id，其value就是对应顶点id的生命值，所以我们现在对该rdd再做一次mapValues处理即可得到最后的二跳邻居
    //过滤得到二跳邻居 就是value=0 的顶点
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
