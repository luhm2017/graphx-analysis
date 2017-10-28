import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

/**
  * Created by lys on 2017/4/23.
  */
object NDegreeResult {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("DTWWW")
    val sc = new SparkContext(conf);
    val edge = List(//边的信息
      (1, 2), (1, 3), (2, 3), (3, 4), (3, 5), (3, 6),
      (4, 5), (5, 6), (7, 8), (7, 9), (8, 9),(2,11),(6,11),(2,12),(6,12))
    //构建边的rdd
    val edgeRdd = sc.parallelize(edge).map(x => {
      Edge(x._1.toLong, x._2.toLong, None)
    })
    //构建图 顶点Int类型
    val g = Graph.fromEdges(edgeRdd, 0)
    //可以了解图中“超级节点”的个数和规模，以及所有节点度的分布曲线。
    g.degrees.collect.foreach(println(_))
    //使用两次遍历，首先进行初始化的时候将自己的生命值设为2，
    // 第一次遍历向邻居节点传播自身带的ID以及生命值为1(2-1)的消息，
    // 第二次遍历的时候收到消息的邻居再转发一次，生命值为0，
    // 最终汇总统计的时候 只需要对带有消息为0 ID的进行统计即可得到二跳邻居


    type VMap = Map[VertexId, Int]

    /**
      * 节点数据的更新 就是集合的union
      */
    def vprog(vid: VertexId, vdata: VMap, message: VMap): Map[VertexId, Int] = addMaps(vdata, message)

    /**
      * 发送消息
      */
    def sendMsg(e: EdgeTriplet[VMap, _]) = {
      //取两个集合的差集  然后将生命值减1
      val srcMap:Map[VertexId, Int] = (e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k -> (e.dstAttr(k) - 1) }.toMap
      val dstMap:Map[VertexId, Int] = (e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k -> (e.srcAttr(k) - 1) }.toMap

      if (srcMap.size == 0 && dstMap.size == 0)
        Iterator.empty
      else
        Iterator((e.dstId, dstMap), (e.srcId, srcMap))
    }

    /**
      * 消息的合并
      */
    def addMaps(spmap1: VMap, spmap2: VMap): VMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

    val two = 2 //这里是二跳邻居 所以只需要定义为2即可
    val newG = g.mapVertices((vid, _) => Map[VertexId, Int](vid -> two))
      .pregel(Map[VertexId, Int](), two, EdgeDirection.Out)(vprog, sendMsg, addMaps)

    //可以看一下二次遍历之后各个顶点的数据：
    newG.vertices.collect().foreach(println(_))
    //    (4,Map(5 -> 1, 1 -> 0, 6 -> 0, 2 -> 0, 3 -> 1, 4 -> 2))
    //    (6,Map(5 -> 1, 1 -> 0, 6 -> 2, 2 -> 0, 3 -> 1, 4 -> 0))
    //    (8,Map(8 -> 2, 7 -> 1, 9 -> 1))
    //    (2,Map(5 -> 0, 1 -> 1, 6 -> 0, 2 -> 2, 3 -> 1, 4 -> 0))
    //    (1,Map(5 -> 0, 1 -> 2, 6 -> 0, 2 -> 1, 3 -> 1, 4 -> 0))
    //    (3,Map(5 -> 1, 1 -> 1, 6 -> 1, 2 -> 1, 3 -> 2, 4 -> 1))
    //    (7,Map(7 -> 2, 8 -> 1, 9 -> 1))
    //    (9,Map(9 -> 2, 7 -> 1, 8 -> 1))
    //    (5,Map(5 -> 2, 1 -> 0, 6 -> 1, 2 -> 0, 3 -> 1, 4 -> 1))
    //    Map中的key表示周边的顶点id，其value就是对应顶点id的生命值，所以我们现在对该rdd再做一次mapValues处理即可得到最后的二跳邻居
    //过滤得到二跳邻居 就是value=0 的顶点
    val twoJumpFirends = newG.vertices
      .mapValues(_.filter(_._2 == 0).keys)

    twoJumpFirends.collect().foreach(println(_))
    //    (4,Set(1, 6, 2))
    //    (6,Set(1, 2, 4))
    //    (8,Set())
    //    (2,Set(5, 6, 4))
    //    (1,Set(5, 6, 4))
    //    (3,Set())
    //    (7,Set())
    //    (9,Set())
    //    (5,Set(1, 2))

  }
}
