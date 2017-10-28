import java.security.InvalidParameterException

import CollectionUtil._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by Administrator on 2017/8/15 0015.
  * Description:用于在图中为指定的节点计算这些节点的N度关系节点，输出这些节点与源节点的路径长度和节点id
  */
object GraphNdegUtil {
  /**
    * 计算节点的N度关系
    *
    * @param edges
    * @param choosedVertex
    * @param degree
    * @tparam ED
    * @return
    */
  def aggNdegreedVertices[ED: ClassTag](edges: RDD[(VertexId, VertexId)], choosedVertex: RDD[VertexId], degree: Int): VertexRDD[Seq[(Int, ArrayBuffer[VertexId])]] =
    aggNdegreedVertices(Graph.fromEdgeTuples(edges, 0), choosedVertex, degree)

  /**
    * 计算指定节点的小于等于N度的所有对应度数下的各个关系节点
    * 计算步骤：
    * 1.在图中给被选节点找Tag，标记要为这些节点找N度关系节点
    * 2.为图中每个节点定义初始的距离向量：(vertexId,distance)
    * 3.迭代发消息，对某一端包含Tag的边发双向消息(如果dstType不在choosedType集合中则不发)，消息内容是：消息接收者不包含的距离向量
    * 4.merge消息：合并关系节点，新的tag取并集
    * 5.更新消息接收节点的路径向量集合：把接收到的距离向量的值分别加1
    * 6.迭代结束，去掉距离向量值为0的，剩下的按距离值汇总即得到结果
    *
    * @param graph         注意：图参数的顶点和边属性应该尽量简单，除了filter函数会使用到顶点的属性参数外，整个计算过程中只使用了顶点的id和边的srcId及dstId
    * @param choosedVertex ，要计算的节点的Id
    * @param filter        ，只取符合过滤条件的关系节点
    * @param degree        ，指定最多取几度关系节点，在实际迭代计算中，迭代的次数要比degree参数大1才能保证找的N度关系没有漏的
    * @return
    */
  def aggNdegreedVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                      choosedVertex: RDD[VertexId],
                                                      degree: Int,
                                                      filter: VD => Boolean = (_: VD) => true): VertexRDD[Seq[(Int, ArrayBuffer[VertexId])]] = {
    if (degree < 1) {
      throw new InvalidParameterException("度参数错误:" + degree)
    }
    val tag = true
    //要找的标记，仅一个boolean值
    val tupleRdd = choosedVertex.map(e => (e, tag)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //打标记并定义初始距离向量
    val tagedGraph: Graph[DegVertex[VD], ED] = graph.outerJoinVertices(tupleRdd)((id, attr, tg) => DegVertex(attr, tg.getOrElse(false), mutable.Map(id -> 0)))
    val result = tagedGraph.pregel[(Boolean, mutable.Map[VertexId, Int])]((false, mutable.Map()), degree + 1, EdgeDirection.Either)((_, attr, msg) => attr.addMsg(msg), sendMsg(_, filter), mergeMsg)
    val maped = result.vertices.join(tupleRdd).map(e => (e._1, sortResult(e._2._1))).persist(StorageLevel.MEMORY_AND_DISK_SER)
    tupleRdd.unpersist()
    tagedGraph.unpersist()
    VertexRDD(maped)
  }

  private def sortResult[VD: ClassTag](degs: DegVertex[VD]): Seq[(Int, ArrayBuffer[VertexId])] = degs.degVertices.toSeq.map(e => (e._2, ArrayBuffer(e._1))).filter(e => e._1 > 0).reduceByKey(_ ++ _).toSeq.sortBy(e => e._1)

  case class DegVertex[VD: ClassTag](attr: VD, var taged: Boolean, degVertices: mutable.Map[VertexId, Int]) {
    def addMsg(msg: (Boolean, mutable.Map[VertexId, Int])): DegVertex[VD] = {
      try {
        val newMsg = msg._2.map(e => (e._1, e._2 + 1))
        val newMap: mutable.Map[VertexId, Int] = mutable.Map() //注意，这里一定要新起一个map，不要直接在this.degVertices上操作，否则二轮迭代时，值传不过去
        for (k <- this.degVertices.keySet) {
          val v = this.degVertices.get(k)
          if (v.isDefined) {
            newMap.put(k, v.get)
          }
        }
        for (k <- newMsg.keySet) {
          val v = newMsg.get(k)
          if (!newMap.contains(k) && v.isDefined) {
            newMap.put(k, v.get)
          }
        }
        val newTag = if (!this.taged) msg._1 else this.taged
        DegVertex(attr, newTag, newMap)
      } catch {
        case e: Exception =>
          println("=============error found:" + msg)
          e.printStackTrace()
          this
      }
    }
  }

  private def sendMsg[VD: ClassTag, ED: ClassTag](e: EdgeTriplet[DegVertex[VD], ED], filter: VD => Boolean): Iterator[(VertexId, (Boolean, mutable.Map[VertexId, Int]))] = {
    var r: ArrayBuffer[(VertexId, (Boolean, mutable.Map[VertexId, Int]))] = ArrayBuffer[(VertexId, (Boolean, mutable.Map[VertexId, Int]))]()
    try {
      if ((e.srcAttr.taged || e.dstAttr.taged) && !isAttrSame(e.srcAttr, e.dstAttr)) {
        //1.发给src
        if (filter(e.dstAttr.attr)) {
          val ele: (VertexId, (Boolean, mutable.Map[VertexId, Int])) = (e.srcId, (e.dstAttr.taged, e.dstAttr.degVertices))
          r += ele
        } //2.发给dst
        if (filter(e.srcAttr.attr)) {
          val ele: (VertexId, (Boolean, mutable.Map[VertexId, Int])) = (e.dstId, (e.srcAttr.taged, e.srcAttr.degVertices))
          r += ele
        }
      }
    } catch {
      case ex: Exception =>
        println("==========error found:" + e)
        ex.printStackTrace()
    }
    r.iterator
  }

  private def mergeMsg(a: (Boolean, mutable.Map[VertexId, Int]), b: (Boolean, mutable.Map[VertexId, Int])): (Boolean, mutable.Map[VertexId, Int]) = {
    try {
      for (k <- b._2.keySet) {
        val v = b._2.get(k)
        if (!a._2.contains(k) && v.isDefined) {
          a._2.put(k, v.get)
        }
      }
      (b._1 || a._1, a._2)
    } catch {
      case e: Exception =>
        println("==========error found:" + a + "," + b)
        e.printStackTrace()
        (false, mutable.Map())
    }
  }

  private def isAttrSame[VD: ClassTag](a: DegVertex[VD], b: DegVertex[VD]): Boolean = a.taged == b.taged && hasSameKey(a.degVertices, b.degVertices)

  private def hasSameKey(a: mutable.Map[VertexId, Int], b: mutable.Map[VertexId, Int]): Boolean = {
    if (a.size != b.size) return false
    for (k <- a.keySet) {
      if (!b.contains(k)) return false
    }
    true
  }
}
