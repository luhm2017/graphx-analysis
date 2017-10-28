package utils

/**
  * Created by ASUS-PC on 2017/4/19.
  * Description:用于在图中为指定的节点计算这些节点的N度关系节点，输出这些节点与源节点的路径长度和节点id
  */

import java.security.InvalidParameterException

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.CollectionUtil._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object GraphNdegUtil {
  val maxNDegVerticesCount = 10000
  val maxDegree = 1000

  /**
    * 计算节点的N度关系
    *
    * @param edges
    * @param choosedVertex
    * @param degree
    * @tparam ED
    * @return
    */
  def aggNdegreedVertices[ED: ClassTag](edges: RDD[(VertexId, VertexId)], choosedVertex: RDD[VertexId], degree: Int): VertexRDD[Map[Int, Set[VertexId]]] = {
    val simpleGraph:Graph[Int,Int] = Graph.fromEdgeTuples(edges, 0, Option(PartitionStrategy.EdgePartition2D), StorageLevel.MEMORY_AND_DISK_SER, StorageLevel.MEMORY_AND_DISK_SER)
    aggNdegreedVertices(simpleGraph, choosedVertex, degree)
  }

  def aggNdegreedVerticesWithAttr[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], choosedVertex: RDD[VertexId], degree: Int, sendFilter: (VD, VD) => Boolean = (_: VD, _: VD) => true): VertexRDD[Map[Int, Set[VD]]] = {
    val ndegs: VertexRDD[Map[Int, Set[VertexId]]] = aggNdegreedVertices(graph, choosedVertex, degree, sendFilter)
    val flated: RDD[Ver[VD]] = ndegs.flatMap(e => e._2.flatMap(t => t._2.map(s => Ver(e._1, s, t._1, null.asInstanceOf[VD])))).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val matched: RDD[Ver[VD]] = flated.map(e => (e.id, e)).join(graph.vertices).map(e => e._2._1.copy(attr = e._2._2)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    flated.unpersist(blocking = false)
    ndegs.unpersist(blocking = false)
    val grouped: RDD[(VertexId, Map[Int, Set[VD]])] = matched.map(e => (e.source, ArrayBuffer(e))).reduceByKey(_ ++= _).map(e => (e._1, e._2.map(t => (t.degree, Set(t.attr))).reduceByKey(_ ++ _).toMap))
    matched.unpersist(blocking = false)
    VertexRDD(grouped)
  }

  def aggNdegreedVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                      choosedVertex: RDD[VertexId],
                                                      degree: Int,
                                                      sendFilter: (VD, VD) => Boolean = (_: VD, _: VD) => true
                                                     ): VertexRDD[Map[Int, Set[VertexId]]] = {
    if (degree < 1) {
      throw new InvalidParameterException("度参数错误:" + degree)
    }
    val initVertex:RDD[(VertexId,Boolean)] = choosedVertex.map(e => (e, true)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //创建的图首先进行度数的关联节点的数据保存着(各个节点的度数和原来节点值),利用subgraph刷选出小于最大度的子图
    var g: Graph[DegVertex[VD], Int] = graph.outerJoinVertices(graph.degrees)((_, old, deg) => (deg.getOrElse(0), old))
      .subgraph(vpred = (_, a) => a._1 <= maxDegree)
      //去掉大节点
      //在接着对图进行关联对各个节点数据按照 choosedVertex参数把节点都
      // 封装成DegVertex对象[包含着原先的节点属性,增加标注节点boolean属性若不能join 到choosedVertex设置成false否则true,
      //     增加后面要调用aggregateMessages更改的属性并初始化]
      .outerJoinVertices(initVertex)((id, old, hasReceivedMsg) => {
      DegVertex(old._2, hasReceivedMsg.getOrElse(false), ArrayBuffer((id, 0))) //初始化要发消息的节点
    }).mapEdges(_ => 0).cache() //简化边属性

    choosedVertex.unpersist(blocking = false)

    var i = 0
    var prevG: Graph[DegVertex[VD], Int] = null
    var newVertexRdd: VertexRDD[ArrayBuffer[(VertexId, Int)]] = null
    while (i < degree + 1) {
      prevG = g
      //发第i+1轮消息
      newVertexRdd = prevG.aggregateMessages[ArrayBuffer[(VertexId, Int)]](sendMsg(_, sendFilter), (a, b) => reduceVertexIds(a ++ b)).persist(StorageLevel.MEMORY_AND_DISK_SER)
      g = g.outerJoinVertices(newVertexRdd)((vid, old, msg) => if (msg.isDefined) updateVertexByMsg(vid, old, msg.get) else old.copy(init = false)).cache()
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      newVertexRdd.unpersist(blocking = false)
      i += 1
    }
    newVertexRdd.unpersist(blocking = false)

    val maped = g.vertices.join(initVertex).mapValues(e => sortResult(e._1)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    initVertex.unpersist()
    g.unpersist(blocking = false)
    VertexRDD(maped)
  }

  private case class Ver[VD: ClassTag](source: VertexId, id: VertexId, degree: Int, attr: VD = null.asInstanceOf[VD])

  private def updateVertexByMsg[VD: ClassTag](vertexId: VertexId, oldAttr: DegVertex[VD], msg: ArrayBuffer[(VertexId, Int)]): DegVertex[VD] = {
    val addOne:ArrayBuffer[(VertexId,Int)] = msg.map(e => (e._1, e._2 + 1))
    val newMsg:ArrayBuffer[(VertexId,Int)] = reduceVertexIds(oldAttr.degVertices ++ addOne)
    oldAttr.copy(init = msg.nonEmpty, degVertices = newMsg)
  }

  private def sortResult[VD: ClassTag](degs: DegVertex[VD]): Map[Int, Set[VertexId]] = degs.degVertices.map(e => (e._2, Set(e._1))).reduceByKey(_ ++ _).toMap

  case class DegVertex[VD: ClassTag](var attr: VD, init: Boolean = false, degVertices: ArrayBuffer[(VertexId, Int)])

  case class VertexDegInfo[VD: ClassTag](var attr: VD, init: Boolean = false, degVertices: ArrayBuffer[(VertexId, Int)])

  private def sendMsg[VD: ClassTag](e: EdgeContext[DegVertex[VD], Int, ArrayBuffer[(VertexId, Int)]], sendFilter: (VD, VD) => Boolean): Unit = {
    try {
      val src = e.srcAttr
      val dst = e.dstAttr
      //只有dst是ready状态才接收消息
      if (src.degVertices.size < maxNDegVerticesCount && (src.init || dst.init) && dst.degVertices.size < maxNDegVerticesCount && !isAttrSame(src, dst)) {
        if (sendFilter(src.attr, dst.attr)) {
          e.sendToDst(reduceVertexIds(src.degVertices))
        }
        if (sendFilter(dst.attr, dst.attr)) {
          e.sendToSrc(reduceVertexIds(dst.degVertices))
        }
      }
    } catch {
      case ex: Exception =>
        println(s"==========error found: exception:${ex.getMessage}," +
          s"edgeTriplet:(srcId:${e.srcId},srcAttr:(${e.srcAttr.attr},${e.srcAttr.init},${e.srcAttr.degVertices.size}))," +
          s"dstId:${e.dstId},dstAttr:(${e.dstAttr.attr},${e.dstAttr.init},${e.dstAttr.degVertices.size}),attr:${e.attr}")
        ex.printStackTrace()
        throw ex
    }
  }

  private def reduceVertexIds(ids: ArrayBuffer[(VertexId, Int)]): ArrayBuffer[(VertexId, Int)] = ArrayBuffer() ++= ids.reduceByKey(Math.min)

  private def isAttrSame[VD: ClassTag](a: DegVertex[VD], b: DegVertex[VD]): Boolean = a.init == b.init && allKeysAreSame(a.degVertices, b.degVertices)

  private def allKeysAreSame(a: ArrayBuffer[(VertexId, Int)], b: ArrayBuffer[(VertexId, Int)]): Boolean = {
    val aKeys = a.map(e => e._1).toSet
    val bKeys = b.map(e => e._1).toSet
    if (aKeys.size != bKeys.size || aKeys.isEmpty) return false

    aKeys.diff(bKeys).isEmpty && bKeys.diff(aKeys).isEmpty
  }
}
