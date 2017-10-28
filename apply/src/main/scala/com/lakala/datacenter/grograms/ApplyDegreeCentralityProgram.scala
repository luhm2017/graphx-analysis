package com.lakala.datacenter.grograms

import com.lakala.datacenter.apply.model.NDegreeEntity
import com.lakala.datacenter.core.abstractions.PregelProgram
import com.lakala.datacenter.utils.UtilsToos.hashId
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

/**
  * Created by peter on 2017/4/27.
  */
class ApplyDegreeCentralityProgram(@transient val graph: Graph[Map[VertexId, NDegreeEntity], Int])
  extends PregelProgram[Map[VertexId, NDegreeEntity], Map[VertexId, NDegreeEntity], Int] with Serializable {
  type VMap = Map[VertexId, NDegreeEntity]

  /**
    * For serialization purposes.
    * See: [[org.apache.spark.graphx.impl.GraphImpl]]
    */
  protected def this() = this(null)

  /**
    * The vertex program receives a state update and acts to update its state
    *
    * @param id      is the [[VertexId]] that this program will perform a state operation for
    * @param state   is the current state of this [[VertexId]]
    * @param message is the state received from another vertex in the graph
    * @return a Map[VertexId, NDegreeEntity] resulting from a comparison between current state and incoming state
    */
  override def vertexProgram(id: VertexId, state: VMap, message: VMap): VMap = combiner(state, message)

  /**
    * The message broker sends and receives messages. It will initially receive one message for
    * each vertex in the graph.
    *
    * @param triplet An edge triplet is an object containing a pair of connected vertex objects and edge object.
    *                For example (v1)-[r]->(v2)
    * @return The message broker returns a key value list, each containing a VertexId and a new message
    */
  override def messageBroker(triplet: EdgeTriplet[VMap, Int]): Iterator[(VertexId, VMap)] = {
    //取出源节点差集key值 发送(e.srcId->目标节点)
    var srcId = triplet.srcId.toString
    var dstId = triplet.dstId.toString

    if (srcId.length > 11 || dstId.length > 11) {
      val arr = analyedID(srcId, dstId, triplet.srcAttr ++ triplet.dstAttr)
      srcId = arr(0)
      dstId = arr(1)
    }

    val dstMap = (triplet.srcAttr.keySet -- triplet.dstAttr.keySet).map { k =>
      val e2 = triplet.srcAttr(k)
      k -> e2.copy(s"${e2.attr}->$dstId",initType = e2.initType, loop = e2.loop - 1)
    }.toMap
    Iterator((triplet.dstId, dstMap) /*, (e.srcId, srcMap)*/)
  }

  def analyedID(srcId: String, dstId: String, att: VMap): Array[String] = {
    val totalAttr = att.values.map { k =>
      k.attr.split("-->").flatMap { v => v.split("->") }
    }.flatMap(k => k)
    val src = totalAttr.find(k => srcId.toLong == hashId(k)).getOrElse("" + srcId)
    val dst = totalAttr.find(k => dstId.toLong == hashId(k)).getOrElse("" + dstId)
    Array(src, dst)
  }

  /**
    * This method is used to reduce or combine the set of all state outcomes produced by a vertexProgram
    * for each vertex in each superstep iteration. Each vertex has a list of state updates received from
    * other vertices in the graph via the messageBroker method. This method is used to reduce the list
    * of state updates into a single state for the next superstep iteration.
    *
    * @param a A first Map[VertexId, NDegreeEntity] representing a partial state of a vertex.
    * @param b A second Map[VertexId, NDegreeEntity] representing a different partial state of a vertex
    * @return a merged Map[VertexId, NDegreeEntity] representation from the two Map[VertexId, NDegreeEntity] parameters
    */
  override def combiner(a: VMap, b: VMap): VMap =
    (a.keySet ++ b.keySet).map {
      k =>
        var result: NDegreeEntity = null
        if (a.get(k).isDefined && b.get(k).isDefined) {
          var spmap11 = a.getOrElse(k, new NDegreeEntity(s"$k->$k", loop=Int.MaxValue))
          var spmap12 = b.getOrElse(k, new NDegreeEntity(s"$k->$k", loop=Int.MaxValue))
          val loop:Int = math.min(spmap11.loop, spmap12.loop)
          result = spmap11.copy(mergeMsg(spmap11.attr, spmap12.attr),initType = spmap11.initType, loop=loop)

        } else {
          result = if (a.get(k).isDefined) a.getOrElse(k, new NDegreeEntity(s"$k->$k", loop=Int.MaxValue))
          else b.getOrElse(k, new NDegreeEntity(s"$k->$k", loop=Int.MaxValue))
        }
        k -> result
    }.toMap

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
    * This method wraps Spark's Pregel API entry point from the [[org.apache.spark.graphx.GraphOps]] class.
    * This provides a simple way to write a suite of graph algorithms by extending the [[PregelProgram]]
    * abstract class and implementing vertexProgram, messageBroker, and combiner methods.
    */
  def run(): Graph[VMap, Int] = {
    graph.pregel(Map[VertexId, NDegreeEntity]())(this.vertexProgram, this.messageBroker, this.combiner)
  }

  def run(degree: Int): Graph[VMap, Int] = {
    graph.pregel(Map[VertexId, NDegreeEntity](), degree)(this.vertexProgram, this.messageBroker, this.combiner)
  }

  def run(degree: Int, activeDirection: EdgeDirection): Graph[VMap, Int] = {
    graph.pregel(Map[VertexId, NDegreeEntity](), degree, activeDirection)(this.vertexProgram, this.messageBroker, this.combiner)
  }

}
