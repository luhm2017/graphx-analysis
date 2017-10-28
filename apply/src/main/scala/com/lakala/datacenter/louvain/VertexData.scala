package com.lakala.datacenter.louvain

import scala.collection.mutable.HashSet

/**
  * Created by chenqingqing on 2017/4/4.
  */
class VertexData(val vId: Long, var cId: Long) extends Serializable {
  var innerDegree = 0.0 //内部结点的权重
  var innerVertices = new HashSet[Long]() //内部的结点
  var degree = 0.0 //结点的度
  var commVertices = new HashSet[Long]() //社区中的结点
}