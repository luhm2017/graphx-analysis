package com.lakala.datacenter.louvain

/**
  * Louvain vertex state
  * Contains all information needed for louvain community detection
  */
class VertexState extends Serializable {

  var community = -1L //社区ID
  var communitySigmaTot = 0D //入度
  var internalWeight = 0D // self edges
  var nodeWeight = 0D; //out degree //出度
  var changed = false
  var q = 0D //模块度的值

  override def toString(): String = {
    //        "{community:"+community+",communitySigmaTot:"+communitySigmaTot+
    //          ",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+"}"
//    s"community:$community,communitySigmaTot:$communitySigmaTot,internalWeight:$internalWeight,nodeWeight:$nodeWeight"
    s"community:$community,q:$q"
//        community.toString
  }
}
