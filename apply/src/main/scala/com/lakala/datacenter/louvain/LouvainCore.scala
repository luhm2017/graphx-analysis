package com.lakala.datacenter.louvain

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._

import scala.reflect.ClassTag


/**
  * Provides low level louvain community detection algorithm functions.  Generally used by LouvainHarness
  * to coordinate the correct execution of the algorithm though its several stages.
  *
  * For details on the sequential algorithm see:  Fast unfolding of communities in large networks, Blondel 2008
  */
object LouvainCore {


  /**
    * Generates a new graph of type Graph[VertexState,Long] based on an input graph of type.
    * Graph[VD,Double].  The resulting graph can be used for louvain computation.
    * 将RDD转换成Graph[VertexState,Double]类型的Graph,其中Long是权重类型,默认是1
    * Louvain算法将使用返回Graph[VertexState,Double]进行计算
    * @param graph 最原始的图
    * @tparam VD 节点属性类型
    * @return
    */
  def createLouvainGraph[VD: ClassTag](graph: Graph[VD, Double]): Graph[VertexState, Double] = {
    // Create the initial Louvain graph.  
    //    val nodeWeightMapFunc = (e:EdgeTriplet[VD,Double]) => Iterator((e.srcId,e.attr), (e.dstId,e.attr))
    //    val nodeWeightReduceFunc = (e1:Long,e2:Double) => e1+e2
    //计算每个目的节点的入度,作为权值(原算法是把出入度都相加)
    //(Double,Double)表示这个节点的(入度,出度)
    val nodeWeights: VertexRDD[Double] = graph.aggregateMessages(
      trip => {
        //        trip.sendToSrc((0d,trip.attr))
        //        trip.sendToDst((trip.attr,0d))
        trip.sendToSrc(trip.attr)
        trip.sendToDst(trip.attr)
      },
      //对一个节点的边信息做聚合,入度相加或者出度相加
      (a, b) => a + b
    )

    val louvainGraph = graph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0D)
      val state = new VertexState()
      state.community = vid //将每个节点的社区ID初始化为自身ID
      state.changed = false
      state.communitySigmaTot = weight //入度
      state.internalWeight = 0D
      state.nodeWeight = weight //出度
      state.q = 0D //模块度
      state
    })/*.partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)*/
//    4 {community:4,communitySigmaTot:3.0,internalWeight:0.0,nodeWeight:3.0}
//    1 {community:1,communitySigmaTot:9.0,internalWeight:0.0,nodeWeight:9.0}
//    6 {community:6,communitySigmaTot:3.0,internalWeight:0.0,nodeWeight:3.0}
//    3 {community:3,communitySigmaTot:7.0,internalWeight:0.0,nodeWeight:7.0}
//    7 {community:7,communitySigmaTot:5.0,internalWeight:0.0,nodeWeight:5.0}
//    9 {community:9,communitySigmaTot:2.0,internalWeight:0.0,nodeWeight:2.0}
//    8 {community:8,communitySigmaTot:2.0,internalWeight:0.0,nodeWeight:2.0}
//    5 {community:5,communitySigmaTot:3.0,internalWeight:0.0,nodeWeight:3.0}
//    2 {community:2,communitySigmaTot:8.0,internalWeight:0.0,nodeWeight:8.0}
    return louvainGraph
  }


  /**
    * Transform a graph from [VD,Long] to a a [VertexState,Long] graph and label each vertex with a community
    * to maximize global modularity (without compressing the graph)
    */
  def louvainFromStandardGraph[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Double], minProgress: Int = 1, progressCounter: Int = 1): (Double, Graph[VertexState, Double], Int) = {
    val louvainGraph = createLouvainGraph(graph)
    return louvain(sc, louvainGraph, minProgress, progressCounter)
  }


  /**
    * For a graph of type Graph[VertexState,Long] label each vertex with a community to maximize global modularity.
    * (without compressing the graph)
    * 在Graph[VertexState,Double]结构上,迭代计算每个节点所属的社区,直至模块度达到最大
    * 该函数中不包含对图的压缩,压缩在另一个函数中实现
    * step:
    * (1).在graph:Graph[VertexState, Double]上执行Graph.aggregateMessages计算每个节点
    * 对其所有邻居社区的权重,得到msgRDD:VertexRDD[Map[(Long,Double),Double]]。
    * 这个RDD是个map,(社区ID、社区权重)=>节点相对该社区的权重。
    * (2).对graph: Graph[VertexState, Double]和msgRDD:VertexRDD[Map[(Long,Double),Double]]做关联,
    * 对每个节点计算其属于邻居社区的ΔQ(模块度),找到最大的ΔQ(模块度),更新该节点所属的社区。
    * 得到labeledVerts:VertexRDD[VertexState]集合,记录了节点ID和对应的新VertexState
    * (3).将新社区信息更新到graph: Graph[VertexState, Double]上,分几步实现:
    * (a).更新每个社区的权值,在labeledVerts:VertexRDD[VertexState]上计算每个社区ID新的权值,
    * 得到communtiyUpdate:RDD[Long,Double]集合,key是社区ID,value是新社区权重
    * (b).将labeledVerts:VertexRDD[VertexState]与communtiyUpdate:RDD[Long,Double] 关联,
    * 得到communityMapping:RDD[(VertexId,(Long,Double))]集合,key是节点ID,value是(社区ID,新社区权重)
    * (c).将labeledVerts:VertexRDD[VertexState]与communityMapping:RDD[(VertexId,(Long,Double))]执行join操作,
    * 得到updatedVerts:RDD[(VertexId,VertexState)]集合,key是节点ID,value是包含了新社区ID和社区权重的VertexState
    * (d)最后将graph: Graph[VertexState, Double]和updatedVerts:RDD[(VertexId,VertexState)]执行outerJoinVertices操作,
    * 得到更新了本轮计算的新社区信息的louvainGraph:Graph[VertexState, Double]
    * (4).根据新的社区信息,更新msgRDD
    * (5).判断是否需要停止迭代,若继续迭代,则重复(2)的步骤
    * (6).重新计算整个图的模块度,返回结果
    *
    * @param sc
    * @param graph
    * @param minProgress
    * @param progressCounter
    * @return
    */
  def louvain(sc: SparkContext, graph: Graph[VertexState, Double], minProgress: Int = 1, progressCounter: Int = 1): (Double, Graph[VertexState, Double], Int) = {
    var louvainGraph = graph.cache()
    //所有节点对的边权重之和
    val graphWeight = louvainGraph.vertices.values.map(vdata => vdata.internalWeight + vdata.nodeWeight).reduce(_ + _)
    var totalGraphWeight = sc.broadcast(graphWeight)
    println("totalEdgeWeight: " + totalGraphWeight.value)

    // 构造msgRDD，获得(相邻社区，社区度数)-->度数 的Map
    //收集每个节点的邻居节点社区ID、社区权重并累加,为后面判断所属社区做准备
    // gather community information from each vertex's local neighborhood
    var msgRDD = louvainGraph.aggregateMessages(sendMsg, mergeMsg)
    //scala有延后执行的特性,这里为保证数据可用,强制执行
    var activeMessages = msgRDD.count() //materializes the msgRDD and caches it in memory
//    4 (1,9.0) -> 2.0,(3,7.0) -> 1.0
//    1 (7,5.0) -> 2.0,(2,8.0) -> 2.0,(8,2.0) -> 1.0,(3,7.0) -> 2.0,(4,3.0) -> 2.0
//    6 (7,5.0) -> 1.0,(2,8.0) -> 2.0
//    3 (7,5.0) -> 1.0,(2,8.0) -> 2.0,(1,9.0) -> 2.0,(5,3.0) -> 1.0,(4,3.0) -> 1.0
//    7 (6,3.0) -> 1.0,(1,9.0) -> 2.0,(8,2.0) -> 1.0,(3,7.0) -> 1.0
//    9 (5,3.0) -> 1.0,(2,8.0) -> 1.0
//    8 (1,9.0) -> 1.0,(7,5.0) -> 1.0
//    5 (9,2.0) -> 1.0,(2,8.0) -> 1.0,(3,7.0) -> 1.0
//    2 (6,3.0) -> 2.0,(1,9.0) -> 2.0,(3,7.0) -> 2.0,(5,3.0) -> 1.0,(9,2.0) -> 1.0
    var updated = 0L - minProgress
    var even = false
    var count = 0
    val maxIter = 100000
    var stop = 0
    var updatedLastPhase = 0L
    do {
      count += 1
      even = !even

      // label each vertex with its best community based on neighboring community information
      //计算当前节点所属的最佳社区
      val labeledVerts = louvainVertJoin(louvainGraph, msgRDD, totalGraphWeight, even).cache()

      // calculate new sigma total value for each community (total weight of each community)
      //统计所有社区的权重
      val communtiyUpdate = labeledVerts
        .map({ case (vid, vdata) => (vdata.community, vdata.nodeWeight + vdata.internalWeight) })
        .reduceByKey(_ + _).cache()

      // map each vertex ID to its updated community information
      //将节点ID与新社区ID、权重关联
      val communityMapping = labeledVerts
        .map({ case (vid, vdata) => (vdata.community, vid) })
        .join(communtiyUpdate)
        .map({ case (community, (vid, sigmaTot)) => (vid, (community, sigmaTot)) })
        .cache()

      // join the community labeled vertices with the updated community info
      //更新VertexState值,建立节点ID=>新VertexState映射
      val updatedVerts = labeledVerts.join(communityMapping).map({ case (vid, (vdata, communityTuple)) =>
        vdata.community = communityTuple._1
        vdata.communitySigmaTot = communityTuple._2
        (vid, vdata)
      }).cache()
      updatedVerts.count()
      labeledVerts.unpersist(blocking = false)
      communtiyUpdate.unpersist(blocking = false)
      communityMapping.unpersist(blocking = false)
      //得到更新后的Graph
      val prevG = louvainGraph
      louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
      louvainGraph.cache()

      // gather community information from each vertex's local neighborhood
      //更新msgRDD用于下次迭代,并将不同的临时表释放
      val oldMsgs = msgRDD
      msgRDD = louvainGraph.aggregateMessages(sendMsg, mergeMsg).cache()
      activeMessages = msgRDD.count() // materializes the graph by forcing computation

      oldMsgs.unpersist(blocking = false)
      updatedVerts.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)

      // half of the communites can swtich on even cycles
      // and the other half on odd cycles (to prevent deadlocks)
      // so we only want to look for progess on odd cycles (after all vertcies have had a chance to move)
      if (even) updated = 0
      updated = updated + louvainGraph.vertices.filter(_._2.changed).count
      if (!even) {
        println("  # vertices moved: " + java.text.NumberFormat.getInstance().format(updated))
        if (updated >= updatedLastPhase - minProgress) stop += 1
        updatedLastPhase = updated
      }


    } while (stop <= progressCounter && (even || (updated > 0 && count < maxIter)))
    println("\nCompleted in " + count + " cycles")


    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    //重新计算整体图的模块度
    val newVerts = louvainGraph.vertices.innerJoin(msgRDD)((vid, vdata, msgs) => {
      // sum the nodes internal weight and all of its edges that are in its community
      val community = vdata.community
      var k_i_in = vdata.internalWeight
      var sigmaTot = vdata.communitySigmaTot.toDouble
      msgs.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        if (vdata.community == communityId) k_i_in += communityEdgeWeight
      })
      val M = totalGraphWeight.value
      val k_i = vdata.nodeWeight + vdata.internalWeight
      //计算新模块度
      var q = (k_i_in.toDouble / M) - ((sigmaTot * k_i) / math.pow(M, 2))
      println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
      if (q < 0) 0 else q
    })
    val actualQ = newVerts.values.reduce(_ + _)

    // return the modularity value of the graph along with the 
    // graph. vertices are labeled with their community
    //返回更新后模块度和完成图
    return (actualQ, louvainGraph, count / 2)

  }


  /**
    * Creates the messages passed between each vertex to convey neighborhood community data.
    */
  private def sendMsg(et: EdgeContext[VertexState, Double, Map[(Long, Double), Double]]) = {
    //    val m1 = (et.dstId,Map((et.srcAttr.community,et.srcAttr.communitySigmaTot)->et.attr))
    //    val m2 = (et.srcId,Map((et.dstAttr.community,et.dstAttr.communitySigmaTot)->et.attr))
    et.sendToDst(Map((et.srcAttr.community, et.srcAttr.communitySigmaTot) -> et.attr))
    et.sendToSrc(Map((et.dstAttr.community, et.dstAttr.communitySigmaTot) -> et.attr))
  }


  /**
    * Merge neighborhood community data into a single message for each vertex
    */
  private def mergeMsg(m1: Map[(Long, Double), Double], m2: Map[(Long, Double), Double]) = {
    val newMap = scala.collection.mutable.HashMap[(Long, Double), Double]()
    m1.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }


  /**
    * Join vertices with community data form their neighborhood and select the best community for each vertex to maximize change in modularity.
    * Returns a new set of vertices with the updated vertex state.
    * 计算当前节点所属的最佳社区
    *
    * @param louvainGraph
    * @param msgRDD :VertexRDD[Map[(Long,Double),Double]] 每个节点的社区ID和社区度数和度数
    * @param totalEdgeWeight
    * @param even
    * @return
    */
  private def louvainVertJoin(louvainGraph: Graph[VertexState, Double], msgRDD: VertexRDD[Map[(Long, Double), Double]], totalEdgeWeight: Broadcast[Double], even: Boolean): VertexRDD[VertexState] = {
    louvainGraph.vertices.innerJoin(msgRDD)((vid, vdata, msgs) => {
      var bestCommunity = vdata.community
      var startingCommunityId = bestCommunity
      var maxDeltaQ = BigDecimal(0.0)
      var bestSigmaTot = 0d
      msgs.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight,
          vdata.nodeWeight, vdata.internalWeight, totalEdgeWeight.value)

        println("   communtiy: " + communityId + " sigma:" + sigmaTotal + " edgeweight:" + communityEdgeWeight + "  q:" + deltaQ)
        if (deltaQ > maxDeltaQ ||
          (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))) {
          maxDeltaQ = deltaQ
          bestCommunity = communityId
          bestSigmaTot = sigmaTotal
        }
      })
      // only allow changes from low to high communties on even cyces and high to low on odd cycles
      if (vdata.community != bestCommunity &&
        ((even && vdata.community > bestCommunity) || (!even && vdata.community < bestCommunity))) {
        println("  " + vid + " SWITCHED from " + vdata.community + " to " + bestCommunity)
        vdata.community = bestCommunity
        vdata.communitySigmaTot = bestSigmaTot
        vdata.changed = true
      }
      else {
        vdata.changed = false
      }
      vdata
    })
  }


  /**
    * Returns the change in modularity that would result from a vertex moving to a specified community.
    *求出每个节点ΔQ(模块度)
    * 模块度也可以理解是社区内部边的权重减去所有与社区节点相连的边的权重和，对无向图更好理解，即社区内部边的度数减去社区内节点的总度数
    * @param currCommunityId 图的节点ID=startingCommunityId
    * @param testCommunityId msgRDD 节点的社区节点ID=communityId
    * @param testSigmaTot msgRDD 节点它所属社区节点ID的权重(度数)=sigmaTotal
    * @param edgeWeightInCommunity msgRDD 图的节点的与社区ID的边权重(度数)=communityEdgeWeight
    * @param nodeWeight  图节点的权重(度数)=vdata.nodeWeight
    * @param internalWeight 图节点的所有社区的权重=vdata.internalWeight
    * @param totalEdgeWeight 所有节点总的权重=totalEdgeWeight.value
    * @return
    */
  private def q(currCommunityId: Long, testCommunityId: Long, testSigmaTot: Double,
                edgeWeightInCommunity: Double, nodeWeight: Double, internalWeight: Double, totalEdgeWeight: Double): BigDecimal = {

    val isCurrentCommunity = (currCommunityId.equals(testCommunityId));
    val M = BigDecimal(totalEdgeWeight);
    val k_i_in_L = if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity;
    val k_i_in = BigDecimal(k_i_in_L);
    val k_i = BigDecimal(nodeWeight + internalWeight);
    val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot);

    var deltaQ = BigDecimal(0.0);
    if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
      deltaQ = k_i_in - (k_i * sigma_tot / M)
      println(s"      $deltaQ = $k_i_in - ( $k_i * $sigma_tot / $M")
    }

    return deltaQ;
  }


  /**
    * Compress a graph by its communities, aggregate both internal node weights and edge
    * weights within communities.
    *
    * @param graph
    * @param debug
    * @return
    */
  def compressGraph(graph: Graph[VertexState, Double], debug: Boolean = true): Graph[VertexState, Double] = {

    // aggregate the edge weights of self loops. edges with both src and dst in the same community.
    // WARNING  can not use graph.mapReduceTriplets because we are mapping to new vertexIds
    val internalEdgeWeights = graph.triplets.flatMap(et => {
      if (et.srcAttr.community == et.dstAttr.community) {
        Iterator((et.srcAttr.community, 2 * et.attr)) // count the weight from both nodes  // count the weight from both nodes
      }
      else Iterator.empty
    }).reduceByKey(_ + _)


    // aggregate the internal weights of all nodes in each community
    var internalWeights = graph.vertices.values.map(vdata => (vdata.community, vdata.internalWeight)).reduceByKey(_ + _)

    // join internal weights and self edges to find new interal weight of each community
    val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({ case (vid, (weight1, weight2Option)) =>
      val weight2 = weight2Option.getOrElse(0D)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = 0L
      state.internalWeight = weight1 + weight2
      state.nodeWeight = 0L
      (vid, state)
    }).cache()


    // translate each vertex edge to a community edge
    val edges = graph.triplets.flatMap(et => {
      val src = math.min(et.srcAttr.community, et.dstAttr.community)
      val dst = math.max(et.srcAttr.community, et.dstAttr.community)
      if (src != dst) Iterator(new Edge(src, dst, et.attr))
      else Iterator.empty
    }).cache()


    // generate a new graph where each community of the previous
    // graph is now represented as a single vertex
    val compressedGraph = Graph(newVerts, edges)
      .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    // calculate the weighted degree of each node
    val nodeWeightMapFunc = (e: EdgeTriplet[VertexState, Double]) => Iterator((e.srcId, e.attr), (e.dstId, e.attr))
    val nodeWeightReduceFunc = (e1: Double, e2: Double) => e1 + e2
    val nodeWeights: VertexRDD[Double] = graph.aggregateMessages(
      trip => {
        trip.sendToSrc(trip.attr)
        trip.sendToDst(trip.attr)
      },
      (a, b) => a + b
    )

    // fill in the weighted degree of each node
    // val louvainGraph = compressedGraph.joinVertices(nodeWeights)((vid,data,weight)=> {
    val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0D)
      data.communitySigmaTot = weight + data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count() // materialize the graph

    newVerts.unpersist(blocking = false)
    edges.unpersist(blocking = false)
    return louvainGraph


  }


  // debug printing
  private def printlouvain(graph: Graph[VertexState, Long]) = {
    print("\ncommunity label snapshot\n(vid,community,sigmaTot)\n")
    graph.vertices.mapValues((vid, vdata) => (vdata.community, vdata.communitySigmaTot)).collect().foreach(f => println(" " + f))
  }


  // debug printing
  private def printedgetriplets(graph: Graph[VertexState, Long]) = {
    print("\ncommunity label snapshot FROM TRIPLETS\n(vid,community,sigmaTot)\n")
    (graph.triplets.flatMap(e => Iterator((e.srcId, e.srcAttr.community, e.srcAttr.communitySigmaTot), (e.dstId, e.dstAttr.community, e.dstAttr.communitySigmaTot))).collect()).foreach(f => println(" " + f))
  }


}