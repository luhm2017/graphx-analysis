/*
package com.lakala.datacenter.load.spark

import com.lakala.datacenter.enums.Labels
import org.neo4j.graphdb.index.IndexHits
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.helpers.collection.MapUtil
import org.neo4j.index.impl.lucene.legacy.LuceneIndexImplementation
import org.neo4j.rest.graphdb.index.RestIndex
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine
import org.neo4j.rest.graphdb.{RestAPI, RestAPIFacade}

/**
  * Created by Administrator on 2017/6/19 0019.
  */
object ClusterGraphDatabase {
  private var restAPI: RestAPI = null
  private val serverBaseUrl = "http://192.168.0.33:7474/db/data"
  private val user = "neo4j"
  private val password = "123456"

  def main(args: Array[String]): Unit = {
    try
      setUp
      countExistingNodes
      tearDown
  }

  @throws[Throwable]
  def setUp(): Unit = {
    restAPI = new RestAPIFacade(serverBaseUrl, user, password)
    validateServerIsUp()
    val queryEngine = new RestCypherQueryEngine(restAPI)
    //    graphdb = queryEngine.asInstanceOf[GraphDatabaseService]
  }

  @throws[Throwable]
  private def validateServerIsUp() = {
    try
      restAPI.getAllLabelNames
    catch {
      case e: Throwable =>
        println(" !!!!!!!!!!!!!!!! NOTE !!!!!!!!!!!!!!!!!!!!!!!!  \n" + "this test assumes a Neo4j Server is running in a separate process \n" + "on localhost port 7474. You will need to manually start it before \n" + "running these demo tests.")
        throw e
    }
  }

  def tearDown(): Unit = {
    restAPI.close()
  }


  def countExistingNodes(): Unit = {
    //472
    val node2 = restAPI.getNodeById(293)
    println(node2.getLabels.iterator().next().name())
    val indexs = restAPI.createIndex(classOf[Node], "orderno", LuceneIndexImplementation.EXACT_CONFIG)
    val relIndex: RestIndex[Relationship] = restAPI.createIndex(classOf[Relationship], "terminal", LuceneIndexImplementation.EXACT_CONFIG)
    val terminalIndexs = restAPI.createIndex(classOf[Node], "content", LuceneIndexImplementation.EXACT_CONFIG)

    val hitIndex2: IndexHits[Node] = indexs.get("orderno", "XNA20170617214709013851193476043")
    val hitIndex: IndexHits[Node] = terminalIndexs.get("content", "CBC3A110160228103")
    println(hitIndex2.size())
    println(hitIndex2.getSingle)
    println("#################")
    println(hitIndex.size())
    println(hitIndex.getSingle)
    val applyNode = restAPI.getOrCreateNode(indexs, "orderno", "XNA20170617214709013851193476043", MapUtil.map("term_id", "CBC3A110160228103"))
    applyNode.addLabel(Labels.ApplyInfo)
    println(applyNode.getLabels.iterator().next().name())
    applyNode.setProperty("orderno", "XNA20170617214709013851193476043")
    //    applyNode.setProperty("term_id", "CBC3A110160228103")
    applyNode.setProperty("modelname", Labels.ApplyInfo)

    val terminalNode = restAPI.getOrCreateNode(terminalIndexs, "content", "CBC3A110160228103", MapUtil.map())
    terminalNode.addLabel(Labels.Terminal)
    terminalNode.setProperty("modelname", Labels.Terminal)
    println(terminalNode.getLabels.iterator().next().name())
    val rel = restAPI.getOrCreateRelationship(relIndex, "", "", applyNode, terminalNode, "terminal", MapUtil.map())

    if (applyNode != null) {
      println("====================")
      println("apply  node " + applyNode.getId() + " terminal node " + terminalNode.getId + " relationship " + rel.getId + "  is created.")
    }

  }
}
*/
