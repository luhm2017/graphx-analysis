package com.lakala.datacenter.cypher

/**
  * Created by Administrator on 2017/8/1 0001.
  */
import collection.JavaConverters._
import org.neo4j.driver.v1.types._
import org.neo4j.driver.v1.Session
import scalax.collection.Graph
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._
import scalax.collection.edge.LDiEdge // labeled directed edge
import scalax.collection.edge.Implicits._ // shortcuts
import org.neo4j.driver.v1.StatementResult

object NeoData {

  sealed trait NeoData
  //  a scala version of org.neo4j.driver.v1.types.Node
  case class NeoNode(id: Long, labels: Set[String], properties: Map[String, Object]) extends NeoData
  case class NeoRel(id: Long, `type`: String, properties: Map[String, Object]) extends NeoData

  def run(session: Session)(query: String): Graph[NeoNode, LDiEdge] = {
    toLDiGraph(session)(session.run(query))
  }

  def runEncodeEdge(session: Session)(query: String): Graph[NeoData, DiEdge] = {
    toDiGraph(session)(session.run(query))
  }

  def getNode(session: Session)(id: Long): NeoNode = {

    val out = session.run(
      raw"""MATCH (s)
       WHERE ID(s) = $id
       RETURN s""")
    val node = out.single().get("s").asNode()
    NeoNode(node.id(), node.labels().asScala.toSet, node.asMap().asScala.toMap)

  }

  // TODO still wish there was a type safe LDiEdge
  /**
    * Takes a record from neo4j and stores it in a Graph[NeoNode, LDiEdge],
    * nodes are encoded to as NeoNode, edges are encoded to LDiEdge
    * edge nodes are resolved with additional queries if they are needed
    *
    *   WARNING: since Noe4j uses mutlisets, this will colapse some edges in the Scala graph!!!!!
    */
  def toLDiGraph(session: Session)(rs: StatementResult): Graph[NeoNode, LDiEdge] = {

    var nodeIdMap = Map[Long, NeoNode]()

    //TODO: faster with the mutable graph?
    var g = Graph[NeoNode, LDiEdge]()

    for (rec <- rs.list().asScala) {
      for ((_, o) <- rec.asMap().asScala) {

        if (o.isInstanceOf[Node]) {
          val node = o.asInstanceOf[Node]

          val nn = NeoNode(node.id(), node.labels().asScala.toSet, node.asMap().asScala.toMap)
          nodeIdMap += nn.id -> nn

          g += nn

        } else if (o.isInstanceOf[Relationship]) {
          val rel = o.asInstanceOf[Relationship]

          //make sure both ends are added before the edge is added
          if (!nodeIdMap.contains(rel.startNodeId())) {
            val nn = getNode(session)(rel.startNodeId())

            nodeIdMap += nn.id -> nn
            g += nn
          }

          if (!nodeIdMap.contains(rel.endNodeId())) {
            val nn = getNode(session)(rel.endNodeId())

            nodeIdMap += nn.id -> nn
            g += nn
          }

          // assume directed from start to end, TODO: this is not a safe assumption!
          val rr = NeoRel(rel.id(), rel.`type`(), rel.asMap().asScala.toMap)

          g += (nodeIdMap(rel.startNodeId()) ~+> nodeIdMap(rel.endNodeId()))(rr)

        } else if (o.isInstanceOf[Path]) {
          //don't handle paths yet
          ???
        }
      }
    }
    g
  }


  /**
    * Takes a record from neo4j and stores it in a LDiEdge
    *  WARNING: since Noe4j uses mutlisets, this will colapse some edges in the Scala graph
    */
  def toDiGraph(session: Session)(rs: StatementResult): Graph[NeoData, DiEdge] = {

    var nodeIdMap = Map[Long, NeoNode]()

    //TODO: faster with the mutable graph?
    var g = Graph[NeoData, DiEdge]()

    for (rec <- rs.asScala) {

      println(rec)
      for ((_, o) <- rec.asMap().asScala) {

        if (o.isInstanceOf[Node]) {
          val node = o.asInstanceOf[Node]

          val nn = NeoNode(node.id(), node.labels().asScala.toSet, node.asMap().asScala.toMap)
          nodeIdMap += nn.id -> nn

          g += nn

        } else if (o.isInstanceOf[Relationship]) {
          val rel = o.asInstanceOf[Relationship]

          //make sure both ends are added before the edge is added
          if (!nodeIdMap.contains(rel.startNodeId())) {
            val nn = getNode(session)(rel.startNodeId())

            nodeIdMap += nn.id -> nn
            g += nn
          }

          if (!nodeIdMap.contains(rel.endNodeId())) {
            val nn = getNode(session)(rel.endNodeId())

            nodeIdMap += nn.id -> nn
            g += nn
          }

          // assume directed from start to end, TODO: this is not necissarily a safe assumption!
          val rr = NeoRel(rel.id(), rel.`type`(), rel.asMap().asScala.toMap)
          g += rr

          g += nodeIdMap(rel.startNodeId()) ~> rr

          g += rr ~> nodeIdMap(rel.endNodeId())

        } else if (o.isInstanceOf[Path]) {
          //don't handle paths yet
          ???
        }
      }
    }
    g
  }

  def allNodes(session: Session): Iterator[NeoNode] = {

    val recs = session.run("""MATCH (s) RETURN s""")

    for (rec <- recs.asScala) yield {
      val node = rec.get("s").asNode()

      NeoNode(node.id(), node.labels().asScala.toSet, node.asMap().asScala.toMap)
    }
  }

  def allEdges(session: Session): Iterator[(Long, NeoRel, Long)] = {
    val recs = session.run("""MATCH ()-[r]->() RETURN r """)

    for (rec <- recs.asScala) yield {
      val rel = rec.get("r").asRelationship()

      (rel.startNodeId(), NeoRel(rel.id(), rel.`type`(), rel.asMap().asScala.toMap), rel.endNodeId())
    }

  }

  /** essentally the most efficient way to dump a neo4j database into a scala graph */
  def fullGraph(session: Session): Graph[NeoData, DiEdge] = {
    val nodes = allNodes(session).toSet
    val idToNode = nodes.map(n => n.id -> n).toMap
    val edges = allEdges(session).map(e => (idToNode(e._1), e._2, idToNode(e._3))).toSet
    val scalaEdges = edges.flatMap(e => Set(e._1 ~> e._2, e._2 ~> e._3))
    // val scalaEdges = edges.flatMap(e => Set(e._1 ~> e._3))
    Graph.from(nodes, scalaEdges)
  }

}
