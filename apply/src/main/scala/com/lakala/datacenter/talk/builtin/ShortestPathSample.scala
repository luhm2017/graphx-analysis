package com.lakala.datacenter.talk.builtin

/**
  * Created by peter on 2017/4/26.
  */

import com.lakala.datacenter.common.graphstream.SimpleGraphViewer
import com.lakala.datacenter.talk.types.{City, VertexAttribute}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object ShortestPathSample {
  type CityName = String
  type Distance = Double

  val vertices = "C:/Users/peter/lakalaFinance_workspaces/graphx-analysis/apply/data3/us_cities_vertices.txt"
  val edges = "C:/Users/peter/lakalaFinance_workspaces/graphx-analysis/apply/data3/us_cities_edges.txt"

  def main(args: Array[String]): Unit = {
    // launches the viewer of the graph
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

//    val sgv = new SimpleGraphViewer(vertices, edges, false)
    val sgv = new SimpleGraphViewer("", "", false)
    sgv.runIteratro(Source.fromFile(vertices).getLines(), Source.fromFile(edges).getLines(), true)
    sgv.run()
    // loads the graph
    val sparkConf = new SparkConf().setAppName("RDD Sample Application").setMaster("local")

    val sparkContext = new SparkContext(sparkConf)
    val graph: Graph[String, Double] = loadCitiesGraphFromFiles(sparkContext, s"file:///$vertices", s"file:///$edges")

    // launches pregel computation for shortest path
    shortestPath(sparkContext, graph)
  }


  /**
    * Pregel implementation of Dijkstra algorithm for shortest path:
    * https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm
    *
    * @param sparkContext
    * @param graph
    */
  def shortestPath(sparkContext: SparkContext, graph: Graph[String, Double]) = {

    // we want to know the shortest paths from the vertex 1 (city of Arad)
    // to all the others vertices (all the other cities)
    val sourceCityId: VertexId = 1L

    // initialize a new graph with data of the old one, plus distance and path;
    // for all vertices except the city we want to compute the path from, the
    // distance is set to infinity
    val initialGraph: Graph[VertexAttribute, Double] = graph.mapVertices(
      (vertexId, cityName) =>
        if (vertexId == sourceCityId) {
          VertexAttribute(
            cityName,
            0.0,
            List[City](new City(cityName, sourceCityId))
          )
        }
        else {
          VertexAttribute(
            cityName,
            Double.PositiveInfinity,
            List[City]())
        }
    )

    // calls pregel
    val shortestPathGraph = initialGraph.pregel(
      initialMsg = VertexAttribute("", Double.PositiveInfinity, List[City]()),
      maxIterations = Int.MaxValue,
      activeDirection = EdgeDirection.Out
    )(
      vprog,
      sendMsg,
      mergeMsg
    )

    // writes the results
    println("\n\nshortestPahtSample start .....")
    for ((destVertexId, attribute) <- shortestPathGraph.vertices) {
      println(s"Going from Washington to ${attribute.cityName} " +
        s"has a distance of ${attribute.distance} km. " +
        s"Path is: ${attribute.path.mkString(" => ")}")
    }
    println("\n\nshortestPahtSample end .....")
  }

  val vprog = (vertexId: VertexId, currentVertexAttr: VertexAttribute, newVertexAttr: VertexAttribute) =>
    if (currentVertexAttr.distance <= newVertexAttr.distance) currentVertexAttr else newVertexAttr

  val sendMsg = (edgeTriplet: EdgeTriplet[VertexAttribute, Double]) => {
    if (edgeTriplet.srcAttr.distance < (edgeTriplet.dstAttr.distance - edgeTriplet.attr)) {
      Iterator(
        (edgeTriplet.dstId,
          new VertexAttribute(
            edgeTriplet.dstAttr.cityName,
            edgeTriplet.srcAttr.distance + edgeTriplet.attr,
            edgeTriplet.srcAttr.path :+ new City(edgeTriplet.dstAttr.cityName, edgeTriplet.dstId)
          )
        )
      )
    }
    else {
      Iterator.empty
    }
  }

  val mergeMsg = (attribute1: VertexAttribute, attribute2: VertexAttribute) =>
    if (attribute1.distance < attribute2.distance) {
      attribute1
    }
    else {
      attribute2
    }


  /**
    * creates a Graph of cities loading the nodes and the edges from filesystem
    *
    * @param sparkContext
    * @param edgesFilename
    * @param verticesFilename
    * @return
    */
  def loadCitiesGraphFromFiles(sparkContext: SparkContext, verticesFilename: String,
                               edgesFilename: String): Graph[CityName, Distance] = {

    val edges = sparkContext.textFile(edgesFilename)
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(1).toLong, fields(2).toDouble)
      }

    val vertices = sparkContext.textFile(verticesFilename)
      .filter(line => line.length > 0 && line.charAt(0) != '#')
      .map { line =>
        val fields = line.split(" ")
        (fields(0).toLong, (fields(1)))
      }

    Graph(vertices, edges)
  }

}
