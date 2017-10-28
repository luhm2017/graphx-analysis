package com.lakala.datacenter.common.graphstream

/**
  * Created by peter on 2017/4/26.
  */

import java.awt.event.{MouseWheelEvent, MouseWheelListener}
import java.io.IOException
import org.apache.commons.lang3.StringUtils
import org.graphstream.graph.implementations.MultiGraph
import org.graphstream.graph.{Edge, Graph, Node}
import org.graphstream.ui.swingViewer.ViewPanel

import scala.collection.Iterator
import scala.io.Source


object SimpleGraphViewer {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val USERS_VERTICES_FILENAME = "F:/lakalaFinance_workspaces/graphx-analysis/common/src/test/data/users_vertices.txt"
    val LIKENESS_EDGES_FILENAME = "F:/lakalaFinance_workspaces/graphx-analysis/common/src/test/data/likeness_edges.txt"
    val simpleGraphViewer = new SimpleGraphViewer(USERS_VERTICES_FILENAME, LIKENESS_EDGES_FILENAME)
    simpleGraphViewer.run()
  }
}

class SimpleGraphViewer @throws[IOException]
(val verticesFilename: String, val edgesFilename: String, val isDirected: Boolean) {
  // creates the graph and its attributes
  private var graph: MultiGraph = null
  private var view: ViewPanel = null
  private var viewPercent: Double = 0.7


  System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")
  graph = new MultiGraph("Relationships")
  graph.addAttribute("ui.quality")
  graph.addAttribute("ui.antialias")
  val filePath = this.getClass.getClassLoader.getResource("css/style.css").getPath
  graph.addAttribute("ui.stylesheet", "url('" + filePath + "')")
  // adds nodes and edges to the graph

  // adds nodes and edges to the graph
  if (StringUtils.isNotBlank(verticesFilename) && StringUtils.isNotBlank(edgesFilename))
    addDataFromFile(graph, verticesFilename, edgesFilename, isDirected)


  def this(verticesFilename: String, edgesFilename: String) {
    this(verticesFilename, edgesFilename, true)
  }


  def runIteratro(verticesIterator: Iterator[String], edgesIterator: Iterator[String], isDirected: Boolean): Unit = {
    addDataFromIterator(graph: Graph, verticesIterator, edgesIterator, isDirected)
  }


  def run(): Unit = {
    // starts the GUI with a custom mouse wheel listener for zooming in and out
    view = graph.display(true).getDefaultView
    view.resizeFrame(800, 600)
    view.addMouseWheelListener(new MouseWheelListener() {
      override def mouseWheelMoved(e: MouseWheelEvent): Unit = {
        zoom(e.getWheelRotation < 0)
      }
    })
  }

  def zoom(zoomOut: Boolean): Unit = {
    viewPercent += viewPercent * 0.1 * (if (zoomOut) -1 else 1)
    view.getCamera.setViewPercent(viewPercent)
  }


  @throws[IOException]
  def addDataFromFile(graph: Graph, verticesFilename: String, edgesFilename: String, isDirected: Boolean): Unit = {
    val verticesIterator = Source.fromFile(verticesFilename).getLines()
    val edgesIterator = Source.fromFile(edgesFilename).getLines()
    addDataFromIterator(graph, verticesIterator, edgesIterator, isDirected)
  }

  @throws[IOException]
  def addDataFromIterator(graph: Graph, verticesIterator: Iterator[String], edgesIterator: Iterator[String], isDirected: Boolean,split:String=" "): Unit = {
    val nodesMap = new java.util.HashMap[String, String]
    val addedEdges = new java.util.HashSet[String]
    // loads the nodes
    verticesIterator.filter(line => line.charAt(0) != '#').foreach { line =>
      val values = line.split(split)
      nodesMap.put(values(0), values(1))
      val node: Node = graph.addNode(values(1))
      val label = new StringBuilder(values(1))
      if (values.length > 2) label.append(",").append(values(2))
      label.append("[").append(values(0)).append("]")
      node.addAttribute("ui.label", label.toString)
    }

    // loads the edges
    edgesIterator.filter(line => line.charAt(0) != '#').foreach { line =>
      val values = line.split(" ")
      val hasLabel = values.length > 2
      val id = new StringBuilder(values(0)).append("-").append(values(1))
      var counter = 0
      // for allowing multiple edges with the same source and same destination
      while ( {
        addedEdges.contains(id.toString)
      }) {
        if (counter > 0) id.delete(id.lastIndexOf("-") + 1, id.length - 1)
        id.append("-").append({
          counter += 1;
          counter - 1
        })
      }
      // FIXME: has to be fixed for multiple edges with same source and same dest
      val reverseId = new StringBuilder(values(1)).append("-").append(values(0)).toString
      val edge: Edge = graph.addEdge(id.toString, nodesMap.get(values(0)), nodesMap.get(values(1)), isDirected)
      // set layout attributes according to graph type
      val uiStyleAttribute = new StringBuilder
      if (hasLabel) {
        edge.setAttribute("ui.label", values(2))
        uiStyleAttribute.append("fill-color:").append(if (values(2) == "likes") {
          "#00CC00"
        }
        else {
          "#0000BB"
        }).append(";")
      }
      if (isDirected) uiStyleAttribute.append("text-offset: ").append(if (addedEdges.contains(reverseId)) "0,-10;"
      else "0,10;")
      else uiStyleAttribute.append("text-offset: 0,-10;").append("fill-color: #0000BB;")
      edge.setAttribute("ui.style", uiStyleAttribute.toString)
      addedEdges.add(id.toString)
    }

  }
}

