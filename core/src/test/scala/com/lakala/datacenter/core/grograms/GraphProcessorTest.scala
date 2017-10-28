/*
package com.lakala.datacenter.core.grograms

/**
  * Created by peter on 2017/4/26.
  */

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.util.GraphGenerators
import com.lakala.datacenter.core.algorithms._
import com.lakala.datacenter.core.config.ConfigurationLoader
import com.lakala.datacenter.core.hdfs.FileUtil
import com.lakala.datacenter.core.models.ProcessorMessage
import com.lakala.datacenter.core.models.ProcessorMode
import collection.JavaConversions._
import scala.collection.mutable
import java.io.IOException
import java.net.URISyntaxException
import java.util

import com.lakala.datacenter.core.processor.GraphProcessor
import junit.framework.Assert.assertEquals


object GraphProcessorTest {
  def main(args: Array[String]): Unit = {
    //    testProcessEdgeList()
    //    testEdgeBetweenness()
    //    testBetweennessCentrality()
    //    performanceTestBetweennessCentrality()
    singleSourceShortestPathTest()
  }

  @throws[Exception]
  def testProcessEdgeList(): Unit = {
    ConfigurationLoader.testPropertyAccess = true
    // Create test path
    val path = ConfigurationLoader.getInstance.getHadoopHdfsUri + "/test/edgeList.txt"
    // Test writing the PageRank result to HDFS path
    FileUtil.writeListFile(path, new util.ArrayList[String](util.Arrays.asList("0 1\n", "1 3\n", "3 0")).iterator)
    GraphProcessor.processEdgeList(new ProcessorMessage(path, GraphProcessor.TRIANGLE_COUNT, ProcessorMode.Unpartitioned))
  }

  //  @Test
  @throws[Exception]
  def testEdgeBetweenness(): Unit = {
    ConfigurationLoader.testPropertyAccess = true
    // Test case A
    val expectedA = "0 0.0\n1 3.0\n2 4.0\n3 3.0\n4 0.0\n"
    val nodeListA = util.Arrays.asList("2 1\n", "3 2\n", "4 3\n", "5 4\n", "6 5\n", "7 6\n", "8 7\n", "9 8\n", "10 9\n", "1 10\n", "1 3\n", "2 3\n", "3 5\n", "4 5\n", "5 7\n", "6 7\n", "7 9\n", "8 9\n", "9 1\n", "10 1")
    val actualA = getEdgeBetweennessCentrality("a", nodeListA)
    assertEquals(expectedA, actualA)
  }

  //  @Test
  @throws[Exception]
  def testBetweennessCentrality(): Unit = {
    ConfigurationLoader.testPropertyAccess = true
    val expectedA = "0 0.0\n1 3.0\n2 4.0\n3 3.0\n4 0.0\n"
    val nodeListA = util.Arrays.asList("0 1\n", "1 2\n", "2 3\n", "3 4\n")
    val actualA = getBetweennessCentrality("a", nodeListA)
    assertEquals(expectedA, actualA)
    // Test case B
    val expectedB = "0 0.0\n1 1.0\n2 0.5\n3 0.0\n4 1.5\n"
    // (4)<--(0)-->(1)-->(2)-->(3)<--(4)<--(1)
    val nodeListB = util.Arrays.asList("0 1\n", "1 4\n", "0 4\n", "4 3\n", "1 2\n", "2 3")
    val actualB = getBetweennessCentrality("b", nodeListB)
    assertEquals(expectedB, actualB)
    // Test case C
    val expectedC = "1 22.0\n" + "2 4.0\n" + "3 22.0\n" + "4 4.0\n" + "5 22.0\n" + "6 4.0\n" + "7 22.0\n" + "8 4.0\n" + "9 22.0\n" + "10 4.0\n"
    val nodeListC = util.Arrays.asList("2 1\n", "3 2\n", "4 3\n", "5 4\n", "6 5\n", "7 6\n", "8 7\n", "9 8\n", "10 9\n", "1 10\n", "1 3\n", "2 3\n", "3 5\n", "4 5\n", "5 7\n", "6 7\n", "7 9\n", "8 9\n", "9 1\n", "10 1")
    val actualC = getBetweennessCentrality("c", nodeListC)
    assertEquals(expectedC, actualC)
  }

  //  @Test
  @throws[Exception]
  def performanceTestBetweennessCentrality(): Unit = {
    ConfigurationLoader.testPropertyAccess = true
    if (GraphProcessor.javaSparkContext == null) GraphProcessor.initializeSparkContext
    // Generate random graph
    val graph: Graph[Long, Int] = GraphGenerators.logNormalGraph(GraphProcessor.javaSparkContext.sc, 100, 0, 4, 5, 423)
    val starGraph = graph.edges.map((a: Edge[Int]) => a.srcId + " " + a.dstId + "\n").collect().toList
    println(starGraph)
    System.out.println(getBetweennessCentrality("performance-graph", starGraph))
  }

  @throws[IOException]
  @throws[URISyntaxException]
  private def getBetweennessCentrality(test: String, nodeList: util.List[String]) = {
    val path = ConfigurationLoader.getInstance.getHadoopHdfsUri + "/test/" + test + "/edgeList.txt"
    FileUtil.writeListFile(path, nodeList.iterator)
    if (GraphProcessor.javaSparkContext == null) GraphProcessor.initializeSparkContext
    val sb = new StringBuffer
    val results = Algorithms.betweennessCentrality(GraphProcessor.javaSparkContext.sc, path).foreach(value => sb.append(value))
    sb.toString
  }

  @throws[IOException]
  @throws[URISyntaxException]
  private def getEdgeBetweennessCentrality(test: String, nodeList: util.List[String]) = {
    val path = ConfigurationLoader.getInstance.getHadoopHdfsUri + "/test/" + test + "/edgeList.txt"
    FileUtil.writeListFile(path, nodeList.iterator)
    if (GraphProcessor.javaSparkContext == null) GraphProcessor.initializeSparkContext
    val sb = new StringBuffer
    val results = Algorithms.edgeBetweenness(GraphProcessor.javaSparkContext.sc, path).foreach(value => sb.append(value))
    sb.toString
  }

  //  @Test
  @throws[Exception]
  def testVertexPath(): Unit = {
    val tree = new DecisionTree[Long](0L, new mutable.HashMap[Long, DecisionTree[Long]])
    tree.traverseTo(0L).addLeaf(1L)
    tree.traverseTo(0L).addLeaf(2L).addLeaf(3L).addLeaf(4L).addLeaf(5L)
    tree.traverseTo(4L).addLeaf(6L)
    tree.traverseTo(4L).addLeaf(7L).addLeaf(8L)
    tree.traverseTo(7L).addLeaf(9L)
    System.out.println(tree.renderGraph)
    System.out.println(tree.shortestPathTo(9L))
  }

  //  @Test
  @throws[Exception]
  def collaborativeFilteringTest(): Unit = {
    ConfigurationLoader.testPropertyAccess = true
    val path = "src/test/resources/recommendation"
    if (GraphProcessor.javaSparkContext == null) GraphProcessor.initializeSparkContext
    Algorithms.collaborativeFiltering(GraphProcessor.javaSparkContext.sc, path).foreach(println)

  }

  //  @Test
  @throws[Exception]
  def singleSourceShortestPathTest(): Unit = {
    ConfigurationLoader.testPropertyAccess = true
    val path = "C:/Users/peter/lakalaFinance_workspaces/graphx-analysis/apply/data3/us_cities_edges.txt"
    if (GraphProcessor.javaSparkContext == null) GraphProcessor.initializeSparkContext
    Algorithms.singleSourceShortestPath(GraphProcessor.javaSparkContext.sc, path).foreach(println)

  }
}
*/
