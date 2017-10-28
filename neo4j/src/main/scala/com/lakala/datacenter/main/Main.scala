package com.lakala.datacenter.main

import java.io.File

import com.lakala.datacenter.enums.RelationshipTypes
import com.lakala.datacenter.grogram.Neo4jDataGenerator
import com.lakala.datacenter.utils.Config
import org.joda.time.DateTime
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{Direction, GraphDatabaseService}
import org.neo4j.io.fs.FileUtils
import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2017/5/31 0031.
  */
object Main {
  private val logger = LoggerFactory.getLogger("Main")
  val COUNT = 100000 //数据批量提交
  //F:\tmp\applydir F:\tmp\neo4j\tmp01
  val FRIENDS_PER_USER = 50

  def main(args: Array[String]): Unit = {
    val mainTime = DateTime.now()
    println("start generateGraphData time " + DateTime.now())
    //    chackArgs(args) 13199050
    //    val config = ArgsCommon.parseArgs(args)
    val config = new Config()
    config.input = args(0)
    config.output = args(1)
    generateGraphData(config)
    val endtime = DateTime.now()
    println("end generateGraphData time " + endtime + "+run long time " + (endtime.getMillis - mainTime.getMillis) / 36000)
  }

  def generateGraphData(config: Config): Unit = {
    FileUtils.deleteRecursively(new File(config.output + "/" + config.neo4jDB))
    var graphdb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(config.output + "/" + config.neo4jDB))
    val neo4jDataGenerator = new Neo4jDataGenerator(graphdb)
    //生成数据
    neo4jDataGenerator.generateUsers(config)
    registerShutdownHook(graphdb)
  }

  /**
    * START SNIPPET: shutdownHook
    * @param graph
    */
  def registerShutdownHook(graph: GraphDatabaseService): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        graph.shutdown()
      }
    })
  }

  def chackArgs(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: class com.lakala.datacenter.grogress.ExportNDegreeData$ [options]\n" +
        "[<property>=<value>....]\n  " +
        "-i <value> | --Input <value>\n     applyInput file or path  Required.\n  " +
        "-o <value> | --output <value>\n     output path Required\n  " +
        "-m <value> | --master <value>\n     spark master, local[N] or spark://host:port default=local\n  " +
        "-h <value> | --sparkhome <value>\n     SPARK_HOME Required to run on cluster\n  " +
        "-n <value> | --jobname <value>\n     job name\n  " +
        "-s <value> | --startDate <value>\n     use start date load data\n  " +
        "-t <value> | --endDate <value>\n     use end date load data\n  " +
        "-p <value> | --parallelism <value>\n     sets spark.default.parallelism and minSplits on the edge file. default=based on input partitions\n  " +
        "-x <value> | --minprogress <value>\n     Number of vertices that must change communites for the algorithm to consider progress. default=2000\n  " +
        "-y <value> | --progresscounter <value>\n     Number of times the algorithm can fail to make progress before exiting. default=1\n  " +
        "-d <value> | --edgedelimiter <value>\n     specify input file edge delimiter. default=\",\"\n  " +
        "-j <value> | --jars <value>\n     comma seperated list of jars\n  " +
        "-e <value> | --encrypy <value>\n     Set to true to  all data  convert encrypy need all data use google hash's MD5 generage Long ids. Defaults to false\n  " +
        "-b <value> | --blacType <value>\n     Set to true to exprot black result data, Defaults to false\n  " +
        " <property>=<value>.... ")
      sys.exit(1)
    }
  }

}
