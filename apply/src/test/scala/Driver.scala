/**
  * Created by Administrator on 2017/5/3 0003.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import org.apache.log4j.Logger
import sys.process._
import java.io._
import java.io.FileNotFoundException
import java.io.IOException
import java.util.Calendar


// Exception to be throw in case of invalid config
case class InvalidConfigException(message: String) extends Exception(message)

/**
  * Config class with all default values set
  */
case class Config(
                   numVertices: Int = 100,
                   numEdges: Int = 100,
                   graphType: String = "",
                   partitionCount: Int = 0,
                   mu: Double = 4.0,
                   sigma: Double = 1.3,
                   seed: Long = 1,
                   edgeListFile: String = null,
                   partitionStrategy: String = "EdgePartition2D"
                 )


/**
  * Implements a Breadth First Search on the user provided graph
  *
  * @example this program can be used to generate various kinds of graph such as
  *          1) Star graph - with all nodes pointing to one central node
  *          2) Small world graph - This is also known as Log Normal graph,
  *          whose out degree distribution is log normal
  *          The default values of mu and sigma are taken from the pregel paper
  *          3) rmatGraph - A random graph generator using the R-MAT model,
  *          Graphx has fixed the values for rmat{a-d} ,
  *          if required the graph generator can be extended and implemented to take user required values
  */
class GraphBFS {

  /**
    * Parse the arguments and generate config object
    *
    * @tparam args the array of command line arguments
    *              on successful parsing
    * @return valid config object
    *         Throws exception in case of any error in validations
    */
  def parseArgs(args: Array[String]): Config = {
    val graphTypes = List("LogNormal", "RMAT", "EdgeList", "Star")
    val partitionStrategies = List("CanonicalRandomVertexCut", "EdgePartition1D", "EdgePartition2D", "RandomVertexCut")

    // Generating the parser with all validations
    val parser = new scopt.OptionParser[Config]("Jar") {
      head("GraphX BFS", "0.1")
      opt[String]('g', "graphType") required() validate { x => if (graphTypes.contains(x)) success else failure("Invalid type of graph") } action { (x, c) => c.copy(graphType = x) } text ("Type of graph to be generated")
      opt[String]('p', "partitionStrategy") validate { x => if (partitionStrategies.contains(x)) success else failure("Invalid partitionStrategy") } action { (x, c) => c.copy(partitionStrategy = x) } text ("partitionStrategy to be used")
      opt[Int]('v', "numVertices") validate { x => if (x <= 0) failure("vertices shud be > 0") else success } action { (x, c) => c.copy(numVertices = x) } text ("Number of Vertices")
      opt[Int]('e', "numEdges") validate { x => if (x <= 0) failure("edges shud be > 0") else success } action { (x, c) => c.copy(numEdges = x) } text ("Number of Edges")
      opt[Int]('n', "partitionCount") validate { x => if (x < 0) failure("number of edge parts shud be > 0") else success } action { (x, c) => c.copy(partitionCount = x) } text ("Number of edge partitions to be made")
      opt[Double]('m', "mu") action { (x, c) => c.copy(mu = x) } text ("Mu value for graph generation")
      opt[Double]('s', "sigma") action { (x, c) => c.copy(sigma = x) } text ("Sigma  value for graph generation")
      opt[Long]("seed") action { (x, c) => c.copy(seed = x) } text ("Seed value for graph generation")
      opt[String]('f', "edgeListFile") action { (x, c) => c.copy(edgeListFile = x) } text ("Edge List file name")
      help("help") text ("prints this usage text")
    }

    parser.parse(args, Config()) match {
      case Some(c) => c
      case None => throw new InvalidConfigException("Invalid config")
    }
  }

  /**
    * Vertex function to be used by pregel API
    *
    * @tparam id     - of the vertex being operated on
    * @tparam 	attr - attribute already stored with the vertex
    * @tparam 	msg  - inbound message recieved during pregel iteration
    * @return - min of attr and msg
    */
  val vprog = { (id: VertexId, attr: Double, msg: Double) => math.min(attr, msg) }

  /**
    * SendMessage function to be used by pregel API
    *
    * @tparam 		triplet :EdgeTriplet[Double, Int]	- the edge triplet
    *                     if Either one of the vertices is visited , then a message is sent to the other vertex
    *                     if both are visited or not visited then no action is performed
    * @return Iterator of vertexid and the message to be sent
    */
  val sendMessage = { (triplet: EdgeTriplet[Double, Int]) =>
    var iter: Iterator[(VertexId, Double)] = Iterator.empty
    val isSrcMarked = triplet.srcAttr != Double.PositiveInfinity
    val isDstMarked = triplet.dstAttr != Double.PositiveInfinity
    if (!(isSrcMarked && isDstMarked)) {
      if (isSrcMarked) {
        iter = Iterator((triplet.dstId, triplet.srcAttr + 1))
      } else {
        iter = Iterator((triplet.srcId, triplet.dstAttr + 1))
      }
    }
    iter
  }


  /**
    * Reduce Message function to be used by pregel API
    *
    * @tparam messages recieved by one vertex
    * @return the min of messages
    */
  val reduceMessage = { (a: Double, b: Double) => math.min(a, b) }

  /**
    * Utility function to calculate time difference and round off
    *
    * @tparam 2 timestamps in nano seconds
    * @return Returns the difference of them in milliseconds
    */
  def calcTime(s1: Long, s2: Long): String = {
    BigDecimal((s1 - s2) / 1e6).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString
  }

  /**
    * Generates a graph with the given config and spark context object
    *
    * @tparam 		config - the configuration object
    * @tparam 		spark  context object of the current running application
    * @return the graph generated as per the config object
    */
  def generateGraph(config: Config, sparkContext: SparkContext) = {
    if (config.graphType == "LogNormal") {
      GraphGenerators.logNormalGraph(sparkContext, config.numVertices, config.partitionCount, config.mu, config.sigma, config.seed)
    }
    else if (config.graphType == "RMAT") {
      GraphGenerators.rmatGraph(sparkContext, config.numVertices, config.numEdges)
    }
    else if (config.graphType == "Star") {
      GraphGenerators.starGraph(sparkContext, config.numVertices)
    }
    else {
      GraphLoader.edgeListFile(sparkContext, config.edgeListFile)
    }
  }

  /**
    * The entrypoint to the graph processing function
    */
  def main(args: Array[String]) {

    val mainTime = System.nanoTime()
    val config: Config = parseArgs(args)
    val timeStamp = (System.currentTimeMillis / 1000).toString

    // Setting the appId based on type of graph specified
    val appIdName = config.graphType + "_" + timeStamp

    val sparkContext = new SparkContext(new SparkConf().setMaster("local[2]").setAppName(appIdName))
    // Getting the log object
    val log = Logger.getLogger(getClass.getName)
    // Log prefix to identify the user generated logs
    val logPrefix = "PRASAD:" + appIdName + ":"
    log.info(logPrefix + "args: " + args.mkString(","))

    log.info(logPrefix + "Final generated Config")
    sparkContext.getConf.getAll.foreach(con => log.info(logPrefix + "Conf: " + con))

    log.info(logPrefix + "START:Time: " + Calendar.getInstance.getTime.toString)
    log.info(logPrefix + s" appName: ${sparkContext.appName}, appId: ${sparkContext.applicationId}, master:${sparkContext.master} , numPartitions: ${config.partitionCount}")
    val partitionStrategy = PartitionStrategy.fromString(config.partitionStrategy)

    // Generating graph based on user configuration
    val graph = generateGraph(config, sparkContext)
    graph.cache()
    val numPartitions: Int = if (config.partitionCount > 0) config.partitionCount else graph.edges.partitions.size

    log.info(logPrefix + "LOAD:Time: " + calcTime(System.nanoTime, mainTime))

    // Python script to log the metrics for the current running application
    val pythonScript = sys.env("PYTHON_METRICS_SCRIPT")
    // File to store the extracted metrics
    val metricsJson = sys.env("METRICS_OUTPUT_FILE")

    // Graph properties used for logging
    val vCount = graph.vertices.count
    val eCount = graph.edges.count

    val master = sparkContext.master.trim.split(":")(1).replace("//", "")

    // Root vertex from which BFS is destined to start
    val rootVertex: VertexId = graph.vertices.first()._1
    val graphMapTime = System.nanoTime()

    // Graph with Root vertex marked as 0 and rest are marked as unvisited
    val initialGraph = graph.partitionBy(partitionStrategy, numPartitions).mapVertices((id, attr) => if (id == rootVertex) 0.0 else Double.PositiveInfinity)

    // Unpersisting the previous graph and caching newly generated graph
    graph.unpersist(blocking = false)
    initialGraph.cache()

    log.info(logPrefix + "BFS root Vertex: " + rootVertex)
    log.info(logPrefix + "MapVertices: Time: " + calcTime(System.nanoTime, graphMapTime))

    // Pregel required properties
    // Initial message sent to all vertices
    val initialMessage = Double.PositiveInfinity
    // Number of iterations the pregel api is run for
    val maxIterations = Int.MaxValue
    val activeEdgeDirection = EdgeDirection.Either

    val bfsStart = System.nanoTime()

    // Invoke pregel function with required parameters to perform BFS
    val bfs = initialGraph.pregel(initialMessage, maxIterations, activeEdgeDirection)(vprog, sendMessage, reduceMessage)

    // Unpersisting the graph
    initialGraph.unpersist(blocking = false)

    val bfsTime = calcTime(System.nanoTime, bfsStart)

    log.info(logPrefix + "BFS: Time: " + bfsTime)
    //Generating a unique name for the metrics to be logged
    val graphName = List(appIdName, config.partitionStrategy, vCount.toString, eCount.toString, numPartitions.toString, bfsTime.toString).mkString("_")
    val metricsScript = List("python", pythonScript, master, graphName, metricsJson).mkString(" ")

    log.info(logPrefix + s"metricScript: $pythonScript, outputFile: $metricsJson , graphName: $graphName, master: $master, ")
    val logMetricTime = System.nanoTime()

    //Logging all the metrics required for evaluation
    val metricProcess = Process(metricsScript).!!
    log.info(logPrefix + s"Log metrics output: $metricProcess")
    log.info(logPrefix + "Log Metrics: Time: " + calcTime(System.nanoTime, logMetricTime))
    log.info(logPrefix + "Total: Time: " + calcTime(System.nanoTime, mainTime))
    log.info(logPrefix + "END: Time: " + Calendar.getInstance.getTime.toString)
  }
}

object Driver extends App {
  override def main(args: Array[String]) = {
    val graph = new GraphBFS()
    graph.main(args)
  }
}
