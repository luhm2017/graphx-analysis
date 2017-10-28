package com.lakala.datacenter.utils

/**
  * Created by Administrator on 2017/5/4 0004.
  */
object SparkCommon {

  //  lazy val conf = {
  //    new SparkConf(false).setMaster("local[*]").setAppName("ExportNDegreeData")
  //  }
  //
  //  lazy val sparkContext = new SparkContext(conf)
  //  lazy val sqlContext = SQLContext.getOrCreate(sparkContext)

  def parseArgs(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config](this.getClass().toString()) {
      opt[String]('i', "applyInput") action { (x, c) => c.copy(applyInput = x) } text ("applyInput file or path  Required.")
      opt[String]('c', "callInput") action { (x, c) => c.copy(callInput = x) } text ("callInput file or path  Required.")
      opt[String]('r', "correspondInput") action { (x, c) => c.copy(correspondInput = x) } text ("correspondInput file or path  Required.")
      opt[String]('o', "output") action { (x, c) => c.copy(output = x) } text ("output path Required")
      opt[String]('m', "master") action { (x, c) => c.copy(master = x) } text ("spark master, local[N] or spark://host:port default=local")
      opt[String]('h', "sparkhome") action { (x, c) => c.copy(sparkHome = x) } text ("SPARK_HOME Required to run on cluster")
      opt[String]('n', "jobname") action { (x, c) => c.copy(appName = x) } text ("job name")
      opt[String]('s', "startDate") action { (x, c) => c.copy(startDate = x) } text ("use start date load data")
      opt[String]('t', "endDate") action { (x, c) => c.copy(endDate = x) } text ("use end date load data")
      opt[Int]('p', "parallelism") action { (x, c) => c.copy(parallelism = x) } text ("sets spark.default.parallelism and minSplits on the edge file. default=based on input partitions")
      opt[Int]('x', "minprogress") action { (x, c) => c.copy(minProgress = x) } text ("Number of vertices that must change communites for the algorithm to consider progress. default=2000")
      opt[Int]('y', "progresscounter") action { (x, c) => c.copy(progressCounter = x) } text ("Number of times the algorithm can fail to make progress before exiting. default=1")
      opt[String]('d', "edgedelimiter") action { (x, c) => c.copy(edgedelimiter = x) } text ("specify input file edge delimiter. default=\",\"")
      opt[String]('j', "jars") action { (x, c) => c.copy(jars = x) } text ("comma seperated list of jars")
      opt[Boolean]('e', "encrypy") action { (x, c) => c.copy(encrypy = x) } text ("Set to true to  all data  convert encrypy need all data use google hash's MD5 generage Long ids. Defaults to false")
      opt[Boolean]('b', "blacType") action { (x, c) => c.copy(blacType = x) } text ("Set to true to exprot black result data, Defaults to false")
      arg[(String, String)]("<property>=<value>....") unbounded() optional() action { case ((k, v), c) => c.copy(properties = c.properties :+ (k, v)) }
    }
    parser.parse(args, Config()) match {
      case Some(c) => c
      case None => throw new InvalidConfigException("Invalid config")
    }
  }

}

case class Config(
                   applyInput: String = "",
                   callInput: String = "",
                   correspondInput: String = "",
                   output: String = "",
                   master: String = "local[2]",
                   appName: String = "ExportNDegreeData",
                   jars: String = "",
                   sparkHome: String = "",
                   startDate: String = "",
                   endDate: String = "",
                   parallelism: Int = -1,
                   edgedelimiter: String = ",",
                   minProgress: Int = 2000,
                   progressCounter: Int = 2,
                   encrypy: Boolean = false,
                   blacType: Boolean = false,
                   properties: Seq[(String, String)] = Seq.empty[(String, String)])


// Exception to be throw in case of invalid config
case class InvalidConfigException(message: String) extends Exception(message)

