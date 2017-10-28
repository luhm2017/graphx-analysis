package com.lakala.datacenter.utils

import org.apache.spark.Logging

/**
  * Created by Administrator on 2017/5/4 0004.
  */
object ArgsCommon extends Logging {

  def parseArgs(args: Array[String]): Config = {
    assertArgs(args)
    val parser = new scopt.OptionParser[Config](this.getClass().toString()) {
      opt[String]('i', "input") action { (x, c) => c.copy(input = x) } text ("file Input file or path  Required.")
      opt[String]('o', "output") action { (x, c) => c.copy(output = x) } text ("output path Required")
      opt[String]('m', "master") action { (x, c) => c.copy(master = x) } text ("spark master, local[N] or spark://host:port default=local")
      opt[String]('h', "sparkhome") action { (x, c) => c.copy(sparkHome = x) } text ("SPARK_HOME Required to run on cluster")
      opt[String]('n', "jobname") action { (x, c) => c.copy(appName = x) } text ("job name")
      opt[String]('s', "startDate") action { (x, c) => c.copy(startDate = x) } text ("use start date load data")
      opt[String]('e', "endDate") action { (x, c) => c.copy(endDate = x) } text ("use end date load data")
      opt[String]('d', "edgedelimiter") action { (x, c) => c.copy(edgedelimiter = x) } text ("specify input file edge delimiter. default=\",\"")
      opt[String]('j', "jars") action { (x, c) => c.copy(jars = x) } text ("comma seperated list of jars")
      opt[String]('f', "batchDuration") action { (x, c) => c.copy(batchDuration = x) } text (" Set to batchDuration time ,Defaults to 500")
      opt[String]('b', "neo4jDB") action { (x, c) => c.copy(neo4jDB = x) } text ("Set to neo4j DB use exprot black result data, Defaults to graph.db")
      opt[Boolean]('r', "renew") action { (x, c) => c.copy(renew = x) } text ("jude Set to kafka reset offset value,Defaults to false")
      opt[String]('v', "offset") action { (x, c) => c.copy(offset = x) } text ("Set to kafka reset offset value")
      opt[String]('z', "zkIPS") action { (x, c) => c.copy(zkIPs = x) } text ("Set to zkClient ips contnect zookeeper")
      opt[String]('p', "brokerIPs") action { (x, c) => c.copy(brokerIPs = x) } text ("Set to metadata.broker.list ips contnect kafka")
      opt[String]('t', "topic") action { (x, c) => c.copy(topic = x) } text ("Set to kafka topic")
      opt[String]('g', "group") action { (x, c) => c.copy(group = x) } text ("Set to kafka group")
      arg[(String, String)]("<property>=<value>....") unbounded() optional() action { case ((k, v), c) => c.copy(properties = c.properties :+ (k, v)) }
    }
    parser.parse(args, Config()) match {
      case Some(c) => c
      case None => throw new InvalidConfigException("Invalid config")
    }
  }

  /**
    * 对入参进行校验
    *
    * @param args
    */
  def assertArgs(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: class com.lakala.datacenter.grogress.ExportNDegreeData$ [options]\n" +
        "[<property>=<value>....]\n  " +
        "-i <value> | --input <value>\n     input file or path  Required.\n  " +
        "-o <value> | --output <value>\n     output path Required\n  " +
        "-m <value> | --master <value>\n     spark master, local[N] or spark://host:port default=local\n  " +
        "-h <value> | --sparkhome <value>\n     SPARK_HOME Required to run on cluster\n  " +
        "-n <value> | --jobname <value>\n     job name\n  " +
        "-s <value> | --startDate <value>\n     use start date load data\n  " +
        "-e <value> | --endDate <value>\n     use end date load data\n  " +
        "-d <value> | --edgedelimiter <value>\n     specify input file edge delimiter. default=\",\"\n  " +
        "-j <value> | --jars <value>\n     comma seperated list of jars\n  " +
        "-f <value> | --batchDuration <value>\n     Set to batchDuration time ,Defaults to 500\n  " +
        "-b <value> | --neo4jDB <value>\n     Set to neo4j DB use exprot black result data, Defaults to graph.db\n  " +
        "-z <value> | --zkIPs <value>\n     Set to zkClient ips contnect zookeeper\n  " +
        "-p <value> | --brokerIPs <value>\n     Set to metadata.broker.list ips contnect kafka\n  " +
        "-t <value> | --topic <value>\n     Set to kafka topic\n  " +
        "-g <value> | --group <value>\n     Set to kafka group\n  " +
        " <property>=<value>.... ")

      logError(
        "Usage: class com.lakala.datacenter.grogress.ExportNDegreeData$ [options]\n" +
          "[<property>=<value>....]\n  " +
          "-i <value> | --input <value>\n     input file or path  Required.\n  " +
          "-o <value> | --output <value>\n     output path Required\n  " +
          "-m <value> | --master <value>\n     spark master, local[N] or spark://host:port default=local\n  " +
          "-h <value> | --sparkhome <value>\n     SPARK_HOME Required to run on cluster\n  " +
          "-n <value> | --jobname <value>\n     job name\n  " +
          "-s <value> | --startDate <value>\n     use start date load data\n  " +
          "-e <value> | --endDate <value>\n     use end date load data\n  " +
          "-d <value> | --edgedelimiter <value>\n     specify input file edge delimiter. default=\",\"\n  " +
          "-j <value> | --jars <value>\n     comma seperated list of jars\n  " +
          "-f <value> | --batchDuration <value>\n  Set to batchDuration time ,Defaults to 500 \n  " +
          "-b <value> | --neo4jDB <value>\n     Set to neo4j DB use exprot black result data, Defaults to graph.db\n  " +
          "-r <value> | --renew <value>\n     jude Set to kafka reset offset value,Defaults to false \n  " +
          "-v <value> | --offset <value>\n     Set to kafka reset offset value\n  " +
          "-z <value> | --zkIPs <value>\n     Set to zkClient ips contnect zookeeper\n  " +
          "-p <value> | --brokerIPs <value>\n     Set to metadata.broker.list ips contnect kafka\n  " +
          "-t <value> | --topic <value>\n     Set to kafka topic\n  " +
          "-g <value> | --group <value>\n     Set to kafka group\n  " +
          " <property>=<value>.... ")
      System.exit(1)
    }
  }

}

case class Config(
                   var input: String = "",
                   var output: String = "",
                   master: String = "local[7]",
                   appName: String = "Spark Application",
                   jars: String = "",
                   sparkHome: String = "",
                   startDate: String = "",
                   endDate: String = "",
                   edgedelimiter: String = ",",
                   batchDuration: String = "50",
                   neo4jDB: String = "graph.db",
                   renew: Boolean = false,
                   var offset: String = "",
                   zkIPs: String = "",
                   brokerIPs: String = "",
                   topic: String = "",
                   group: String = "",
                   properties: Seq[(String, String)] = Seq.empty[(String, String)])


// Exception to be throw in case of invalid config
case class InvalidConfigException(message: String) extends Exception(message)

