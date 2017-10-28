package com.lakala.datacenter.grogress

import com.lakala.datacenter.apply.model.NDegreeEntity
import com.lakala.datacenter.grograms.ApplyDegreeCentralityProgram
import com.lakala.datacenter.utils.UtilsToos.hashId
import com.lakala.datacenter.utils.{Config, SparkCommon, UtilsToos}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/4 0004.
  */
class ExportNDegreeData extends Logging {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length < 1) {
      println("Usage: class com.lakala.datacenter.grogress.ExportNDegreeData$ [options]\n" +
        "[<property>=<value>....]\n  " +
        "-i <value> | --applyInput <value>\n     applyInput file or path  Required.\n  " +
        "-c <value> | --callInput <value>\n     callInput file or path  Required.\n  " +
        "-r <value> | --correspondInput <value>\n     correspondInput file or path  Required.\n  " +
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

    val mainTime = System.nanoTime()
    // Parse Command line options
    var properties: Seq[(String, String)] = Seq.empty[(String, String)]

    val config = SparkCommon.parseArgs(args)

    // set system properties
    properties.foreach({ case (k, v) =>
      logDebug(s"System.setProperty($k, $v)")
      System.setProperty(k, v)
    })

    val conf = if (config.master.indexOf("local") == 0) {
      new SparkConf().setMaster(config.master).setAppName(config.appName)
    } else {
      new SparkConf().setMaster(config.master).setAppName(config.appName)
        .setSparkHome(config.sparkHome) /*.setJars(config.jars.split(","))*/
    }
    // Create the spark context
    var sc: SparkContext = new SparkContext(conf)
    //    val graph = if (config.blacType) {
    //黑名单
    //TODO
    //      generateGraph(config, sc, generBalckRDD(sc))
    //    } else generateGraph(config, sc)
    val graph = if (config.blacType) {
      loadRddRawData(sc, config, generBalckRDD(sc))
    } else {
      loadRddRawData(sc, config)
    }

    val graphResult = new ApplyDegreeCentralityProgram(graph).run(config.progressCounter, EdgeDirection.Out)
    generateResultSaveCSV(sc, config, graphResult)
  }


  def loadRddRawData(sc: SparkContext, config: Config): Graph[Map[VertexId, NDegreeEntity], Int] = {
    val tupe = generateVertexRDDByHive(config, sc)
    println(s" total vertex nums ${tupe._1.count()} total edge nums ${tupe._2.count()}")
    val g: Graph[String, Int] = Graph(tupe._1, tupe._2)
    val newG: Graph[Map[VertexId, NDegreeEntity], Int] =
      g.mapVertices((id, att) => Map[VertexId, NDegreeEntity](id -> new NDegreeEntity(att, loop = config.progressCounter)))
    newG
  }


  def loadRddRawData(sc: SparkContext, config: Config, blackApply: RDD[Long]): Graph[Map[VertexId, NDegreeEntity], Int] = {
    val tupe = generateVertexRDDByHive(config, sc)
    println(s" total vertex nums ${tupe._1.count()} total edge nums ${tupe._2.count()}")
    val g: Graph[String, Int] = Graph(tupe._1, tupe._2)
    //黑名单
    val initVertex = blackApply.map(v => (v, 1))
    var newG: Graph[Map[VertexId, NDegreeEntity], Int] = g.outerJoinVertices(initVertex) { case (vid, oldAttr, newAttr) => Map[VertexId, NDegreeEntity](vid -> new NDegreeEntity(oldAttr, initType = newAttr.getOrElse(0), loop = config.progressCounter)) }
    newG
  }


  /**
    * by vertex and edge build graph
    *
    * @param config
    * @param sc
    * @return
    */
  def generateGraph(config: Config, sc: SparkContext): Graph[Map[VertexId, NDegreeEntity], Int] = {
    val tupe = generateVertexRDD(config, sc)
    println(s" total vertex nums ${tupe._1.count()} total edge nums ${tupe._2.count()}")
    val g: Graph[String, Int] = Graph(tupe._1, tupe._2)
    val newG: Graph[Map[VertexId, NDegreeEntity], Int] =
      g.mapVertices((id, att) => Map[VertexId, NDegreeEntity](id -> new NDegreeEntity(att, loop = config.progressCounter)))
    newG
  }


  /**
    * by vertex and edge build graph
    *
    * @param config
    * @param sc
    * @param blackApply
    * @return
    */
  def generateGraph(config: Config, sc: SparkContext, blackApply: RDD[Long]): Graph[Map[VertexId, NDegreeEntity], Int] = {
    val tupe = generateVertexRDD(config, sc)
    val g: Graph[String, Int] = Graph(tupe._1, tupe._2)
    //黑名单
    val initVertex = blackApply.map(v => (v, 1))
    var newG: Graph[Map[VertexId, NDegreeEntity], Int] = g.outerJoinVertices(initVertex) { case (vid, oldAttr, newAttr) => Map[VertexId, NDegreeEntity](vid -> new NDegreeEntity(oldAttr, initType = newAttr.getOrElse(0), loop = config.progressCounter)) }
    newG
  }

  def generBalckRDD(sc: SparkContext, path: String = ""): RDD[Long] = {
    //TODO
    val blackApply = sc.parallelize(Seq(hashId("XNA20170504110555012961804159413"), hashId("XNA20170504105136005696100975953")))
    //    val blackApply = sc.parallelize(Seq(13867764858L, 13981960691L))
    blackApply
  }

  /**
    * by not Empty path load source data generate vertex edge
    *
    * @param config
    * @param sc
    * @return
    */
  def generateVertexRDDByHive(config: Config, sc: SparkContext): (RDD[(Long, String)], RDD[Edge[Int]]) = {
    //load apply data
    val startDT = config.startDate.split("-")
    val endDT = config.endDate.split("-")

    val hc = new HiveContext(sc)
    //CREDITLOAN.S_DATA_GENERALAPPLY_CREDITLOAN
    hc.sql("use creditloan")
    val applyVertexEdge = hc.sql(
      s"""
         |SELECT orderno as o,mobile as m,emergencymobile as em,usermobile as um,msgmobile as mm,channelmobile as cm,merchantmobile as mtm,usercoremobile as ucm,partnercontantmobile as pm,contactmobile as ctm
         |   FROM ${config.applyInput} ac
         | WHERE ac.orderno is not null and (ac.year >=${startDT(0)} and ac.month >=${startDT(1)} and ac.day >=${startDT(2)}) or(ac.year <= ${endDT(0)} and ac.month <= ${endDT(1)}  and ac.day<=${endDT(2)})
      """.stripMargin).repartition(100).mapPartitions { rows =>
      rows.map { row => {
        val orderno = if (StringUtils.isNotBlank(row.getAs[String]("o")) && !"null".equals(row.getAs[String]("o").toLowerCase)) row.getAs[String]("o") else "0"
        val mobile = if (StringUtils.isNotBlank(row.getAs[String]("m")) && !"null".equals(row.getAs[String]("m").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("m"))) row.getAs[String]("m") else "0"
        val emergencymobile = if (StringUtils.isNotBlank(row.getAs[String]("em")) && !"null".equals(row.getAs[String]("em").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("em"))) row.getAs[String]("em") else "0"
        val usermobile = if (StringUtils.isNotBlank(row.getAs[String]("um")) && !"null".equals(row.getAs[String]("um").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("um"))) row.getAs[String]("um") else "0"
        val msgmobile = if (StringUtils.isNotBlank(row.getAs[String]("mm")) && !"null".equals(row.getAs[String]("mm").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("mm"))) row.getAs[String]("mm") else "0"
        val channelmobile = if (StringUtils.isNotBlank(row.getAs[String]("cm")) && !"null".equals(row.getAs[String]("cm").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("cm"))) row.getAs[String]("cm") else "0"
        val merchantmobile = if (StringUtils.isNotBlank(row.getAs[String]("mtm")) && !"null".equals(row.getAs[String]("mtm").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("mtm"))) row.getAs[String]("mtm") else "0"
        val usercoremobile = if (StringUtils.isNotBlank(row.getAs[String]("ucm")) && !"null".equals(row.getAs[String]("ucm").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("ucm"))) row.getAs[String]("ucm") else "0"
        val partnercontantmobile = if (StringUtils.isNotBlank(row.getAs[String]("pm")) && !"null".equals(row.getAs[String]("pm").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("pm"))) row.getAs[String]("pm") else "0"
        val contactmobile = if (StringUtils.isNotBlank(row.getAs[String]("ctm")) && !"null".equals(row.getAs[String]("ctm").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("ctm"))) row.getAs[String]("ctm") else "0"

        (Iterator(
          (hashId(orderno), orderno),
          (mobile.toLong, mobile),
          (emergencymobile.toLong, emergencymobile),
          (usermobile.toLong, usermobile),
          (msgmobile.toLong, msgmobile),
          (channelmobile.toLong, channelmobile),
          (merchantmobile.toLong, merchantmobile),
          (usercoremobile.toLong, usercoremobile),
          (partnercontantmobile.toLong, partnercontantmobile),
          (contactmobile.toLong, contactmobile)),

          Iterator(
            (Edge(hashId(orderno), mobile.toLong, 0)),
            (Edge(mobile.toLong, hashId(orderno), 0)),

            (Edge(hashId(orderno), emergencymobile.toLong, 0)),
            (Edge(emergencymobile.toLong, hashId(orderno), 0)),

            (Edge(hashId(orderno), usermobile.toLong, 0)),
            (Edge(usermobile.toLong, hashId(orderno), 0)),

            (Edge(hashId(orderno), msgmobile.toLong, 0)),
            (Edge(msgmobile.toLong, hashId(orderno), 0)),

            (Edge(hashId(orderno), channelmobile.toLong, 0)),
            (Edge(channelmobile.toLong, hashId(orderno), 0)),

            (Edge(hashId(orderno), merchantmobile.toLong, 0)),
            (Edge(merchantmobile.toLong, hashId(orderno), 0)),

            (Edge(hashId(orderno), usercoremobile.toLong, 0)),
            (Edge(usercoremobile.toLong, hashId(orderno), 0)),

            (Edge(hashId(orderno), partnercontantmobile.toLong, 0)),
            (Edge(partnercontantmobile.toLong, hashId(orderno), 0)),

            (Edge(hashId(orderno), contactmobile.toLong, 0)),
            (Edge(contactmobile.toLong, hashId(orderno), 0))
          )
        )
      }
      }
    }


    var vertexRDD: RDD[(Long, String)] = applyVertexEdge.flatMap(k => k._1).distinct()
    var edgeRdd = applyVertexEdge.flatMap(k => k._2).filter(k => k.srcId != 0L && k.dstId != 0L)
    println(s"vertexRDD partitions@@ ${vertexRDD.getNumPartitions} edgeRdd partitions@@ ${edgeRdd.getNumPartitions}")
    vertexRDD.cache()
    edgeRdd.cache()
    println(s"apply total vertex num ${vertexRDD.count()} total edge num ${edgeRdd.count}")

    //load call data
    if (StringUtils.isNotBlank(config.callInput)) {

      hc.sql("use datacenter")
      //DATACENTER.R_CALLHISTORY_WEEK    and dr.month >=${startDT(1)}  and dr.day >=${startDT(2)}   or (dr.year =${endDT(0)}) and dr.month <=${endDT(1)}  and dr.day <=${endDT(2)}
      val sql =
        s"""
           |SELECT 	loginname as ln,caller_phone as cp
           |   FROM ${config.callInput} dr
           | WHERE dr.year =${endDT(0)} and ((dr.month =04 and  dr.day >=26) or (dr.month =${endDT(1)} and  dr.day <=${endDT(2)}))
      """.stripMargin
      println("sql@@@ " + sql)
      val callVertexEdge = hc.sql(sql).repartition(40).mapPartitions { rows =>
        rows.map { row => {
          val loginname = if (StringUtils.isNotBlank(row.getAs[String]("ln")) && !"null".equals(row.getAs[String]("ln").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("ln"))) row.getAs[String]("ln").replace("-", "") else "0"
          val caller_phone = if (StringUtils.isNotBlank(row.getAs[String]("cp")) && !"null".equals(row.getAs[String]("cp").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("cp"))) row.getAs[String]("cp").replace("-", "") else "0"
          (Iterable((loginname.toLong, loginname), (caller_phone.toLong, caller_phone)), (Edge(loginname.toLong, caller_phone.toLong, 0)))
        }
        }
      }
      callVertexEdge.cache()
      println(s"callVertexEdge partitions@@ ${callVertexEdge.getNumPartitions}")
      println(s"callVertexEdge total size@@ ${callVertexEdge.count()}")
      var callVertexRDD: RDD[(Long, String)] = callVertexEdge.flatMap(k => k._1).filter(k => k._1 != 0L).distinct(40)
      var callEdgeRdd = callVertexEdge.filter(k => k._2.srcId != 0L && k._2.dstId != 0L).map(k => k._2)
      callVertexRDD.cache()
      callEdgeRdd.cache()


      vertexRDD = sc.union(vertexRDD, callVertexRDD)
      edgeRdd = sc.union(edgeRdd, callEdgeRdd)
      //      println(s"call total vertex num ${callVertexRDD.count()} union call total vertex num ${vertexRDD.count()} total edge num ${callEdgeRdd.count()} union call total edge num ${edgeRdd.count}")

      callVertexEdge.unpersist()
      vertexRDD.unpersist()
      edgeRdd.unpersist()

      callVertexRDD.unpersist()
      callEdgeRdd.unpersist()
    }

    (vertexRDD, edgeRdd)
  }

  /**
    * by not Empty path load source data generate vertex edge
    *
    * @param config
    * @param sc
    * @return
    */
  def generateVertexRDD(config: Config, sc: SparkContext): (RDD[(Long, String)], RDD[Edge[Int]]) = {
    //load apply data
    val applyVertexEdge = sc.textFile(config.applyInput).mapPartitions(lines =>
      lines.map { line =>
        var arr = line.split(",")
        val term_id = if (StringUtils.isNotBlank(arr(7)) && !"null".equals(arr(7).toLowerCase)) arr(7) else "OL"
        val return_pan = if (StringUtils.isNotBlank(arr(16)) && !"null".equals(arr(16).toLowerCase)) arr(16) else "0"
        val empmobile = if (StringUtils.isNotBlank(arr(41)) && !"null".equals(arr(41).toLowerCase)) arr(41) else "0"

        (Iterator((hashId(arr(1)), arr(1)),
          (empmobile.toLong, empmobile)),
          Iterator(
            (Edge(hashId(arr(1)), empmobile.toLong, 0)),
            (Edge(empmobile.toLong, hashId(arr(1)), 0))
          )
        )
      }
    )

    var vertexRDD: RDD[(Long, String)] = applyVertexEdge.flatMap(k => k._1).distinct()
    var edgeRdd = applyVertexEdge.flatMap(k => k._2).filter(k => k.srcId != 0L && k.dstId != 0L)

    println(s"apply total vertex num ${vertexRDD.count()} total edge num ${edgeRdd.count}")

    //load call data
    if (StringUtils.isNotBlank(config.callInput)) {
      val callVertexEdge: RDD[(Iterable[(Long, String)], Edge[Int])] = sc.textFile(config.callInput).mapPartitions { lines =>
        lines.map { line =>
          var arr = line.split(" ")
          (Iterable((arr(0).toLong, arr(0)), (arr(1).toLong, arr(1))), (Edge(arr(0).toLong, arr(1).toLong, 0)))
        }
      }
      var callVertexRDD: RDD[(Long, String)] = callVertexEdge.flatMap(k => k._1).filter(k => k._1 != 0L).distinct()
      var callEdgeRdd = callVertexEdge.filter(k => k._2.srcId != 0L && k._2.dstId != 0L).map(k => k._2)

      vertexRDD = sc.union(vertexRDD, callVertexRDD)
      edgeRdd = sc.union(edgeRdd, callEdgeRdd)

      println(s"call total vertex num ${callVertexRDD.count()} union call total vertex num ${vertexRDD.count()} total edge num ${callEdgeRdd.count()} union call total edge num ${edgeRdd.count}")

    }

    //load correspond data
    if (StringUtils.isNotBlank(config.correspondInput)) {
      val correspondVertexEdge: RDD[(Iterable[(Long, String)], Edge[Int])] = sc.textFile(config.callInput).mapPartitions { lines =>
        lines.map { line =>
          var arr = line.split(config.edgedelimiter)
          (Iterable((arr(0).toLong, arr(0)), (arr(1).toLong, arr(1))), (Edge(arr(0).toLong, arr(1).toLong, 0)))
        }
      }
      var correspondVertexRDD: RDD[(Long, String)] = correspondVertexEdge.flatMap(k => k._1).filter(k => k._1 != 0L).distinct()
      var correspondEdgeRdd = correspondVertexEdge.filter(k => k._2.srcId != 0L && k._2.dstId != 0L).map(k => k._2)

      vertexRDD = sc.union(vertexRDD, correspondVertexRDD)
      edgeRdd = sc.union(edgeRdd, correspondEdgeRdd)
      println(s"corropond total vertex num ${correspondVertexRDD.count()} union corropond total vertex num ${vertexRDD.count()} total edge num ${correspondEdgeRdd.count()} union corropond total edge num ${edgeRdd.count}")
    }

    (vertexRDD, edgeRdd)
  }

  /**
    *
    * @param sc
    * @param config
    * @param graph
    */
  def generateResultSaveCSV(sc: SparkContext, config: Config, graph: Graph[Map[VertexId, NDegreeEntity], Int]) = {

    val degreeJumpRdd = if (config.blacType)
      graph.vertices.mapValues(_.filter(_._2.loop == 0)).mapPartitions { vs =>
        vs.map { v =>
          val values = v._2.filter(_._2.loop == 0)
          val forRs = for (v <- values; if (v._2.loop == 0 && (v._2.initType == 1))) yield (v._2.attr)
          forRs
        }
      }.flatMap(k => k).sortBy(s => s)
    else graph.vertices.mapValues(_.filter(_._2.loop == 0)).mapPartitions { vs =>
      vs.map { v =>
        val values = v._2.filter(_._2.loop == 0)
        val forRs = for (v <- values; if (v._2.loop == 0)) yield (v._2.attr)
        forRs
      }
    }.flatMap(k => k).sortBy(s => s)

    println("============================")
    println("total num " + degreeJumpRdd.count())
    degreeJumpRdd.collect().foreach(rs => println("result: " + rs))

    degreeJumpRdd.saveAsTextFile(s"${config.output}_${config.progressCounter}.csv")
    println("************* end ***************")

  }
}

