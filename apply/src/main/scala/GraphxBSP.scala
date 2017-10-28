import com.lakala.datacenter.core.utils.UtilsToos.hashId
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/6/16 0016.
  * --files hdfs://ns1/user/guozhijie/runExecute/hive-site.xml --driver-class-path hdfs://ns1/user/guozhijie/sparkJar/mysql-connector-java-5.1.36.jar --jars hdfs://ns1/user/guozhijie/sparkJar/original-graphx-analysis-core-1.0.0-SNAPSHOT.jar,hdfs://ns1/user/guozhijie/sparkJar/guava-14.0.1.jar --queue szbigdata --executor-memory 40g --num-executors 10 --executor-cores 3 --driver-memory 2g --conf spark.yarn.executor.memoryOverhead=8192 --conf spark.shuffle.io.retryWait=60s --conf spark.shuffle.io.maxRetries=30 --conf spark.default.parallelism=1000 --conf spark.executor.heartbeatInterval=100000 --conf spark.network.timeout=100000
  * --files hdfs://ns1/user/guozhijie/runExecute/hive-site.xml --driver-class-path hdfs://ns1/user/guozhijie/sparkJar/mysql-connector-java-5.1.36.jar --jars hdfs://ns1/user/guozhijie/sparkJar/original-graphx-analysis-core-1.0.0-SNAPSHOT.jar,hdfs://ns1/user/guozhijie/sparkJar/guava-14.0.1.jar --queue szbigdata --executor-memory 60g --num-executors 10 --executor-cores 3 --driver-memory 2g --conf spark.yarn.executor.memoryOverhead=8192 --conf spark.shuffle.io.retryWait=60s --conf spark.shuffle.io.maxRetries=30 --conf spark.default.parallelism=2000 --conf spark.executor.extraJavaOptions="-XX:PermSize=5120M -XX:MaxPermSize=10240M -XX:SurvivorRatio=4 -XX:NewRatio=4 -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=15 -XX:CMSInitiatingOccupancyFraction=70 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf spark.core.connection.ack.wait.timeout=120 --conf spark.storage.memoryFraction=0.3
  */
object GraphxBSP {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val gbsp = new GraphxBSP(args)
    gbsp.runNHopResult()
    //    gbsp.runNHopPregelResult()
    //    F:\\graphx-analysis\\apply\\bin\\test.csv 3 F:\\out\\output 7,13,15,16,17,19,20
  }
}


/*extends Serializable*/

class GraphxBSP(args: Array[String]) extends Serializable {

  @transient
  val conf = new SparkConf().setAppName("GraphxBSP")
  //    .setMaster("local[7]")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //设置序列化为kryo
  // 容错相关参数
  conf.set("spark.task.maxFailures", "8")
  //  conf.set("spark.akka.timeout", "500")
  //  conf.set("spark.network.timeout", "500")
  //  conf.set("spark.executor.heartbeatInterval", "10000000")
  conf.set("spark.yarn.max.executor.failures", "100")
  //建议打开map（注意，在spark引擎中，也只有map和reduce两种task，spark叫ShuffleMapTask和ResultTask）中间结果合并及推测执行功能：
  conf.set("spark.shuffle.consolidateFiles", "true")
  conf.set("spark.speculation", "true")
  conf.set("spark.shuffle.compress", "true")
  conf.set("spark.rdd.compress", "true")
  conf.set("spark.kryoserializer.buffe", "512m")
  conf.registerKryoClasses(Array(classOf[EdgeArr]))


  @transient
  val sc = new SparkContext(conf)
  //  sc.setCheckpointDir("/user/guozhijie/checkpoint")

  def vprop = (vid: VertexId, vdata: String, msg: String) => msg

  def pregelSendMsg = (ctx: EdgeTriplet[String, EdgeArr]) => {
    var it: Iterable[(VertexId, String)] = scala.collection.mutable.Iterable()
    if (StringUtils.isEmpty(ctx.srcAttr) && StringUtils.isEmpty(ctx.dstAttr) &&
      judgSendMsg(args(3).split(","), ctx.attr)) {
      it = it.++(Iterator((ctx.dstId, s"${ctx.attr.srcV}&${ctx.attr.srcType}->${ctx.attr.dstV}")))
    }
    if (StringUtils.isEmpty(ctx.srcAttr) && StringUtils.isNotEmpty(ctx.dstAttr)) {
      ctx.dstAttr.split("-->").map { att =>
        if (att.endsWith(ctx.attr.dstV) && !att.contains(s"${ctx.attr.srcV}&${ctx.attr.srcType}->${ctx.attr.dstV}")) {
          it = it.++(Iterator((ctx.srcId, s"$att->${ctx.attr.srcV}&${ctx.attr.srcType}")))
        }
        it = it.++(Iterator((ctx.dstId, "")))
      }
    }

    if (StringUtils.isNotEmpty(ctx.srcAttr) && StringUtils.isEmpty(ctx.dstAttr)) {
      ctx.srcAttr.split("-->").map { att =>
        if (att.endsWith(s"${ctx.attr.srcV}&${ctx.attr.srcType}") && !att.contains(s"${ctx.attr.dstV}->${ctx.attr.srcV}&${ctx.attr.srcType}") && !att.contains(s"${ctx.attr.dstV}")) {
          it = it.++(Iterator((ctx.dstId, s"$att->${ctx.attr.dstV}")))
        }
        it = it.++(Iterator((ctx.srcId, "")))
      }
    }
    it.toIterator
  }

  def runNHopPregelResult(): Unit = {
    val edge = genEdgeRdd(args(0))
    var g = Pregel(Graph.fromEdges(edge, "", edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER), "", args(1).toInt)(vprop, pregelSendMsg, merageMsg)

    val result = g.vertices
      .mapPartitions(vs => vs.filter(v => StringUtils.isNotBlank(v._2)).flatMap(v => v._2.split("-->")).filter(_.split("->").length == args(1).toInt + 1))
    result.saveAsTextFile(args(2))
  }

  def runNHopResult(): Unit = {
    var g = Graph.fromEdges(genEdgeRdd(args(0)), "", edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER, vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER)
    var preG: Graph[String, EdgeArr] = null
    var iterCount = 0
    while (iterCount < args(1).toInt) {
      println(s"iteration $iterCount start .....")
      preG = g
      //      if ((iterCount + 1) % 2 == 0) {
      //        g.checkpoint()
      //        println(g.numEdges)
      //      }
      var tmpG = g.aggregateMessages[String](sendMsg, merageMsg).cache()
      print("下一次迭代要发送的messages:\n" + tmpG.take(10).mkString("\n"))
      g = g.outerJoinVertices(tmpG) { (_, _, updateAtt) => updateAtt.getOrElse("") }
      //新的graph cache起来，下一次迭代使用
      g.persist(StorageLevel.MEMORY_ONLY_SER)
      //      print(g.vertices.take(10).mkString("\n"))
      tmpG.unpersist(blocking = false)
      preG.vertices.unpersist(blocking = false)
      preG.edges.unpersist(blocking = false)
      println(s"iteration $iterCount end .....")
      iterCount += 1
    }
    //************************
    val result = g.vertices.filter(vs => StringUtils.isNotBlank(vs._2))
      .mapPartitions(vs => vs.flatMap(v => v._2.split("-->")).filter(_.split("->").length == args(1).toInt + 1))
    result.saveAsTextFile(args(2))
  }

  def judgSendMsg = (sendType: Array[String], edge: EdgeArr) => {
    var flag = false
    for (stype <- sendType) if (edge.srcType.equals(stype)) flag = true
    flag
  }

  def sendMsg(ctx: EdgeContext[String, EdgeArr, String]): Unit = {
    if (StringUtils.isEmpty(ctx.srcAttr) && StringUtils.isEmpty(ctx.dstAttr) &&
      judgSendMsg(args(3).split(","), ctx.attr)) {
      ctx.sendToDst(s"${ctx.attr.srcV}&${ctx.attr.srcType}->${ctx.attr.dstV}")
    }

    if (StringUtils.isEmpty(ctx.srcAttr) && StringUtils.isNotEmpty(ctx.dstAttr)) {
      ctx.dstAttr.split("-->").map { att =>
        if (att.endsWith(ctx.attr.dstV) && !att.contains(s"${ctx.attr.srcV}&${ctx.attr.srcType}->${ctx.attr.dstV}")) {
          ctx.sendToSrc(s"$att->${ctx.attr.srcV}&${ctx.attr.srcType}")
        }
        ctx.sendToDst("")
      }
    }
    if (StringUtils.isNotEmpty(ctx.srcAttr) && StringUtils.isEmpty(ctx.dstAttr)) {
      ctx.srcAttr.split("-->").map { att =>
        if (att.endsWith(s"${ctx.attr.srcV}&${ctx.attr.srcType}") && !att.contains(s"${ctx.attr.dstV}->${ctx.attr.srcV}&${ctx.attr.srcType}") && !att.contains(s"${ctx.attr.dstV}")) {
          ctx.sendToDst(s"$att->${ctx.attr.dstV}")
        }
        ctx.sendToSrc("")
      }
    }
  }


  def merageMsg = (msgA: String, msgB: String) => {
    if (msgA.equals(msgB)) {
      msgA
    } else if (msgA.contains(msgB)) msgA
    else if (msgB.contains(msgA)) msgB
    else {
      msgA + "-->" + msgB
    }
  }

  //orderId,contractNo,termId,loanPan,returnPan,insertTime,recommend,userId,
  // deviceId
  //certNo,email,company,mobile,compAddr,compPhone,emergencyContactMobile,contactMobile,ipv4,msgphone,telecode
  /**
    *
    * @param path
    * @return
    */
  def genEdgeRdd(path: String): RDD[Edge[EdgeArr]] = {
    //    val edgeRDD = sc.textFile(args(0), 35).mapPartitions(lines => lines.map { line =>
    val edgeRDD = sc.textFile(args(0)).mapPartitions(lines => lines.map { line =>
      import scala.collection.JavaConversions._
      val fields = line.split(",").toList
      val orderno = fields.get(0)
      val edgeList = ListBuffer[Edge[EdgeArr]]()

      //      if (StringUtils.isNotBlank(fields.get(2))) {
      //        val term_id = fields.get(2)
      //        edgeList.+=(Edge(hashId(term_id), hashId(orderno),
      //          EdgeArr(term_id, orderno, "3", "1")))
      //      }

      if (StringUtils.isNotBlank(fields.get(3))) {
        val loan_pan = fields.get(3)
        edgeList.+=(Edge(hashId(s"${loan_pan}&4"), hashId(orderno),
          EdgeArr(loan_pan, orderno, "4", "1")))
      }

      if (StringUtils.isNotBlank(fields.get(4))) {
        val return_pan = fields.get(4)
        edgeList.+=(Edge(hashId(s"${return_pan}&5"), hashId(orderno),
          EdgeArr(return_pan, orderno, "5", "1")))
      }

      if (fields.size() > 6 && StringUtils.isNotBlank(fields.get(6))) {
        val recommend = fields.get(6).replaceAll("-", "")
        edgeList.+=(Edge(hashId(s"${recommend}&7"), hashId(orderno),
          EdgeArr(recommend, orderno, "7", "1")))
      }

      if (fields.size() > 8 && StringUtils.isNotBlank(fields.get(8)) &&
        !"00000000-0000-0000-0000-000000000000".equals(fields.get(8))) {
        val device_id = fields.get(8)
        edgeList.+=(Edge(hashId(s"${device_id}&9"), hashId(orderno),
          EdgeArr(device_id, orderno, "9", "1")))
      }

      if (fields.size() > 9 && StringUtils.isNotBlank(fields.get(9))) {
        val certNo = fields.get(9)
        edgeList.+=(Edge(hashId(s"${certNo}&10"), hashId(orderno),
          EdgeArr(certNo, orderno, "10", "1")))
      }

      if (fields.size() > 10 && StringUtils.isNotBlank(fields.get(10))) {
        val email = fields.get(10)
        edgeList.+=(Edge(hashId(s"${email}&11"), hashId(orderno),
          EdgeArr(email, orderno, "11", "1")))
      }
//      if (fields.size() > 11 && StringUtils.isNotBlank(fields.get(11))) {
//        val company = fields.get(11)
//        edgeList.+=(Edge(hashId(company), hashId(orderno),
//          EdgeArr(company, orderno, "12", "1")))
//      }

      if (fields.size() > 12 && StringUtils.isNotBlank(fields.get(12))) {
        val mobile = fields.get(12).replaceAll("-", "")
        edgeList.+=(Edge(hashId(s"${mobile}&13"), hashId(orderno),
          EdgeArr(mobile, orderno, "13", "1")))
      }

//      if (fields.size() > 13 && StringUtils.isNotBlank(fields.get(13))) {
//        val comp_addr = fields.get(13)
//        edgeList.+=(Edge(hashId(comp_addr), hashId(orderno),
//          EdgeArr(comp_addr, orderno, "14", "1")))
//      }

      //      if (fields.size() > 14 && StringUtils.isNotBlank(fields.get(14))) {
      //        val comp_phone = fields.get(14)
      //        edgeList.+=(Edge(hashId(comp_phone), hashId(orderno),
      //          EdgeArr(comp_phone, orderno, "15", "1")))
      //      }

      if (fields.size() > 15 && StringUtils.isNotBlank(fields.get(15))) {
        val emergencymobile = fields.get(15).replaceAll("-", "")
        edgeList.+=(Edge(hashId(s"${emergencymobile}&16"), hashId(orderno),
          EdgeArr(emergencymobile, orderno, "16", "1")))
      }
      if (fields.size() > 16 && StringUtils.isNotBlank(fields.get(16))) {
        val contact_mobile = fields.get(16).replaceAll("-", "")
        edgeList.+=(Edge(hashId(s"${contact_mobile}&17"), hashId(orderno),
          EdgeArr(contact_mobile, orderno, "17", "1")))
      }

      //      if (fields.size() > 17 && StringUtils.isNotBlank(fields.get(17))) {
      //        val ipv4 = fields.get(17)
      //        edgeList.+=(Edge(hashId(ipv4), hashId(orderno),
      //          EdgeArr(ipv4, orderno, "18", "1")))
      //      }

      if (fields.size() > 18 && StringUtils.isNotBlank(fields.get(18))) {
        val msgphone = fields.get(18).replaceAll("-", "")
        edgeList.+=(Edge(hashId(s"${msgphone}&19"), hashId(orderno),
          EdgeArr(msgphone, orderno, "19", "1")))
      }

      if (fields.size() > 19 && StringUtils.isNotBlank(fields.get(19))) {
        val hometel = fields.get(19).replaceAll("-", "")
        edgeList.+=(Edge(hashId(s"${hometel}&20"), hashId(orderno),
          EdgeArr(hometel, orderno, "20", "1")))
      }
      edgeList
    }.flatten)
    println("GraphxBSP NumPartitions:" + edgeRDD.getNumPartitions)
    edgeRDD
  }

}
