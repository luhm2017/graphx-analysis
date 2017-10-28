import com.lakala.datacenter.core.utils.UtilsToos.hashId
import org.apache.commons.lang.StringUtils
import org.apache.spark.graphx.{Edge, EdgeContext, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/6/16 0016.
  */
object GraphxBSP3 {
  def main(args: Array[String]): Unit = {
    val gbsp = new GraphxBSP3(args)
    gbsp.runNHopResult()
    //    F:\\graphx-analysis\\apply\\bin\\test.csv 3 F:\\out\\output
  }
}

case class EdgeArr(srcV: String, dstV: String, srcType: String, dstType: String)

/*extends Serializable*/

class GraphxBSP3(args: Array[String]) extends Serializable {

  @transient
  val conf = new SparkConf().setAppName("GraphxBSP3")
    .setMaster("local[7]")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //设置序列化为kryo
  // 容错相关参数
  conf.set("spark.task.maxFailures", "8")
  conf.set("spark.akka.timeout", "300")
  conf.set("spark.network.timeout", "300")
  conf.set("spark.yarn.max.executor.failures", "100")
  //建议打开map（注意，在spark引擎中，也只有map和reduce两种task，spark叫ShuffleMapTask和ResultTask）中间结果合并及推测执行功能：
  conf.set("spark.shuffle.consolidateFiles", "true")
  conf.set("spark.speculation", "true")
  conf.set("spark.rdd.compress", "true")


  @transient
  val sc = new SparkContext(conf)

  def runNHopResult(): Unit = {
    var g = Graph.fromEdges(genEdgeRdd(args(0)), "")
    var preG: Graph[String, EdgeArr] = null
    var iteration = 0
    while (iteration < args(1).toInt) {
      println(s"iteration $iteration start .....")
      var tmpG = g.aggregateMessages[String](sendMsg, merageMsg).persist(StorageLevel.MEMORY_AND_DISK_SER)
      preG = g
      g = g.joinVertices(tmpG) { (_, _, updateAtt) => updateAtt }.persist(StorageLevel.MEMORY_AND_DISK_SER)
      //      g = Graph.apply(tmpG,g.edges)
      //      g.edges.foreachPartition(x => {})
      tmpG.unpersist(true)
      preG.unpersistVertices(true)
      preG.edges.unpersist()
      println(s"iteration $iteration end .....")
      iteration += 1
    }
    //************************
    val result = g.vertices
      .mapPartitions(vs => vs.filter(v => StringUtils.isNotBlank(v._2)).flatMap(v => v._2.split("-->")).filter(_.split("->").length == args(1).toInt + 1))
    result.repartition(1).saveAsTextFile(args(2))
  }

  def judgSendMsg(sendType: Array[String], edge: EdgeArr): Boolean = {
    var flag = false
    for (stype <- sendType) if (edge.srcType.equals(stype)) flag = true
    flag
  }

  def sendMsg(ctx: EdgeContext[String, EdgeArr, String]): Unit = {
    if (StringUtils.isEmpty(ctx.srcAttr) && StringUtils.isEmpty(ctx.dstAttr) &&
      judgSendMsg(args(3).split(","),ctx.attr)) {
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


  def merageMsg(msgA: String, msgB: String): String = {
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
    val edgeRDD = sc.textFile(args(0)).mapPartitions(lines => lines.map { line =>
      import scala.collection.JavaConversions._
      val fields = line.split(",").toList
      val orderno = fields.get(0)
      val edgeList = ListBuffer[Edge[EdgeArr]]()

      if (StringUtils.isNotBlank(fields.get(2))) {
        val term_id = fields.get(2)
        edgeList.+=(Edge(hashId(term_id), hashId(orderno),
          EdgeArr(term_id, orderno, "3", "1")))
      }

      if (StringUtils.isNotBlank(fields.get(3))) {
        val loan_pan = fields.get(3)
        edgeList.+=(Edge(hashId(loan_pan), hashId(orderno),
          EdgeArr(loan_pan, orderno, "4", "1")))
      }

      if (StringUtils.isNotBlank(fields.get(4))) {
        val return_pan = fields.get(4)
        edgeList.+=(Edge(hashId(return_pan), hashId(orderno),
          EdgeArr(return_pan, orderno, "5", "1")))
      }

      if (fields.size() > 6 && StringUtils.isNotBlank(fields.get(6))) {
        val recommend = fields.get(6)
        edgeList.+=(Edge(hashId(recommend), hashId(orderno),
          EdgeArr(recommend, orderno, "7", "1")))
      }

      if (fields.size() > 8 && StringUtils.isNotBlank(fields.get(8))) {
        val device_id = fields.get(8)
        edgeList.+=(Edge(hashId(device_id), hashId(orderno),
          EdgeArr(device_id, orderno, "9", "1")))
      }


      if (fields.size() > 10 && StringUtils.isNotBlank(fields.get(10))) {
        val email = fields.get(10)
        edgeList.+=(Edge(hashId(email), hashId(orderno),
          EdgeArr(email, orderno, "11", "1")))
      }
      if (fields.size() > 11 && StringUtils.isNotBlank(fields.get(11))) {
        val company = fields.get(11)
        edgeList.+=(Edge(hashId(company), hashId(orderno),
          EdgeArr(company, orderno, "12", "1")))
      }

      if (fields.size() > 12 && StringUtils.isNotBlank(fields.get(12))) {
        val mobile = fields.get(12)
        edgeList.+=(Edge(hashId(mobile), hashId(orderno),
          EdgeArr(mobile, orderno, "13", "1")))
      }

      if (fields.size() > 13 && StringUtils.isNotBlank(fields.get(13))) {
        val comp_addr = fields.get(13)
        edgeList.+=(Edge(hashId(comp_addr), hashId(orderno),
          EdgeArr(comp_addr, orderno, "14", "1")))
      }

      if (fields.size() > 14 && StringUtils.isNotBlank(fields.get(14))) {
        val comp_phone = fields.get(14)
        edgeList.+=(Edge(hashId(comp_phone), hashId(orderno),
          EdgeArr(comp_phone, orderno, "15", "1")))
      }

      if (fields.size() > 15 && StringUtils.isNotBlank(fields.get(15))) {
        val emergencymobile = fields.get(15)
        edgeList.+=(Edge(hashId(emergencymobile), hashId(orderno),
          EdgeArr(emergencymobile, orderno, "16", "1")))
      }
      if (fields.size() > 16 && StringUtils.isNotBlank(fields.get(16))) {
        val contact_mobile = fields.get(16)
        edgeList.+=(Edge(hashId(contact_mobile), hashId(orderno),
          EdgeArr(contact_mobile, orderno, "17", "1")))
      }

      if (fields.size() > 17 && StringUtils.isNotBlank(fields.get(17))) {
        val ipv4 = fields.get(17)
        edgeList.+=(Edge(hashId(ipv4), hashId(orderno),
          EdgeArr(ipv4, orderno, "18", "1")))
      }

      if (fields.size() > 18 && StringUtils.isNotBlank(fields.get(18))) {
        val msgphone = fields.get(18)
        edgeList.+=(Edge(hashId(msgphone), hashId(orderno),
          EdgeArr(msgphone, orderno, "19", "1")))
      }

      if (fields.size() > 19 && StringUtils.isNotBlank(fields.get(19))) {
        val hometel = fields.get(19)
        edgeList.+=(Edge(hashId(hometel), hashId(orderno),
          EdgeArr(hometel, orderno, "20", "1")))
      }
      edgeList
    }.flatten)
    edgeRDD
  }

}
