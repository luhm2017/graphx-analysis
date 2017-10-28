import java.io.File

import com.lakala.datacenter.core.utils.UtilsToos.hashId
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, EdgeContext, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.io.fs.FileUtils

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/6/16 0016.
  */
object GraphxBSP23 {
  def main(args: Array[String]): Unit = {
    val gbsp = new GraphxBSP23(args)
    //    F:\\graphx-analysis\\apply\\bin\\test.csv 3 F:\\out\\output
  }
}

//case class EdgeArr(srcV: String, dstV: String, srcType: String, dstType: String)

/*extends Serializable*/

class GraphxBSP23(args: Array[String]) extends Serializable {

  @transient
  val conf = new SparkConf().setAppName("GraphxBSP23").setMaster("local[7]")
  @transient
  val sc = new SparkContext(conf)
  //orderId,contractNo,termId,loanPan,returnPan,insertTime,recommend,userId,
  // deviceId
  //certNo,email,company,mobile,compAddr,compPhone,emergencyContactMobile,contactMobile,ipv4,msgphone,telecode
  FileUtils.deleteRecursively(new File(args(2)))
  val edgeRDD = sc.textFile(args(0)).mapPartitions(lines => lines.map { line =>
    import scala.collection.JavaConversions._
    val fields = line.split(",").toList
    val orderno = fields.get(0)
    val edgeList = ListBuffer[Edge[EdgeArr]]()

    //    if (StringUtils.isNoneEmpty(fields.get(1))) {
    //      val contract_no = fields.get(1)
    //      edgeList.+=(Edge(hashId(orderno), hashId(contract_no),
    //        EdgeArr(orderno, contract_no, "1", "2")))
    //      edgeList.+=(Edge(hashId(contract_no), hashId(orderno),
    //        EdgeArr(contract_no, orderno, "2", "1")))
    //    }

    if (StringUtils.isNoneEmpty(fields.get(2))) {
      val term_id = fields.get(2)
      //      edgeList.+=(Edge(hashId(orderno), hashId(term_id),
      //        EdgeArr(orderno, term_id, "1", "3")))
      edgeList.+=(Edge(hashId(term_id), hashId(orderno),
        EdgeArr(term_id, orderno, "3", "1")))
    }

    if (StringUtils.isNoneEmpty(fields.get(3))) {
      val loan_pan = fields.get(3)
      //      edgeList.+=(Edge(hashId(orderno), hashId(loan_pan),
      //        EdgeArr(orderno, loan_pan, "1", "4")))
      edgeList.+=(Edge(hashId(loan_pan), hashId(orderno),
        EdgeArr(loan_pan, orderno, "4", "1")))
    }

    if (StringUtils.isNoneEmpty(fields.get(4))) {
      val return_pan = fields.get(4)
      //      edgeList.+=(Edge(hashId(orderno), hashId(return_pan),
      //        EdgeArr(orderno, return_pan, "1", "5")))
      edgeList.+=(Edge(hashId(return_pan), hashId(orderno),
        EdgeArr(return_pan, orderno, "5", "1")))
    }
    //    if (fields.size() > 5 && StringUtils.isNoneEmpty(fields.get(5))) {
    //      val insert_time = fields.get(5)
    //      edgeList.+=(Edge(hashId(orderno), hashId(insert_time),
    //        EdgeArr(orderno, insert_time, "1", "6")))
    //      edgeList.+=(Edge(hashId(insert_time), hashId(orderno),
    //        EdgeArr(insert_time, orderno, "6", "1")))
    //    }

    if (fields.size() > 6 && StringUtils.isNoneEmpty(fields.get(6))) {
      val recommend = fields.get(6)
      //      edgeList.+=(Edge(hashId(orderno), hashId(recommend),
      //        EdgeArr(orderno, recommend, "1", "7")))
      edgeList.+=(Edge(hashId(recommend), hashId(orderno),
        EdgeArr(recommend, orderno, "7", "1")))
    }
    //    if (fields.size() > 7 && StringUtils.isNoneEmpty(fields.get(7))) {
    //      val user_id = fields.get(7)
    //      edgeList.+=(Edge(hashId(orderno), hashId(user_id),
    //        EdgeArr(orderno, user_id, "1", "8")))
    //      edgeList.+=(Edge(hashId(user_id), hashId(orderno),
    //        EdgeArr(user_id, orderno, "8", "1")))
    //    }

    if (fields.size() > 8 && StringUtils.isNoneEmpty(fields.get(8))) {
      val device_id = fields.get(8)
      //      edgeList.+=(Edge(hashId(orderno), hashId(device_id),
      //        EdgeArr(orderno, device_id, "1", "9")))
      edgeList.+=(Edge(hashId(device_id), hashId(orderno),
        EdgeArr(device_id, orderno, "9", "1")))
    }

    //    if (fields.size() > 9 && StringUtils.isNoneEmpty(fields.get(9))) {
    //      val cert_no = fields.get(9)
    //      edgeList.+=(Edge(hashId(orderno), hashId(cert_no),
    //        EdgeArr(orderno, cert_no, "1", "10")))
    //      edgeList.+=(Edge(hashId(cert_no), hashId(orderno),
    //        EdgeArr(cert_no, orderno, "10", "1")))
    //    }

    if (fields.size() > 10 && StringUtils.isNoneEmpty(fields.get(10))) {
      val email = fields.get(10)
      //      edgeList.+=(Edge(hashId(orderno), hashId(email),
      //        EdgeArr(orderno, email, "1", "11")))
      edgeList.+=(Edge(hashId(email), hashId(orderno),
        EdgeArr(email, orderno, "11", "1")))
    }
    if (fields.size() > 11 && StringUtils.isNoneEmpty(fields.get(11))) {
      val company = fields.get(11)
      //      edgeList.+=(Edge(hashId(orderno), hashId(company),
      //        EdgeArr(orderno, company, "1", "12")))
      edgeList.+=(Edge(hashId(company), hashId(orderno),
        EdgeArr(company, orderno, "12", "1")))
    }

    if (fields.size() > 12 && StringUtils.isNoneEmpty(fields.get(12))) {
      val mobile = fields.get(12)
      //      edgeList.+=(Edge(hashId(orderno), hashId(mobile),
      //        EdgeArr(orderno, mobile, "1", "13")))
      edgeList.+=(Edge(hashId(mobile), hashId(orderno),
        EdgeArr(mobile, orderno, "13", "1")))
    }

    if (fields.size() > 13 && StringUtils.isNoneEmpty(fields.get(13))) {
      val comp_addr = fields.get(13)
      //      edgeList.+=(Edge(hashId(orderno), hashId(comp_addr),
      //        EdgeArr(orderno, comp_addr, "1", "14")))
      edgeList.+=(Edge(hashId(comp_addr), hashId(orderno),
        EdgeArr(comp_addr, orderno, "14", "1")))
    }

    if (fields.size() > 14 && StringUtils.isNoneEmpty(fields.get(14))) {
      val comp_phone = fields.get(14)
      //      edgeList.+=(Edge(hashId(orderno), hashId(comp_phone),
      //        EdgeArr(orderno, comp_phone, "1", "15")))
      edgeList.+=(Edge(hashId(comp_phone), hashId(orderno),
        EdgeArr(comp_phone, orderno, "15", "1")))
    }

    if (fields.size() > 15 && StringUtils.isNoneEmpty(fields.get(15))) {
      val emergencymobile = fields.get(15)
      //      edgeList.+=(Edge(hashId(orderno), hashId(emergencymobile),
      //        EdgeArr(orderno, emergencymobile, "1", "16")))
      edgeList.+=(Edge(hashId(emergencymobile), hashId(orderno),
        EdgeArr(emergencymobile, orderno, "16", "1")))
    }
    if (fields.size() > 16 && StringUtils.isNoneEmpty(fields.get(16))) {
      val contact_mobile = fields.get(16)
      //      edgeList.+=(Edge(hashId(orderno), hashId(contact_mobile),
      //        EdgeArr(orderno, contact_mobile, "1", "17")))
      edgeList.+=(Edge(hashId(contact_mobile), hashId(orderno),
        EdgeArr(contact_mobile, orderno, "17", "1")))
    }

    if (fields.size() > 17 && StringUtils.isNoneEmpty(fields.get(17))) {
      val ipv4 = fields.get(17)
      //      edgeList.+=(Edge(hashId(orderno), hashId(ipv4),
      //        EdgeArr(orderno, ipv4, "1", "18")))
      edgeList.+=(Edge(hashId(ipv4), hashId(orderno),
        EdgeArr(ipv4, orderno, "18", "1")))
    }

    if (fields.size() > 18 && StringUtils.isNoneEmpty(fields.get(18))) {
      val msgphone = fields.get(18)
      //      edgeList.+=(Edge(hashId(orderno), hashId(msgphone),
      //        EdgeArr(orderno, msgphone, "1", "19")))
      edgeList.+=(Edge(hashId(msgphone), hashId(orderno),
        EdgeArr(msgphone, orderno, "19", "1")))
    }

    if (fields.size() > 19 && StringUtils.isNoneEmpty(fields.get(19))) {
      val hometel = fields.get(19)
      //      edgeList.+=(Edge(hashId(orderno), hashId(hometel),
      //        EdgeArr(orderno, hometel, "1", "20")))
      edgeList.+=(Edge(hashId(hometel), hashId(orderno),
        EdgeArr(hometel, orderno, "20", "1")))
    }
    edgeList
  }.flatten)
  val g = Graph.fromEdges(edgeRDD, "")
  var bakG = g
  var preG: Graph[String, EdgeArr] = null
  var iteration = 0
  while (iteration < args(1).toInt) {
    bakG.cache()
    var tmpG = bakG.aggregateMessages[String](sendMsg, merageMsg)
    preG = bakG
    bakG = bakG.joinVertices(tmpG) { (_, _, updateAtt) => updateAtt }.cache()
    preG.vertices.unpersist(false)
    preG.edges.unpersist(false)
    iteration += 1
  }
  val result = bakG.vertices
    .mapPartitions(vs => vs.filter(v => StringUtils.isNotBlank(v._2)).flatMap(v => v._2.split("-->")).filter(_.split("->").length == args(1).toInt + 1))
  result.repartition(1).saveAsTextFile(args(2))

  def sendMsg(ctx: EdgeContext[String, EdgeArr, String]): Unit = {
    if (StringUtils.isEmpty(ctx.srcAttr) && StringUtils.isEmpty(ctx.dstAttr)) {
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

}
