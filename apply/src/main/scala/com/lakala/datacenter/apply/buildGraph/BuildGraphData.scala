package com.lakala.datacenter.apply.buildGraph

import com.lakala.datacenter.utils.UtilsToos
import org.apache.commons.lang.StringUtils
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * Created by linyanshi on 2017/9/1 0001.
  */
object BuildGraphData/* extends Serializable */{

  private var param: Array[String] = _

  def apply(hc: HiveContext, param: Array[String]): Graph[Array[String], NewEdgeArr] = {
    this.param = param
    Graph.fromEdges(getApplyUserDeviceData(hc, param), Array[String](), edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
  }


  def getApplyUserDeviceData(hc: HiveContext, param: Array[String]): RDD[Edge[NewEdgeArr]] = {
    hc.sql("use creditloan")
    val sql =
      s"""select aa.order_id,aa.contract_no,aa.term_id,aa.loan_pan,aa.return_pan,aa.insert_time,aa.recommend,aa.user_id,bb.cert_no,bb.email,bb.company,bb.mobile,bb.comp_addr,bb.comp_phone,bb.emergency_contact_mobile,bb.contact_mobile,bb.ipv4,bb.msgphone,bb.telecode,cc.device_id
         |from (select a.order_id,a.contract_no,a.term_id,a.loan_pan,a.return_pan,a.insert_time,a.recommend,a.id as user_id
         |         from creditloan.s_c_loan_apply a
         |            where a.year="${param(2)}" and a.month="${param(3)}" and a.day="${param(4)}" ) aa
         |left join(select c.order_no,c.device_id
         |              from creditloan.s_c_loan_deviceidauth c
         |                  where c.year="${param(2)}" and c.month="${param(3)}" and c.day="${param(4)}") cc
         | on aa.order_id =cc.order_no
         	|left join (select b.cert_no,b.email,b.company,b.mobile,b.comp_addr,b.comp_phone,b.emergency_contact_mobile,b.contact_mobile,b.ipv4,b.msgphone,b.telecode,b.id as user_id
         |               from creditloan.s_c_apply_user b
         |                  where b.year="${param(2)}" and b.month="${param(3)}" and b.day="${param(4)}") bb
         | on aa.user_id =bb.user_id   """.stripMargin

    val df = hc.sql(sql)

    df.mapPartitions { rows =>
      rows.map { row =>
        val orderno = row.getAs[String]("order_id")
        val contractNo = if (StringUtils.isNotBlank(row.getAs[String]("contract_no")) && !"null".equals(row.getAs[String]("contract_no").toLowerCase)) row.getAs[String]("contract_no") else ""
        val termId = if (StringUtils.isNotBlank(row.getAs[String]("term_id")) && !"null".equals(row.getAs[String]("term_id").toLowerCase)) row.getAs[String]("term_id") else ""
        val loan_pan = if (StringUtils.isNotBlank("" + row.getAs[Int]("loan_pan")) && !"null".equals((("" + row.getAs[Int]("loan_pan")).toLowerCase))) "" + row.getAs[Int]("loan_pan") else ""
        val return_pan = if (StringUtils.isNotBlank(row.getAs[String]("return_pan")) && !"null".equals(row.getAs[String]("return_pan").toLowerCase)) row.getAs[String]("return_pan") else ""
        val insertTime = if (StringUtils.isNotBlank(row.getAs[String]("insert_time")) && !"null".equals(row.getAs[String]("insert_time").toLowerCase)) row.getAs[String]("insert_time") else ""
        val recommend = if (StringUtils.isNotBlank(row.getAs[String]("recommend")) && !"null".equals(row.getAs[String]("recommend").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("recommend"))) row.getAs[String]("recommend") else ""
        val userId = if (StringUtils.isNotBlank("" + row.getAs[Int]("user_id")) && !"null".equals(("" + row.getAs[Int]("user_id")).toLowerCase)) "" + row.getAs[Int]("user_id") else ""
        val cert_no = if (StringUtils.isNotBlank(row.getAs[String]("cert_no")) && !"null".equals(row.getAs[String]("cert_no").toLowerCase)) row.getAs[String]("cert_no") else ""
        val email = if (StringUtils.isNotBlank(row.getAs[String]("email")) && !"null".equals(row.getAs[String]("email").toLowerCase)) row.getAs[String]("email") else ""
        val company = if (StringUtils.isNotBlank(row.getAs[String]("company")) && !"null".equals(row.getAs[String]("company").toLowerCase)) row.getAs[String]("company") else ""
        val mobile = if (StringUtils.isNotBlank(row.getAs[String]("mobile")) && !"null".equals(row.getAs[String]("mobile").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("mobile"))) row.getAs[String]("mobile") else ""
        val compAddr = if (StringUtils.isNotBlank(row.getAs[String]("comp_addr")) && !"null".equals(row.getAs[String]("comp_addr").toLowerCase)) row.getAs[String]("comp_addr") else ""
        val compPhone = if (StringUtils.isNotBlank(row.getAs[String]("comp_phone")) && !"null".equals(row.getAs[String]("comp_phone").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("comp_phone"))) row.getAs[String]("comp_phone") else ""
        val emergencymobile = if (StringUtils.isNotBlank(row.getAs[String]("emergency_contact_mobile")) && !"null".equals(row.getAs[String]("emergency_contact_mobile").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("emergency_contact_mobile"))) row.getAs[String]("emergency_contact_mobile") else ""
        val contact_mobile = if (StringUtils.isNotBlank(row.getAs[String]("contact_mobile")) && !"null".equals(row.getAs[String]("contact_mobile").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("contact_mobile"))) row.getAs[String]("contact_mobile") else ""
        val ipv4 = if (StringUtils.isNotBlank(row.getAs[String]("ipv4")) && !"null".equals(row.getAs[String]("ipv4").toLowerCase)) row.getAs[String]("ipv4") else ""
        val msgphone = if (StringUtils.isNotBlank(row.getAs[String]("msgphone")) && !"null".equals(row.getAs[String]("msgphone").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("msgphone"))) row.getAs[String]("msgphone") else ""
        val hometel = if (StringUtils.isNotBlank(row.getAs[String]("telecode")) && !"null".equals(row.getAs[String]("telecode").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("telecode"))) row.getAs[String]("telecode") else ""
        val device_id = if (StringUtils.isNotBlank(row.getAs[String]("device_id")) && !"null".equals(row.getAs[String]("device_id").toLowerCase)) row.getAs[String]("device_id") else ""

        val dt = if (insertTime.indexOf(".") > 0) insertTime.substring(0, insertTime.indexOf(".")) else insertTime
        val init = UtilsToos.jugeInit(dt, param(0), param(1))

        val edgeList = ListBuffer[Edge[NewEdgeArr]]()

        if (StringUtils.isNotBlank(loan_pan)) {
          edgeList.+=(Edge(UtilsToos.hashId(s"${loan_pan}&4"), UtilsToos.hashId(orderno),
            NewEdgeArr(loan_pan, s"${orderno}#${insertTime}", "4", "1", init)))
        }

        if (StringUtils.isNotBlank(return_pan)) {
          edgeList.+=(Edge(UtilsToos.hashId(s"${return_pan}&5"), UtilsToos.hashId(orderno),
            NewEdgeArr(return_pan, s"${orderno}#${insertTime}", "5", "1", init)))
        }

        if (StringUtils.isNotBlank(recommend)) {
          edgeList.+=(Edge(UtilsToos.hashId(s"${recommend}&7"), UtilsToos.hashId(orderno),
            NewEdgeArr(recommend, s"${orderno}#${insertTime}", "7", "1", init)))
        }

        if (StringUtils.isNotBlank(device_id) &&
          !"00000000-0000-0000-0000-000000000000".equals(device_id)) {
          edgeList.+=(Edge(UtilsToos.hashId(s"${device_id}&9"), UtilsToos.hashId(orderno),
            NewEdgeArr(device_id, s"${orderno}#${insertTime}", "9", "1", init)))
        }
        if (StringUtils.isNotBlank(cert_no)) {
          edgeList.+=(Edge(UtilsToos.hashId(s"${cert_no}&10"), UtilsToos.hashId(orderno),
            NewEdgeArr(cert_no, s"${orderno}#${insertTime}", "10", "1", init)))
        }

        if (StringUtils.isNotBlank(email)) {
          edgeList.+=(Edge(UtilsToos.hashId(s"${email}&11"), UtilsToos.hashId(orderno),
            NewEdgeArr(email, s"${orderno}#${insertTime}", "11", "1", init)))
        }

        if (StringUtils.isNotBlank(mobile)) {
          edgeList.+=(Edge(UtilsToos.hashId(s"${mobile}&13"), UtilsToos.hashId(orderno),
            NewEdgeArr(mobile, s"${orderno}#${insertTime}", "13", "1", init)))
        }

        if (StringUtils.isNotBlank(emergencymobile)) {
          edgeList.+=(Edge(UtilsToos.hashId(s"${emergencymobile}&16"), UtilsToos.hashId(orderno),
            NewEdgeArr(emergencymobile, s"${orderno}#${insertTime}", "16", "1", init)))
        }


        if (StringUtils.isNotBlank(msgphone)) {
          edgeList.+=(Edge(UtilsToos.hashId(s"${msgphone}&19"), UtilsToos.hashId(orderno),
            NewEdgeArr(msgphone, s"${orderno}#${insertTime}", "19", "1", init)))
        }

        if (StringUtils.isNotBlank(hometel)) {
          edgeList.+=(Edge(UtilsToos.hashId(s"${hometel}&20"), UtilsToos.hashId(orderno),
            NewEdgeArr(hometel, s"${orderno}#${insertTime}", "20", "1", init)))
        }
        edgeList
      }.flatten
    }.distinct()
  }

}
