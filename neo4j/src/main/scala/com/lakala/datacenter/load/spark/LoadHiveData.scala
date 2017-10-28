package com.lakala.datacenter.load.spark

import com.lakala.datacenter.core.utils.UtilsToos
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on "2017"/5/"31" 0031.
  */
object LoadHiveData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LoadHiveData")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    hc.sql("use creditloan")
    val sql =
      s"""select a.order_id,a.contract_no,a.term_id,a.loan_pan,a.return_pan,a.insert_time,a.recommend,a.user_id,b.cert_no,b.email,b.company,b.mobile,b.comp_addr,b.comp_phone,b.emergency_contact_mobile,b.contact_mobile,c.device_id
         |from creditloan.s_c_loan_apply a
         | left join creditloan.s_c_apply_user b on a.user_id =b.id and (a.year="2017" and a.month="05" and a.day="31") and (b.year="2017" and b.month="05" and b.day="31")
         | left join creditloan.s_c_loan_deviceidauth c on a.order_id =c.order_no and (a.year="2017" and a.month="05" and a.day="31") and (c.year="2017" and c.month="05" and c.day="31") """.stripMargin

    val df = hc.sql(sql)
    val lineRDD = df.mapPartitions { rows =>
      rows.map { row =>
        val orderId = row.getAs[String]("order_id")
        val contractNo = if (StringUtils.isNotBlank(row.getAs[String]("contract_no"))) row.getAs[String]("contract_no") else ""
        val termId = if (StringUtils.isNotBlank(row.getAs[String]("term_id"))) row.getAs[String]("term_id") else ""
        val loanPan = if (StringUtils.isNotBlank(row.getAs[String]("loan_pan"))) row.getAs[String]("loan_pan") else ""
        val returnPan = if (StringUtils.isNotBlank(row.getAs[String]("return_pan"))) row.getAs[String]("return_pan") else ""
        val insertTime = if (StringUtils.isNotBlank(row.getAs[String]("insert_time"))) row.getAs[String]("insert_time") else ""
        val recommend = if (StringUtils.isNotBlank(row.getAs[String]("recommend")) && UtilsToos.isMobileOrPhone(row.getAs[String]("recommend"))) row.getAs[String]("recommend") else ""
        val userId = if (StringUtils.isNotBlank(row.getAs[String]("user_id"))) row.getAs[String]("user_id") else ""
        val certNo = if (StringUtils.isNotBlank(row.getAs[String]("cert_no"))) row.getAs[String]("cert_no") else ""
        val email = if (StringUtils.isNotBlank(row.getAs[String]("email"))) row.getAs[String]("email") else ""
        val company = if (StringUtils.isNotBlank(row.getAs[String]("company"))) row.getAs[String]("company") else ""
        val mobile = if (StringUtils.isNotBlank(row.getAs[String]("mobile")) && UtilsToos.isMobileOrPhone(row.getAs[String]("mobile"))) row.getAs[String]("mobile") else ""
        val compAddr = if (StringUtils.isNotBlank(row.getAs[String]("comp_addr"))) row.getAs[String]("comp_addr") else ""
        val compPhone = if (StringUtils.isNotBlank(row.getAs[String]("comp_phone"))) row.getAs[String]("comp_phone") else ""
        val emergencyContactMobile = if (StringUtils.isNotBlank(row.getAs[String]("emergency_contact_mobile"))) row.getAs[String]("emergency_contact_mobile") else ""
        val contactMobile = if (StringUtils.isNotBlank(row.getAs[String]("contact_mobile"))) row.getAs[String]("contact_mobile") else ""
        val deviceId = if (StringUtils.isNotBlank(row.getAs[String]("device_id"))) row.getAs[String]("device_id") else ""
        s"$orderId,$contractNo,$termId,$loanPan,$returnPan,$insertTime,$recommend,$userId,$certNo,$email,$company,$mobile,$compAddr,$compPhone,$emergencyContactMobile,$contactMobile,$deviceId"
      }
    }

    lineRDD.saveAsTextFile(args(1))

  }
}
