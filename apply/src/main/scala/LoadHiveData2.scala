import com.lakala.datacenter.core.utils.UtilsToos
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linyanshi on 2017/5/31 0031.
  * and a.insert_time >= "2017-01-01"
  */
object LoadHiveData2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LoadHiveData2")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val date = args(1).split("-")
    val year = date(0)
    val month = date(1)
    val day = date(2)
    hc.sql("use creditloan")
    val sql =
      s"""select a.order_id,a.contract_no,a.term_id,a.loan_pan,a.return_pan,a.insert_time,a.recommend,a.id as user_id
         |from creditloan.s_c_loan_apply a
         | where a.year="${year}" and a.month="${month}" and a.day="${day}" """.stripMargin

    val df = hc.sql(sql)
    val applylineRDD = df.mapPartitions { rows =>
      rows.map { row =>
        val orderId = row.getAs[String]("order_id")
        val contractNo = if (StringUtils.isNotBlank(row.getAs[String]("contract_no")) && !"null".equals(row.getAs[String]("contract_no").toLowerCase)) row.getAs[String]("contract_no") else ""
        val termId = if (StringUtils.isNotBlank(row.getAs[String]("term_id")) && !"null".equals(row.getAs[String]("term_id").toLowerCase)) row.getAs[String]("term_id") else ""
        val loanPan = if (StringUtils.isNotBlank("" + row.getAs[Int]("loan_pan")) && !"null".equals((("" + row.getAs[Int]("loan_pan")).toLowerCase))) "" + row.getAs[Int]("loan_pan") else ""
        val returnPan = if (StringUtils.isNotBlank(row.getAs[String]("return_pan")) && !"null".equals(row.getAs[String]("return_pan").toLowerCase)) row.getAs[String]("return_pan") else ""
        val insertTime = if (StringUtils.isNotBlank(row.getAs[String]("insert_time")) && !"null".equals(row.getAs[String]("insert_time").toLowerCase)) row.getAs[String]("insert_time") else ""
        val recommend = if (StringUtils.isNotBlank(row.getAs[String]("recommend")) && !"null".equals(row.getAs[String]("recommend").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("recommend"))) row.getAs[String]("recommend") else ""
        val userId = if (StringUtils.isNotBlank("" + row.getAs[Int]("user_id")) && !"null".equals(("" + row.getAs[Int]("user_id")).toLowerCase)) "" + row.getAs[Int]("user_id") else ""
        (orderId, s"$orderId,$contractNo,$termId,$loanPan,$returnPan,$insertTime,$recommend,$userId")
      }
    }

    val sql2 =
      s"""select b.cert_no,b.email,b.company,b.mobile,b.comp_addr,b.comp_phone,b.emergency_contact_mobile,b.contact_mobile,b.ipv4,b.msgphone,b.telecode,b.id as user_id
         |from creditloan.s_c_apply_user b
         | where b.year="${year}" and b.month="${month}" and b.day="${day}" """.stripMargin

    val df2 = hc.sql(sql2)
    val userlineRDD = df2.mapPartitions { rows =>
      rows.map { row =>
        val certNo = if (StringUtils.isNotBlank(row.getAs[String]("cert_no")) && !"null".equals(row.getAs[String]("cert_no").toLowerCase)) row.getAs[String]("cert_no") else ""
        val email = if (StringUtils.isNotBlank(row.getAs[String]("email")) && !"null".equals(row.getAs[String]("email").toLowerCase)) row.getAs[String]("email") else ""
        val company = if (StringUtils.isNotBlank(row.getAs[String]("company")) && !"null".equals(row.getAs[String]("company").toLowerCase)) row.getAs[String]("company") else ""
        val mobile = if (StringUtils.isNotBlank(row.getAs[String]("mobile")) && !"null".equals(row.getAs[String]("mobile").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("mobile"))) row.getAs[String]("mobile") else ""
        val compAddr = if (StringUtils.isNotBlank(row.getAs[String]("comp_addr")) && !"null".equals(row.getAs[String]("comp_addr").toLowerCase)) row.getAs[String]("comp_addr") else ""
        val compPhone = if (StringUtils.isNotBlank(row.getAs[String]("comp_phone")) && !"null".equals(row.getAs[String]("comp_phone").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("comp_phone"))) row.getAs[String]("comp_phone") else ""
        val emergencyContactMobile = if (StringUtils.isNotBlank(row.getAs[String]("emergency_contact_mobile")) && !"null".equals(row.getAs[String]("emergency_contact_mobile").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("emergency_contact_mobile"))) row.getAs[String]("emergency_contact_mobile") else ""
        val contactMobile = if (StringUtils.isNotBlank(row.getAs[String]("contact_mobile")) && !"null".equals(row.getAs[String]("contact_mobile").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("contact_mobile"))) row.getAs[String]("contact_mobile") else ""
        val ipv4 = if (StringUtils.isNotBlank(row.getAs[String]("ipv4")) && !"null".equals(row.getAs[String]("ipv4").toLowerCase)) row.getAs[String]("ipv4") else ""
        val msgphone = if (StringUtils.isNotBlank(row.getAs[String]("msgphone")) && !"null".equals(row.getAs[String]("msgphone").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("msgphone"))) row.getAs[String]("msgphone") else ""
        val telecode = if (StringUtils.isNotBlank(row.getAs[String]("telecode")) && !"null".equals(row.getAs[String]("telecode").toLowerCase) && UtilsToos.isMobileOrPhone(row.getAs[String]("telecode"))) row.getAs[String]("telecode") else ""
        val userId = if (StringUtils.isNotBlank("" + row.getAs[Int]("user_id")) && !"null".equals(("" + row.getAs[Int]("user_id")).toLowerCase)) "" + row.getAs[Int]("user_id") else ""
        (userId, s"$certNo,$email,$company,$mobile,$compAddr,$compPhone,$emergencyContactMobile,$contactMobile,$ipv4,$msgphone,$telecode")
      }
    }

    val sql3 =
      s"""select c.order_no,c.device_id
         |from creditloan.s_c_loan_deviceidauth c
         | where c.year="${year}" and c.month="${month}" and c.day="${day}" """.stripMargin

    val df3 = hc.sql(sql3)
    val deviceRDD = df3.mapPartitions { rows =>
      rows.map { row =>
        val deviceId = if (StringUtils.isNotBlank(row.getAs[String]("device_id")) && !"null".equals(row.getAs[String]("device_id").toLowerCase)) row.getAs[String]("device_id") else ""
        val orderNo = if (StringUtils.isNotBlank(row.getAs[String]("order_no")) && !"null".equals(row.getAs[String]("order_no").toLowerCase)) row.getAs[String]("order_no") else ""
        (orderNo, s"$deviceId")
      }
    }

    val rdd = applylineRDD.leftOuterJoin(deviceRDD).mapPartitions(kvs => kvs.map { kv =>
      val applyContext = kv._2._1
      val deviceContext = kv._2._2.getOrElse("")
      (applyContext.substring(applyContext.lastIndexOf(",") + 1), s"$applyContext,$deviceContext")
    }).leftOuterJoin(userlineRDD).mapPartitions(kvs => kvs.map { kv =>
      s"${kv._2._1},${kv._2._2.getOrElse("")}"
    }).distinct()

   val outpath = if(sc.isLocal) {args(0)}  else args(0)
    println("outpath========="+outpath)
    rdd.saveAsTextFile(outpath)
    println("********* end ******************")
    sc.stop()
  }
}
