package com.lakala.datacenter.constant

import com.lakala.datacenter.enums.{Labels, RelationshipTypes}

/**
  * Created by Administrator on 2017/6/22 0022.
  */
object StreamingConstant extends Serializable {
  //配置文件名称
  val CONFIG = "config.properties"
  val NEOIP = "neoIP"
  val USER = "user"
  val PASSWORD = "password"

  val GENERALAPPLY_CREDITLOAN = "generalApply_CreditLoan"
  val LOGNO = "logNo"
  val FLATMAP = "flatMap"

  // kafka 配置文件参数
  val METADATA_BROKER_LIST = "metadata.broker.list"
  val AUTO_OFFSET_RESET = "auto.offset.reset"
  val KAFKA_GROUP_ID = "group.id"
  val CODE = "UTF-8"
  //spark stream 配置参数
  val SPARK_UI_SHOWCONSOLEPROGRESS = "spark.ui.showConsoleProgress"
  val SPARK_LOCALITY_WAIT = "spark.locality.wait"
  val SPARK_STREAMING_KAFKA_MAXRETRIES = "spark.streaming.kafka.maxRetries"
  val SPARK_SERIALIZER = "spark.serializer"
  val SPARK_STREAMING_CONCURRENTJOBS = "spark.streaming.concurrentJobs"
  val SPARK_STREAMING_BACKPRESSURE_ENABLED = "spark.streaming.backpressure.enabled"

  //neo4j 执行需要建立字段名称
  val ORDERNO = "orderno"
  val CONTENT = "content"
  val MODELNAME = "modelname"
  val INSERTTIME = "insertTime"
  val INSERT_TIME = "insert_time"
  val USERID = "userid"
  val USER_ID = "user_id"
  val CERTNO="certno"
  val CERT_NO="cert_no"
  val EMAIL="email"
  val RECOMMEND="recommend"
  val DEVICE_ID="device_id"

  val fieldMap = Map(
    "emergencymobile" -> "emergencymobile,recommend",
    "usermobile" -> "usermobile,recommend",
    "contactmobile" -> "contact_mobile,relativemobile",
    "certno" -> "cert_no,identification",
    "debitcard" -> "debitcard,bankcard",
    "hometel" -> "hometel,hometel",
    "termid" -> "termid,terminal",
    "email" -> "email,email",
    "creditcard" -> "creditcard,bankcard",
    "ipv4" -> "ipv4,ipv4",
    "mobile" -> "mobile,applymymobile",
    "usercoremobile" -> "usercoremobile,loginmobile",
    "recommend" -> "recommend,recommend",
    "_DeviceId" -> "device_id,device",
    "companyname" -> "company,company",
    "companyaddress" -> "comp_addr,companyaddress",
    "companytel" -> "comp_phone,companytel",
    "merchantmobile" -> "merchantmobile,merchantmobile",
    "channelmobile" -> "channelmobile,channelmobile",
    "partnercontantmobile" -> "merchantmobile,merchantmobile")

  val labelMap = Map(
    "emergencymobile" -> Labels.Mobile,
    "usermobile" -> Labels.Mobile,
    "contactmobile" -> Labels.Mobile,
    "certno" -> Labels.Identification,
    "debitcard" -> Labels.BankCard,
    "hometel" -> Labels.Mobile,
    "termid" -> Labels.Terminal,
    "email" -> Labels.Email,
    "creditcard" -> Labels.BankCard,
    "ipv4" -> Labels.IPV4,
    "mobile" -> Labels.Mobile,
    "usercoremobile" -> Labels.Mobile,
    "recommend" -> Labels.Mobile,
    "_DeviceId" -> Labels.Device,
    "companyname" -> Labels.Company,
    "companyaddress" -> Labels.CompanyAddress,
    "companytel" -> Labels.CompanyTel,
    "merchantmobile" -> Labels.Mobile,
    "channelmobile" -> Labels.Mobile,
    "partnercontantmobile" -> Labels.Mobile)

  val relationShipMap = Map(
    "emergencymobile" -> RelationshipTypes.recommend,
    "usermobile" ->  RelationshipTypes.recommend,
    "contactmobile" ->  RelationshipTypes.relativemobile,
    "certno" ->  RelationshipTypes.identification,
    "debitcard" ->  RelationshipTypes.bankcard,
    "hometel" ->  RelationshipTypes.hometel,
    "termid" ->  RelationshipTypes.terminal,
    "email" ->  RelationshipTypes.email,
    "creditcard" ->  RelationshipTypes.bankcard,
    "ipv4" ->  RelationshipTypes.ipv4,
    "mobile" ->  RelationshipTypes.applymymobile,
    "usercoremobile" ->  RelationshipTypes.loginmobile,
    "recommend" -> RelationshipTypes.recommend,
    "_DeviceId" ->  RelationshipTypes.device,
    "companyname" -> RelationshipTypes.company,
    "companyaddress" ->  RelationshipTypes.companyaddress,
    "companytel" ->  RelationshipTypes.companytel,
    "merchantmobile" ->  RelationshipTypes.merchantmobile,
    "channelmobile" ->  RelationshipTypes.channelmobile,
    "partnercontantmobile" ->  RelationshipTypes.merchantmobile)

  val PSUBSCRIBE = "psubscribe"

}
