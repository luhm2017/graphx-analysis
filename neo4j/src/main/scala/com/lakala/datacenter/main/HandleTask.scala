package com.lakala.datacenter.main

import com.alibaba.fastjson.{JSON, TypeReference}
import com.google.gson.Gson
import com.lakala.datacenter.common.utils.DateTimeUtils
import com.lakala.datacenter.constant.StreamingConstant
import com.lakala.datacenter.realtimeBuildGraphx.SendMsg
import org.apache.commons.lang3.StringUtils
import org.joda.time.DateTime
import org.neo4j.driver.v1.{Session, Transaction, TransactionWork}

import scala.util.{Failure, Success, Try}

/**
  * Created by Administrator on 2017/8/8 0008.
  */
class HandleTask extends Runnable {

  //  private var m_stream: KafkaStream[_, _] = null
  //  private var m_threadNumber: Int = 0
  //  private var redis: JedisCluster = null
  //  private var session: Session = null
  //  private var sessionBak: Session = null
  private var msgParpam: MessageParam = null

  def this(msgParpam: MessageParam) {
    this()
    this.msgParpam = msgParpam
  }

  def run(): Unit = {
    val it = msgParpam.m_stream.iterator
    while (it.hasNext()) {
      val value = it.next().message().asInstanceOf[String]
      println("Thread " + msgParpam.m_threadNumber + " deal message")
      import scala.collection.JavaConverters._
      val map: java.util.Map[String, String] = msgpackageToMap(value).asJava
      Try {
        println("======"+value)
        runCypherApply(msgParpam.session, map)
      } match {
        case Success(sf) => {
          try {
            //用订阅发布模式 把进件号id 存入redis
            val msg = new SendMsg(map.getOrDefault(StreamingConstant.ORDERNO, ""), DateTimeUtils.formatter.print(map.getOrDefault(StreamingConstant.INSERTTIME, s"${DateTime.now().getMillis}").toLong), map.getOrDefault(StreamingConstant.CERTNO, "00000"))
            msgParpam.redis.publish(msgParpam.psubscribe, new Gson().toJson(msg))
          } catch {
            case e: Exception => println("redis exception push message:orderno= " + map.getOrDefault(StreamingConstant.ORDERNO, "") + ":exceptions" + e.getMessage)
          }
          //          msgParpam.session.close()
        }
        case Failure(sf) => {
          msgParpam.session.close()
          println(s"${DateTime.now()} real time applyInfo insert neo4j throw EXCEPTION: ${sf.getMessage} orderno= " + map.getOrDefault(StreamingConstant.ORDERNO, ""))
        }
      }
      System.out.println("Shutting down Thread: " + msgParpam.m_threadNumber)
    }
  }

  private def msgpackageToMap(value: String): Map[String, String] = {
    val map: java.util.HashMap[String, String] = JSON.parseObject(value, new TypeReference[java.util.HashMap[String, String]]() {})
    val cLoanApplyMap: java.util.HashMap[String, String] = JSON.parseObject(map.get("cLoanApply"), new TypeReference[java.util.HashMap[String, String]]() {})
    val cApplyUserMap: java.util.HashMap[String, String] = JSON.parseObject(map.get("cApplyUser"), new TypeReference[java.util.HashMap[String, String]]() {})

    Map("orderno" -> cLoanApplyMap.get("orderId"), "userid" -> cApplyUserMap.get("userId"),
      "insertTime" -> cLoanApplyMap.getOrDefault(StreamingConstant.INSERTTIME, DateTimeUtils.formatter.print(DateTime.now())),
      "termid" -> cLoanApplyMap.get("termId"), "loanPan" -> cLoanApplyMap.get("loanPan"),
      "mobile" -> cApplyUserMap.get("mobile"), "certno" -> cApplyUserMap.get("certNo"),
      "contactmobile" -> cApplyUserMap.get("contactMobile"), "emergencymobile" -> cApplyUserMap.get("emergencyContactMobile"),
      "companyaddress" -> cApplyUserMap.get("compAddr"), "companyname" -> cApplyUserMap.get("company"),
      "email" -> cApplyUserMap.get("email"), "companyname" -> cApplyUserMap.get("company")
    )
  }


  /**
    * cypher 入节点数据
    *
    * @param session
    * @param map
    */
  private def runCypherApply(session: Session, map: java.util.Map[String, String]): Unit = {
    val applyStatementTemplate = new StringBuffer("MERGE (apply:ApplyInfo {orderno:$orderno})")
    applyStatementTemplate.append(" ON CREATE SET apply.modelname='ApplyInfo',apply.insertTime=$insertTime,apply.user_id=$user_id")
    val otherStatementTemplate = new StringBuffer()
    val relStatementTemplate = new StringBuffer()

    var paramMap: java.util.HashMap[String, Object] = new java.util.HashMap[String, Object]()
    paramMap.put(StreamingConstant.ORDERNO, map.getOrDefault(StreamingConstant.ORDERNO, ""))
    paramMap.put(StreamingConstant.INSERTTIME, map.getOrDefault(StreamingConstant.INSERTTIME, DateTimeUtils.formatter.print(DateTime.now())))
    paramMap.put(StreamingConstant.USER_ID, map.getOrDefault(StreamingConstant.USERID, ""))

    for (key <- StreamingConstant.fieldMap.keySet) {
      val fieldRelation = StreamingConstant.fieldMap.get(key).get.split(",")
      var value = if (StringUtils.isNotBlank(map.get(key))) map.get(key) else ""
      if (StreamingConstant.DEVICE_ID.equals(fieldRelation(0)) && StringUtils.isNotBlank(value) && "00000000-0000-0000-0000-000000000000".equals(value)) value = ""
      if (StreamingConstant.EMAIL.equals(key) && StringUtils.isNotBlank(value)) value = value.toLowerCase
      if (StreamingConstant.RECOMMEND.equals(key) && StringUtils.isNotBlank(value) && ((value.length < 5) || value.length > 11)) value = ""

      if (StringUtils.isNotBlank(value)) {
        val modelname = "" + StreamingConstant.labelMap.get(key).get
        val rel = "" + StreamingConstant.relationShipMap.get(key).get
        otherStatementTemplate.append(" MERGE (" + key + ":" + modelname + "{modelname:'" + modelname + "',content:$" + fieldRelation(0) + "}) ON CREATE SET "+key +".modelname='"+modelname+"'")
        otherStatementTemplate.append(" MERGE (apply)-[:" + rel + "]->(" + key + ")")
        applyStatementTemplate.append(",apply." + fieldRelation(0) + "=$" + fieldRelation(0))
        paramMap.put(fieldRelation(0), value)
      }
    }

    val statementStr = applyStatementTemplate.append(otherStatementTemplate).toString
    println(statementStr)
    session.writeTransaction(new TransactionWork[Integer]() {
      override def execute(tx: Transaction): Integer = {
        tx.run(statementStr, paramMap)
        tx.success()
        1
      }
    })
    println("add neo4j sucess...")
  }
}

