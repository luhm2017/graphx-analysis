package com.lakala.datacenter.load.spark

import java.sql.{Connection, DriverManager, Statement}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/5/9 0009.
  * "BankCard", "Device", "Mobile", "Email"
  */
object ExplortApplyData2 {
  private var conf: SparkConf = _
  private var sc: SparkContext = _
  //  private val ip = "jdbc:neo4j:bolt:10.16.65.15:7687"
  private val username = "neo4j"
  private val password = "123456"


  def main(args: Array[String]): Unit = {
    val ip = s"jdbc:neo4j:bolt:${args(0)}:7687"
    setUP

    val map = Map("applymymobile" -> "Mobile", "loanapply" -> "Mobile", "emergencymobile" -> "Mobile", "device" -> "Device", /*"bankcard" -> "BankCard",*/ "identification" -> "Identification", "email" -> "Email")
    val modelRdd = sc.parallelize(List(args(2)))

    val broadcastVar2 = sc.broadcast(map)
    val hc = new HiveContext(sc)
    //    s"1,${rs.getString("n.content")},${arr(1).split("==")(0)},${rs.getString("p.orderno")},${arr(1).split("==")(1)},${rs.getString("m.content")},${arr(1).split("==")(2)},${rs.getString("m.orderno")}"
    val schema = StructType(StructType(Array(
      StructField("degreeType", StringType, true),
      StructField("tagging_edge10", StringType, true),
      StructField("edge10", StringType, true),
      StructField("apply1", StringType, true),
      StructField("edge11", StringType, true),
      StructField("tagging_edge11", StringType, true),
      StructField("edge12", StringType, true),
      StructField("apply2", StringType, true))))

    val degree0 = modelRdd.mapPartitions { models =>
      var con: Connection = DriverManager.getConnection(ip, username, password)
      var stmt: Statement = con.createStatement
      stmt.setQueryTimeout(120)
      models.map { model =>
        runQueryApplyByApplyLevel1(broadcastVar2.value, stmt, model)
      }.flatten
    }.distinct().map { v =>
      var arr = v.split(",")
      Row(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7))
    }.repartition(4)
    hc.sql("use lkl_card_score ")
    val df = hc.createDataFrame(degree0, schema)
    df.registerTempTable("fqz_black_related_data1_tmp")
    hc.sql(" CREATE TABLE IF NOT EXISTS  fqz_black_related_data1  (degreeType STRING, tagging_edge10 STRING,edge10 STRING,apply1 STRING,edge11 STRING,tagging_edge11 STRING,edge12 STRING,apply2 STRING) STORED AS TEXTFILE")
//    hc.sql(" truncate table fqz_black_related_data1 ")

    val indf = hc.sql("INSERT into TABLE fqz_black_related_data1 select degreeType,tagging_edge10,edge10,apply1,edge11,tagging_edge11,edge12,apply2 FROM fqz_black_related_data1_tmp")
    println(s"total 0 degree insert into table fqz_black_related_data1 size  ${indf.count()}")
    closeDown
  }

  def setUP() = {
    //    conf = new SparkConf().setAppName("ExplortApplyData").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", "bolt://10.16.65.15:7687")
    conf = new SparkConf().setMaster("local[1]").setAppName("ExplortApplyData2") /*.set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", "bolt://192.168.0.33:7687")*/
    sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  }

  def closeDown() = {
    sc.stop()
  }


  def runQueryApplyByApplyLevel1(map: Map[String, String], stmt: Statement, modelname: String): List[String] = {
    val sqlList = new ArrayBuffer[String]()
    for (k <- map.keySet) {
      for (k2 <- map.keySet) {
        if (k2.equals("applymymobile") || k2.equals("loanapply") || k2.equals("emergencymobile")) {
          sqlList += s"match (n:$modelname {type:'1'})-[r1:${k}] -(p:ApplyInfo)-[r2:${k2}]-(m:${map.get(k2).get})-[r3:applymymobile]-(q:ApplyInfo) return n.content,p.orderno,m.content,q.orderno@@$k==$k2==applymymobile"
          sqlList += s"match (n:$modelname {type:'1'})-[r1:${k}] -(p:ApplyInfo)-[r2:${k2}]-(m:${map.get(k2).get})-[r3:loanapply]-(q:ApplyInfo) return n.content,p.orderno,m.content,q.orderno@@$k==$k2==loanapply"
          sqlList += s"match (n:$modelname {type:'1'})-[r1:${k}] -(p:ApplyInfo)-[r2:${k2}]-(m:${map.get(k2).get})-[r3:emergencymobile]-(q:ApplyInfo) return n.content,p.orderno,m.content,q.orderno@@$k==$k2=emergencymobile"
        } else {
          sqlList += s"match (n:$modelname {type:'1'})-[r1:${k}] -(p:ApplyInfo)-[r2:${k2}]-(m:${map.get(k2).get})-[r3:${k2}]-(q:ApplyInfo) return n.content,p.orderno,m.content,q.orderno@@$k==$k2==$k2"
        }
      }
    }
    val buff = new ArrayBuffer[String]()
    sqlList.map { sql =>
      val arr = sql.split("@@")
      val rs = stmt.executeQuery(arr(0))
      try {
        val it = while (rs.next) {
          buff += s"1,${rs.getString("n.content")},${arr(1).split("==")(0)},${rs.getString("p.orderno")},${arr(1).split("==")(1)},${rs.getString("m.content")},${arr(1).split("==")(2)},${rs.getString("q.orderno")}"
        }
      } catch {
        case e: Exception => println(s"${e.getMessage} cureent sql ${arr(0)}")
      }
    }
    buff.toList
  }

}
