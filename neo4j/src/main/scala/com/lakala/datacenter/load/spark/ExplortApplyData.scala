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
  */
object ExplortApplyData {
  private var conf: SparkConf = _
  private var sc: SparkContext = _
  //  private val ip = "jdbc:neo4j:bolt:10.16.65.15:7687"
  private val username = "neo4j"
  private val password = "123456"

  def main(args: Array[String]): Unit = {
    val ip = s"jdbc:neo4j:bolt:${args(0)}:7687"
    setUP
    val modelRdd = sc.parallelize(List("ApplyInfo", "BankCard", "Device", "Mobile", "MobileIMEI", "Terminal", "Email", "Identification"))

    val broadcastVar = sc.broadcast(List("applymymobile", "loanapply", "emergencymobile", "device", "bankcard", "identification", "email"))
    val hc = new HiveContext(sc)
    val schema = StructType(StructType(Array(
      StructField("degreeType", StringType, true),
      StructField("tagging_edge", StringType, true),
      StructField("edge0", StringType, true),
      StructField("apply0", StringType, true))))

    val degree0 = modelRdd.mapPartitions { models =>
      var con: Connection = DriverManager.getConnection(ip, username, password)
      var stmt: Statement = con.createStatement
      stmt.setQueryTimeout(120)
      models.map { model =>
        broadcastVar.value.map(relV => runQueryApplyByApplyLevel0(stmt, model, relV))
      }.flatten
    }.flatMap(k => k).distinct()
      .map { v =>
        var arr: Array[String] = v.split(",")
        Row(arr(0), arr(1), arr(2), arr(3))
      }
      .repartition(4)
    val df = hc.createDataFrame(degree0, schema)
    df.registerTempTable("fqz_black_related_data0_tmp")

    //    degree0.saveAsTextFile(args(1))

    hc.sql("use lkl_card_score ")
    //    hc.sql(" drop table fqz_black_related_data0")
    hc.sql(" CREATE TABLE IF NOT EXISTS  fqz_black_related_data0 (degreeType STRING, tagging_edge STRING,edge0 STRING,apply0 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    hc.sql(" truncate table fqz_black_related_data0 ")
    //    hc.sql("select degreeType,tagging_edge,edge0,apply0 FROM fqz_black_related_data0_tmp").foreach(row =>
    //      println((row.getAs[String]("degreeType") + "," + row.getAs[String]("tagging_edge") + "," + row.getAs[String]("edge0") + "," + row.getAs[String]("apply0")))
    //    )
    val indf = hc.sql("INSERT OVERWRITE TABLE fqz_black_related_data0 select degreeType,tagging_edge,edge0,apply0 FROM fqz_black_related_data0_tmp")
    println(s"total 0 degree insert into table fqz_black_related_data0 size  ${indf.count()}")
    //        hc.sql(s"LOAD DATA LOCAL INPATH ${args(1)} INTO TABLE fqz_black_related_data0;")
    println(degree0.count())

    closeDown
  }

  def setUP() = {
    //    conf = new SparkConf().setAppName("ExplortApplyData").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", "bolt://10.16.65.15:7687")
    conf = new SparkConf().setMaster("local[1]").setAppName("ExplortApplyData") /*.set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", "bolt://192.168.0.33:7687")*/
    sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  }

  def closeDown() = {
    sc.stop()
  }


  def runQueryApplyByApplyLevel0(stmt: Statement, modelname: String, relation: String): List[String] = {
    val rs = stmt.executeQuery("match (n:" + modelname + " {type:'1'})-[r:" + relation + "]-(m:ApplyInfo) return n.content,r,m.orderno")
    val buff = new ArrayBuffer[String]()
    val it = while (rs.next) {
      //      println(s"0,${rs.getString("n.content")},$relation,${rs.getString("m.orderno")}")
      buff += s"0,${rs.getString("n.content")},$relation,${rs.getString("m.orderno")}"
    }
    buff.toList
  }

}
