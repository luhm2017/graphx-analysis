package org.neo4j.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/5/9 0009.
  *
  */
object ExplortApplyDataTest {
  def main(args: Array[String]): Unit = {
//    ExplortApplyData2.main(Array("192.168.0.33","file:///F:/output/out","BankCard,Device,Mobile",Email"))
    val conf = new SparkConf().setMaster("local[1]").setAppName("test")
    val  sc = new SparkContext(conf)
    printSql(sc)
    println(System.getProperty("java.io.tmpdir"))
  }
  def printSql(sc:SparkContext)={
    val map = Map("applymymobile" -> "Mobile","loanapply" -> "Mobile","emergencymobile" -> "Mobile", "device" -> "Device", "bankcard" -> "BankCard", "identification" -> "Identification", "email" -> "Email")
    val modelRdd = sc.parallelize(List("BankCard", "Device", "Mobile", "Email"))

    val broadcastVar2 = sc.broadcast(map)
    modelRdd.foreachPartition { models =>
      models.foreach { model =>
        runQueryApplyByApplyLevel1(broadcastVar2.value, model)
      }
    }
  }
  def runQueryApplyByApplyLevel1(map: Map[String, String],modelname: String):Unit = {

    val list = new ArrayBuffer[String]()
    for (k <- map.keySet) {
      for (k2 <- map.keySet) {
        if (k2.equals("applymymobile") || k2.equals("loanapply") || k2.equals("emergencymobile")) {
          list += s"match (n:$modelname {type:'1'})-[r1:${k}] -(p:ApplyInfo)-[r2:${k2}]-(m:${map.get(k2).get})-[r3:applymymobile]-(q:ApplyInfo) return n.content,p.orderno,m.content,q.orderno@@$k==$k2==applymymobile"
          list += s"match (n:$modelname {type:'1'})-[r1:${k}] -(p:ApplyInfo)-[r2:${k2}]-(m:${map.get(k2).get})-[r3:loanapply]-(q:ApplyInfo) return n.content,p.orderno,m.content,q.orderno@@$k==$k2==loanapply"
          list += s"match (n:$modelname {type:'1'})-[r1:${k}] -(p:ApplyInfo)-[r2:${k2}]-(m:${map.get(k2).get})-[r3:emergencymobile]-(q:ApplyInfo) return n.content,p.orderno,m.content,q.orderno@@$k==$k2=emergencymobile"
        } else {
          list += s"match (n:$modelname {type:'1'})-[r1:${k}] -(p:ApplyInfo)-[r2:${k2}]-(m:${map.get(k2).get})-[r3:${k2}]-(q:ApplyInfo) return n.content,p.orderno,m.content,q.orderno@@$k==$k2==$k2"
        }
      }
    }
    list.map { sql =>
      val arr = sql.split("@@")
      println(arr(0))
    }
  }
}
