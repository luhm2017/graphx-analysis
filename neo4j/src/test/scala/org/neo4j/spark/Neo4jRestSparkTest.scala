package org.neo4j.spark

import com.lakala.datacenter.load.spark.Neo4j
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/11 0011.
  */
object Neo4jRestSparkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("neoTest").setMaster("local[2]")
    /*.set("spark.neo4j.bolt.url","jdbc:neo4j:bolt:192.168.0.33:7687")*//*.set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", server.boltURI.toString)*/
    val sc = new SparkContext(conf)
    runCypherRelQueryWithPartition(sc)
  }

  def runCypherRelQueryWithPartition(sc: SparkContext) {
    val neo4j: Neo4j = Neo4j(sc).cypher("match (n:Mobile {type:'1'})-[r1:loanapply] -(p:ApplyInfo)-[r2:loanapply]-(m:Mobile)-[r3:loanapply]-(q:ApplyInfo) return n.content as content1 ,type(r1) as value1,p.orderno as orderno1,type(r2) as value2,m.content as content2,type(r3) as value3,q.orderno as orderno2 ").partitions(7).batch(200)
    val knows: Long = neo4j.loadRowRdd.count()
    println(knows)
  }
}
