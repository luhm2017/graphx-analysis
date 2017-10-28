import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import utils.GraphNdegUtil2

/**
  * Created by ASUS-PC on 2017/4/19.
  */
object EdgeTuplesTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("CreateApplyData")
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//    val orderMobile = GraphLoader.edgeListFile(sc, "file:///F:/lakalaFinance_workspaces/graphx-analysis/apply/data3/part-00003")
    val orderMobile = GraphLoader.edgeListFile(sc, "file:///F:/lakalaFinance_workspaces/graphx-analysis/apply/data/friends.txt")
    //    val orderMobile = sc.textFile("file:///F:/lakalaFinance_workspaces/graphx-analysis/apply/data3/part-00000")
//    val edgeTuple = orderMobile.mapPartitions { lines =>
//      lines.map { line =>
//        val arr = line.split(",")
//        (arr(0).toLong, arr(1).toLong)
//      }
//    }

    val validGraph = orderMobile.subgraph(k => k.srcId != 0 && k.dstId != 0)
//    val choiceRdd = sc.parallelize(Seq(18028726374L, 18692892122L, 13761981426L))
    val choiceRdd = sc.parallelize(Seq(6L))

    val rss: VertexRDD[Map[Int, Set[VertexId]]] = GraphNdegUtil2.aggNdegreedVertices(validGraph, choiceRdd, 3)
   println("00000++++++0000000")
    rss.foreach { k =>
      println(s"${k._1}${k._2.map(kk => k._2.map(kkk => kkk._2.toArray.mkString(",")))}")
    }

    //    val applyLine = sc.textFile("file:///F:/lakalaFinance_workspaces/applogs/query_result.csv").filter(line => (!line.startsWith("s_c_loan_apply")))
    //    val rs = applyLine.mapPartitions { lines =>
    //      lines.map { line =>
    //        val arr = line.split(",")
    //        val term_id = if (StringUtils.isNotBlank(arr(7)) && !"null".equals(arr(7).toLowerCase)) arr(7) else "OL"
    //        val return_pan = if (StringUtils.isNotBlank(arr(16)) && !"null".equals(arr(16).toLowerCase)) arr(16) else "0L"
    //        val empmobile = if (StringUtils.isNotBlank(arr(41)) && !"null".equals(arr(41).toLowerCase)) arr(41) else "0L"
    //        ((s"${hashId(arr(1))},${hashId(term_id)}"), (s"${hashId(arr(1))},${hashId(return_pan)}"), (s"${hashId(arr(1))},${empmobile}"))
    //      }
    //    }
    //    val edge1: RDD[String] = rs.map(ve => ve._1).filter(k => !k.endsWith("," + hashId("0L")))
    //    val edge2: RDD[String] = rs.map(ve => ve._2).filter(k => !k.endsWith("," + hashId("0L")))
    //    val edge3: RDD[String] = rs.map(ve => ve._3).filter(k => !k.endsWith(",0L"))
    //    edge1.union(edge2).union(edge3).repartition(1).saveAsTextFile("file:///F:/lakalaFinance_workspaces/graphx-analysis/apply/data3")
  }
}
