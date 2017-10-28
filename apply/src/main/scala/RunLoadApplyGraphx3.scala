import com.lakala.datacenter.apply.model.NDegreeEntity
import com.lakala.datacenter.grograms.ApplyDegreeCentralityProgram
import com.lakala.datacenter.utils.UtilsToos._
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ASUS-PC on 2017/4/18.
  */
object RunLoadApplyGraphx3 {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case 0 => "local[2]"
      case _ => "spark://datacenter17:7077,datacenter18:7077"
    }
    val conf = new SparkConf().setMaster(master).setAppName("RunLoadApplyGraphx")
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val degree = 3 //这里是二跳邻居 所以只需要定义为2即可
    val applyPath = "file:///F:/lakalaFinance_workspaces/applogs/query_result.csv"
    val callPath = "file:///F:/lakalaFinance_workspaces/graphx-analysis/apply/data3/part-00003"
    //    val applyPath = "/user/linyanshi/query_result.csv"
    //    val callPath = "/user/linyanshi/part-00003"

    val applyVertexEdge = sc.textFile(applyPath).mapPartitions(lines =>
      lines.map { line =>
        var arr = line.split(",")
        val term_id = if (StringUtils.isNotBlank(arr(7)) && !"null".equals(arr(7).toLowerCase)) arr(7) else "OL"
        val return_pan = if (StringUtils.isNotBlank(arr(16)) && !"null".equals(arr(16).toLowerCase)) arr(16) else "0L"
        val empmobile = if (StringUtils.isNotBlank(arr(41)) && !"null".equals(arr(41).toLowerCase)) arr(41) else "0L"

        (Iterator((hashId(arr(1)), arr(1)),
          //          (hashId(term_id), term_id),
          //          (hashId(return_pan), return_pan),
          (empmobile.toLong, empmobile)),
          //创建枚举类
          Iterator(
            //            (Edge(hashId(arr(1)), hashId(term_id), 0)),
            //            (Edge(hashId(term_id), hashId(arr(1)), 0)),
            //            (Edge(hashId(arr(1)), hashId(arr(1)), 0)),
            //            (Edge(hashId(return_pan), hashId(return_pan), 0)),
            (Edge(hashId(arr(1)), empmobile.toLong, 0)),
            (Edge(empmobile.toLong, hashId(arr(1)), 0))
          )
        )
      }
    )

    val callVertexEdge: RDD[(Iterable[(Long, String)], Edge[Int])] = sc.textFile(callPath).mapPartitions { lines =>
      lines.map { line =>
        var arr = line.split(" ")
        (Iterable((arr(0).toLong, arr(0)), (arr(1).toLong, arr(1))), (Edge(arr(0).toLong, arr(1).toLong, 0)))
      }
    }

    var applyVertexRDD: RDD[(Long, String)] = applyVertexEdge.flatMap(k => k._1).distinct()
    var applyEdgeRdd = applyVertexEdge.flatMap(k => k._2).filter(k => k.srcId != 0L && k.dstId != 0L)

    var callVertexRDD: RDD[(Long, String)] = callVertexEdge.flatMap(k => k._1).filter(k => k._1 != 0L).distinct()
    var callEdgeRdd = callVertexEdge.filter(k => k._2.srcId != 0L && k._2.dstId != 0L).map(k => k._2)

    println("**********************************************************")

    println(s" total vertex num ${(applyVertexRDD ++ callVertexRDD).count()} total edge num ${(applyEdgeRdd ++ callEdgeRdd).count()}")
    val g: Graph[String, Int] = Graph(applyVertexRDD ++ callVertexRDD, applyEdgeRdd ++ callEdgeRdd)
    //            val g: Graph[String, Int] = Graph(applyVertexRDD, applyEdgeRdd)
    //    val g: Graph[String, Int] = Graph(callVertexRDD, callEdgeRdd)
    //黑名单
    //    val blackApply = sc.parallelize(Seq(hashId("TNA20170417141529012640002847101"), hashId("TNA20170417143735006484055517796")))
    //    val blackApply = sc.parallelize(Seq(13867764858L, 13981960691L))
    val blackApply = sc.parallelize(Seq(13867764858L, 13981960691L))
    val initVertex = blackApply.map(v => (v, 1))
    var newG: Graph[Map[VertexId, NDegreeEntity], Int] = g.outerJoinVertices(initVertex) { case (vid, oldAttr, newAttr) => Map[VertexId, NDegreeEntity](vid -> new NDegreeEntity(oldAttr, initType = newAttr.getOrElse(0), loop = degree)) }
    //        var newG = g.mapVertices((vid, att) => Map[VertexId, NDegreeEntity](vid -> new NDegreeEntity(att, loop = degree)))

    val graphResult = new ApplyDegreeCentralityProgram(newG).run(degree, EdgeDirection.Out)

    val degreeJumpRdd = graphResult.vertices.mapValues(_.filter(_._2.loop == 0)).mapPartitions { vs =>
      vs.map { v =>
        val values = v._2.filter(_._2.loop == 0)
        val forRs = for (v <- values; if (v._2.loop == 0 && (v._2.initType == 1))) yield (v._2.attr)
        //        val forRs = for (v <- values; if (v._2.loop == 0)) yield (v._2.attr)
        forRs
      }
    }.flatMap(k => k).sortBy(s => s)

    println("============================")
    println("total num " + degreeJumpRdd.count())
    degreeJumpRdd.collect().foreach(rs => println("result: " + rs))
    println("************* end ***************")
    sc.stop()
  }

}
