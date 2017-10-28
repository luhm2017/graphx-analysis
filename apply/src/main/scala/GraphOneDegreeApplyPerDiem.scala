import com.lakala.datacenter.apply.buildGraph.{BuildGraphData, NewEdgeArr}
import com.lakala.datacenter.common.utils.DateTimeUtils
import org.apache.spark.graphx.{EdgeContext, Graph}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import com.lakala.datacenter.utils.UtilsToos

/**
  * Created by linyanshi on 2017/9/1 0001.
  */
object GraphOneDegreeApplyPerDiem {
  def main(args: Array[String]): Unit = {
    val date = args(1).split("-")
    val year = date(0)
    val month = date(1)
    val day = date(2)
    Array(args(0), args(1), year, month, day, args(2))
    val gbsp = new GraphOneDegreeApplyPerDiem(args)
    println(s"start time ${DateTimeUtils.formatter.print(System.currentTimeMillis())}")
    gbsp.runNHopResult()
    println(s"end time ${DateTimeUtils.formatter.print(System.currentTimeMillis())}")


  }

}

class GraphOneDegreeApplyPerDiem(args: Array[String]) extends Serializable {
  val maxNDegVerticesCount = 100
  val maxOutDegree = 100
  val takeN = 100
  @transient
  val conf = new SparkConf().setAppName("GraphOneDegreeApplyPerDiem")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //设置序列化为kryo
  // 容错相关参数
  conf.set("spark.task.maxFailures", "8")
  conf.set("spark.akka.timeout", "500")
  conf.set("spark.yarn.max.executor.failures", "100")
  conf.set("spark.shuffle.consolidateFiles", "true")
  conf.set("spark.speculation", "true")
  conf.set("spark.shuffle.compress", "true")
  conf.set("spark.rdd.compress", "true")
  conf.set("spark.kryoserializer.buffe", "512m")
  conf.registerKryoClasses(Array(classOf[NewEdgeArr]))

  @transient
  val sc = new SparkContext(conf)
  val hc = new HiveContext(sc)

  def runNHopResult(): Unit = {
    var g: Graph[Array[String], NewEdgeArr] = BuildGraphData(hc, Array[String]())
    g = g.outerJoinVertices(g.outDegrees)((_, old, deg) => (old, deg.getOrElse(0)))
      .subgraph(vpred = (_, a) => a._2 < maxOutDegree).mapVertices((_, vdeg) => vdeg._1)

    var preG: Graph[Array[String], NewEdgeArr] = null
    var iterCount = 0
    while (iterCount < args(1).toInt) {
      println(s"iteration $iterCount start .....")
      preG = g

      var tmpG = g.aggregateMessages[Array[String]](sendMsg, merageMsg).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
      g = g.outerJoinVertices(tmpG) { (_, _, updateAtt) => updateAtt.getOrElse(Array[String]()) }
      g.persist(StorageLevel.MEMORY_AND_DISK_SER)
      tmpG.unpersist(blocking = false)
      preG.vertices.unpersist(blocking = false)
      preG.edges.unpersist(blocking = false)
      println(s"iteration $iterCount end .....")
      iterCount += 1
    }
    //************************
    val result = g.vertices
      .mapPartitions(vs => vs.filter(v => v._2.nonEmpty)
        .map(v =>
          Iterator(v._2, v._2.reverse).flatten)).flatMap(l => l)
      .filter(s => s.count(c => c.equals(',')) == args(1).toInt)
      .mapPartitions(ls => ls.map(l => l.replaceAll("&", ",").replaceAll("#", ",")))
    result.distinct().saveAsTextFile(args(5))
  }

  def sendMsg(ctx: EdgeContext[Array[String], NewEdgeArr, Array[String]]): Unit = {
    if (ctx.srcAttr.isEmpty && ctx.dstAttr.isEmpty) {
      ctx.sendToSrc(Array[String](s"${ctx.attr.dstV},${ctx.attr.srcV}&${ctx.attr.srcType}"))
    }

    if (ctx.srcAttr.nonEmpty && ctx.dstAttr.isEmpty) {
      val filterArray = ctx.srcAttr.filter(v => !v.contains(ctx.attr.dstV)).take(takeN)
      val toDstArrayBuffer = new Array[String](filterArray.size)

      for (i <- 0 until (filterArray.size)) {
        toDstArrayBuffer.update(i, s"${filterArray(i)},${ctx.attr.dstV}")
      }

      ctx.sendToDst(toDstArrayBuffer)
      ctx.sendToSrc(Array[String]())
    }

    if (ctx.srcAttr.isEmpty && ctx.dstAttr.nonEmpty) {
      val filterArray = ctx.dstAttr.filter(v => !v.contains(s"${ctx.attr.dstV},${ctx.attr.srcV}&${ctx.attr.srcType}") /* && !v.contains(s"${ctx.attr.srcV}&${ctx.attr.srcType},${ctx.attr.dstV}")*/).take(takeN)

      val toSrcArrayBuffer = new Array[String](filterArray.size)
      for (i <- 0 until (filterArray.size)) {
        toSrcArrayBuffer.update(i, s"${filterArray(i)},${ctx.attr.srcV}&${ctx.attr.srcType}")
      }
      ctx.sendToSrc(toSrcArrayBuffer)
      ctx.sendToDst(Array[String]())
    }

  }

  def merageMsg = (msgAarr: Array[String], msgBarr: Array[String]) => {
    msgAarr.union(msgBarr).distinct.take(takeN)
  }
}