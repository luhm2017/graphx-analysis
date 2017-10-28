
import com.lakala.datacenter.apply.model.ApplyInfo
import com.lakala.datacenter.core.utils.UtilsToos._
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ASUS-PC on 2017/4/12.
  */
object RunGraphx {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(RunGraphx.getClass.getName)
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val lineRdd: RDD[String] = sc.textFile("file:///F:/lakalaFinance_workspaces/graphx-analysis/apply/data2/part-00000")
    //**********************************test**************************************
    //   lineRdd.mapPartitions { lines =>
    //      lines.map { line =>
    //        var arr = line.split("\u0001")
    //                (s"${arr(1)}${arr(2)}${arr(4)}${arr(7)}${arr(10)}${arr(15)}${arr(41)}")
    ////        (s"${arr(1)}")
    //      }
    //    }.repartition(1).saveAsTextFile("file:///F:/lakalaFinance_workspaces/graphx-analysis/apply/data2")
    //    println(s"total orderid @@${filterLineRdd.count()}")
    //    lineRdd.mapPartitions { lines =>
    //      lines.map { line =>
    //        var arr = line.split("\u0001")
    //        (s"${arr(0)},${hashId(arr(0))},${arr(1)},${hashId(arr(1))},${arr(2)},${hashId(arr(2))},${arr(3)},${hashId(arr(3))},${arr(4)},${hashId(arr(4))},${arr(5)},${hashId(arr(5))},${arr(6)},${hashId(arr(6))}")
    //      }
    //    }.repartition(1).saveAsTextFile("file:///F:/lakalaFinance_workspaces/graphx-analysis/apply/data3")
    //顶点的数据类型是VD:(Int,String)
    //过滤出想要的字段值[order_id{}，contract_no，business_no，term_id {}，loan_pan，return_pan{}，empmobile{}]
    val vertexsAndEdgesRdd: RDD[((Long, ApplyInfo),
      (Long, ApplyInfo), (Long, ApplyInfo), (Long, ApplyInfo), (Edge[String]), (Edge[String]), (Edge[String]))] =
    lineRdd.mapPartitions { lines =>
      lines.map { line =>
        var arr: Array[String] = line.split("\u0001")
        ((hashId(arr(0)), new ApplyInfo(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6))),
          (hashId(arr(3)), new ApplyInfo("", "", "", arr(3), "", "", "")),
          (hashId(arr(5)), new ApplyInfo("", "", "", "", "", arr(5), "")),
          (hashId(arr(6)), new ApplyInfo("", "", "", "", "", "", arr(6))),
          //创建枚举类
          (Edge(hashId(arr(0)), hashId(arr(3)), "term_id")),
          (Edge(hashId(arr(0)), hashId(arr(5)), "return_pan")),
          (Edge(hashId(arr(0)), hashId(arr(6)), "empmobile"))
          )
      }
    }
    //rout table ver -edge
    val vertex1: RDD[(Long, ApplyInfo)] = vertexsAndEdgesRdd.map(ve => ve._1)
    val vertex2: RDD[(Long, ApplyInfo)] = vertexsAndEdgesRdd.map(ve => ve._2)
    val vertex3: RDD[(Long, ApplyInfo)] = vertexsAndEdgesRdd.map(ve => ve._3)
    val vertex4: RDD[(Long, ApplyInfo)] = vertexsAndEdgesRdd.map(ve => ve._4)
    //
    val edge1: RDD[Edge[String]] = vertexsAndEdgesRdd.map(ve => ve._5)
    val edge2: RDD[Edge[String]] = vertexsAndEdgesRdd.map(ve => ve._6)
    val edge3: RDD[Edge[String]] = vertexsAndEdgesRdd.map(ve => ve._7)

    val disVertex = vertex1.union(vertex2).union(vertex3).union(vertex4).reduceByKey((x, y) => x)

    val distEdges = edge1.union(edge2).union(edge3).distinct()
    val applyGraph: Graph[ApplyInfo, String] = Graph(disVertex, distEdges)

    //创建图时候需要可能初始化默认的属性值,否则在结构化操作遇到空的指针问题
    //    val applyArrtsGraph: Graph[(String, String, String, String, String, String, String), String]
    //    = Graph(vertex1, unionEdges, ("John Doe", "Missing", "John Doe1", "John Doe2", "0", "John Doe4", "John Doe5"))
    //    val applyArrtGraph: Graph[String, String] = Graph(vertex2.union(vertex3).union(vertex4), unionEdges)
    //    applyArrtsGraph.cache()
    //过滤出想要的字段值[order_id{}，contract_no，business_no，term_id {}，loan_pan，return_pan{}，empmobile{}]
    println("**********************************************************")
    println("找出图中loan_pan==1222的顶点：")
    applyGraph.vertices.filter {
      case (id, apply)
      => "1222".equals(apply.loan_pan)
    }.collect().foreach {
      case (id, apply) =>
        println(apply.toString)
    }
    println
    applyGraph.vertices.collect().foreach { v =>
      println(s"vertices=${v._1},property=${v._2}")
    }
    println
    //边操作：找出图中属性等于empmobile的边
    println("找出图中属性等于empmobile的边：")
    applyGraph.edges.filter(e => e.attr.equals("empmobile")).collect().foreach(e => println(s"${e.srcId} to ${e.dstId} arr ${e.attr}"))
    println
    //triplets操作，((srcId, srcAttr), (dstId, dstAttr), attr)
    println("列出边属性等于empmobile的tripltes：")
    for (triplet <- applyGraph.triplets.filter(t => t != null && t.attr.equals("empmobile")).collect()) {
      if (triplet.srcAttr != null) {
        println(s"${triplet.srcAttr.empmobile} like ${triplet.dstAttr.empmobile}")
      }
    }
    println
    //Degrees操作
    println("找出图中最大的出度、入度、度数：")

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    println(s"max of outDegrees: ${applyGraph.outDegrees.reduce((a, b) => max(a, b))} max of inDegrees:${applyGraph.inDegrees.reduce(max)} max of Degrees:${applyGraph.degrees.reduce(max)}")
    println
    println("**********************************************************")
    println("转换操作")
    println("**********************************************************")
    println("顶点的转换操作，顶点第二个属性contract_no全部替换成001address：")

    val tmpGraph: Graph[ApplyInfo, String] = applyGraph.mapVertices[ApplyInfo] {
      case (id, applyInfo)
        //    =>(id,(order_id, contract_no, business_no, term_id, loan_pan, return_pan, empmobile))
      => {
        applyInfo.contract_no = "001address"
        applyInfo
      }
    }
    tmpGraph.vertices.collect().foreach(v => println(s"${v._1} is'arrts'${v._2.toString}"))

    println
    println("边的转换操作，边的属性追加上：apply")
    applyGraph.mapEdges(e => s"${e.attr}Apply").edges.collect().foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println
    println("**********************************************************")
    println("结构操作")
    println("**********************************************************")
    println("顶点business_no==BID的子图：")
    val subGraph = applyGraph.subgraph(vpred = (id, applyInfo) => applyInfo.business_no.equals("BID"))
    println
    println("子图所有顶点：")
    subGraph.vertices.collect().foreach(v => println(v._2.toString()))
    subGraph.edges.collect().foreach(e => println(s"${e.srcId} to ${e.dstId} attrs ${e.attr}"))
    println
    println("**********************************************************")
    println("连接操作")
    println("**********************************************************")
    val inDegrees: VertexRDD[Int] = applyGraph.inDegrees
    val degreeApplyGraph = applyGraph.outerJoinVertices(inDegrees) {
      case (id, apply, inDegOpt) => {
        apply.inDeg = inDegOpt.getOrElse(0)
        apply
      }
    }.outerJoinVertices(applyGraph.outDegrees) {
      case (id, apply, outDegOpt) => {
        apply.outDeg = outDegOpt.getOrElse(0)
        apply
      }
    }

    println("连接图的属性：")
    degreeApplyGraph.vertices.collect().foreach(v => println(s"${v._2.toString} inDeg ${v._2.inDeg} outDeg ${v._2.outDeg}"))
    println
    println("出度和入读相同的进件：")
    degreeApplyGraph.vertices.filter { case (id, apply) => apply.inDeg == apply.outDeg }.collect().foreach { case (id, apply) => println(apply.toString) }

    println
    //***********************************************************************************
    //***************************  聚合操作    ****************************************
    //**********************************************************************************
    println("**********************************************************")
    println("聚合操作")
    println("**********************************************************")
    println("找出年纪最大的进件者：")
    //定义一个相邻聚合，统计empmobile不为空的contract_no（count）及其平均contract_no数量（totalContract_no/count)
    val contractNos= degreeApplyGraph.aggregateMessages[(Int,Int)](
      //方括号内的元组(Int,Int)是函数返回值的类型，也就是Reduce函数（mergeMsg )右侧得到的值（count，totalContract_no）
      triplet =>{
        if(StringUtils.isNotBlank(triplet.srcAttr.empmobile)&&StringUtils.isNotBlank(triplet.dstAttr.empmobile)){
          triplet.sendToDst((1,1))
        }
      },//(1)--函数左侧是边三元组，也就是对边三元组进行操作，有两种发送方式sendToSrc和 sendToDst
      (a,b)=>(a._1+b._1,a._2+b._2),//(2)相当于Reduce函数，a，b各代表一个元组（count，totalContract_no）
      //对count和Age不断相加（reduce），最终得到总的count和totalContract_no
      TripletFields.All)//(3)可选项,TripletFields.All/Src/Dst
    contractNos.collect().foreach(println)
    //计算出每个手机号它的合同数量和平均合同数
    val empmobileGraph = contractNos.mapValues((id,value)=>value match {
      case (count,totalConNo)=>(count,totalConNo/count)
    })
//
    degreeApplyGraph.vertices.leftJoin(empmobileGraph) {
      case(id,apply,optEmpmoblie) => optEmpmoblie match {
        case None=>
        case Some((count,avg)) =>s"totalContract_no  is ${count} avgContract ${avg} of ${apply.empmobile}."
      }
    }.collect().foreach(v=>println(v._2))

    println
    println("**********************************************************")
    println("聚合操作")
    println("**********************************************************")
    println("找出15814645811到各顶点的最短：")
    val sourceId = hashId("15814645811")  // 定义源点
    val initialGraph = applyGraph.mapVertices((id,_)=>if(id==sourceId) 0.0 else Double.PositiveInfinity)
    val sssp= initialGraph.pregel(Double.PositiveInfinity) (
      (id,dist,newDist)=> math.min(dist,newDist),
      // 计算权重
      triplet =>{
        if(triplet.srcAttr+hashId(triplet.attr)<triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + hashId(triplet.attr)))
        } else {
          Iterator.empty
        }
      },
      (a,b)=>math.min(a,b) // 最短距离
    )
    println(sssp.vertices.collect.mkString("\n"))
  }

}
