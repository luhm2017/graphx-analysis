import java.io.File

import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/27 0027.
  */
object ApplyPageRank {
  var args: Array[String] = null

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ApplyPageRank")
    val sc = new SparkContext(conf)
    // Control the log level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    if (sc.isLocal) FileUtils.deleteDirectory(new File(args(1)))

    this.args = args
    val runFull = args(2)
    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)
    val edges = sc.textFile(args(0)).filter(filterLine).mapPartitions(lines =>
      lines.map { line =>
        //13528613969&13->XNA20161003231031321222->117.136.39.230&18->XNA20160922130907636777
        val arr = line.replaceAll("&", "->").replaceAll("-", "").split(">")
        //13528613969->13->XNA20161003231031321222->117.136.39.230->18->XNA20160922130907636777
        Edge(arr(0).toLong, arr(3).toLong, 1)
      })
    println("size++" + edges.count())
    val graph: Graph[Int, Int] = Graph.fromEdges(edges, 1, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
      .partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((a, b) => a + b)
    val graphVec1: RDD[Double] = createCombinedFeatureVec(graph, sqlContext, runFull.toBoolean)
    graph.unpersist(blocking = true)
    println("FingerPrinting Complete")

  }

  def createCombinedFeatureVec(g: Graph[Int, Int], sdf: SQLContext, runFull: Boolean, saveFP: Boolean = false): RDD[Double] = {
    //将图传给提取特征向量的方法,结果保存到磁盘
    // Pass the graph to the feature extract methods and save results to disk
    //numNodes numEdges numComp NumVerticesInLCC PercentageInLCC maxDeg avgDeg Tri
    val gFeatures: DataFrame = extractGlobalFeatures(g, sdf, runFull)

    //中位数,标准偏差,最小值,最大值,偏度,峰值,方差
    val vFeatures: DataFrame = extractVertexFeatures(g, sdf)
    gFeatures.cache()
    vFeatures.cache()
    val totalFeatures: DataFrame = gFeatures.join(vFeatures)
    val dist = canberraDist(sdf.sparkContext, gFeatures.rdd.map(x => x.toSeq.map(y => y.asInstanceOf[Double])).flatMap(z => z), vFeatures.rdd.map(x => x.toSeq.map(y => y.asInstanceOf[Double])).flatMap(z => z))
    println("Total Canberra Distance Between Graphs = "+dist)
    val totalFeaturesVec: RDD[Double] = totalFeatures.rdd.map { x => x.toSeq.map { y => y.asInstanceOf[Double] } }.flatMap { z => z }

    // Save to disk if passed
    //"GraphFingerPrint.fp"
    totalFeaturesVec.cache()
    if (saveFP) (totalFeatures.rdd.coalesce(1).saveAsTextFile(args(1)))
    totalFeaturesVec.unpersist(true)
    gFeatures.unpersist()
    vFeatures.unpersist()
    return totalFeaturesVec
  }

  def extractVertexFeatures(g: Graph[Int, Int], sdf: SQLContext): DataFrame = {

    // Page Rank RDD
    //用pagerank算法 求顶点的pagerank得分值
    val pagerankGraph: Graph[Double, Double] = g.pageRank(0.00015)
    val pageRankRDD: VertexRDD[Double] = pagerankGraph.vertices

    // Total Degree RDD
    //每个顶点的总的度数
    val degreeRDD: VertexRDD[Int] = g.degrees

    // Local Clustering Score
    //求出 局部聚类分
    val clusteringGraph: Graph[Double, Int] = g.localClustering(VertexMeasureConfiguration(treatAsUndirected = true))
    val localCentralityRDD: VertexRDD[Double] = clusteringGraph.vertices

    // Eigen Vector Score
    //求出 特征向量的忠心度
    val eigenvectorRDD: VertexRDD[Double] = g.eigenvectorCentrality(VertexMeasureConfiguration(treatAsUndirected = true)).vertices
    val eigenVecDF: DataFrame = featureCreation(eigenvectorRDD, sdf)
    eigenvectorRDD.unpersist(blocking = true)
    //求节点属性是度数的图
    val degreeGraph: Graph[Int, Int] = g.outerJoinVertices(degreeRDD) { (id, oldAttr, degOpt) =>
      degOpt match {
        case Some(deg) => deg
        case None => 0 // No outDegree means zero outDegree
      }
    }

    // Calculte the number of two hop away neigbours
    //求出 两跳的邻居数
    val twoHopAway: VertexRDD[Double] = degreeGraph.aggregateMessages[Double](
      triplet => { // Map Function
        // Send the degree of each vertex to the reduce function. Not sure if both things are needed here.
        //将每个顶点的度数发送到reduce 函数,但不能确定都需要这个步骤
        triplet.sendToDst(triplet.srcAttr)
        triplet.sendToSrc(triplet.dstAttr)
      },
      // Sum the degree values
      //求和
      (a, b) => (a + b) // Reduce Function
    )
    //求出 中位数,标准偏差,最小值,最大值,偏度,峰值,方差
    val twoHopDF: DataFrame = featureCreation(twoHopAway, sdf)
    val degreeDF: DataFrame = featureCreation(degreeRDD.mapValues(x => x.toDouble), sdf)

    // Calculate the average clustering score of the each
    //求出图每个顶点局部中心度的次数和总的中心度和
    val totalClusteringScore: VertexRDD[(Int, Double)] = clusteringGraph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        // Send the local clustering score and a counter to get the mean
        //发送本地聚类得分和计数器获得平均值
        triplet.sendToDst(1, triplet.srcAttr)
        triplet.sendToSrc(1, triplet.dstAttr)
      },
      // Add the local clustering scores
      //添加本地聚类得分
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )

    // Compute the average clustering for each vertex
    //计算每个顶点的平均聚类
    val averageNeibourhoodClustering: VertexRDD[Double] = totalClusteringScore.mapValues(
      (id, value) => value match {
        case (count, clusteringScore) => clusteringScore / count
      })

    // Create the dataframe for the local centrality graphs and rdd then clear
    //求出图每个顶点局部中心度的聚合后顶点信息(中位数,标准偏差,最小值,最大值,偏度,峰值,方差)
    //求出图每个顶点平均局部中心度聚合后顶点信息(中位数,标准偏差,最小值,最大值,偏度,峰值,方差)
    val localCentDF: DataFrame = featureCreation(localCentralityRDD, sdf)
    val avgNeibDF: DataFrame = featureCreation(averageNeibourhoodClustering, sdf)
    clusteringGraph.unpersist(blocking = true)


    // Calculate the average pagerank score of the each
    //求出 pagerank得分图的每个顶点的次数和pagerank得分值
    val totalPagerankScore: VertexRDD[(Int, Double)] = pagerankGraph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        // Send the local clustering score and a counter to get the mean
        triplet.sendToDst(1, triplet.srcAttr)
        triplet.sendToSrc(1, triplet.dstAttr)
      },
      // Add the pagerank scores
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )

    // Compute the average pagerank for each vertex
    //求出 pagerank得分图的平均pagerank得分值
    val averagePagerankScore: VertexRDD[Double] = totalPagerankScore.mapValues(
      (id, value) => value match {
        case (count, clusteringScore) => clusteringScore / count
      })
    //求出pagerank得分图的每个顶点的聚合后顶点信息(中位数,标准偏差,最小值,最大值,偏度,峰值,方差)
    val PageRankDF: DataFrame = featureCreation(pageRankRDD, sdf)
    //求出平均pagerank得分图的每个顶点的聚合后顶点信息(中位数,标准偏差,最小值,最大值,偏度,峰值,方差)
    val avPageRankDF: DataFrame = featureCreation(averagePagerankScore, sdf)
    pagerankGraph.unpersist(blocking = true)

    // Compute the final joined vertex feature RDD and return
    //用所有的节点特征join起来 并返回
    val vertexFeatures: DataFrame = PageRankDF.join(avPageRankDF).join(avgNeibDF).join(twoHopDF).join(eigenVecDF).join(localCentDF).join(degreeDF)

    return vertexFeatures
  }

  /**
    * 提取全局特征向量,包含有 numNodes, numEdges, maxDeg, avgDeg
    * 如果是跑全量数据还包含 numComp, NumVerticesInLCC, PercentageInLCC,Tri
    *
    * @param g
    * @param sdfa
    * @param RunFull
    * @return
    */
  def extractGlobalFeatures(g: Graph[Int, Int], sdfa: SQLContext, RunFull: Boolean = false): DataFrame = {

    import sdfa.implicits._

    // Count number of edges and vertices
    //计算图的顶点边数和节点数
    val numEdges: Long = g.numEdges
    val numNodes: Long = g.numVertices

    // Compute the maximum and average degree
    //计算图的顶点最大度数和平均数
    val maxDeg: Int = g.degrees.reduce(max)._2
    val avgDeg: Double = g.degrees.map(_._2).sum / numNodes

    // Check if all features are needed
    if (RunFull) {
      println("Runing Full Global Extraction")
      // Number of triangles in graph
      //求出三角形的个数
      val numTriangles: Double = g.triangleCount().vertices.map(x => x._2).sum() / 3

      // Connected components
      //图的联通组件的个数
      val Components: VertexRDD[VertexId] = g.connectedComponents().vertices.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val numComp: Long = Components.map(x => x._2).distinct().count()
      val NumVerticesInLCC: Int = Components.map(x => (x._2, 1)).reduceByKey(_ + _).map(x => x._2).max()
      val PercentageInLCC: Double = (NumVerticesInLCC / numNodes) * 100.0
      Components.unpersist(blocking = true)

      val gFeats = Seq((numNodes.toDouble, numEdges.toDouble, numComp.toDouble, NumVerticesInLCC.toDouble, PercentageInLCC.toDouble, maxDeg.toDouble, avgDeg.toDouble, numTriangles))
        .toDF("numNodes", "numEdges", "numComp", "NumVerticesInLCC", "PercentageInLCC", "maxDeg", "avgDeg", "Tri")

      return gFeats
    }
    else {
      println("Runing  Global Extraction")
      val gFeats = Seq((numNodes.toDouble, numEdges.toDouble, maxDeg.toDouble, avgDeg.toDouble))
        .toDF("numNodes", "numEdges", "maxDeg", "avgDeg")

      return gFeats
    }
  }

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }


  /**
    * 用聚合顶点信息来创建特征向量的函数
    *
    * @param vertexInfo
    * @param sdf
    * @return 中位数,标准偏差,最小值,最大值,偏度,峰值,方差
    */
  def featureCreation(vertexInfo: VertexRDD[Double], sdf: SQLContext): DataFrame = {
    import sdf.implicits._

    // Function to aggregate the vertex information to create the feature vector.
    // 用聚合顶点信息来创建特征向量的函数
    val vertexInfoDF: DataFrame = vertexInfo.toDF().persist(StorageLevel.MEMORY_AND_DISK_SER)
    val mean: DataFrame = vertexInfoDF.agg("_2" -> "mean")
    val sd: DataFrame = vertexInfoDF.agg("_2" -> "stddev")
    val min: DataFrame = vertexInfoDF.agg("_2" -> "min")
    val max: DataFrame = vertexInfoDF.agg("_2" -> "max")
    val skew: DataFrame = vertexInfoDF.agg("_2" -> "skewness")
    val kurt: DataFrame = vertexInfoDF.agg("_2" -> "kurtosis")
    val vari: DataFrame = vertexInfoDF.agg("_2" -> "variance")

    val joinedStats: DataFrame = sd.join(mean).join(min).join(max).join(skew).join(kurt).join(vari)
    vertexInfoDF.unpersist(blocking = true)
    return joinedStats
  }

  def filterLine(line: String): Boolean = {

    val arr = line.replaceAll("&", "->").replaceAll("-", "").split(">")
    (arr(1) match {
      //      case s if (s.contains("&7") || s.contains("&13") || s.contains("&15") || s.contains("&16") || s.contains("&17") || s.contains("&19") || s.contains("&20")) => true
      case s if (s.contains("7") || s.contains("13") || s.contains("15") || s.contains("16")
        || s.contains("17") || s.contains("19") || s.contains("20")) => true
      case _ => false
    }) && (arr(4) match {
      case s if (s.contains("7") || s.contains("13") || s.contains("15") || s.contains("16")
        || s.contains("17") || s.contains("19") || s.contains("20")) => true
      case _ => false
    })
  }

  def canberraDist(sc: SparkContext, X: RDD[Double], Y: RDD[Double]): Double = {
    // Create an index based on length for each RDD.
    //创建每个rdd基于长度的索引
    // Index is added as second value so use map to switch order allowing join to work properly.
    // This can be done in the join step, but added here for clarity.
    val RDDX: RDD[(Long, Double)] = X.zipWithIndex().map(x => (x._2, x._1))
    val RDDY: RDD[(Long, Double)] = Y.zipWithIndex().map(x => (x._2, x._1))

    // Join the 2 RDDs on index value. Returns: RDD[(Long, (Double, Double))]
    val RDDJoined: RDD[(Long, (Double, Double))] = RDDX.map(x => (x._1, x._2)).join(RDDY.map(x => (x._1, x._2)))

    // Calculate Canberra Distance
    //兰氏距离 //http://blog.sina.com.cn/s/blog_66d4b4620101gbvs.html 结算算法
    val distance: Double = RDDJoined.map { case (id, (x, y)) => {
      if (x != 0 && y != 0 && !x.isNaN() && !y.isNaN()) (math.abs(x - y) / (math.abs(x) + math.abs(y))) else (0.0)
    }
    }.reduce(_ + _)

    // Return Value
    return distance
  }

}
