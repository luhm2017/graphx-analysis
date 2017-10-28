import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/27 0027.
  */
object TestSql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val list = List("1","2","3","3","5")
    import sqlContext.implicits._
    val vertexInfoDF = sc.parallelize(list).toDF().persist(StorageLevel.MEMORY_AND_DISK_SER)
    // 用聚合顶点信息来创建特征向量的函数
    val mean: DataFrame = vertexInfoDF.agg("_1" -> "mean")
    val sd: DataFrame = vertexInfoDF.agg("_1" -> "stddev")
//    val median: DataFrame = vertexInfoDF.agg("_1" -> "median")
    val min: DataFrame = vertexInfoDF.agg("_1" -> "min")
    val max: DataFrame = vertexInfoDF.agg("_1" -> "max")
    val skew: DataFrame = vertexInfoDF.agg("_1" -> "skewness")
    val kurt: DataFrame = vertexInfoDF.agg("_1" -> "kurtosis")
    val vari: DataFrame = vertexInfoDF.agg("_1" -> "variance")

    val joinedStats: DataFrame = sd.join(mean).join(min).join(max).join(skew).join(kurt).join(vari)
//      .join(median)
    println(joinedStats.printSchema())
    println(joinedStats.foreach(row=>println(row.get(0))))
    vertexInfoDF.unpersist(blocking = true)
    val sdtestDF = Seq((1.2.toDouble, 1.6.toDouble, 1.8.toDouble, 1.9.toDouble))
      .toDF("numNodes", "numEdges", "maxDeg", "avgDeg")
    val df = sdtestDF.join(joinedStats)
    println(df.count())  }

}

