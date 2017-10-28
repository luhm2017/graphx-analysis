import org.apache.spark.graphx.Edge
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linyanshi on 2017/9/14 0014.
  */
object ExploreLPAData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ExploreLPAData").set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(args(0), 100).mapPartitions(lines => lines.map { line =>
      val arr = line.split(",")
      Edge(arr(1).toLong,arr(2).toLong)
    })
  }
}
