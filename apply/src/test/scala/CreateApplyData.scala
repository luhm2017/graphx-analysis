import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import com.lakala.datacenter.utils.UtilsToos._
import scala.util.Random

/**
  * Created by ASUS-PC on 2017/4/18.
  */
object CreateApplyData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("CreateApplyData")
    val sc = new SparkContext(conf)
    val callLine = sc.textFile("file:///F:/lakalaFinance_workspaces/applogs/000000_0")
    val applyLine = sc.textFile("file:///F:/lakalaFinance_workspaces/applogs/query_result.csv").filter(line => (!line.startsWith("s_c_loan_apply")))
    val call = callLine.mapPartitions { lines =>
      lines.map { line =>
        var arr = line.split("\u0001")
        (if (StringUtils.isNotBlank(arr(4)) && isMobileOrPhone(arr(4))) arr(4) else "", if (StringUtils.isNotBlank(arr(6)) && isMobileOrPhone(arr(6))) arr(6) else "")
      }
    }
    val list = call.filter(k => StringUtils.isNotBlank(k._1)).map(k => k._1.toLong).union(call.filter(k => StringUtils.isNotBlank(k._2)).map(k => k._2.toLong)).collect().toSet.toList
    println("mobil ************************")
    list.sorted.foreach(println)
    println("mobil ************************")
    val ac = sc.broadcast(list)
    applyLine.mapPartitions {
      val list: List[Long] = ac.value
      val seed: Int = list.size
      lines => lines.map {
        line =>
          var arr = line.split(",")
          val index = getIndex(seed)
          val s = if (StringUtils.isBlank(arr(41)) || "null".equals(arr(41).toLowerCase)) "," + list(index) + ","
          else if (StringUtils.isNotBlank(arr(41)) && !isMobileOrPhone(arr(41))) "," + list(index) + ","
          else "," + arr(41) + ","
          s"${arr.slice(0, 41).mkString(",")}$s${arr.slice(42, arr.length).mkString(",")}"
      }
    }.repartition(1).saveAsTextFile("file:///F:/lakalaFinance_workspaces/applogs2/query_result.csv")
  }

  def getIndex(seed: Int): Int = {
    val rand = new Random()
    rand.nextInt(seed)
  }

}
