
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import com.lakala.datacenter.utils.UtilsToos._
import scala.util.Random
/**
  * Created by ASUS-PC on 2017/4/18.
  */
object CreateApplyData2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("CreateApplyData2")
    val sc = new SparkContext(conf)
    val callLine = sc.textFile("file:///F:/lakalaFinance_workspaces/applogs/000000_0")
    val call = callLine.mapPartitions { lines =>
      lines.map { line =>
        var arr = line.split("\u0001")
        (s"${if (StringUtils.isNotBlank(arr(4)) && isMobileOrPhone(arr(4))) arr(4) else "0"},${if (StringUtils.isNotBlank(arr(6)) && isMobileOrPhone(arr(6))) arr(6) else "0"}")
      }
    }
    call.distinct().repartition(1).saveAsTextFile("file:///F:/lakalaFinance_workspaces/applogs3/query_result.csv")
  }

  def getIndex(seed: Int): Int = {
    val rand = new Random()
    rand.nextInt(seed)
  }

}
