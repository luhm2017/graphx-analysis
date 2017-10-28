/**
  * Created by linyanshi on 2017/8/19 0019.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object NumOnce {
  //利用异或运算将列表中的所有ID异或，之后得到的值即为所求ID。先将每个分区的数据
  //异或，然后将结果进行异或运算。
  def computeOneNum(args:Array[String]) {
    val conf  = new SparkConf().setAppName("NumOnce").setMaster("local[1]")
    val spark = new SparkContext(conf)
    val data = spark.textFile("data")
    /*每个分区分别对数据进行异或运算,最后在reduceByKey阶段,将各分区异或运算的结果再做异或运算合并。
    偶数次出现的数字,异或运算之后为0,奇数次出现的数字,异或后为数字本身*/
    val result = data.mapPartitions(iter => {
      var temp = iter.next().toInt
      while(iter.hasNext) {
        temp = temp^(iter.next()).toInt
      }
      Seq((1, temp)).iterator
    }).reduceByKey(_^_).collect()
    println("num appear once is: "+result(0))
  }
}
