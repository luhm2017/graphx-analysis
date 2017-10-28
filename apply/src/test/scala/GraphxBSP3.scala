/*
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/16 0016.
  */
object GraphxBSP3 {
  def main(args: Array[String]): Unit = {
    @transient
    val conf = new SparkConf().setAppName("GraphxBSP").setMaster("local[4]")
    @transient
    val sc = new SparkContext(conf)
    //orderId,contractNo,termId,loanPan,returnPan,insertTime,recommend,userId,
    // deviceId
    //certNo,email,company,mobile,compAddr,compPhone,emergencyContactMobile,contactMobile,ipv4,msgphone,telecode
    val edgeRDD = sc.textFile("F:\\graphx-analysis\\apply\\bin\\test.csv").mapPartitions(lines => lines.map { line =>
      val fields = line.split(",")
      val kv = if (StringUtils.isNoneEmpty(fields(2))) {
        (fields(2), 1)
      } else
        ("0", 0)
      kv
    }).reduceByKey(_ + _).filter(_._2 > 2)
    edgeRDD.foreach(kv=>println(kv._1+"  === "+kv._2))

  }

}
*/
