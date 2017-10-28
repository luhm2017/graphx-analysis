import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linyanshi on 2017/9/14 0014.
  */
object LoadCallhistoryData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LoadCallhistoryData")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val date = args(0).split("-")
    val year = date(0)
    val month = date(1)
    val day = date(2)
    hc.sql("use datacenter")
    hc.udf.register("isMobile", new JudgeIsMobile(), DataTypes.BooleanType)
    hc.udf.register("castInt", new CastToInt(), DataTypes.LongType)
    val hql =
      s"""SELECT a.deviceid,a.loginname,a.caller_phone,sum(castInt(a.duration)) AS duration,max(a.date) AS date,max(a.collecttime) AS  collecttime
         |FROM r_callhistory_week a WHERE a.year='${year}' AND a.month='${month}' AND a.day='${day}'
         |     AND a.loginname is not null AND a.caller_phone is not null AND isMobile(a.loginname)
         |     AND isMobile(a.caller_phone) AND a.duration is not null AND a.collecttime <>'null'
         |     group by a.deviceid,a.loginname,a.caller_phone
       """.stripMargin
    hc.sql(hql).repartition(100).mapPartitions(rows => rows.map { row => s"${row.getAs("deviceid")},${row.getAs("loginname")},${row.getAs("caller_phone")},${row.getAs("duration")},${row.getAs("date")},${row.getAs("collecttime")}" })
      .saveAsTextFile(args(1))
  }
}
