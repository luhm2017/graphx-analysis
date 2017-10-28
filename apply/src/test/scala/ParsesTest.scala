import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Administrator on 2017/8/4 0004.
  */
object ParsesTest {
  //  case class Data(index: String, title: String, content: String)
  //  def main(args: Array[String]): Unit = {
  //    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
  //    val sc = new SparkContext(conf)
  //    val input = sc.textFile("F:\\out\\output")
  //    //wholeTextFiles读出来是一个RDD(String,String)
  //    val result = input.map{line=>
  //      val reader = new CSVReader(new StringReader(line));
  //      reader.readAll().map(x => Data(x(0), x(1), x(2)))
  //    }
  //    for(res <- result){
  //      println(res)
  //    }
  //  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParsesTest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "F:\\out\\output\\*", "header" -> "true"))
    df.select("index", "title").foreach(row=>println(row.get(0)))
  }
}
