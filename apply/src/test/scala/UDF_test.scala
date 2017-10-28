import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linyanshi on 2017/9/14 0014.
  */
object UDF_test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    implicit val sc = new SparkContext(conf)
    implicit val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._

    val data = sc.parallelize(Seq(("a", 1), ("bb", 5), ("cccc", 10), ("dddddd", 15))).toDF("a", "b")
    data.registerTempTable("data")


    {
      //函数体采用原生类型（非Column类型），使用udf包装函数体，将函数体注册到sqlContext.udf
      import org.apache.spark.sql.functions._

      //函数体
      val filter_length_f = (str: String, _length: Int) => {
        str.length > _length;
      }

      //注册函数体到当前sqlContext，注意，注册到sqlContext的函数体，参数不能为Column
      //注册后，可以在以下地方使用：1、df.selectExpr 2、df.filter ,3、将该df注册为temptable，之后在sql中使用
      sqlContext.udf.register("filter_length", filter_length_f)

      val filter_length = udf(filter_length_f) //为方便使用Column，我们对函数体进行包装，包装后的输入参数为Column

      data.select($"*", filter_length($"a", lit(2))).show //使用udf包装过的，必须传入Column，注意 lit(2)
      data.selectExpr("*", " filter_length(a,2) as ax").show //select 若写表达式调用函数，则需要使用selectExpr

      data.filter(filter_length($"a", lit(2))).show //同select
      data.filter("filter_length(a,2)").show //filter调用表达式，可以直接使用df.filter函数，

      sqlContext.sql("select *,filter_length(a,2) from data").show
      sqlContext.sql("select *,filter_length(a,2) from data where filter_length(a,2)").show
    }
    {
      //函数体使用Column类型，无法注册到sqlContext.udf
      //使用udf包装后，每列都必须输入column，能否我们自己定义呢，比如一个参数是Column，一个是其他类型
      import org.apache.spark.sql.Column
      import org.apache.spark.sql.functions._

      val filter_length_f2 = (str: Column, _length: Int) => {
        length(str) > _length
      }
      sqlContext.udf.register("filter_length", filter_length_f2) //todo：不好意思，这里注册不了，注册到sqlContext.udf的函数，入参不支持Column类型

      data.select($"*", filter_length_f2($"a", 2)).show //不用udf包装，我们就可以完全自定义，这时 length 就可以传入整型了
      data.selectExpr("*", " filter_length_f2(a,2) as ax").show //todo：不好意思，这里用不了了，

      data.filter(filter_length_f2($"a", 2)).show //同select
      data.filter("filter_length(a,2)").show //todo：不好意思，这里用不了了

    }
    //最后，我们写一个相对通用的吧
    {
      //定义两个函数体，入参一个使用column类型，一个使用原生类型，将原生类型函数注册到sqlContext.udf

      import org.apache.spark.sql.Column
      import org.apache.spark.sql.functions._

      //函数体
      val filter_length_f = (str: String, _length: Int) => {
        str.length > _length;
      }
      //主函数，下面df.select df.filter 等中使用
      val filter_length = (str: Column, _length: Int) => {
        length(str) > _length
      }
      //注册函数体到当前sqlContext，注意，注册到sqlContext的函数体，参数不能为Column
      //注册后，可以在以下地方使用：1、df.selectExpr 2、df.filter ,3、将该df注册为temptable，之后在sql中使用
      sqlContext.udf.register("filter_length", filter_length_f)

      //这里我们不使用udf了，直接使用自己定义的支持Column的函数
      //val filter_length = udf(filter_length_f) //为方便使用Column，我们对函数体进行包装，包装后的输入参数为Column

      data.select($"*", filter_length($"a", 2)).show //使用udf包装过的，必须传入Column，注意 lit(2)
      data.selectExpr("*", " filter_length(a,2) as ax").show //select 若写表达式调用函数，则需要使用selectExpr

      data.filter(filter_length($"a", 2)).show //同select
      data.filter("filter_length(a,2)").show //filter调用表达式，可以直接使用df.filter函数，

      sqlContext.sql("select *,filter_length(a,2) from data").show
      sqlContext.sql("select *,filter_length(a,2) from data where filter_length(a,2)").show
    }


  }
}
