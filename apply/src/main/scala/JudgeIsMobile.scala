import org.apache.spark.sql.api.java.UDF1

import scala.util.matching.Regex

/**
  * Created by linyanshi on 2017/9/14 0014.
  */
class JudgeIsMobile extends UDF1[String,Boolean]{
  val pattern = new Regex("^((17[0-9])|(14[0-9])|(13[0-9])|(15[^4,\\D])|(18[0,5-9]))\\d{8}$")
  override def call(value: String): Boolean = {
    pattern.pattern.matcher(value).matches()
  }
}
