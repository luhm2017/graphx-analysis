import org.apache.spark.sql.api.java.UDF1

import scala.util.matching.Regex

/**
  * Created by linyanshi on 2017/9/14 0014.
  */
class CastToInt extends UDF1[String, Long] {
  val pattern = new Regex("[0-9]{1,}")

  override def call(value: String): Long = {
    if (pattern.pattern.matcher(value).matches() && value.toLong < 86400l) value.trim.toLong
    else if (pattern.pattern.matcher(value).matches() && value.toLong >= 86400l) 86400l
    else 0L
  }
}
