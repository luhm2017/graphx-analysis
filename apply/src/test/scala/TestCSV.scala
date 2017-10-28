import com.lakala.datacenter.core.utils.UtilsToos

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by Administrator on 2017/8/4 0004.
  */
object TestCSV {

  case class Data(index: String, title: String, content: String)

  val arr = Array(4)

  def main(args: Array[String]) {
    val value ="1472100411047"
    val pattern = new Regex("[0-9]{1,}")
    if(pattern.pattern.matcher(value).matches())
      println(value.toLong)
  }

  private def splitSpecificDelimiterData(line: String): String = {
    val context = new StringBuffer()
    val haveSplitAtt = line.split(",")

    val oneSplitAtt = haveSplitAtt(1).split("\\|")
    for (i <- 0 until (oneSplitAtt.length)) {
      if (arr(0) == 4) {
        val secondSplitAtt = haveSplitAtt(3).split("\\|")
        for (j <- 0 until (secondSplitAtt.length)) {
          if (j == secondSplitAtt.length - 1)
            context.append(s"${haveSplitAtt(0)},${oneSplitAtt(i)},${haveSplitAtt(2)},${secondSplitAtt(j)},${haveSplitAtt(haveSplitAtt.size - 1)}")
          else
            context.append(s"${haveSplitAtt(0)},${oneSplitAtt(i)},${haveSplitAtt(2)},${secondSplitAtt(j)},${haveSplitAtt(haveSplitAtt.size - 1)}\n")
        }
      } else {
        if (i == oneSplitAtt.length - 1)
          context.append(s"${haveSplitAtt(0)},${oneSplitAtt(i)},${haveSplitAtt(haveSplitAtt.size - 1)}")
        else
          context.append(s"${haveSplitAtt(0)},${oneSplitAtt(i)},${haveSplitAtt(haveSplitAtt.size - 1)}\n")
      }
    }
    context.toString
  }
}
