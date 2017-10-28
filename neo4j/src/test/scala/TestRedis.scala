import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lakala.datacenter.common.utils.DateTimeUtils
import com.lakala.datacenter.constant.StreamingConstant
import com.lakala.datacenter.utils.RedisUtils
import org.joda.time.DateTime
import redis.clients.jedis.JedisPubSub



/**
  * Created by Administrator on 2017/6/29 0029.
  */
object TestRedis {
  def main(args: Array[String]): Unit = {

//
    val jedis = RedisUtils.jedisCluster()
    try {
      val orderno = args(0)
      val insertTime=Map(StreamingConstant.INSERTTIME->"2017-06-30 12:01:10").getOrElse(StreamingConstant.INSERTTIME, DateTimeUtils.formatter.print(DateTime.now()))
      val s= "{\""+StreamingConstant.ORDERNO+"\":\""+orderno+"\",\""+StreamingConstant.INSERT_TIME+"\":\""+insertTime+"\"}"
      jedis.publish("testsub12",  s)
      println(s)
      println(JSON.parseObject(s).getString(StreamingConstant.INSERT_TIME))
    } catch {
      case e: Exception => println("AAAAAAAAA"+e.getMessage)
    }

  }


}
