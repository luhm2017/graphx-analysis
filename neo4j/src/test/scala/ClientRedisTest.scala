import com.lakala.datacenter.utils.RedisUtils
import redis.clients.jedis.JedisPubSub

/**
  * Created by Administrator on 2017/6/29 0029.
  */
object ClientRedisTest {
  def main(args: Array[String]): Unit = {
    val jedis = RedisUtils.jedisCluster()
    println(jedis.subscribe(new ApplyPubSubListener(),args(0)))
  }

  class ApplyPubSubListener extends JedisPubSub {

    override def onMessage(channel: String, message: String): Unit = {
      System.out.println(channel + " onMessage=" + message)
      super.onMessage(channel, message)
    }
    // 初始化订阅时候的处理
    override def onSubscribe(channel: String, subscribedChannels: Int) {
       System.out.println(channel + " onSubscribe=" + subscribedChannels);
    }

    // 取消订阅时候的处理
    override def onUnsubscribe(channel: String, subscribedChannels: Int) {
       System.out.println(channel + "onUnsubscribe=" + subscribedChannels);
    }

    // 初始化按表达式的方式订阅时候的处理
    override def onPSubscribe(pattern: String, subscribedChannels: Int) {
       System.out.println(pattern + "onPSubscribe=" + subscribedChannels);
    }

    // 取消按表达式的方式订阅时候的处理
    override def onPUnsubscribe(pattern: String, subscribedChannels: Int) {
       System.out.println(pattern + "onPUnsubscribe=" + subscribedChannels);
    }

    // 取得按表达式的方式订阅的消息后的处理
    override def onPMessage(pattern: String, channel: String, message:String ) {
      System.out.println(pattern + "onPMessage=" + channel + "=" + message);
    }
  }
}
