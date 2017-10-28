/**
  * Created by Administrator on 2017/8/7 0007.
  */


import com.lakala.datacenter.main.TrialConsumerKafka

object ConsumerGroupExample {
  def main(args: Array[String]): Unit = {
    TrialConsumerKafka.main(Array("192.168.0.208:2181,192.168.0.211:2181,192.168.0.212:2181", "test-consumer-group",
      "logCollect_cleanData", "3"))
  }
}

