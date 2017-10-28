/**
  * Created by Administrator on 2017/6/22 0022.
  */

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingFromKafka {
  val groupId = "testcheatgraph"
  val topic = "logCollect_cleanData"
  val zkClient = new ZkClient("192.168.0.211:2181,192.168.0.212:2181", 60000, 60000, new ZkSerializer {
    override def serialize(data: Object): Array[Byte] = {
      try {
        return data.toString().getBytes("UTF-8")
      } catch {
        case e: ZkMarshallingError => return null

      }
    }

    override def deserialize(bytes: Array[Byte]): Object = {
      try {
        return new String(bytes, "UTF-8")
      } catch {
        case e: ZkMarshallingError => return null
      }
    }
  })
  val topicDirs = new ZKGroupTopicDirs("testcheatgraph", topic)
  val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")

    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "2")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val kafkaParams = Map("metadata.broker.list" -> "192.168.0.211:9092,192.168.0.212:9092", "group.id" -> groupId, "zookeeper.connect" -> "192.168.0.211:2181,192.168.0.212:2181",
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString)
    val topics = Set(topic)
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    var kafkaStream: InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    if (children > 0) {
      //---get partition leader begin----
      val topicList = List(topic)
      //得到该topic的一些信息，比如broker,partition分布情况
      val req = new TopicMetadataRequest(topicList, 0)
      // brokerList的host 、brokerList的port、过期时间、过期时间
      val getLeaderConsumer = new SimpleConsumer("192.168.0.211", 9092, 10000, 10000, "OffsetLookup")
      //TopicMetadataRequest   topic broker partition 的一些信息
      val res = getLeaderConsumer.send(req)
      val topicMetaOption = res.topicsMetadata.headOption
      val partitions = topicMetaOption match {
        case Some(tm) =>
          tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None =>
          Map[Int, String]()
      }
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        //---additional begin-----
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1))) // -2,1
        val consumerMin = new SimpleConsumer(partitions(i), 9092, 10000, 10000, "getMinOffset")
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        var nextOffset = partitionOffset.toLong
        //如果下一个offset小于当前的offset
        if (curOffsets.length > 0 && nextOffset < curOffsets.head) {
          nextOffset = curOffsets.head
        }
        //---additional end-----
        fromOffsets += (tp -> nextOffset)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }
    var offsetRanges = Array[OffsetRange]()
    kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD {
      rdd => {
        rdd.map(_._2).foreachPartition { element =>
          element.foreach {
            println
          }
        }
        for (o <- offsetRanges) {
//          ZkUtils.apply(zkClient, true).updatePersistentPath(s"${topicDirs.consumerOffsetDir}/${o.partition}", o.fromOffset.toString)
                    ZkUtils.updatePersistentPath(zkClient,s"${topicDirs.consumerOffsetDir}/${o.partition}", o.fromOffset.toString)
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
