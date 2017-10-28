package com.lakala.datacenter.realtimeBuildGraphx

/**
  * Created by Administrator on 2017/6/8 0008.
  */

import com.alibaba.fastjson.{JSON, TypeReference}
import com.google.gson.Gson
import com.lakala.datacenter.common.utils.DateTimeUtils
import com.lakala.datacenter.constant.StreamingConstant
import com.lakala.datacenter.utils.UtilsTools.properties
import com.lakala.datacenter.utils.{ArgsCommon, Config, RedisUtils}
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.StringUtils.trim
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}
import org.joda.time.DateTime
import org.neo4j.driver.v1.{Driver, _}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object SparkStreamingOnKafkaDirect extends Logging {
  def main(args: Array[String]): Unit = {
    //    -z 192.168.0.208:2181,192.168.0.211:2181,192.168.0.212:2181 -p 192.168.0.211:9092,192.168.0.212:9092 -t logCollect_cleanData -g testcheatgraph -r true -v 217411
    //生产kafka地址：10.16.65.5:2181,10.16.65.6:2181,10.16.65.7:2181
    val config = ArgsCommon.parseArgs(args)
    //load config
    val properies = properties(StreamingConstant.CONFIG)

    val zkClient = getZkClient(config.zkIPs)
    //kafka paramter
    val kafkaParams = Map[String, String](
      StreamingConstant.METADATA_BROKER_LIST -> config.brokerIPs,
      StreamingConstant.AUTO_OFFSET_RESET -> kafka.api.OffsetRequest.SmallestTimeString,
      StreamingConstant.KAFKA_GROUP_ID -> config.group)
    //streaming paramter
    var kafkaStream: InputDStream[(String, String)] = null
    var offsetRanges = mutable.Map[org.apache.spark.streaming.Time, Array[OffsetRange]]()
    val conf = new SparkConf().setMaster(config.master).setAppName("SparkStreamingOnKafkaDirect")
    conf.set(StreamingConstant.SPARK_STREAMING_CONCURRENTJOBS, "4") //设置streaming并行job的参数
    conf.set(StreamingConstant.SPARK_STREAMING_BACKPRESSURE_ENABLED, "true") //设置streaming开启背压机制
    conf.set(StreamingConstant.SPARK_UI_SHOWCONSOLEPROGRESS, "false") //在控制台不打印每个任务的箭头完成度信息
    conf.set(StreamingConstant.SPARK_LOCALITY_WAIT, "1") //设置任务调度等待间隔为0
    conf.set(StreamingConstant.SPARK_STREAMING_KAFKA_MAXRETRIES, "1000") //设置kafka异常的时候重连kafka信息的次数 每次重连等待的时间通过"refresh.leader.backoff.ms"设置，默认是200ms
    conf.set(StreamingConstant.SPARK_SERIALIZER, "org.apache.spark.serializer.KryoSerializer") //设置序列化为kryo

    val ssc = new StreamingContext(conf, Milliseconds(500))
    ssc.addStreamingListener(new MsgOffsetStreamListener(config, offsetRanges))

    val topicDirs = new ZKGroupTopicDirs(config.group, config.topic)
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")

    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
    try{
    if (children > 0) {
      fromOffsets = getFromOffsets(config, children, zkClient, topicDirs)
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(config.topic))
    }
    } catch {case e:Exception=>e.printStackTrace()}
    kafkaStream.transform { (rdd, time) =>
      if (!rdd.partitions.isEmpty) {
        // 打印一下这一批batch的处理时间段以及累计的有效记录数(不含档次)
        println("--" + new DateTime(time.milliseconds).toString("yyyy-MM-dd HH:mm:ss") + "--totalcounts:" + rdd.count() + "-----")
        offsetRanges += (time -> Array[OffsetRange](rdd.asInstanceOf[HasOffsetRanges].offsetRanges: _*))
      }
      rdd
    }.map(_._2).filter(line => line.contains(StreamingConstant.GENERALAPPLY_CREDITLOAN) && line.contains(StreamingConstant.LOGNO))
      .foreachRDD { rdds =>
        def func(records: Iterator[String]) {
          var driver: Driver = null
          var gson: Gson = new Gson
          implicit var redis: JedisCluster = null
          try {
            driver = GraphDatabase.driver(trim(properies.getProperty(StreamingConstant.NEOIP)), AuthTokens.basic(trim(properies.getProperty(StreamingConstant.USER)), trim(properies.getProperty(StreamingConstant.PASSWORD))))
            redis = RedisUtils.jedisCluster()
            records.foreach { line =>
              var map: java.util.HashMap[String, String] = new java.util.HashMap[String, String]()
              Try {
                map = JSON.parseObject(JSON.parseObject(line).getString(StreamingConstant.FLATMAP), new TypeReference[java.util.HashMap[String, String]]() {})
                runCypherApply(driver.session, map)
              } match {
                case Success(sf) => {
                  try {
                    //用订阅发布模式 把进件号id 存入redis
                    val msg = new SendMsg(map.getOrDefault(StreamingConstant.ORDERNO, ""), map.getOrDefault(StreamingConstant.INSERTTIME, DateTimeUtils.formatter.print(DateTime.now())), map.getOrDefault(StreamingConstant.CERTNO, "00000"))
                    redis.publish(properies.getProperty(StreamingConstant.PSUBSCRIBE), gson.toJson(msg))
                  } catch {
                    case e: Exception => println("redis exception push message")
                  }
                }
                case Failure(sf) => {
                  println(s"${DateTime.now()} real time applyInfo insert neo4j throw EXCEPTION: ${sf.getMessage} orderno:" + map.getOrDefault(StreamingConstant.ORDERNO, ""))
                }
              }
            }
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            if (driver != null) {
              driver.close()
            }
          }
        }

        if (!rdds.partitions.isEmpty) {
          rdds.foreachPartition(func)
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * cypher 入节点数据
    *
    * @param session
    * @param map
    */
  private def runCypherApply(session: Session, map: java.util.HashMap[String, String]): Unit = {
    val applyStatementTemplate = new StringBuffer("MERGE (apply:ApplyInfo {orderno:$orderno})")
    applyStatementTemplate.append(" ON MATCH SET apply.modelname='ApplyInfo',apply.insertTime=$insertTime,apply.user_id=$user_id")
    val otherStatementTemplate = new StringBuffer()
    val relStatementTemplate = new StringBuffer()

    var paramMap: java.util.HashMap[String, Object] = new java.util.HashMap[String, Object]()
    paramMap.put(StreamingConstant.ORDERNO, map.getOrDefault(StreamingConstant.ORDERNO, ""))
    paramMap.put(StreamingConstant.INSERTTIME, map.getOrDefault(StreamingConstant.INSERTTIME, DateTimeUtils.formatter.print(DateTime.now())))
    paramMap.put(StreamingConstant.USER_ID, map.getOrDefault(StreamingConstant.USERID, ""))

    for (key <- StreamingConstant.fieldMap.keySet) {
      val fieldRelation = StreamingConstant.fieldMap.get(key).get.split(",")
      var value = if (StringUtils.isNotBlank(map.get(key))) map.get(key) else ""
      if ("_DeviceId".equals(key) && StringUtils.isNotBlank(value) && "00000000-0000-0000-0000-000000000000".equals(value)) value = ""
      if (StreamingConstant.EMAIL.equals(key) && StringUtils.isNotBlank(value)) value = value.toLowerCase
      if (StreamingConstant.RECOMMEND.equals(key) && StringUtils.isNotBlank(value) && ((value.length < 5) || value.length > 11)) value = ""

      if (StringUtils.isNotBlank(value)) {
        val modelname = "" + StreamingConstant.labelMap.get(key).get
        val rel = "" + StreamingConstant.relationShipMap.get(key).get
        otherStatementTemplate.append(" MERGE (" + key + ":" + modelname + "{modelname:'" + modelname + "',content:$" + fieldRelation(0) + "})")
        otherStatementTemplate.append(" MERGE (apply)-[:" + rel + "]->(" + key + ")")
        applyStatementTemplate.append(",apply." + fieldRelation(0) + "=$" + fieldRelation(0))
        paramMap.put(fieldRelation(0), value)
      }
    }

    val statementStr = applyStatementTemplate.append(otherStatementTemplate).toString
    println(statementStr)
    session.writeTransaction(new TransactionWork[Integer]() {
      override def execute(tx: Transaction): Integer = {
        tx.run(statementStr, paramMap)
        tx.success()
        1
      }
    })
  }

  /**
    * connect zk
    *
    * @param zkServers
    * @param sessionTimeout
    * @param connectionTimeout
    * @return
    */
  def getZkClient(zkServers: String, sessionTimeout: Int = 60000, connectionTimeout: Int = 60000): ZkClient = {

    val zkClient = new ZkClient(zkServers, sessionTimeout, connectionTimeout, new ZkSerializer {
      override def serialize(data: Object): Array[Byte] = {
        try {
          return data.toString().getBytes(StreamingConstant.CODE)
        } catch {
          case e: ZkMarshallingError => return null

        }
      }

      override def deserialize(bytes: Array[Byte]): Object = {
        try {
          return new String(bytes, StreamingConstant.CODE)
        } catch {
          case e: ZkMarshallingError => return null
        }
      }
    })
    zkClient
  }

  /**
    * 获得最早偏移或者需要指定更新偏移量
    *
    * @param config
    * @param children
    * @param zkClient
    * @param topicDirs
    * @return
    */
  def getFromOffsets(config: Config, children: Int, zkClient: ZkClient, topicDirs: ZKGroupTopicDirs): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    if (children > 0) {
      //---get partition leader begin----
      val topicList = List(config.topic)
      //得到该topic的一些信息，比如broker,partition分布情况
      val req = new TopicMetadataRequest(topicList, 0)
      // brokerList的host 、brokerList的port、过期时间、过期时间
      val getLeaderConsumer = getSimpleConsumer(config)
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
        val tp = TopicAndPartition(config.topic, i)
        //---additional begin-----
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1))) // -2,1
        val consumerMin = new SimpleConsumer(partitions(i), 9092, 10000, 10000, "getMinOffset")
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        var nextOffset = partitionOffset.toLong
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        fromOffsets += (tp -> partitionOffset.toLong)
        //如果下一个offset小于当前的offset
        if (curOffsets.length > 0 && nextOffset < curOffsets.head) {
          nextOffset = curOffsets.head
          fromOffsets += (tp -> nextOffset)
        }

        //---additional end-----
        if (config.renew && curOffsets.length > 0 && config.offset.toLong >= curOffsets.head && config.offset.toLong <= nextOffset) {
//          ZkUtils.apply(zkClient,true).updatePersistentPath(s"${topicDirs.consumerOffsetDir}/${i}", config.offset)
                    ZkUtils.updatePersistentPath(zkClient, s"${topicDirs.consumerOffsetDir}/${i}", config.offset)
          fromOffsets += (tp -> config.offset.toLong)
        }
        println(s"grogram start kafka offset data value:${topicDirs.consumerOffsetDir}/${i}==${fromOffsets.get(tp).get}")
        logInfo(s"grogram start kafka offset data value:${topicDirs.consumerOffsetDir}/${i}==${fromOffsets.get(tp).get}")
      }

    }
    fromOffsets
  }

  /**
    * get SimpleConsumer instance
    *
    * @param config
    * @return
    */
  def getSimpleConsumer(config: Config): SimpleConsumer = {
    val arrIps = config.brokerIPs.split(",")
    val simCon = for (ip <- arrIps) yield {
      val contect = Try(new SimpleConsumer(ip.substring(0, ip.indexOf(":")), 9092, 10000, 10000, "OffsetLookup"))
      if (contect.isSuccess) contect.get
    }
    simCon(0).asInstanceOf[SimpleConsumer]
  }

}
