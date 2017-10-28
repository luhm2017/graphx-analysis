package com.lakala.datacenter.main

import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.lakala.datacenter.constant.StreamingConstant
import com.lakala.datacenter.utils.RedisUtils
import com.lakala.datacenter.utils.UtilsTools.properties
import kafka.consumer.{ConsumerConfig, ConsumerConnector, KafkaStream}
import kafka.serializer.StringDecoder
import kafka.utils.VerifiableProperties
import org.apache.commons.lang3.StringUtils.trim
import org.neo4j.driver.v1.{AuthTokens, Driver, GraphDatabase}
import redis.clients.jedis.JedisCluster

import scala.collection.Map

/**
  * Created by Administrator on 2017/8/7 0007.
  *
  */

object TrialConsumerKafka{
  def main(args: Array[String]): Unit = {
    val zooKeeper: String = args(0)
    val groupId: String = args(1)
    val topic: String = args(2)
    val threads: Int = args(3).toInt
    println("start trial consumer kafaka message .....")
    val example:TrialConsumerKafka= new TrialConsumerKafka(zooKeeper, groupId, topic)
    example.run(threads)

    try {
      Thread.sleep(10000)
    } catch {
      case ie: InterruptedException =>
        println("==============")
    }

  }
}

class TrialConsumerKafka {
  private var consumer: ConsumerConnector = null
  private var topic: String = null
  private var executor: ExecutorService = null
  private var driver: Driver = null
  private var redis: JedisCluster = RedisUtils.jedisCluster()
  val properies = properties(StreamingConstant.CONFIG)
  def this(a_zookeeper: String, a_groupId: String, a_topic: String) {
    this()
    this.topic = a_topic
    consumer = kafka.consumer.Consumer.create(createConsumerConfig(a_zookeeper, a_groupId))
    driver = GraphDatabase.driver(trim(properies.getProperty(StreamingConstant.NEOIP)), AuthTokens.basic(trim(properies.getProperty(StreamingConstant.USER)), trim(properies.getProperty(StreamingConstant.PASSWORD))))
  }

  def shutdown(): Unit = {
    if (consumer != null) consumer.shutdown
    if (executor != null) executor.shutdown
    try {
      if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly")
    } catch {
      case e: InterruptedException =>
        System.out.println("Interrupted during shutdown, exiting uncleanly")
    }
  }


  def run(a_numThreads: Int): Unit = {
    val topicCountMap = Map(topic -> a_numThreads)
    //    val topicCountMap = Map(topic -> 1)
    val keyDecoder = new StringDecoder(new VerifiableProperties)
    val valueDecoder = new StringDecoder(new VerifiableProperties)
    val consumerMap: Map[String, List[KafkaStream[String, String]]] = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder)
    val streams: List[KafkaStream[String, String]] = consumerMap.get(topic).get

    executor = Executors.newFixedThreadPool(a_numThreads)
    var threadNumber = 0
    streams.foreach { stream =>
      executor.submit(new HandleTask(MessageParam(stream, threadNumber, redis,
        driver.session, driver.session, properies.getProperty(StreamingConstant.PSUBSCRIBE))))
      threadNumber += 1
    }
  }

  private def createConsumerConfig(a_zookeeper: String, a_groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", a_zookeeper)
    props.put("group.id", a_groupId)
    props.put("zookeeper.session.timeout.ms", "60000")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "smallest")
    props.put("rebalance.max.retries", "5")
    props.put("rebalance.backoff.ms", "12000")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    new ConsumerConfig(props)
  }
}