package com.lakala.datacenter.main

import kafka.consumer.KafkaStream
import org.neo4j.driver.v1.Session
import redis.clients.jedis.JedisCluster

/**
  * Created by Administrator on 2017/8/7 0007.
  */
case class MessageParam(m_stream: KafkaStream[_, _], m_threadNumber: Int, redis: JedisCluster,
                        session: Session, sessionBak: Session,psubscribe:String) {

}
