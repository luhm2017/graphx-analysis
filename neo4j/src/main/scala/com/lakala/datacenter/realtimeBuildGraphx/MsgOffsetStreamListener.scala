package com.lakala.datacenter.realtimeBuildGraphx

import com.lakala.datacenter.constant.StreamingConstant
import com.lakala.datacenter.utils.Config
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.spark.Logging
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerReceiverError, StreamingListenerReceiverStopped}

import scala.collection.mutable

/**
  * Created by Administrator on 2017/6/9 0009.
  */
class MsgOffsetStreamListener(config: Config, offsetRanges: mutable.Map[Time, Array[OffsetRange]]) extends StreamingListener with Logging {

  var zkClient = getZkClient(config.zkIPs)
//  val zkUtils = ZkUtils.apply(zkClient,true)
  val topicDirs = new ZKGroupTopicDirs(config.group, config.topic)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    //创建一个 ZKGroupTopicDirs 对象，对保存
    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    //    println(batchCompleted.batchInfo.numRecords)
    if (batchCompleted.batchInfo.numRecords > 0) {
      val currOffsetRange = offsetRanges.remove(batchCompleted.batchInfo.batchTime).getOrElse(Array[OffsetRange]())
      currOffsetRange.foreach { x =>
        val zkPath = s"${topicDirs.consumerOffsetDir}/${x.partition}"
        //将该 partition 的 offset 保存到 zookeeper
//        ZkUtils.apply(zkClient,true).updatePersistentPath(zkPath, s"${x.fromOffset}")
        ZkUtils.updatePersistentPath(zkClient, zkPath, s"${x.fromOffset}")
        println(s"zkPath:${zkPath} offset:fromOffset ${x.fromOffset} untilOffset ${x.untilOffset}")
//        logInfo(s"zkPath:${zkPath} offset:fromOffset ${x.fromOffset} untilOffset ${x.untilOffset}")
      }
    }
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    val topicDirs = new ZKGroupTopicDirs(config.group, config.topic)
    logError(s"ERROR:${receiverError.receiverInfo.lastError}\n Message:${receiverError.receiverInfo.lastErrorMessage}")
    val currOffsetRange = offsetRanges.remove(Time.apply(receiverError.receiverInfo.lastErrorTime)).getOrElse(Array[OffsetRange]())
    currOffsetRange.foreach { x =>
      val zkPath = s"${topicDirs.consumerOffsetDir}/${x.partition}"
//      ZkUtils.apply(zkClient,true).updatePersistentPath(zkPath, s"${x.fromOffset}")
      ZkUtils.updatePersistentPath(zkClient, zkPath, s"${x.fromOffset}")
      println(s"zkPath:${zkPath} offset:fromOffset ${x.fromOffset} untilOffset ${x.untilOffset}")
//      logInfo(s"zkPath:${zkPath} offset:fromOffset ${x.fromOffset} untilOffset ${x.untilOffset}")
    }
  }

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

}
