import com.lakala.datacenter.constant.StreamingConstant
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
/**
  * Created by Administrator on 2017/6/12 0012.
  */
object TestKafka {
  def main(args: Array[String]): Unit = {
        val topic = "logCollect_cleanData"
        val zkConnect = "192.168.0.211:2181,192.168.0.212:2181"
        var zkClient: ZkClient = null
        try {
          zkClient = new ZkClient(zkConnect, 30000, 30000, new ZkSerializer {
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
          zkClient.deleteRecursive(ZkUtils.getTopicPath(topic))  //其实最终还是通过删除zk里面对应的路径来实现删除topic的功能
          println("deletion succeeded!")
        }
        catch {
          case e: Throwable =>
            println("delection failed because of " + e.getMessage)
    //        println(Utils.stackTrace(e))
        }
        finally {
          if (zkClient != null)
            zkClient.close()
        }


    //    import org.I0Itec.zkclient.ZkClient
    //    val arrys = new Array[String](6)
    //    arrys(0) = "--replication-factor"
    //    arrys(1) = "1"
    //    arrys(2) = "--partitions"
    //    arrys(3) = "3"
    //    arrys(4) = "--topic"
    //    arrys(5) = "logCollect_cleanData"
    //    val client = new ZkClient("192.168.0.211:2181,192.168.0.212:2181", 30000, 30000, ZKStringSerializer)
    //    client.setZkSerializer(ZKStringSerializer) //一定要加上ZkSerializer
    //
    //
    //    val opts = new TopicCommand.TopicCommandOptions(arrys)
    //    TopicCommand.createTopic(client, opts)

//    import kafka.admin.AdminUtils
//    val client = new ZkClient("192.168.0.211:2181,192.168.0.212:2181", 30000, 30000)
    //     创建一个单分区单副本名为t1的topic
//    val props: Properties = new Properties
    //此处配置的是kafka的端口
//    props.put("metadata.broker.list", "192.168.0.211:9092,192.168.0.212:9092")
    //配置value的序列化类
//    props.put("serializer.class", "kafka.serializer.StringEncoder")
    //配置key的序列化类
//    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    //request.required.acks
//    props.put("request.required.acks", "-1")
//    AdminUtils.createTopic(client, "logCollect_cleanData", 3, 1, props)
  }
}
