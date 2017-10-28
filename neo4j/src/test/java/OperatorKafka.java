//import com.lakala.datacenter.constant.StreamingConstant;
//import kafka.admin.AdminUtils;
//import org.I0Itec.zkclient.ZkClient;
//import org.I0Itec.zkclient.exception.ZkMarshallingError;
//import org.I0Itec.zkclient.serialize.ZkSerializer;
//
//import java.io.UnsupportedEncodingException;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Properties;
//
///**
// * Created by Administrator on 2017/8/2 0002.
// */
//public class OperatorKafka {
//    public static void main(String[] args) {
//        createTopic();
//    }
//
//    public static void createTopic() {
//        ZkClient zkUtils = getZk();
//// 创建一个单分区单副本名为t1的topic
//        AdminUtils.createTopic(zkUtils, "logCollect_cleanData", 3, 1, new Properties());
//        zkUtils.close();
//    }
//
//    public static void deleteTopic() {
//        ZkClient zkUtils = getZk();
//// 创建一个单分区单副本名为t1的topic
//        AdminUtils.deleteTopic(zkUtils, "logCollect_cleanData");
//        zkUtils.close();
//    }
//
//    public static void queryTopic() {
//        ZkClient zkUtils = getZk();
//        // 获取topic 'test'的topic属性属性
//        Properties props = AdminUtils.fetchTopicConfig(zkUtils, "logCollect_cleanData");
//// 查询topic-level属性
//        Iterator it = props.entrySet().iterator();
//        while (it.hasNext()) {
//            Map.Entry entry = (Map.Entry) it.next();
//            Object key = entry.getKey();
//            Object value = entry.getValue();
//            System.out.println(key + " = " + value);
//        }
//        zkUtils.close();
//    }
//
//
//    public static void updateTopic() {
//        ZkClient zkUtils = getZk();
//        Properties props = AdminUtils.fetchTopicConfig(zkUtils, "logCollect_cleanData");
//// 增加topic级别属性
//        props.put("min.cleanable.dirty.ratio", "0.3");
//// 删除topic级别属性
//        props.remove("max.message.bytes");
//// 修改topic 'test'的属性
//        AdminUtils.changeTopicConfig(zkUtils, "logCollect_cleanData", props);
//    }
//
//    public static ZkClient getZk() {
//        ZkClient zkUtils = new ZkClient("192.168.0.208:2181,192.168.0.211:2181,192.168.0.212:2181", 60000, 60000, new ZkSerializer() {
//            @Override
//            public byte[] serialize(Object data) throws ZkMarshallingError {
//                try {
//                    return data.toString().getBytes(StreamingConstant.CODE());
//                } catch (UnsupportedEncodingException e) {
//                    e.printStackTrace();
//                }
//                return new byte[0];
//            }
//
//            @Override
//            public Object deserialize(byte[] bytes) throws ZkMarshallingError {
//                try {
//                    return new String(bytes, StreamingConstant.CODE());
//                } catch (UnsupportedEncodingException e) {
//                    e.printStackTrace();
//                }
//                return new byte[0];
//            }
//        });
//        return zkUtils;
//    }
//}
