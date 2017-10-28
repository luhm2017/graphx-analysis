/**
 * Created by Administrator on 2017/8/7 0007.
 */

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringEncoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerKafka {
    private ConsumerConfig config;
    private String topic;
    private int partitionsNum;
    private MessageExecutor executor;
    private ConsumerConnector connector;
    private ExecutorService threadPool;

    public ConsumerKafka(String topic, int partitionsNum, MessageExecutor executor) throws Exception {
        Properties prop = new Properties();
        prop.put("auto.offset.reset", "smallest"); //必须要加，如果要读旧数据
        prop.put("zookeeper.connect", "192.168.0.208:2181,192.168.0.211:2181,192.168.0.212:2181");
        prop.put("serializer.class", StringEncoder.class.getName());
        prop.put("metadata.broker.list", "192.168.0.211:9092,192.168.0.212:9092");
        prop.put("group.id", "test-consumer-group");
        config = new ConsumerConfig(prop);
        this.topic = topic;
        this.partitionsNum = partitionsNum;
        this.executor = executor;
    }

    public void start() throws Exception {
        connector = Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put(topic, partitionsNum);
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topics);
        List<KafkaStream<byte[], byte[]>> partitions = streams.get(topic);
        threadPool = Executors.newFixedThreadPool(partitionsNum);
        for (KafkaStream<byte[], byte[]> partition : partitions) {
            threadPool.execute(new MessageRunner(partition));
        }
    }


    public void close() {
        try {
            threadPool.shutdownNow();
        } catch (Exception e) {
            //
        } finally {
            connector.shutdown();
        }

    }

    class MessageRunner implements Runnable {
        private KafkaStream<byte[], byte[]> partition;

        MessageRunner(KafkaStream<byte[], byte[]> partition) {
            this.partition = partition;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = partition.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> item = it.next();
                System.out.println("partiton:" + item.partition());
                System.out.println("offset:" + item.offset());
                executor.execute(new String(item.message()));//UTF-8
            }
        }
    }

    interface MessageExecutor {

        public void execute(String message);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        ConsumerKafka consumer = null;
        try {
            MessageExecutor executor = new MessageExecutor() {

                public void execute(String message) {
                    System.out.println(message);
                }
            };
            consumer = new ConsumerKafka("topic1", 3, executor);
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }

    }

}
