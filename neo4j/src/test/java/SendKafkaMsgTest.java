/*******************************************************************************
 * Project Key : BIGDATACENTER 
 * Create on 2016年10月26日 上午11:17:18
 * Copyright (c) 2004 - 2016. 拉卡拉支付有限公司版权所有. 京ICP备12007612号
 * 注意：本内容仅限于拉卡拉支付有限公司内部传阅，禁止外泄以及用于其他的商业目的
 ******************************************************************************/

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**  
 * <P>对功能点的描述</P>
 * @author tianhuaxing 2016年10月26日 上午11:17:18
 * @since 1.0.0.000 
 */
public class SendKafkaMsgTest {
	
	private final static Logger logger = LoggerFactory.getLogger(SendKafkaMsgTest.class);
	
	

	public static void chectTime(long ll){
		
		
		Date date=new Date(ll);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String str = format.format(date);
		System.out.println("tttt"+str);

	}
	public static void main(String[] args) throws Exception {
		StringBuffer sub =new StringBuffer();
		String result="";
		BufferedReader reader =null;
		FileReader in =null;
		String line = "";
		try {

			in = new FileReader("F:/lakalaFinance_workspaces/applogs/log.txt");
//			in = new FileReader("D:/test/applogs/contactNeo4j.txt");
//			in = new FileReader("D:/test/applogs/contact-ios1.txt");
//			in = new FileReader("D:/test/applogs/callHistory.txt");
			
			reader = new BufferedReader(in);
			while (StringUtils.isNoneBlank(line=reader.readLine())) {
				result+= line;
				//break;
			}
			
		} finally{
		   if(reader!=null){
			   reader.close();
		   }
		   if(in!=null){
			   in.close();
		   }
		}
		  Properties props = new Properties();
		  String kafkaBrokerList = "192.168.0.211:9092,192.168.0.212:9092";
		  String kafkaTopic = "sparksteamingTest2";
		  // String kafkaTopic = "stormTest/contactList/mytopic/logCollect_test";
		// 此处配置的是kafka的端口
				props.put("metadata.broker.list", kafkaBrokerList);
				// 配置value的序列化类
				props.put("serializer.class", "kafka.serializer.StringEncoder");
				// 配置key的序列化类
				props.put("key.serializer.class", "kafka.serializer.StringEncoder");
				// 为不影响客户端，异步发送
				props.put("producer.type", "async");
//				props.put("zk.connect", "192.168.10.101:2182,192.168.10.102:2182,192.168.10.103:2182");
				// 此处配置的是kafka的端口
				// 0, which means that the producer never waits for an acknowledgement
				// from the broker (the same behavior as 0.7). This option provides the
				// lowest latency but the weakest durability guarantees (some data will
				// be lost when a server fails).
				// 1, which means that the producer gets an acknowledgement after the
				// leader replica has received the data. This option provides better
				// durability as the client waits until the server acknowledges the
				// request as successful (only messages that were written to the
				// now-dead leader but not yet replicated will be lost).
				// -1, which means that the producer gets an acknowledgement after all
				// in-sync replicas have received the data. This option provides the
				// best durability, we guarantee that no messages will be lost as long
				// as at least one in sync replica remains.
				// props.put("request.required.acks", "-1");
		int messageNo=1;
		String key = String.valueOf(messageNo);
		System.out.println("【日志发送客户端】发送数据中心的kafka。主题={}"+kafkaTopic+"tianhuaxing");
		//Producer send每个都持有自己的锁,不会造成锁等待 
		Producer<String, String> producer=null;
		try {
			producer = new Producer<String, String>(new ProducerConfig(props));
			producer.send(new KeyedMessage<String, String>(kafkaTopic, key, result));
		} catch (Exception e) {
			logger.error("【日志发送客户端】发送数据到kafka异常。内容={}","");
			logger.error(e.getMessage(), e);
		}finally {
			//生产必须加close释放资源。生产服务器已被搞挂
			if(producer!=null){
			     producer.close();
			}
		}
		messageNo++;
	}

}
