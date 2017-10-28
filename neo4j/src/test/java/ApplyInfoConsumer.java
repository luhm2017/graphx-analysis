//import java.lang.reflect.Field;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Set;
//
//import kafka.consumer.ConsumerConfig;
//import kafka.consumer.ConsumerIterator;
//import kafka.consumer.KafkaStream;
//import kafka.javaapi.consumer.ConsumerConnector;
//import kafka.serializer.StringDecoder;
//import kafka.utils.VerifiableProperties;
//
//import org.apache.commons.lang3.StringUtils;
//import org.apache.hadoop.hbase.util.CollectionUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.fastjson.JSON;
//import com.lakala.cheatservice.constant.BlackType;
//import com.lakala.cheatservice.constant.ConstantsConfig;
//import com.lakala.cheatservice.constant.DataTypeEnum;
//import com.lakala.cheatservice.constant.LakalaTable;
//import com.lakala.cheatservice.constant.ModelType;
//import com.lakala.cheatservice.constant.RelationType;
//import com.lakala.cheatservice.dao.HbaseUtilServiceImpl;
//import com.lakala.cheatservice.dao.Neo4jUtilServiceImpl;
//import com.lakala.cheatservice.models.BlackNeo4j;
//import com.lakala.cheatservice.models.CallHistory;
//import com.lakala.cheatservice.models.CollectData;
//import com.lakala.cheatservice.models.Contact;
//import com.lakala.cheatservice.models.LogSession;
//import com.lakala.cheatservice.models.MobileV;
//import com.lakala.cheatservice.utils.JsonToBeanConverter;
//import com.lakala.cheatservice.utils.MyUtil;
//
//public class ApplyInfoConsumer {
//
//	private final Logger logger = LoggerFactory.getLogger(getClass());
//	private ConsumerConnector consumer;
//	public static void main(String[] args) {
//
//		System.out.println("开始启动kafka接收消息.");
//		ApplyInfoConsumer customer1=new ApplyInfoConsumer();
//		customer1.consume();
//		System.out.println("程序接收结束!");
//
//	}
//
//
//    private void initial() {
//    	System.out.println("初始化kafka配置 zookeeper.connect:"+ConstantsConfig.kafkaZookeeperConnect+"   group.id:"+ConstantsConfig.kafkaGroup+"   kafkaTopic:"+ConstantsConfig.kafkaApplyInfoTopic);
//        Properties props = new Properties();
//        //zookeeper 配置
//        props.put("zookeeper.connect", ConstantsConfig.kafkaZookeeperConnect);
//        //group kafka消费组
//        props.put("group.id", ConstantsConfig.kafkaGroup);
//        //zk连接超时时长
//        props.put("zookeeper.session.timeout.ms", ConstantsConfig.sessionTimeOut);
//        props.put("zookeeper.sync.time.ms", ConstantsConfig.sysncTimeOut);
//        props.put("auto.commit.interval.ms", ConstantsConfig.commitIntervalTime);
////        props.put("auto.offset.reset", "smallest");
//        props.put("auto.offset.reset", ConstantsConfig.autoOffset);
//        //序列化类
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        ConsumerConfig config = new ConsumerConfig(props);
//        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
//    }
//
//
//
//	private List<String> getPropertyList() {
//		List<String> propertyList=new ArrayList<String>();
//		try {
//
//			   Class<?> clz = BlackNeo4j.class;
//			   // 获取实体类的所有属性，返回Field数组
//			   Field[] fields = clz.getDeclaredFields();
//			   for (Field field : fields) {// --for() begin
//				   propertyList.add(field.getName());
//			   }
//		} catch (Exception e) {
//			System.err.println("BlackNeo4j 获取属性失败 error："+e.getMessage());
//		}
//		return propertyList;
//
//	}
//
//	void consume() {
//    	initial();
//    	List<String> propertyList = getPropertyList();
//    	try {
//	        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
////	        topicCountMap.put(kafkaTopic, new Integer(1));
//	        topicCountMap.put(ConstantsConfig.kafkaApplyInfoTopic, new Integer(1));
//	        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
//	        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
//	        Map<String, List<KafkaStream<String, String>>> consumerMap =
//	                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
//	        KafkaStream<String, String> stream = consumerMap.get(ConstantsConfig.kafkaApplyInfoTopic).get(0);
//	        System.out.println("开始接受kafka消息!");
//	        ConsumerIterator<String, String> it = stream.iterator();
//	        while (true) {
//	        	try {
//	        		recieveKafkaMsg(it,propertyList);
//				} catch (Exception e) {
//					System.err.println("接收到的消息处理失败 ERROR:"+e.getMessage());
//				}
//				Thread.sleep(5000);
//			}
//
//		} catch (Exception e) {
//			System.err.println("MessageServiceImpl consume error:"+e.getMessage());
//		}
//    }
//
//	private void recieveKafkaMsg(ConsumerIterator<String, String> it,List<String> propertyList)
//			throws Exception {
//		long i=0L;
//		long callCount=0L;
//		long contactCount=0L;
//		long blackCount=0L;
//		long applyCount=0L;
//		while (it.hasNext())
//		{
//			i++;
//			System.out.println("一次性消费第："+i+"条");
//			String strmsg = it.next().message();
//
////			System.out.println("logSession msg:"+strmsg);
//
//		    LogSession logSession=null;
//
//		    if(strmsg!=null&&!strmsg.equals("")){
//		    	logSession = JSON.parseObject(strmsg, LogSession.class);
//		    }
//
//		    if(logSession.getLogNo()!=null&&!logSession.getLogNo().equals("")){
//		    	String logNoString=logSession.getLogNo();
//				if(logNoString.equals("generalApply_CreditLoan")){
//					applyCount++;
//
//					Map<String,Object> maps= logSession.getFlatMap();
//					String mobile=maps.get("mobile")+"";
//					if (!mobile.equals("")) {
//						Set<String> mobileSet = new HashSet<String>();
//						if(maps.get("emergencymobile")!=null&&StringUtils.isNotEmpty(maps.get("emergencymobile").toString())){
//							mobileSet.add(maps.get("emergencymobile").toString());//紧急联系人
//						}
//						if(maps.get("hometel")!=null&&StringUtils.isNotEmpty(maps.get("hometel").toString())){
//							mobileSet.add(maps.get("hometel").toString());//紧急联系人
//						}
//						if(maps.get("contactmobile")!=null&&StringUtils.isNotEmpty(maps.get("contactmobile").toString())){
//							mobileSet.add(maps.get("contactmobile").toString());//紧急联系人
//						}
//						if(maps.get("channelmobile")!=null&&StringUtils.isNotEmpty(maps.get("channelmobile").toString())){
//							mobileSet.add(maps.get("channelmobile").toString());//紧急联系人
//						}
//						if(maps.get("partnercontantmobile")!=null&&StringUtils.isNotEmpty(maps.get("partnercontantmobile").toString())){
//							mobileSet.add(maps.get("partnercontantmobile").toString());//紧急联系人
//						}
//						if(maps.get("merchantmobile")!=null&&StringUtils.isNotEmpty(maps.get("merchantmobile").toString())){
//							mobileSet.add(maps.get("merchantmobile").toString());//紧急联系人
//						}
//						if(maps.get("recommend")!=null&&StringUtils.isNotEmpty(maps.get("recommend").toString())){
//							mobileSet.add(maps.get("recommend").toString());//紧急联系人
//						}
//
//						SaveApplyInfoHbase(mobile, mobileSet,maps);//保存hbase
//						System.out.println("一次性消费进件第："+applyCount+"条!"+"[orderno:"+maps.get("orderno")+"]");
//						SaveApplyInfoNeo4j(mobile,maps);
//					}
//
//				}
//				else if(logNoString.equals("BigDataCenter_LakalaApp_CallHistoryNeo4j")){
//					callCount++;
//
//					CollectData collectData = JSON.parseObject(JSON.toJSONString(logSession.getFlatMap()), CollectData.class);
//					List<CallHistory> list = MyUtil.jsonToBeanList(collectData.getMode());
//					Set<String> callMobileList = new HashSet<String>();
//					if (list != null) {// 将所有联系人添加到一个集合中
//						for (CallHistory model : list) {
//							if (model.getCaller_phone() != null&&!model.getCaller_phone().equals("")) {
//								callMobileList.add(model.getCaller_phone());
//							}
//						}
//					}
//					SaveMobileHbase(collectData.getLoginName(), callMobileList,DataTypeEnum.CALL_HISTORY);
//					System.out.println("一次性消费通话记录第："+callCount+"条!"+"[mobile:"+collectData.getLoginName()+"]");
//					SaveMobileNeo4J(collectData.getLoginName(),callMobileList,RelationType.CALL.getRelationType(),ModelType.MOBILE.getEntityGroup());
//
//				}
//				else if(logNoString.equals("BigDataCenter_LakalaApp_ContactListNeo4j")){
//					contactCount++;
//
//					CollectData collectData = JSON.parseObject(JSON.toJSONString(logSession.getFlatMap()), CollectData.class);
//					if (StringUtils.isNotEmpty(collectData.getLoginName())&& StringUtils.isNotEmpty(collectData.getMode())) {
//						List<Contact> list = null;
//						Set<String> contactMobileList = new HashSet<String>();
//						if (ConstantsConfig.OS_TYPE_ANDROID.equalsIgnoreCase(collectData.getPlatform())) {
//							list = JsonToBeanConverter.jsonToContact4Android(collectData.getMode());
//						} else {
//							list = JsonToBeanConverter.jsonToContact4IOS(collectData.getMode());
//						}
//						if (list != null) {// 将所有联系人添加到一个集合中
//							for (Contact model : list) {
//								if (model.getPhones() != null&&!model.getPhones().equals("")) {
//									contactMobileList.addAll(model.getPhones());
//								}
//							}
//						}
//						SaveMobileHbase(collectData.getLoginName(),contactMobileList, DataTypeEnum.CONTACT_LIST);
//						System.out.println("一次性消费通讯录第："+contactCount+"条!"+"[mobile:"+collectData.getLoginName()+"]");
//						SaveMobileNeo4J(collectData.getLoginName(),contactMobileList,RelationType.CONTACT.getRelationType(),ModelType.MOBILE.getEntityGroup());
//					}
//				} else if(logNoString.equals("RiskControl_Black_Manage")){
//					blackCount++;
//					System.out.println("一次性消费黑名单第："+blackCount+"条");
//					String blackstr=JSON.toJSONString(logSession.getFlatMap());
//					if(blackstr!=null&&!blackstr.equals("")){
//
//						Map<String, Object> blacklist=JSON.parseObject(blackstr.toString(),(new HashMap<>()).getClass());
//						Map<String, String> blackMap=blackModelTransferBlackMap(blacklist);
//
//						saveBlacklist(LakalaTable.TABLE_NAME_NEO4J_BLAKLIST,LakalaTable.TABLE_FAMILY,blackMap,propertyList);
//
//					}
//				}
//		    }
//		}
//	}
//
//	/**
//	 *
//	 * 功能描述：入库hbase
//	 * 1.首先根据电话查询hbase已主键rowkey查询已包含的关系
//	 * 2.set去掉hbase已有的关系
//	 * 3.创建新的关系
//	 * @author tianhuaxing 2016年12月26日 上午11:03:22
//	 * @since 1.0.0.000
//	 * @param mobile
//	 * @param set
//	 * @param _deviceid
//	 * @return
//	 */
//	public void SaveApplyInfoHbase(String mobile,Set<String> set,Map<String,Object> maps){
//		//创建手机号mobile和字段：emergencymobile,hometel,contactmobile,usercoremobile的关系
//		if(StringUtils.isNotBlank(mobile)){
//			Set<String> applyinfoListSet = HbaseUtilServiceImpl.get(mobile,DataTypeEnum.LOANMOBILE_LIST);
//			if(!CollectionUtils.isEmpty(set)){
//				set.removeAll(applyinfoListSet);
//				for (String content : set) {
//					if(StringUtils.isNotBlank(content)){
//						HbaseUtilServiceImpl.put(mobile,"",DataTypeEnum.LOANMOBILE_LIST, content);
//					}
//				}
//			}
//			//通过orderno插入hbase一个记录
//			String orderNo=maps.get("orderno")+"";
//			if(StringUtils.isNotBlank(orderNo)){
//				Set<String> applyinfoSet = HbaseUtilServiceImpl.get(orderNo,DataTypeEnum.APPLYINFO_ORDERNO);
//				if(CollectionUtils.isEmpty(applyinfoSet)){
//					HbaseUtilServiceImpl.put(orderNo,orderNo,DataTypeEnum.APPLYINFO_ORDERNO, JSON.toJSONString(maps));
//				}
//			}
//		}
//		//进件手机号码字段：mobile
//		//取出联系人字段hbase保存的列applyinfo_手机号，字段：emergencymobile,hometel,contactmobile,usercoremobile
//		//取出设备id保存到neo4j:_deviceid
//	}
//
//
//	/**
//	 *
//	 * 功能描述
//	 * 创建手机号实体
//	 * @author tianhuaxing 2016年12月26日 上午11:10:28
//	 * @since 1.0.0.000
//	 * @param mobile
//	 * @param set
//	 * @param _deviceid
//	 * @return
//	 */
//	public void SaveApplyInfoNeo4j(String mobile,Map<String,Object> maps){
//
//		StringBuilder sbApply=new StringBuilder();
//		//MERGE (p)-[r:"+RelationType.LOANAPPLY+"]->(m)
//		StringBuilder sbMobileAndRelation=new StringBuilder();
//		sbApply.append("merge (m:"+ModelType.APPLYINFO.getModelName()+" {orderno:'"+maps.get("orderno")+"'}) set m.modelname='"+ModelType.APPLYINFO.getModelName()+"',m.orderno='"+maps.get("orderno")+"',m.group='"+ModelType.APPLYINFO.getEntityGroup()+"',");
//		int count=maps.size();
//		int i=0;
//		String keyString=null;
//		String valueString=null;
//		for(Map.Entry<String, Object> map:maps.entrySet()){
//			i++;
//			keyString=map.getKey();
//			valueString=(map.getValue()==null?null:map.getValue().toString());
//			switch (keyString) {
//			case "mobile":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (a:"+ModelType.MOBILE.getModelName()+" {content:'"+valueString+"'}) set a.modelname='"+ModelType.MOBILE.getModelName()+"',a.content='"+valueString+"',a.group='"+ModelType.MOBILE.getEntityGroup()+"' merge (m)-[:"+RelationType.APPLYMYMOBILE.getRelationType()+"]->(a)");
//				}
//				break;
//			case "recommend":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (a1:"+ModelType.MOBILE.getModelName()+" {content:'"+valueString+"'}) set a1.modelname='"+ModelType.MOBILE.getModelName()+"',a1.content='"+valueString+"',a1.group='"+ModelType.MOBILE.getEntityGroup()+"' merge (m)-[:"+RelationType.RECOMMEND.getRelationType()+"]->(a1)");
//				}
//				break;
//			case "emergencymobile":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (b:"+ModelType.MOBILE.getModelName()+" {content:'"+valueString+"'}) set b.modelname='"+ModelType.MOBILE.getModelName()+"',b.content='"+valueString+"',b.group='"+ModelType.MOBILE.getEntityGroup()+"' merge (m)-[:"+RelationType.EMERGENCYMOBILE.getRelationType()+"]->(b)");
//				}
//				break;
//			case "hometel":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (c:"+ModelType.MOBILE.getModelName()+" {content:'"+valueString+"'}) set c.modelname='"+ModelType.MOBILE.getModelName()+"',c.content='"+valueString+"',c.group='"+ModelType.MOBILE.getEntityGroup()+"' merge (m)-[:"+RelationType.HOMETEL.getRelationType()+"]->(c)");
//
//				}
//				break;
//			case "contactmobile":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (d:"+ModelType.MOBILE.getModelName()+" {content:'"+valueString+"'}) set d.modelname='"+ModelType.MOBILE.getModelName()+"',d.content='"+valueString+"',d.group='"+ModelType.MOBILE.getEntityGroup()+"' merge (m)-[:"+RelationType.RELATIVEMOBILE.getRelationType()+"]->(d)");
//
//				}
//				break;
//			case "usercoremobile":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (e:"+ModelType.MOBILE.getModelName()+" {content:'"+valueString+"'}) set e.modelname='"+ModelType.MOBILE.getModelName()+"',e.content='"+valueString+"',e.group='"+ModelType.MOBILE.getEntityGroup()+"' merge (m)-[:"+RelationType.LOGINMOBILE.getRelationType()+"]->(e)");
//
//				}
//				break;
//			case "channelmobile":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (f:"+ModelType.MOBILE.getModelName()+" {content:'"+valueString+"'}) set f.modelname='"+ModelType.MOBILE.getModelName()+"',f.content='"+valueString+"',f.group='"+ModelType.MOBILE.getEntityGroup()+"' merge (m)-[:"+RelationType.CHANNELMOBILE.getRelationType()+"]->(f)");
//
//				}
//				break;
//			case "partnercontantmobile":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (g:"+ModelType.MOBILE.getModelName()+" {content:'"+valueString+"'}) set g.modelname='"+ModelType.MOBILE.getModelName()+"',g.content='"+valueString+"',g.group='"+ModelType.MOBILE.getEntityGroup()+"' merge (m)-[:"+RelationType.MERCHANTMOBILE.getRelationType()+"]->(g)");
//
//				}
//				break;
//			case "merchantmobile":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (h:"+ModelType.MOBILE.getModelName()+" {content:'"+valueString+"'}) set h.modelname='"+ModelType.MOBILE.getModelName()+"',h.content='"+valueString+"',h.group='"+ModelType.MOBILE.getEntityGroup()+"' merge (m)-[:"+RelationType.MERCHANTMOBILE.getRelationType()+"]->(h)");
//
//				}
//				break;
//			case "debitcard":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (i:"+ModelType.BANKCARD.getModelName()+" {content:'"+valueString+"'}) set i.modelname='"+ModelType.BANKCARD.getModelName()+"',i.content='"+valueString+"',i.group='"+ModelType.BANKCARD.getEntityGroup()+"' merge (m)-[:"+RelationType.BANKCARD.getRelationType()+"]->(i)");
//
//				}
//				break;
//			case "certno":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (j:"+ModelType.IDENTIFICATION.getModelName()+" {content:'"+valueString+"'}) set j.modelname='"+ModelType.IDENTIFICATION.getModelName()+"',j.content='"+valueString+"',j.group='"+ModelType.IDENTIFICATION.getEntityGroup()+"' merge (m)-[:"+RelationType.IDENTIFICATION.getRelationType()+"]->(j)");
//
//				}
//				break;
//
//			case "creditcard":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (k:"+ModelType.BANKCARD.getModelName()+" {content:'"+valueString+"'}) set k.modelname='"+ModelType.BANKCARD.getModelName()+"',k.content='"+valueString+"',k.group='"+ModelType.BANKCARD.getEntityGroup()+"' merge (m)-[:"+RelationType.CREDITCARD.getRelationType()+"]->(k)");
//
//				}
//				break;
//			case "_deviceid":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (l:"+ModelType.DEVICE.getModelName()+" {content:'"+valueString+"'}) set l.modelname='"+ModelType.DEVICE.getModelName()+"',l.content='"+valueString+"',l.group='"+ModelType.DEVICE.getEntityGroup()+"' merge (m)-[:"+RelationType.DEVICE.getRelationType()+"]->(l)");
//					if(i<count){
//						sbApply.append(",");
//					}
//				}
//				break;
//
//			case "ipv4":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (n:"+ModelType.IPV4.getModelName()+" {content:'"+valueString+"'}) set n.modelname='"+ModelType.IPV4.getModelName()+"',n.content='"+valueString+"',n.group='"+ModelType.IPV4.getEntityGroup()+"' merge (m)-[:"+RelationType.IPV4.getRelationType()+"]->(n)");
//
//				}
//				break;
////			case "lbs":
////				if(valueString!=null&&!"".equals(valueString))
////				{
////					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
////					sbMobileAndRelation.append(" merge (o:"+ModelType.LBS.getModelName()+" {content:'"+valueString+"'}) set o.modelname='"+ModelType.LBS.getModelName()+"',o.content='"+valueString+"',o.group='"+ModelType.LBS.getEntityGroup()+"' merge (m)-[:"+RelationType.LBS.getRelationType()+"]->(o)");
////
////				}
////				break;
//			case "companyname":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (p:"+ModelType.COMPANY.getModelName()+" {content:'"+valueString+"'}) set p.modelname='"+ModelType.COMPANY.getModelName()+"', p.content='"+valueString+"',p.group='"+ModelType.COMPANY.getEntityGroup()+"' merge (m)-[:"+RelationType.COMPANY.getRelationType()+"]->(p)");
//
//				}
//				break;
//			case "companyaddress":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (q:"+ModelType.COMPANYADDRESS.getModelName()+" {content:'"+valueString+"'}) set q.modelname='"+ModelType.COMPANYADDRESS.getModelName()+"',q.content='"+valueString+"',q.group='"+ModelType.COMPANYADDRESS.getEntityGroup()+"' merge (m)-[:"+RelationType.COMPANYADDRESS.getRelationType()+"]->(q)");
//				}
//				break;
//			case "companytel":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString()).replace("|", "")+"',");
//					sbMobileAndRelation.append(" merge (s:"+ModelType.COMPANYTEL.getModelName()+" {content:'"+valueString.replace("|", "")+"'}) set s.modelname='"+ModelType.COMPANYTEL.getModelName()+"',s.content='"+valueString.replace("|", "")+"',s.group='"+ModelType.COMPANYTEL.getEntityGroup()+"' merge (m)-[:"+RelationType.COMPANYTEL.getRelationType()+"]->(s)");
//				}
//				break;
//
//			case "imei":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (t:"+ModelType.MOBILEIMEI.getModelName()+" {content:'"+valueString+"'}) set t.modelname='"+ModelType.MOBILEIMEI.getModelName()+"',t.content='"+valueString+"',t.group='"+ModelType.MOBILEIMEI.getEntityGroup()+"' merge (m)-[:"+RelationType.MOBILEIMEI.getRelationType()+"]->(t)");
//
//				}
//				break;
//			case "email":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (u:"+ModelType.EMAIL.getModelName()+" {content:'"+valueString+"'}) set u.modelname='"+ModelType.EMAIL.getModelName()+"',u.content='"+valueString+"',u.group='"+ModelType.EMAIL.getEntityGroup()+"' merge (m)-[:"+RelationType.EMAIL.getRelationType()+"]->(u)");
//
//				}
//				break;
//			case "termid":
//				if(valueString!=null&&!"".equals(valueString))
//				{
//					sbApply.append("m."+keyString+"='"+(map.getValue()==null?null:map.getValue().toString())+"',");
//					sbMobileAndRelation.append(" merge (v:"+ModelType.TERMINAL.getModelName()+" {content:'"+valueString+"'}) set v.modelname='"+ModelType.TERMINAL.getModelName()+"',v.content='"+valueString+"',v.group='"+ModelType.TERMINAL.getEntityGroup()+"' merge (m)-[:"+RelationType.TERMINAL.getRelationType()+"]->(v)");
//
//				}
//				break;
//			default:
//				break;
//			}
//		}
//		String applysql=sbApply.toString().substring(0, sbApply.length()-1);//�����һ������ȥ��
//		StringBuilder sbBuilder=new StringBuilder();
//		sbBuilder.append(applysql);
//		sbBuilder.append(sbMobileAndRelation);
////		System.out.println(sbBuilder.toString());
//		Neo4jUtilServiceImpl.executeInsert(sbBuilder.toString());
//	}
//
//
//	/**
//	 *
//	 * 功能描述：入库hbase
//	 * 1.首先查询电话查询的查询hbase已主键rowkey查询包含的关系
//	 * 2.set去掉hbase已有的关系
//	 * 3.创建新的关系
//	 *
//	 * @author tianhuaxing 2016年12月22日 下午5:20:46
//	 * @since 1.0.0.000
//	 * @param mobile
//	 * @param contactList
//	 * @param dataTypeEnum
//	 * @return
//	 */
//	public void SaveMobileHbase(String mobile, Set<String> set,DataTypeEnum dataTypeEnum) throws Exception{
//		if ("contactlist".equals(dataTypeEnum.getDataType())) {
//			Set<String> contactlistSet = HbaseUtilServiceImpl.get(mobile,DataTypeEnum.CONTACT_LIST);
//			if (!CollectionUtils.isEmpty(set)) {
//				set.removeAll(contactlistSet);
//				for (String contact : set) {
//					if(StringUtils.isNotBlank(contact)){
//						HbaseUtilServiceImpl.put(mobile,contact,DataTypeEnum.CONTACT_LIST, contact);
//					}
//				}
//			}
//		} else if ("callhistory".equals(dataTypeEnum.getDataType())) {
//			Set<String> callhistorySet = HbaseUtilServiceImpl.get(mobile,DataTypeEnum.CALL_HISTORY);
//			if (!CollectionUtils.isEmpty(set)) {
//				set.removeAll(callhistorySet);
//				for (String callhistory : set) {
//					if(StringUtils.isNotBlank(callhistory)){
//						HbaseUtilServiceImpl.put(mobile,callhistory,DataTypeEnum.CALL_HISTORY, callhistory);
//					}
//				}
//			}
//		}
//	}
//
//	/**
//	 * 功能描述：入NEO4j  1.首先查询neo4j里面有没有该实体
//	 *                2.然后建立实体
//	 *                3.建立关系
//	 *
//	 * @author tianhuaxing 2016年12月25日 上午10:52:54
//	 * @since 1.0.0.000
//	 * @param mobile
//	 * @param set
//	 * @param dataTypeEnu
//	 */
//	public void SaveMobileNeo4J(String mobile,Set<String> set,String relationType,String group) {
//		   String  sql="";
//		   MobileV mobileV=new MobileV();
//		   mobileV.setMobile(mobile);
//		   mobileV.setGroup(group);
//		   //ֱ�Ӵ�����ϵ
//		   StringBuilder sbSql=new StringBuilder();
//
////		   List<String> sqlList=new ArrayList<String>();
//		   if (!CollectionUtils.isEmpty(set)) {
////			   Neo4jUtilServiceImpl.executeInsert("merge (p:"+ModelType.MOBILE.getModelName()+" { content:'"+mobile+"'}) set p.modelname='"+ModelType.MOBILE.getModelName()+"',p.content='"+mobile+"',p.group='"+group+"' ");
//			   int i=0;
//			   for (String phone : set) {
//				   i++;
//				   if(StringUtils.isNotBlank(phone)){
//					   if(!phone.trim().equals(mobile.trim()))
//					   {
//						   sql="merge (p"+i+":"+ModelType.MOBILE.getModelName()+" { content:'"+mobile+"'}) set p"+i+".modelname='"+ModelType.MOBILE.getModelName()+"',p"+i+".content='"+mobile+"',p"+i+".group='"+group+"' merge (m"+i+":"+ModelType.MOBILE.getModelName()+" {content:'"+phone+"'}) set m"+i+".modelname='"+ModelType.MOBILE.getModelName()+"',m"+i+".content='"+phone+"',m"+i+".group='"+group+"' merge (p"+i+")-[r"+i+":"+relationType+"]->(m"+i+")";
////						   sqlList.add(sql);
//						   sbSql.append(sql); //����sql��ϳ�stringд��
//						   sbSql.append("\n");
//						   if(i==50){
//							   Neo4jUtilServiceImpl.executeInsert(sbSql.toString());
//							   sbSql=new StringBuilder();
//							   i=0;
//						   }
//
//					   }
//				   }
//
//			}
////			   System.out.println(sbSql.toString());
//			   Neo4jUtilServiceImpl.executeInsert(sbSql.toString());
////			   Neo4jUtilServiceImpl.batchInsert(sqlList);
//		}
//	}
//
//	/**
//	 * 黑名单保存
//	 * 名单内容类型：1-手机号；2-银行卡；3-身份证；4-设备指纹；5-邮箱；6-终端号；7-住址；
//	 * 8-手机IMEI；9-LBS；10-单名；11-固话；12-单址
//	 * @param contenttype
//	 * @param content
//	 */
//	private boolean saveBlacklist(String tableName,String family,Map<String, String> map,List<String> propertyList) {
//
//		StringBuilder sbstr=new StringBuilder();
//
//		for(Map.Entry<String, String> entry:map.entrySet()){
//			if(entry.getValue()!=null&&!"".equals(entry.getValue()))
//			{
//
//				Object obj=map.get(entry.getKey());
//				if(propertyList.contains(entry.getKey().toLowerCase())){ //�ж��Ƿ�����Ҫд��neo4j
//					sbstr.append("m."+entry.getKey().toLowerCase()+"='"+obj.toString()+"',");
//				}
//			}
//
//		}
//		/**
//		 * 1,代表黑名单，2，代表灰名单，3，代表白名单
//		 */
//		Object typestr=map.get("type");
//
//		Object contenttype=map.get("contenttype");
//		String content=map.get("content");
//		HbaseUtilServiceImpl.put(LakalaTable.TABLE_NAME_NEO4J_BLAKLIST, LakalaTable.TABLE_FAMILY,content,map);
//		if("1".equals(typestr.toString())){
//			blacklistSaveType(contenttype.toString(), content,sbstr.toString(),BlackType.BLACK.getEntityGroup());
//		}
//		else if("2".equals(typestr.toString())){
//			blacklistSaveType(contenttype.toString(), content,sbstr.toString(),BlackType.GRAY.getEntityGroup());
//		}
//		else  if("3".equals(typestr.toString())){
//			blacklistSaveType(contenttype.toString(), content,sbstr.toString(),BlackType.WHITE.getEntityGroup());
//		}
//		return true;
//
//	}
//
//
//	/**
//	 * 黑名单保存
//	 * 名单内容类型：1-手机号；2-银行卡；3-身份证；4-设备指纹；5-邮箱；6-终端号；7-住址；
//	 * 8-手机IMEI；9-LBS；10-单名；11-固话；12-单址
//	 * @param contenttype
//	 * @param content
//	 */
//	private void blacklistSaveType(String contenttype, String content,String sbstr,String type) {
//		String sql=null;
//		switch (contenttype) {
//		case "1": //手机号
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.MOBILE.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.MOBILE.getModelName()+"',"+sbstr+" m.group='"+ModelType.MOBILE.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "2"://银行卡
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.BANKCARD.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.BANKCARD.getModelName()+"',"+sbstr+" m.group='"+ModelType.BANKCARD.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "3"://身份证
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.IDENTIFICATION.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.IDENTIFICATION.getModelName()+"',"+sbstr+" m.group='"+ModelType.IDENTIFICATION.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "4"://设备指纹
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.DEVICE.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.DEVICE.getModelName()+"',"+sbstr+" m.group='"+ModelType.DEVICE.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "5"://邮箱
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.EMAIL.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.EMAIL.getModelName()+"',"+sbstr+" m.group='"+ModelType.EMAIL.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "6"://端口号
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.TERMINAL.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.TERMINAL.getModelName()+"',"+sbstr+" m.group='"+ModelType.TERMINAL.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "7"://住址
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.HOUSEADDRESS.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.HOUSEADDRESS.getModelName()+"',"+sbstr+" m.group='"+ModelType.HOUSEADDRESS.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "8"://手机imei
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.MOBILEIMEI.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.MOBILEIMEI.getModelName()+"',"+sbstr+" m.group='"+ModelType.MOBILEIMEI.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "9"://LBS
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.LBS.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.LBS.getModelName()+"',"+sbstr+" m.group='"+ModelType.LBS.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "10"://单位名称
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.COMPANY.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.COMPANY.getModelName()+"',"+sbstr+" m.group='"+ModelType.COMPANY.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "11"://固定电话
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.FIXEDPHONE.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.FIXEDPHONE.getModelName()+"',"+sbstr+" m.group='"+ModelType.FIXEDPHONE.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//		case "12"://单位住址
//			if (StringUtils.isNotBlank(content)) {
//				sql="merge (m:"+ModelType.COMPANYADDRESS.getModelName()+" {content:'"+content+"'}) set  m.modelname='"+ModelType.COMPANYADDRESS.getModelName()+"',"+sbstr+" m.group='"+ModelType.COMPANYADDRESS.getEntityGroup()+"',m.type='"+type+"'";
//				Neo4jUtilServiceImpl.executeInsert(sql);
//			}
//			break;
//
//		default:
//			logger.error("saveBlacklist error:"+"{\"code\": \"00\", \"message\": \"请求成功,请求参数错误或者请求值为空\"}");
//			break;
//		}
//	}
//
//
//	private Map<String, String> blackModelTransferBlackMap(Map<String, Object> blacklist){
////		propertyList
//		Map<String, String> blackMap=new HashMap<String,String>();
//		if(blacklist.containsKey("type")&&blacklist.get("type")!=null&&!blacklist.get("type").equals("")){
//			blackMap.put("type", blacklist.get("type").toString());
//		}
//		if(blacklist.containsKey("contentType")&&blacklist.get("contentType")!=null&&!blacklist.get("contentType").equals("")){
//			blackMap.put("contenttype", blacklist.get("contentType").toString());
//		}
//
//		if(blacklist.containsKey("content")&&blacklist.get("content")!=null&&!blacklist.get("content").equals("")){
//			blackMap.put("content", blacklist.get("content").toString());
//		}
//		if(blacklist.containsKey("validTime")&&blacklist.get("validTime")!=null&&!blacklist.get("validTime").equals("")){
//			blackMap.put("validtime", blacklist.get("validTime").toString());
//		}
//
//
//		if(blacklist.containsKey("listDesc")&&blacklist.get("listDesc")!=null&&!blacklist.get("listDesc").equals("")){
//			blackMap.put("list_desc", blacklist.get("listDesc").toString());
//		}
//
//		if(blacklist.containsKey("useRange")&&blacklist.get("useRange")!=null&&!blacklist.get("useRange").equals("")){
//			blackMap.put("userange", blacklist.get("useRange").toString());
//		}
//
//
//		if(blacklist.containsKey("createrTime")&&blacklist.get("createrTime")!=null&&!blacklist.get("createrTime").equals("")){
//			blackMap.put("createtime", blacklist.get("createrTime").toString());
//		}
//
//		if(blacklist.containsKey("modifyTime")&&blacklist.get("modifyTime")!=null&&!blacklist.get("modifyTime").equals("")){
//			blackMap.put("modifytime", blacklist.get("modifyTime").toString());
//		}
//
//		if(blacklist.get("creatorId")!=null&&!blacklist.get("creatorId").equals("")){
//			blackMap.put("creatorid", blacklist.get("creatorId").toString());
//		}
//		if(blacklist.containsKey("creatorName")&&blacklist.get("creatorName")!=null&&!blacklist.get("creatorName").equals("")){
//			blackMap.put("creatorname", blacklist.get("creatorName").toString());
//		}
//
//		if(blacklist.containsKey("modifierId")&&blacklist.get("modifierId")!=null&&!blacklist.get("modifierId").equals("")){
//			blackMap.put("modifierid", blacklist.get("modifierId")+"");
//		}
//		if(blacklist.containsKey("modifierName")&&blacklist.get("modifierName")!=null&&!blacklist.get("modifierName").equals("")){
//			blackMap.put("modifiername", blacklist.get("modifierName")+"");
//		}
//
//		if(blacklist.containsKey("modifyTime")&&blacklist.get("modifyTime")!=null&&!blacklist.get("modifyTime").equals("")){
//			blackMap.put("modifytime", blacklist.get("modifyTime").toString());
//		}
//
//		if(blacklist.containsKey("origin")&&blacklist.get("origin")!=null&&!blacklist.get("origin").equals("")){
//			blackMap.put("origin", blacklist.get("origin").toString());
//		}
//
//		if(blacklist.containsKey("riskType")&&blacklist.get("riskType")!=null&&!blacklist.get("riskType").equals("")){
//			blackMap.put("risktype", blacklist.get("riskType").toString());
//		}
//
//		if(blacklist.containsKey("listType")&&blacklist.get("listType")!=null&&!blacklist.get("listType").equals("")){
//			blackMap.put("list_Type", blacklist.get("listType").toString());
//		}
//
//		return blackMap;
//	}
//
//}
