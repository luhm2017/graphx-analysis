//import com.lakala.cheatservice.constant.BizTypeEnum;
//import com.lakala.cheatservice.constant.ChannelEnum;
//import com.lakala.cheatservice.constant.OperateTypeEnum;
//
//import java.lang.management.ManagementFactory;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.*;
//
//
///**
// * <P>日志采集对象</P>
// *
// * @author tianhuaxing 2016年9月19日 下午5:15:05
// * @since 1.0.0.000
// */
//public class LogSession {
//
//    private static final String PROTOCOL_VERSION = "1.0.4";
//
//    /**
//     * 主机IP
//     */
//    private String hostIp;
//
//    /**
//     * 主机名
//     */
//    private String hostName;
//
//    /**
//     * 系统编号
//     */
//    private String systemCode;
//
//    /**
//     * 渠道枚举
//     */
//    private ChannelEnum channelEnum;
//
//    /**
//     * 渠道枚举id
//     */
//    private String channelId;
//
//    /**
//     * 商户号
//     */
//    private String merchantNo;
//
//    /**
//     * 日志编号
//     */
//    private String logNo;
//
//    /**
//     * 外部业务号唯一标识
//     */
//    private String sessionId;
//
//    /**
//     * 内部业务号
//     * 增加内部业务号，来唯一标识一条日志，
//     * 此内部业务号在业务系统客户端生成，供日志对账等使用
//     */
//    private String innerSessionId;
//
//    /**
//     * 业务参数
//     */
//    private String logConent;
//
//    /**
//     * 进程
//     */
//    private String process;
//
//    /**
//     * 日志采集时间
//     */
//    private Date collectTime;
//
//    /**
//     * 日志接收时间（日志接收处理时间）
//     */
//    private Date receiveTime;
//
//    /**
//     * 产品编号
//     */
//    private String productNo;
//
//    /**
//     * 业务类型枚举
//     */
//    private BizTypeEnum bizTypeEnum;
//
//    /**
//     * 业务类型
//     */
//    private String bizType;
//
//    /**
//     * 操作类型枚举
//     */
//    private OperateTypeEnum operateTypeEnum;
//
//    /**
//     * 操作类型
//     */
//    private String operateType;
//
//    /**
//     * 业务标识
//     */
//    private String bizNo;
//
//    /**
//     * 模块标识
//     */
//    private String modelNo;
//
//    /**
//     * 协议版本号
//     */
//    private String protocolVersion = PROTOCOL_VERSION;
//
//    /**
//     * 业务参数集合
//     */
//    private Map<String, Object> map;
//
//    /**
//     * 业务补充集合
//     */
//    private Map<String, Object> extendMap = new HashMap<>();
//
//    /**
//     * 应用单层业务参数集合
//     */
//    private Map<String, Object> flatMap;
//
//    /**
//     * 应用单层脱敏业务参数集合
//     */
//    private Map<String, Object> desensitizeFlatMap;
//
//    /**
//     * 业务补充字符串,与extendMap的值对应
//     */
//    private String extendMapLogContent;
//
//    /**
//     * 应用一层业务参数字符串,与flatMap的值对应
//     */
//    private String flatMapLogContext;
//
//    /**
//     * 域集合
//     */
//    private Collection<Field> fields = new HashSet<Field>();
//
//
//    public String getHostIp() {
//        return hostIp;
//    }
//
//    public void setHostIp(String hostIp) {
//        this.hostIp = hostIp;
//    }
//
//    public String getHostName() {
//        return hostName;
//    }
//
//    public void setHostName(String hostName) {
//        this.hostName = hostName;
//    }
//
//    public String getSystemCode() {
//        return systemCode;
//    }
//
//    public void setSystemCode(String systemCode) {
//        this.systemCode = systemCode;
//    }
//
//    public ChannelEnum getChannelEnum() {
//        return channelEnum;
//    }
//
//    public void setChannelEnum(ChannelEnum channelEnum) {
//        this.channelEnum = channelEnum;
//    }
//
//    public String getLogNo() {
//        return logNo;
//    }
//
//    public void setLogNo(String logNo) {
//        this.logNo = logNo;
//    }
//
//    public String getSessionId() {
//        return sessionId;
//    }
//
//    public void setSessionId(String sessionId) {
//        this.sessionId = sessionId;
//    }
//
//    public String getInnerSessionId() {
//        return innerSessionId;
//    }
//
//    public void setInnerSessionId(String innerSessionId) {
//        this.innerSessionId = innerSessionId;
//    }
//
//    public String getLogConent() {
//        return logConent;
//    }
//
//    public void setLogConent(String logConent) {
//        this.logConent = logConent;
//    }
//
//    public Map<String, Object> getMap() {
//        return map;
//    }
//
//    public void setMap(Map<String, Object> map) {
//        this.map = map;
//    }
//
//    public String getProcess() {
//        return process;
//    }
//
//    public void setProcess(String process) {
//        this.process = process;
//    }
//
//    public Date getCollectTime() {
//        return collectTime;
//    }
//
//    public void setCollectTime(Date collectTime) {
//        this.collectTime = collectTime;
//    }
//
//    public String getMerchantNo() {
//        return merchantNo;
//    }
//
//    public void setMerchantNo(String merchantNo) {
//        this.merchantNo = merchantNo;
//    }
//
//    public String getProductNo() {
//        return productNo;
//    }
//
//    public void setProductNo(String productNo) {
//        this.productNo = productNo;
//    }
//
//    public Map<String, Object> getExtendMap() {
//        return extendMap;
//    }
//
//    public void setExtendMap(Map<String, Object> extendMap) {
//        this.extendMap = extendMap;
//    }
//
//    public Date getReceiveTime() {
//        return receiveTime;
//    }
//
//    public void setReceiveTime(Date receiveTime) {
//        this.receiveTime = receiveTime;
//    }
//
//    public Map<String, Object> getFlatMap() {
//        return flatMap;
//    }
//
//    public void setFlatMap(Map<String, Object> flatMap) {
//        this.flatMap = flatMap;
//    }
//
//    public String getExtendMapLogContent() {
//        return extendMapLogContent;
//    }
//
//    public void setExtendMapLogContent(String extendMapLogContent) {
//        this.extendMapLogContent = extendMapLogContent;
//    }
//
//    public String getFlatMapLogContext() {
//        return flatMapLogContext;
//    }
//
//    public void setFlatMapLogContext(String flatMapLogContext) {
//        this.flatMapLogContext = flatMapLogContext;
//    }
//
//    public String getChannelId() {
//        return channelId;
//    }
//
//    public void setChannelId(String channelId) {
//        this.channelId = channelId;
//    }
//
//    public BizTypeEnum getBizTypeEnum() {
//        return bizTypeEnum;
//    }
//
//    public void setBizTypeEnum(BizTypeEnum bizTypeEnum) {
//        this.bizTypeEnum = bizTypeEnum;
//    }
//
//    public String getBizType() {
//        return bizType;
//    }
//
//    public void setBizType(String bizType) {
//        this.bizType = bizType;
//    }
//
//    public OperateTypeEnum getOperateTypeEnum() {
//        return operateTypeEnum;
//    }
//
//    public void setOperateTypeEnum(OperateTypeEnum operateTypeEnum) {
//        this.operateTypeEnum = operateTypeEnum;
//    }
//
//    public String getOperateType() {
//        return operateType;
//    }
//
//    public void setOperateType(String operateType) {
//        this.operateType = operateType;
//    }
//
//    public String getBizNo() {
//        return bizNo;
//    }
//
//    public void setBizNo(String bizNo) {
//        this.bizNo = bizNo;
//    }
//
//    public String getModelNo() {
//        return modelNo;
//    }
//
//    public void setModelNo(String modelNo) {
//        this.modelNo = modelNo;
//    }
//
//    public Collection<Field> getFields() {
//        return fields;
//    }
//
//    public void setFields(Collection<Field> fields) {
//        this.fields = fields;
//    }
//
//
//    public String getProtocolVersion() {
//        return protocolVersion;
//    }
//
//    public void setProtocolVersion(String protocolVersion) {
//        this.protocolVersion = protocolVersion;
//    }
//
//    public Map<String, Object> getDesensitizeFlatMap() {
//        return desensitizeFlatMap;
//    }
//
//    public void setDesensitizeFlatMap(Map<String, Object> desensitizeFlatMap) {
//        this.desensitizeFlatMap = desensitizeFlatMap;
//    }
//
//    /**
//     * 增加域
//     *
//     * @param field
//     * @author niezhili 2016年11月8日 下午2:45:29
//     * @since 1.0.0.000
//     */
//    public void addField(Field field) {
//        if (field == null) return;
//        if (fields.contains(field)) {
//            throw new RuntimeException("已经存在相同的字段对象,字段名=" + field.getName());
//        } else {
//            fields.add(field);
//        }
//    }
//
//
//    public LogSession() {
//        process = ManagementFactory.getRuntimeMXBean().getName();
//        try {
//            hostName = InetAddress.getLocalHost().getHostName();
//            hostIp = InetAddress.getLocalHost().getHostAddress();
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//    }
//}