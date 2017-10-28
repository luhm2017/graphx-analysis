/**
 * Kafka服务器连接参数
 * Created by Administrator on 2017/6/21 0021.
 */

public class KafkaBrokerInfo {
    // 主机名
    public final String brokerHost;
    // 端口号
    public final int brokerPort;

    /**
     * 构造方法
     *
     * @param brokerHost Kafka服务器主机或者IP地址
     * @param brokerPort 端口号
     */
    public KafkaBrokerInfo(String brokerHost, int brokerPort) {
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    /**
     * 构造方法， 使用默认端口号9092进行构造
     *
     * @param brokerHost
     */
    public KafkaBrokerInfo(String brokerHost) {
        this(brokerHost, 9092);
    }
}