/**
 * Created by Administrator on 2017/6/16 0016.
 */
interface DataInterface{}

public enum DataAttributeType implements DataInterface {
    ORDERID(1, "orderid"), CONTRACTNO(2, "contractno"), TERMID(3, "termid"), LOANPAN(4, "loanpan"), RETURNPAN(5, "returnpan"),
    INSERTTIME(6, "inserttime"), RECOMMEND(7, "recommend"), USERID(8, "userid"), DEVICEID(9, "deviceid"),
    CERTNO(10, "certno"), EMAIL(11, "email"), COMPANY(12, "company"), MOBILE(13, "mobile"), COMPADDR(14, "compaddr"),
    COMPPHONE(15, "compphone"), EMERGENCYCONTACTMOBILE(16, "emergencycontactmobile"),
    CONTACTMOBILE(17, "contactmobile"), IPV4(18, "ipv4"), MSGPHONE(19, "msgphone"), TELECODE(20, "telecode");
    //成员变量
    private int sequence;
    private String name;

    //构造方法
    private DataAttributeType(int sequence, String name) {
        this.sequence = sequence;
        this.name = name;
    }

    //自定义方法
    public static String getColorName(int sequence) {
        for (DataAttributeType c : DataAttributeType.values()) {
            if (c.getSequence() == sequence)
                return c.name;
        }
        return null;
    }

    //getter&setter
    public int getSequence() {
        return sequence;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

