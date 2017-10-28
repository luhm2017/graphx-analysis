package com.lakala.audit.rabbitmqMsg.entityV;

/**
 * Created by Administrator on 2017/8/1 0001.
 */
public class RequestMessageV {
    public RequestMessageV() {
    }

    public RequestMessageV(String orderno, String statue) {
        this.orderno = orderno;
        this.statue = statue;
    }

    String orderno;
    String statue;

    public String getOrderno() {
        return orderno;
    }

    public void setOrderno(String orderno) {
        this.orderno = orderno;
    }

    public String getStatue() {
        return statue;
    }

    public void setStatue(String statue) {
        this.statue = statue;
    }
}
