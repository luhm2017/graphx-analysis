package com.lakala.audit.rabbitmqMsg.produce;

import com.google.gson.Gson;
import com.lakala.audit.rabbitmqMsg.entityV.RequestMessageV;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


/**
 * Created by Administrator on 2017/8/1 0001.
 */
public class Sender {
    private final static String AUDIT_QUEUE_NAME = "audit_mq";
    //    private final static String USERNAME = "lys";
//    private final static String PASSWORD = "123456";
//    private final static String VIRTUALHOST = "/";
    //    private final static String HOST = "localhost";

    private final static String HOST = "192.168.0.182";
    private final static String USERNAME = "antifraud";
    private final static String PASSWORD = "antifraud";
    private final static String VIRTUALHOST = "antifraud";
    private final static int PORTNUMBER = 5672;

    public static void main(String[] args) {
        Gson gson = new Gson();
        RequestMessageV requestMessageV = new RequestMessageV("XNA20170505131153011496369566130", "Q");
        String message = gson.toJson(requestMessageV);
        System.out.println(message);
        //message={"orderno":"XNA20170505131153011496369566130","statue":"Q"}
        try {
            send(message);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }

    public static void send(String message) throws java.io.IOException,
            java.lang.InterruptedException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORTNUMBER);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUALHOST);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(AUDIT_QUEUE_NAME, false, false, false, null);
        channel.basicPublish("", AUDIT_QUEUE_NAME, null, message.getBytes("UTF-8"));
        System.out.println("已经发送消息....." + message);
        channel.close();
        connection.close();
    }
}
