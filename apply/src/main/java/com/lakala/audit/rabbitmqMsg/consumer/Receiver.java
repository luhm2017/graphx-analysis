package com.lakala.audit.rabbitmqMsg.consumer;

import com.google.gson.Gson;
import com.lakala.audit.rabbitmqMsg.entityV.RequestMessageV;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by Administrator on 2017/8/1 0001.
 */
public class Receiver {
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
        try {
            work();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }

    public static void work() throws java.io.IOException,
            java.lang.InterruptedException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("192.168.0.182");
        factory.setHost(HOST);
        factory.setPort(PORTNUMBER);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUALHOST);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(AUDIT_QUEUE_NAME, false, false, false, null);
        channel.basicQos(20);

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(AUDIT_QUEUE_NAME, false, consumer);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());

            System.out.println(" [x] Received '" + message + "'");

            Gson gson = new Gson();
            RequestMessageV requestMessageV = gson.fromJson(message, RequestMessageV.class);
            //TODO 数据解析放到redis

            System.out.println(requestMessageV.getOrderno());
            System.out.println(" [x] Done '" + message + "'");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }
}
