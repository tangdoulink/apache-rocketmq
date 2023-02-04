package com.zpb.rocketmq.normal.producer.scheduled;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author       pengbo.zhao
 * @description  延时消息-生产者-测试
 * @createDate   2022/4/6 15:42
 * @updateDate   2022/4/6 15:42
 * @version      1.0
 */
@DisplayName("延时消息-生产者-测试")
class ScheduledMessageProducerTest {

    private DefaultMQProducer producer;

    @BeforeEach
    void setUp() throws MQClientException {
        producer = new DefaultMQProducer("p5");
        // 指定nameserver
        producer.setNamesrvAddr("150.158.139.25:9876");
        // 开启生产者
        producer.start();

    }

    @Test
    @DisplayName("延时消息-生产者")
    void sendOrderMessage(){
        System.err.println("send message before:"+new SimpleDateFormat("yyyy-MM-dd HH:dd:mm:ss").format(new Date()));
        for (int i = 0; i < 10; i++) {
            String body = "Hello scheduled message " + i;
            Message message = new Message("topic_5","tag_5",body.getBytes());
            // 设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
            // private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
            message.setDelayTimeLevel(3);
            try {
                // 发送消息
                SendResult result = producer.send(message);
                System.out.printf("SendResult status:%s, queueId:%d, body:%s, bronTime:%s%n",
                        result.getSendStatus(),
                        result.getMessageQueue().getQueueId(),
                        body,
                        new SimpleDateFormat("yyyy-MM-dd HH:dd:mm:ss").format(new Date()));

            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail();
            }
        }

        System.err.println("send message after:"+new SimpleDateFormat("yyyy-MM-dd HH:dd:mm:ss").format(new Date()));
        // 关闭生产者
        producer.shutdown();
    }
}
