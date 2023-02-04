package com.zpb.rocketmq.normal.producer.oneway;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;

/**
 * @author       pengbo.zhao
 * @description  单向消息-生产者-测试
 * @createDate   2022/4/6 14:29
 * @updateDate   2022/4/6 14:29
 * @version      1.0
 */
@DisplayName("单向消息-生产者-测试")
class OnewayProducerTest {

    private DefaultMQProducer producer;

    @BeforeEach
    void setUp() throws MQClientException {
        producer = new DefaultMQProducer("p3");
        // 指定nameserver
        producer.setNamesrvAddr("150.158.139.25:9876");
        // 开启生产者
        producer.start();

    }

    @Test
    @DisplayName("单向消息-生产者")
    void sendOnewayMessage() throws UnsupportedEncodingException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            byte[] bytes = "hello rocketmq...p3".getBytes(RemotingHelper.DEFAULT_CHARSET);
            // 创建消息，并指定topic、tag、消息体
            Message message = new Message("topic_3", "tag_3", bytes);
            // 为消息指定key
            message.setKeys("key-"+i);
            try {
                // 发送单向消息，没有任何返回结果
                producer.sendOneway(message);
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail();
            }
        }

        // 关闭producer
        producer.shutdown();
    }

}
