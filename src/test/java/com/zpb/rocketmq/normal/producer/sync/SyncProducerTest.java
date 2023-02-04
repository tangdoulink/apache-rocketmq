package com.zpb.rocketmq.normal.producer.sync;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * @author       pengbo.zhao
 * @description  普通-同步消息-生产者-测试
 * @createDate   2022/4/2 11:31
 * @updateDate   2022/4/2 11:31
 * @version      1.0
 */
@DisplayName("同步消息-生产者-测试")
class SyncProducerTest {

    private DefaultMQProducer producer;

    @BeforeEach
    void setUp() throws MQClientException {
        producer = new DefaultMQProducer("p1");
        // 指定nameserver
        producer.setNamesrvAddr("150.158.139.25:9876");
        // 设置当发送失败时重试发送的次数，默认为2次
        producer.setRetryTimesWhenSendFailed(3);
        // 设置发送超时时限为5s，默认3s
        producer.setSendMsgTimeout(50000);
        // 开启生产者
        producer.start();

    }

    @Test
    @DisplayName("生产者-发送同步消息")
    void sendSyncMessage() {
        for (int i = 0; i < 100; i++) {
            byte[] bytes = "hello rocketmq".getBytes();
            // 创建消息，并指定topic、tag、消息体
            Message message = new Message("topic_1", "tag_1", bytes);
            // 为消息指定key
            message.setKeys("key-"+i);
            try {
                // 发送消息到broker中
                SendResult result = producer.send(message);
                // 可以通过SendResult中的sendStatus来确定发送消息是否成功
                System.err.println(result);
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail();
            }
        }

        // 关闭producer
        producer.shutdown();

    }

}
