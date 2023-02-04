package com.zpb.rocketmq.normal.producer.async;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


/**
 * @author       pengbo.zhao
 * @description  异步消息-生产者-测试
 * @createDate   2022/4/2 19:31
 * @updateDate   2022/4/2 19:31
 * @version      1.0
 */
@DisplayName("异步消息-生产者-测试")
class AsyncProducerTest {

    private DefaultMQProducer producer;

    @BeforeEach
    void setUp() throws MQClientException {
        producer = new DefaultMQProducer("p2");
        // 指定nameserver
        producer.setNamesrvAddr("150.158.139.25:9876");
        // 异步发送失败后不进行重试发送
        producer.setRetryTimesWhenSendFailed(0);
        // 指定创建的topic的queue数量为2，默认为4
        producer.setDefaultTopicQueueNums(2);
        // 开启生产者
        producer.start();
    }

    @Test
    @DisplayName("异步消息-生产者")
    void sentAsyncMessage() throws InterruptedException {
        byte[] body = "hello rocketmq async send message".getBytes();

        for (int i = 0; i < 10; i++) {
            // 创建消息，并指定topic、tag、消息体
            Message message = new Message("topic_2", "tag_2", body);
            try {
                // SendCallback 接收异步返回结果的回调
                producer.send(message,new AsyncSendCallback());
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail();
            }
        }
        // 由于使用的是异步发送，因此需要将线程等待一会，否则会造成还未发送完，
        // 生产者已经结束了,造成无法消费
        Thread.sleep(10000);

        // 如果不再发送消息，就关闭producer实例
        producer.shutdown();

    }


    /**
     * @author       pengbo.zhao
     * @description  异步回调
     * @createDate   2022/4/2 19:40
     * @updateDate   2022/4/2 19:40
     * @version      1.0
     */

    public static class AsyncSendCallback implements SendCallback {

        @Override
        public void onSuccess(SendResult sendResult) {
            // 当producer接收到MQ发送来的ACK后就会触发该回调方法的执行
            System.err.println(sendResult);
        }

        @Override
        public void onException(Throwable throwable) {
           throwable.printStackTrace();
        }

    }
}
