package com.zpb.rocketmq.normal.consumer.async;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author       pengbo.zhao
 * @description  异步消息-消费者-测试
 * @createDate   2022/4/6 16:00
 * @updateDate   2022/4/6 16:00
 * @version      1.0
 */
@DisplayName("异步消息-消费者-测试")
class AsyncConsumerTest {

    private DefaultMQPushConsumer pushConsumer;

    @BeforeEach
    void setUp() {

        // 定义消费者
        pushConsumer = new DefaultMQPushConsumer("c_2");
        // 指定nameserver
        pushConsumer.setNamesrvAddr("150.158.139.25:9876");
        // 指定从第一条消息开始消费
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

    }

    @Test
    @DisplayName("异步消息-消费者")
    void sendSyncMessage() {
        try {
            // 指定消费的topic和tag
            pushConsumer.subscribe("topic_2","tag_2");
            // 注册监听器
            pushConsumer.registerMessageListener(new ASyncMessageListener());
            // 开启消费者消费
            pushConsumer.start();
            System.err.println("Consumer c_2 Started......");
            new CountDownLatch(1).await();
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }

    /**
     * @author       pengbo.zhao
     * @description  异步消息监听器
     * @createDate   2022/4/2 14:42
     * @updateDate   2022/4/2 14:42
     * @version      1.0
     */
    public static class ASyncMessageListener implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
            // 一旦broker中有了其订阅的消息就会触发该方法的执行，
            for (MessageExt messageExt : list) {
                System.out.println("queueId=" + messageExt.getQueueId()
                        + " msgId"+ messageExt.getMsgId()
                        + " offset"+ messageExt.getQueueOffset()
                        + ", content:" + new String(messageExt.getBody()));
            }
            // 其返回值为当前consumer消费的状态
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

    }
}
