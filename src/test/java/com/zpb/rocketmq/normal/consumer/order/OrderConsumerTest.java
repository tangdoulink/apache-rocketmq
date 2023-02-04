package com.zpb.rocketmq.normal.consumer.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author       pengbo.zhao
 * @description  顺序消息-消费者-测试
 * @createDate   2022/4/6 14:53
 * @updateDate   2022/4/6 14:53
 * @version      1.0
 */
@DisplayName("顺序消息-消费者-测试")
class OrderConsumerTest {

    private DefaultMQPushConsumer pushConsumer;

    @BeforeEach
    void setUp() {

        // 定义消费者
        pushConsumer = new DefaultMQPushConsumer("c_4");
        // 指定nameserver
        pushConsumer.setNamesrvAddr("150.158.139.25:9876");

        /*
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

    }

    @Test
    @DisplayName("顺序消费-消费者")
    void testOrderConsumer() {

        try {
            pushConsumer.subscribe("t4", "TagA || TagC || TagD");
            pushConsumer.registerMessageListener(new MessageListenerOrderlyImpl());
            pushConsumer.start();
            System.err.println("order consumer start......");
            new CountDownLatch(1).await();
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }

    /**
     * @author       pengbo.zhao
     * @description  顺序消息-消费者-实现类
     * @createDate   2022/4/6 14:58
     * @updateDate   2022/4/6 14:58
     * @version      1.0
     */
    public static class MessageListenerOrderlyImpl implements MessageListenerOrderly {

        Random random = new Random();

        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
            consumeOrderlyContext.setAutoCommit(true);
            for (MessageExt msg : list) {
                // 可以看到每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
                System.out.println("consumeThread=" + Thread.currentThread().getName() + "queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
            }
            try {
                //模拟业务逻辑处理中...
                TimeUnit.SECONDS.sleep(random.nextInt(10));
            } catch (Exception e) {
                e.printStackTrace();
            }
            return ConsumeOrderlyStatus.SUCCESS;
        }
    }
}
