package com.zpb.rocketmq.normal.consumer.scheduled;

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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author       pengbo.zhao
 * @description  延时消息-消费者-测试
 * @createDate   2022/4/6 15:49
 * @updateDate   2022/4/6 15:49
 * @version      1.0
 */

@DisplayName("延时消息-消费者-测试")
class ScheduledConsumerTest {

    private DefaultMQPushConsumer pushConsumer;

    @BeforeEach
    void setUp() {

        // 定义消费者
        pushConsumer = new DefaultMQPushConsumer("c_5");
        // 指定nameserver
        pushConsumer.setNamesrvAddr("150.158.139.25:9876");

        /*
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

    }

    @Test
    @DisplayName("延时消息-消费者")
    void testConsumer() {
        try {
            // 指定消费的topic和tag
            pushConsumer.subscribe("topic_5","tag_5");
            // 注册监听器
            pushConsumer.registerMessageListener(new ScheduledMessageListener());
            // 开启消费者消费
            pushConsumer.start();
            System.err.println("Consumer c_5 Started......");
            new CountDownLatch(1).await();
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }

    /**
     * @author       pengbo.zhao
     * @description  延时消息-监听器
     * @createDate   2022/4/2 14:42
     * @updateDate   2022/4/2 14:42
     * @version      1.0
     */
    public static class ScheduledMessageListener implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
            // 一旦broker中有了其订阅的消息就会触发该方法的执行，
            for (MessageExt messageExt : list) {
                // System.err.println(", currentTime: "+ new SimpleDateFormat("yyyy-MM-dd HH:dd:mm").format(new Date()));
                // System.out.println("queueId=" + messageExt.getQueueId()
                //         + ", msgId: "+ messageExt.getMsgId()
                //         + ", bronTime: "+ new SimpleDateFormat("yyyy-MM-dd HH:dd:mm").format(new Date(messageExt.getBornTimestamp()))
                //         + ", storeTime: "+ new SimpleDateFormat("yyyy-MM-dd HH:dd:mm").format(new Date(messageExt.getStoreTimestamp()))
                //         + ", offset: "+ messageExt.getQueueOffset()
                //         + ", content:" + new String(messageExt.getBody()));
                String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:dd:mm:ss").format(new Date());
                System.out.println(currentTime + " ," + messageExt);
            }

            // 其返回值为当前consumer消费的状态
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

    }
}
