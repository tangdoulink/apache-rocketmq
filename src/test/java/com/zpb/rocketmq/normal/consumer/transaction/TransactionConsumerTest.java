package com.zpb.rocketmq.normal.consumer.transaction;

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
 * @description  事物消息-消费者-测试
 * @createDate   2022/4/7 11:37
 * @updateDate   2022/4/7 11:37
 * @version      1.0
 */
@DisplayName("事物消息-消费者-测试")
class TransactionConsumerTest {

    private DefaultMQPushConsumer pushConsumer;

    @BeforeEach
    void setUp(){
         pushConsumer = new DefaultMQPushConsumer("c_6");
        // 指定nameserver
        pushConsumer.setNamesrvAddr("150.158.139.25:9876");
        // 指定从第一条消息开始消费
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET );
    }

    @Test
    @DisplayName("事物消息-消费者")
    void testTransactionConsumer()  {

        try {
            pushConsumer.subscribe("topic_6", "TagA || TagB || TagC || TagD");
            pushConsumer.registerMessageListener(new TransactionMessageListener());
            // 开启消费者消费
            pushConsumer.start();
            System.out.println("Consumer c_6 Started......");
            new CountDownLatch(1).await();
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }

    }

    /**
     * @author       pengbo.zhao
     * @description  事务消息-监听器
     * @createDate   2022/4/2 14:42
     * @updateDate   2022/4/2 14:42
     * @version      1.0
     */
    public static class TransactionMessageListener implements MessageListenerConcurrently {

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
