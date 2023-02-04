package com.zpb.rocketmq.normal.producer.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * @author       pengbo.zhao
 * @description  事务消息-生产者-测试
 * @createDate   2022/4/7 10:33
 * @updateDate   2022/4/7 10:33
 * @version      1.0
 */
@DisplayName("事务消息-生产者-测试")
class TransactionProducerTest {

    /**
     *
     */
    private TransactionMQProducer transactionMQProducer;

    private TransactionListener transactionListener;

    private ExecutorService executorService;

    @BeforeEach
    void setUp() throws MQClientException {
        transactionListener = new TransactionListenerImpl();
        transactionMQProducer = new TransactionMQProducer("p6");
        executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread();
                thread.setName("client-transaction-msg-check-thred");
                return thread;
            }
        });

        // 指定nameserver
        transactionMQProducer.setNamesrvAddr("150.158.139.25:9876");
        // 指定线程池
        transactionMQProducer.setExecutorService(executorService);
        // 指定事务监听器
        transactionMQProducer.setTransactionListener(transactionListener);
        // 开启生产者
        transactionMQProducer.start();
    }


    @Test
    @DisplayName("事务消息-生产者")
    void testTransactionSendMessage() throws UnsupportedEncodingException, InterruptedException {
        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            String topic = "topic_6";
            String tag = tags[i % tags.length];
            byte[] body = ("hello rocketmq transaction " + i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            try {
                Message message = new Message(topic,tag,body);
                TransactionSendResult transactionSendResult = transactionMQProducer.sendMessageInTransaction(message, null);
                System.err.println(transactionSendResult);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail();
            }
        }
        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        transactionMQProducer.shutdown();
    }
}
