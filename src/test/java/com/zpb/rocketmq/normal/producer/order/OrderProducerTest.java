package com.zpb.rocketmq.normal.producer.order;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author       pengbo.zhao
 * @description  顺序消息-生产者-测试
 * @createDate   2022/4/6 14:38
 * @updateDate   2022/4/6 14:38
 * @version      1.0
 */

@DisplayName("顺序消息-生产者-测试")
class OrderProducerTest {

    private DefaultMQProducer producer;

    @BeforeEach
    void setUp() throws MQClientException {
        producer = new DefaultMQProducer("p4");
        // 指定nameserver
        producer.setNamesrvAddr("150.158.139.25:9876");
        // 开启生产者
        producer.start();

    }

    @Test
    @DisplayName("顺序消息-生产者")
    void sendOrderMessage(){
        String[] tags = new String[]{"TagA", "TagC", "TagD"};
        // 订单列表
        List<OrderStep> orderList = new OrderStep().buildOrders();
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = sdf.format(date);

        for (int i = 0; i < 10; i++) {
            // 加个时间前缀
            String body = dateStr + " Hello RocketMQ " + orderList.get(i);
            Message msg = new Message("t4", tags[i % tags.length], "KEY" + i, body.getBytes());
            try {
                SendResult result = producer.send(msg, new MessageQueueSelectorImpl(), orderList.get(i).getOrderId());
                System.out.printf("SendResult status:%s, queueId:%d, body:%s%n",
                        result.getSendStatus(),
                        result.getMessageQueue().getQueueId(),
                        body);

            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail();
            }
        }

        // 关闭producer
        producer.shutdown();
    }

    /**
    * 订单的步骤
    */
    public static class OrderStep {
       private long orderId;
       private String desc;

       public long getOrderId() {
           return orderId;
       }

       public void setOrderId(long orderId) {
           this.orderId = orderId;
       }

       public String getDesc() {
           return desc;
       }

       public void setDesc(String desc) {
           this.desc = desc;
       }

       @Override
       public String toString() {
           return "OrderStep{" +
               "orderId=" + orderId +
               ", desc='" + desc + '\'' +
               '}';
       }

        /**
         * 生成模拟订单数据
         */
        public List<OrderStep> buildOrders() {
            List<OrderStep> orderList = new ArrayList<OrderStep>();

            OrderStep orderDemo = new OrderStep();
            orderDemo.setOrderId(15103111039L);
            orderDemo.setDesc("创建");
            orderList.add(orderDemo);

            orderDemo = new OrderStep();
            orderDemo.setOrderId(15103111065L);
            orderDemo.setDesc("创建");
            orderList.add(orderDemo);

            orderDemo = new OrderStep();
            orderDemo.setOrderId(15103111039L);
            orderDemo.setDesc("付款");
            orderList.add(orderDemo);

            orderDemo = new OrderStep();
            orderDemo.setOrderId(15103117235L);
            orderDemo.setDesc("创建");
            orderList.add(orderDemo);

            orderDemo = new OrderStep();
            orderDemo.setOrderId(15103111065L);
            orderDemo.setDesc("付款");
            orderList.add(orderDemo);

            orderDemo = new OrderStep();
            orderDemo.setOrderId(15103117235L);
            orderDemo.setDesc("付款");
            orderList.add(orderDemo);

            orderDemo = new OrderStep();
            orderDemo.setOrderId(15103111065L);
            orderDemo.setDesc("完成");
            orderList.add(orderDemo);

            orderDemo = new OrderStep();
            orderDemo.setOrderId(15103111039L);
            orderDemo.setDesc("推送");
            orderList.add(orderDemo);

            orderDemo = new OrderStep();
            orderDemo.setOrderId(15103117235L);
            orderDemo.setDesc("完成");
            orderList.add(orderDemo);

            orderDemo = new OrderStep();
            orderDemo.setOrderId(15103111039L);
            orderDemo.setDesc("完成");
            orderList.add(orderDemo);

            return orderList;
        }

    }

    /**
     * @author       pengbo.zhao
     * @description  消息队列选择器-实现
     * @createDate   2022/4/6 14:47
     * @updateDate   2022/4/6 14:47
     * @version      1.0
     */
    public static class MessageQueueSelectorImpl implements MessageQueueSelector {

        @Override
        public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
            Long id = (Long) o;  //根据订单id选择发送queue
            long index = id % list.size();
            return list.get((int) index);
        }
    }

}
