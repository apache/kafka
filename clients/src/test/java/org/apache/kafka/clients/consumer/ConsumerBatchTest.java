package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.record.RecordBatch;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

/**
 * @author keashem@163.com
 * @date 2020/12/29 4:42 下午
 */
public class ConsumerBatchTest {
    public static void main(String args[]) {
        String brokerList = "localhost:9092";
        String topic = "1229topic1";
        String groupId = "1229group4";

        Properties properties = new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", brokerList);
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        RecordBatch currentBatch;
        RecordBatch lastBatch = null;
        try {
            while(!Thread.interrupted()){
                currentBatch = consumer.pollForBatch(Duration.ofMillis(1000));
                if (currentBatch != null) {
                    if (lastBatch != null) {
                        if(lastBatch.lastOffset() + 1 == currentBatch.baseOffset()){
                            System.out.println("the check of batch offset is ok~~");
                        } else {
                            System.out.println(lastBatch.lastOffset() +"-------"+currentBatch.baseOffset());
                        }
                    }
                    lastBatch = currentBatch;
                    System.out.println(currentBatch + "-------" + System.currentTimeMillis());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
