package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.DefaultRecordBatchModify;
import org.apache.kafka.common.record.RecordBatch;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

/**
 * @author keashem@163.com
 * @date 2020/12/29 5:15 下午
 */
public class ProducerBatchTest {
    private static KafkaConsumer<String, String> consumer;
    private static KafkaProducer<String, String> producer;
    private static String batchTopic = "1229topic4";

    static {
        String brokerList = "localhost:9092";
        String topic = "1229topic1";
        String groupId = "1229group6";
        Properties propConsumer = new Properties();
        propConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        propConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        propConsumer.put("bootstrap.servers", brokerList);
        propConsumer.put("group.id", groupId);
        propConsumer.put("auto.offset.reset", "earliest");
        consumer = new KafkaConsumer<>(propConsumer);
        consumer.subscribe(Collections.singletonList(topic));

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);

    }

    public static void main(String[] args) {
        try {
            while (!Thread.interrupted()) {
                RecordBatch batch = consumer.pollForBatch(Duration.ofMillis(1000));
                if(Objects.nonNull(batch)){
                    System.out.println(batch);
                    producer.send(batch,batchTopic);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
