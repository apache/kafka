package org.apache.kafka.trogdor.workload;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class AndrewConsumeBench {
    public final static long MAX_MESSAGES = 1000000;

    public static void main(String[] args){
        System.out.println("Andrew consume bench test");
        AndrewConsumeBench bench = new AndrewConsumeBench();
        String clientId = "andrew-consume-bench-1";
        String consumerGroup = "andrew-consume-bench";
        try (KafkaConsumer<String, String> consumer = bench.consumer(consumerGroup, clientId)) {
            bench.call(clientId, consumer);
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace(System.out);
        }
    }

    private KafkaConsumer<String, String> consumer(String consumerGroup, String clientId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 100000);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    private void call(String clientId, KafkaConsumer<String, String> consumer) throws Exception {
        long messagesConsumed = 0;
        long bytesConsumed = 0;
        long startTimeMs = Time.SYSTEM.milliseconds();
        long startBatchMs = startTimeMs;
        long maxMessages = MAX_MESSAGES;
        long lastOffset = 0L;
        try {
            Map<TopicPartition, OffsetAndMetadata> md = consumer.committed(Collections.singleton(new TopicPartition("T1", 0)));
            System.out.println("OffsetAndMetadata: " + md);
            consumer.subscribe(Collections.singleton("T1"));
            while (messagesConsumed < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                if (records.isEmpty()) {
                    continue;
                }
                System.out.println("Returned " + records.count() + " records.");
                long endBatchMs = Time.SYSTEM.milliseconds();
                long elapsedBatchMs = endBatchMs - startBatchMs;

                for (ConsumerRecord<String, String> record : records) {
                    messagesConsumed++;
                    long messageBytes = 0;
                    if (record.key() != null) {
                        messageBytes += record.serializedKeySize();
                    }
                    if (record.value() != null) {
                        messageBytes += record.serializedValueSize();
                    }
                    bytesConsumed += messageBytes;
                    lastOffset = record.offset();
                    if (messagesConsumed >= maxMessages)
                        break;
                }
                startBatchMs = Time.SYSTEM.milliseconds();
                consumer.commitSync();
            }
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace(System.out);
        } finally {
            long curTimeMs = Time.SYSTEM.milliseconds();
            System.out.println(clientId + " Consumed total number of messages=" + messagesConsumed + " in " + (curTimeMs - startTimeMs) + " +ms.");
        }
    }
}