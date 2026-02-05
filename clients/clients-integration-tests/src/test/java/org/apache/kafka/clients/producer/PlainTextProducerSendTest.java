package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

@ClusterTestDefaults(
    brokers = 3
)
public class PlainTextProducerSendTest {

    private final String topic = "topic";
    private final String intMax = "2147483647";
    private final int numRecords = 100;
    private final ClusterInstance clusterInstance;

    PlainTextProducerSendTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest
    public void testSendOffset() throws InterruptedException, ExecutionException {
        int partition = 0;
        try (Producer<Object, Object> producer = clusterInstance.producer()) {
            clusterInstance.createTopic(topic, 1, (short) 2);
            List<ProducerRecord<Object, Object>> records = List.of(
                new ProducerRecord<>(topic, partition, "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8)),
                new ProducerRecord<>(topic, partition, "key".getBytes(StandardCharsets.UTF_8), null),
                new ProducerRecord<>(topic, partition, null, "value".getBytes(StandardCharsets.UTF_8)),
                new ProducerRecord<>(topic, null, "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8))
            );
            int keyLength = "key".getBytes(StandardCharsets.UTF_8).length;
            int valueLength = "value".getBytes(StandardCharsets.UTF_8).length;
            List<Integer> expectedKeyLength = List.of(keyLength, keyLength, -1, keyLength);
            List<Integer> expectedValueLength = List.of(valueLength, -1, valueLength, valueLength);

            for (int i = 0; i < records.size(); i++) {
                RecordMetadata metadata = producer.send(records.get(i)).get();
                assertEquals(i, metadata.offset());
                assertEquals(topic, metadata.topic());
                assertEquals(partition, metadata.partition());
                assertEquals(metadata.serializedKeySize(), expectedKeyLength.get(i));
                assertEquals(metadata.serializedValueSize(), expectedValueLength.get(i));
                assertEquals(i, metadata.offset(), "Should have offset " + i);
            }

            for (int i = 0; i < numRecords; i++) {
                producer.send(records.get(0));
            }
            assertEquals(numRecords + 4, producer.send(records.get(0)).get().offset(), "Should have offset " + (numRecords + 4));
        }
    }

    private void sendAndVerifyTimestamp(TimestampType timestampType) throws InterruptedException, ExecutionException {
        int partition = 0;
        long baseTimestamp = 123456;
        long startTime = System.currentTimeMillis();
        Map<String, String> properties = Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, timestampType.name);
        clusterInstance.createTopic(topic, 1, (short) 2, properties);
        final long[] callbackOffset = {0L};
        try (Producer<Object, Object> producer = clusterInstance.producer()) {
            List<ProducerRecord<Object, Object>> records = new ArrayList<>();
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, partition, baseTimestamp + i,
                    String.format("key%d", i).getBytes(StandardCharsets.UTF_8), String.format("value%d", i).getBytes(StandardCharsets.UTF_8));
                records.add(record);
                futures.add(producer.send(record, (metadata, exception) -> {
                    assertEquals(callbackOffset[0], metadata.offset());
                    callbackOffset[0]++;
                }));
            }
            producer.flush();
            for (int i = 0; i < numRecords; i++) {
                RecordMetadata metadata = futures.get(i).get();
                assertEquals(i, metadata.offset(), "Should have offset " + i);
                assertEquals(topic, metadata.topic());
                if (timestampType == TimestampType.LOG_APPEND_TIME) {
                    assertTrue(metadata.timestamp() >= startTime && metadata.timestamp() <= System.currentTimeMillis());
                } else {
                    assertEquals(baseTimestamp + i, metadata.timestamp());
                    assertEquals(records.get(i).timestamp(), metadata.timestamp());
                }
            }
        }
    }

    @ClusterTest(
        brokers = 2,
        serverProperties = {
            @ClusterConfigProperty(key = ProducerConfig.COMPRESSION_TYPE_CONFIG, value = "gzip"),
            @ClusterConfigProperty(key = ProducerConfig.LINGER_MS_CONFIG, value = intMax),
            @ClusterConfigProperty(key = ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, value = intMax)
        }
    )
    public void testSendCompressedMessageWithCreateTime() throws ExecutionException, InterruptedException {
        sendAndVerifyTimestamp(TimestampType.CREATE_TIME);
    }

    @ClusterTest(
        brokers = 2,
        serverProperties = {
            @ClusterConfigProperty(key = ProducerConfig.LINGER_MS_CONFIG, value = intMax),
            @ClusterConfigProperty(key = ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, value = intMax)
        }
    )
    public void testSendNonCompressedMessageWithCreateTime() throws ExecutionException, InterruptedException {
        sendAndVerifyTimestamp(TimestampType.CREATE_TIME);
    }

    @ClusterTest
    public void testClose() throws InterruptedException, ExecutionException {
        try (Producer<Object, Object> producer = clusterInstance.producer()) {
            clusterInstance.createTopic(topic, 1, (short) 2);
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, null, "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
            for (int i = 0; i < numRecords; i++) {
                producer.send(record);
            }
            Future<RecordMetadata> future = producer.send(record);
            producer.close();
            assertTrue(future.isDone(), "The last message should be acked before producer is shutdown");
            assertEquals(numRecords, future.get().offset(), "Should have offset " + numRecords);
        }
    }

    @ClusterTest
    public void testSendToPartition() throws InterruptedException, ExecutionException {
        try (Producer<Object, Object> producer = clusterInstance.producer(); Consumer<Object, Object> consumer = clusterInstance.consumer()) {
            clusterInstance.createTopic(topic, 2, (short) 2);
            int partition = 1;
            long now = System.currentTimeMillis();
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                futures.add(producer.send(new ProducerRecord<>(topic, partition, now, null, String.format("value%d", i).getBytes(StandardCharsets.UTF_8))));
            }

            for (int i = 0; i < numRecords; i++) {
                RecordMetadata metadata = futures.get(i).get();
                assertEquals(i, metadata.offset());
                assertEquals(topic, metadata.topic());
                assertEquals(partition, metadata.partition());
            }

            consumer.assign(List.of(new TopicPartition(topic, partition)));
            List<ConsumerRecord<Object, Object>> records = ClientsTestUtils.consumeRecords(consumer, numRecords);
            for (int i = 0; i < numRecords; i++) {
                assertEquals(topic, records.get(i).topic());
                assertEquals(partition, records.get(i).partition());
                assertEquals(i, records.get(i).offset());
                assertNull(records.get(i).key());
                String value = new String((byte[]) records.get(i).value(), StandardCharsets.UTF_8);
                assertEquals(String.format("value%d", i), value);
                assertEquals(now, records.get(i).timestamp());
            }
        }
    }

    public void testSendToPartitionWithFollowerShutdownShouldNotTimeout() {
        int follower = 1;
        List<Integer> replicas = List.of(0, follower);

        try (Producer<Object, Object> producer = clusterInstance.producer()) {
        }
    }

}
