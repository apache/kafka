/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.DefaultRecord;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ClusterTestDefaults(
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.NUM_PARTITIONS_CONFIG, value = "4")
    }
)
public class PlaintextProducerSendTest {

    private final String topic = "topic";
    private final int numRecords = 100;
    private final ClusterInstance clusterInstance;

    PlaintextProducerSendTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest
    public void testSendOffset() throws InterruptedException, ExecutionException {
        int partition = 0;
        try (var producer = clusterInstance.producer()) {
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

    private void sendAndVerifyTimestamp(Producer<Object, Object> producer, TimestampType timestampType) throws InterruptedException, ExecutionException {
        int partition = 0;
        long baseTimestamp = 123456;
        long startTime = System.currentTimeMillis();
        Map<String, String> properties = Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, timestampType.name);
        clusterInstance.createTopic(topic, 1, (short) 2, properties);
        var callbackOffset = new AtomicLong(0L);
        List<ProducerRecord<Object, Object>> records = new ArrayList<>();
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, partition, baseTimestamp + i,
                String.format("key%d", i).getBytes(StandardCharsets.UTF_8), String.format("value%d", i).getBytes(StandardCharsets.UTF_8));
            records.add(record);
            futures.add(producer.send(record, (metadata, exception) -> {
                assertEquals(callbackOffset.get(), metadata.offset());
                callbackOffset.incrementAndGet();
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

    @ClusterTest
    public void testSendCompressedMessageWithCreateTime() throws ExecutionException, InterruptedException {
        try (var producer = clusterInstance.producer(Map.of(
            ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip",
            ProducerConfig.LINGER_MS_CONFIG, Integer.MAX_VALUE,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE
        ))) {
            sendAndVerifyTimestamp(producer, TimestampType.CREATE_TIME);
        }
    }

    @ClusterTest
    public void testSendNonCompressedMessageWithCreateTime() throws ExecutionException, InterruptedException {
        try (var producer = clusterInstance.producer(Map.of(
            ProducerConfig.LINGER_MS_CONFIG, Integer.MAX_VALUE,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE
        ))) {
            sendAndVerifyTimestamp(producer, TimestampType.CREATE_TIME);
        }
    }

    @ClusterTest
    public void testClose() throws InterruptedException, ExecutionException {
        try (var producer = clusterInstance.producer()) {
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
    public void testSendToPartitionOverClassicProtocol() {
        try (var consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name()))) {
            testSendToPartition(consumer);
        }
    }

    @ClusterTest
    public void testSendToPartitionOverConsumerProtocol() {
        try (var consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()))) {
            testSendToPartition(consumer);
        }
    }

    private void testSendToPartition(Consumer<Object, Object> consumer) {
        try (var producer = clusterInstance.producer()) {
            assertDoesNotThrow(() -> clusterInstance.createTopic(topic, 2, (short) 2));
            int partition = 1;
            ClientsTestUtils.sendRecordsAndVerify(producer, topic, partition, numRecords, 0);

            consumer.assign(List.of(new TopicPartition(topic, partition)));
            List<ConsumerRecord<Object, Object>> records = assertDoesNotThrow(() -> ClientsTestUtils.consumeRecords(consumer, numRecords));
            for (int i = 0; i < numRecords; i++) {
                assertEquals(topic, records.get(i).topic());
                assertEquals(partition, records.get(i).partition());
                assertEquals(i, records.get(i).offset());
                String key = new String((byte[]) records.get(i).key(), StandardCharsets.UTF_8);
                assertEquals("key" + i, key);
                String value = new String((byte[]) records.get(i).value(), StandardCharsets.UTF_8);
                assertEquals(String.format("value%d", i), value);
            }
        }
    }

    @ClusterTest
    public void testSendToPartitionWithFollowerShutdownShouldNotTimeoutInClassicProtocol() {
        try (var consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name()))) {
            testSendToPartitionWithFollowerShutdownShouldNotTimeout(consumer);
        }
    }

    @ClusterTest
    public void testSendToPartitionWithFollowerShutdownShouldNotTimeoutInConsumerProtocol() {
        try (var consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()))) {
            testSendToPartitionWithFollowerShutdownShouldNotTimeout(consumer);
        }
    }

    private void testSendToPartitionWithFollowerShutdownShouldNotTimeout(Consumer<Object, Object> consumer) {
        int follower = 1;
        try (var producer = clusterInstance.producer()) {
            assertDoesNotThrow(() -> clusterInstance.createTopicWithAssignment(topic, Map.of(0, List.of(0, follower))));
            int partition = 0;
            long now = System.currentTimeMillis();
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                futures.add(producer.send(new ProducerRecord<>(topic, partition, now, null, String.format("value%d", i).getBytes(StandardCharsets.UTF_8))));
            }

            clusterInstance.shutdownBroker(follower);
            for (int i = 0; i < numRecords; i++) {
                var future = futures.get(i);
                RecordMetadata metadata = assertDoesNotThrow(() -> future.get());
                assertEquals(i, metadata.offset());
                assertEquals(topic, metadata.topic());
                assertEquals(partition, metadata.partition());
            }

            consumer.assign(List.of(new TopicPartition(topic, partition)));
            List<ConsumerRecord<Object, Object>> records = assertDoesNotThrow(() -> ClientsTestUtils.consumeRecords(consumer, numRecords));
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

    @ClusterTest
    public void testSendBeforeAndAfterPartitionExpansion() throws InterruptedException {
        try (var producer = clusterInstance.producer(Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000))) {
            clusterInstance.createTopic(topic, 1, (short) 2);
            int partition0 = 0;
            ClientsTestUtils.sendRecordsAndVerify(producer, topic, partition0, numRecords, 0);

            int partition1 = 1;
            TestUtils.assertFutureThrows(TimeoutException.class, producer.send(new ProducerRecord<>(topic, partition1, null, "value".getBytes(StandardCharsets.UTF_8))));
            clusterInstance.createPartitions(Map.of(topic, NewPartitions.increaseTo(2)));
            ClientsTestUtils.sendRecordsAndVerify(producer, topic, partition1, numRecords, 0);
            ClientsTestUtils.sendRecordsAndVerify(producer, topic, partition0, numRecords, numRecords);
        }
    }

    @ClusterTest
    public void testFlush() throws InterruptedException {
        try (var producer = clusterInstance.producer(Map.of(
            ProducerConfig.LINGER_MS_CONFIG, Integer.MAX_VALUE, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE
        ))) {
            clusterInstance.createTopic(topic, 2, (short) 2);
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, "value".getBytes(StandardCharsets.UTF_8));
            for (int i = 0; i < 50; i++) {
                List<Future<RecordMetadata>> futures = new ArrayList<>();
                for (int j = 0; j < numRecords; j++) {
                    futures.add(producer.send(record));
                }
                assertTrue(futures.stream().noneMatch(Future::isDone));
                producer.flush();
                assertTrue(futures.stream().allMatch(Future::isDone));
            }
        }
    }

    @ClusterTest
    public void testCloseWithZeroTimeoutFromCallerThread() throws InterruptedException {
        clusterInstance.createTopic(topic, 2, (short) 2);
        int partition = 0;
        ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, partition, null, "value".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < 50; i++) {
            try (var producer = clusterInstance.producer(Map.of(ProducerConfig.LINGER_MS_CONFIG, Integer.MAX_VALUE, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE))) {
                List<Future<RecordMetadata>> futures = new ArrayList<>();
                for (int j = 0; j < numRecords; j++) {
                    futures.add(producer.send(record));
                }
                assertTrue(futures.stream().noneMatch(Future::isDone), "No request is complete.");
                producer.close(Duration.ZERO);
                for (int j = 0; j < numRecords; j++) {
                    TestUtils.assertFutureThrows(KafkaException.class, futures.get(j));
                }
            }
        }
    }

    @ClusterTest
    public void testCloseWithZeroTimeoutFromSenderThreadOverClassicProtocol() {
        try (var consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name()))) {
            testCloseWithZeroTimeoutFromSenderThread(consumer);
        }
    }

    @ClusterTest
    public void testCloseWithZeroTimeoutFromSenderThreadOverConsumerProtocol() {
        try (var consumer = clusterInstance.consumer(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()))) {
            testCloseWithZeroTimeoutFromSenderThread(consumer);
        }
    }

    /**
     * Test close with zero and non-zero timeout from sender thread
     */
    private void testCloseWithZeroTimeoutFromSenderThread(Consumer<Object, Object> consumer) {
        assertDoesNotThrow(() -> clusterInstance.createTopic(topic, 1, (short) 2));
        int partition = 0;
        consumer.assign(List.of(new TopicPartition(topic, partition)));
        ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, partition, null, "value".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < 50; i++) {
            final boolean sendRecords = i == 0;
            try (var producer = clusterInstance.producer(Map.of(
                ProducerConfig.LINGER_MS_CONFIG, Integer.MAX_VALUE, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE
            ))) {
                // send message to partition 0
                // Only send the records in the first callback since we close the producer in the callback and no records
                // can be sent afterwards.
                List<Future<RecordMetadata>> futures = new ArrayList<>();
                for (int j = 0; j < numRecords; j++) {
                    futures.add(producer.send(record, ((metadata, exception) -> {
                        // Trigger another batch in accumulator before close the producer. These messages should
                        // not be sent.
                        if (sendRecords) {
                            for (int k = 0; k < numRecords; k++) {
                                producer.send(record);
                            }
                        }
                        // The close call will be called by all the message callbacks. This tests idempotence of the close call.
                        producer.close(Duration.ZERO);
                        // Test close with non zero timeout. Should not block at all.
                        producer.close();
                    })));
                }
                assertTrue(futures.stream().noneMatch(Future::isDone));
                producer.flush();
                assertTrue(futures.stream().allMatch(Future::isDone));
                assertDoesNotThrow(() -> ClientsTestUtils.consumeRecords(consumer, numRecords));
            }
        }
    }

    @ClusterTest
    public void testWrongSerializer() {
        Map<String, Object> props = Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"
        );
        try (var producer = clusterInstance.producer(props)) {
            assertThrows(SerializationException.class, () -> producer.send(new ProducerRecord<>(topic, 0, "key".getBytes(), "value".getBytes())));
        }
    }

    private void sendAndVerifyWithBatchSizeZero(boolean nullKey) throws ExecutionException, InterruptedException {
        Map<String, Object> props = Map.of(
            ProducerConfig.LINGER_MS_CONFIG, Integer.MAX_VALUE,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE,
            ProducerConfig.BATCH_SIZE_CONFIG, 0
        );
        int partition = 0;
        clusterInstance.createTopic(topic, 2, (short) 2);
        try (var producer = clusterInstance.producer(props)) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                if (nullKey) {
                    futures.add(producer.send(new ProducerRecord<>(topic, partition,
                        null, String.format("value%d", i).getBytes(StandardCharsets.UTF_8))));
                } else {
                    futures.add(producer.send(new ProducerRecord<>(topic, partition,
                        String.format("key%d", i).getBytes(StandardCharsets.UTF_8), String.format("value%d", i).getBytes(StandardCharsets.UTF_8))));
                }
            }
            for (int i = 0; i < numRecords; i++) {
                // When nullKey the last 2 send requests are blocked
                if (i == numRecords - 2) {
                    producer.flush();
                }
                RecordMetadata metadata = futures.get(i).get();
                assertEquals(topic, metadata.topic());
                assertEquals(partition, metadata.partition());
                assertEquals(i, metadata.offset());
            }
        }
    }

    @ClusterTest
    public void testBatchSizeZero() throws ExecutionException, InterruptedException {
        sendAndVerifyWithBatchSizeZero(false);
    }

    @ClusterTest
    public void testBatchSizeZeroNoPartitionNoRecordKey() throws ExecutionException, InterruptedException {
        sendAndVerifyWithBatchSizeZero(true);
    }

    @ClusterTest
    public void testSendCompressedMessageWithLogAppendTime() throws ExecutionException, InterruptedException {
        Map<String, Object> props = Map.of(
            ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip",
            ProducerConfig.LINGER_MS_CONFIG, Integer.MAX_VALUE,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE
        );
        try (var producer = clusterInstance.producer(props)) {
            sendAndVerifyTimestamp(producer, TimestampType.LOG_APPEND_TIME);
        }
    }

    @ClusterTest
    public void testSendNonCompressedMessageWithLogAppendTime() throws ExecutionException, InterruptedException {
        Map<String, Object> props = Map.of(
            ProducerConfig.LINGER_MS_CONFIG, Integer.MAX_VALUE,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE
        );
        try (var producer = clusterInstance.producer(props)) {
            sendAndVerifyTimestamp(producer, TimestampType.LOG_APPEND_TIME);
        }
    }

    @ClusterTest
    public void testAutoCreateTopic() throws ExecutionException, InterruptedException {
        try (var producer = clusterInstance.producer(); Admin admin = clusterInstance.admin()) {
            assertEquals(0, producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes())).get().offset());
            TestUtils.waitForCondition(() -> {
                List<TopicPartitionInfo> infos = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions();
                return infos.stream().anyMatch(info -> info.partition() == 0 && !info.leader().isEmpty());
            }, "hello");
        }
    }

    @ClusterTest(
        brokers = 2,
        serverProperties = {
            @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false")
        }
    )
    public void testSendTimeoutErrorMessageWhenTopicDoesNotExist() {
        try (var producer = clusterInstance.producer(Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500))) {
            Exception e = TestUtils.assertFutureThrows(TimeoutException.class, producer.send(new ProducerRecord<>(topic, null, "key".getBytes(), "value".getBytes())));
            assertEquals("Topic topic not present in metadata after 500 ms.", e.getMessage());
        }
    }

    @ClusterTest
    public void testSendTimeoutErrorWhenPartitionDoesNotExist() throws ExecutionException, InterruptedException {
        try (var producer = clusterInstance.producer(Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500))) {
            assertEquals(0, producer.send(new ProducerRecord<>(topic, null, "key".getBytes(), "value".getBytes())).get().offset());
            Exception e = TestUtils.assertFutureThrows(TimeoutException.class, producer.send(new ProducerRecord<>(topic, 10, "key".getBytes(), "value".getBytes())));
            assertEquals("Partition 10 of topic topic with partition count 4 is not present in metadata after 500 ms.", e.getMessage());
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false")
        }
    )
    public void testPartitionsForTimeoutErrorWhenTopicDoesNotExist() {
        try (var producer = clusterInstance.producer(Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500))) {
            Exception e = assertThrows(TimeoutException.class, () -> producer.partitionsFor("unexisting-topic"));
            assertEquals("Topic unexisting-topic not present in metadata after 500 ms.", e.getMessage());
        }
    }

    private List<Map<String, Long>> timestampConfigProvider() {
        long now = System.currentTimeMillis();
        long fiveMinutesInMs = 5 * 60 * 1000;
        return List.of(
            Map.of(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, now - fiveMinutesInMs),
            Map.of(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, now + fiveMinutesInMs)
        );
    }

    @ClusterTest
    public void testSendWithInvalidBeforeAndAfterTimestamp() throws InterruptedException {
        long oneMinuteInMs = 60 * 1000;
        for (Map<String, Long> config: timestampConfigProvider()) {
            for (Map.Entry<String, Long> entry: config.entrySet()) {
                String timestampTopic = String.format("topic-%s", entry.getKey());
                clusterInstance.createTopic(timestampTopic, 1, (short) 2, Map.of(entry.getKey(), String.valueOf(oneMinuteInMs)));
                try (var producer = clusterInstance.producer()) {
                    TestUtils.assertFutureThrows(InvalidTimestampException.class, producer.send(new ProducerRecord<>(timestampTopic, 0, entry.getValue(), "key".getBytes(), "value".getBytes())));
                }
                try (var producer = clusterInstance.producer(Map.of(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"))) {
                    TestUtils.assertFutureThrows(InvalidTimestampException.class, producer.send(new ProducerRecord<>(timestampTopic, 0, entry.getValue(), "key".getBytes(), "value".getBytes())));
                }
            }
        }
    }

    @ClusterTest
    public void testValidBeforeAndAfterTimestampsAtThreshold() throws InterruptedException {
        for (Map<String, Long> config: timestampConfigProvider()) {
            for (Map.Entry<String, Long> entry: config.entrySet()) {
                String timestampTopic = String.format("topic-%s", entry.getKey());
                clusterInstance.createTopic(timestampTopic, 1, (short) 2, Map.of(entry.getKey(), String.valueOf(entry.getValue())));
                try (var producer = clusterInstance.producer()) {
                    assertDoesNotThrow(() -> producer.send(new ProducerRecord<>(timestampTopic, 0, entry.getValue(), "key".getBytes(), "value".getBytes())));
                }
                try (var producer = clusterInstance.producer(Map.of(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"))) {
                    assertDoesNotThrow(() -> producer.send(new ProducerRecord<>(timestampTopic, 0, entry.getValue(), "key".getBytes(), "value".getBytes())));
                }
            }
        }
    }

    @ClusterTest
    public void testValidBeforeAndAfterTimestampsWithinThreshold() throws InterruptedException {
        long tenMinutesInMs = 10 * 60 * 1000;
        for (Map<String, Long> config: timestampConfigProvider()) {
            for (Map.Entry<String, Long> entry: config.entrySet()) {
                String timestampTopic = String.format("topic-%s", entry.getKey());
                clusterInstance.createTopic(timestampTopic, 1, (short) 2, Map.of(entry.getKey(), String.valueOf(tenMinutesInMs)));
                try (var producer = clusterInstance.producer()) {
                    assertDoesNotThrow(() -> producer.send(new ProducerRecord<>(timestampTopic, 0, entry.getValue(), "key".getBytes(), "value".getBytes())));
                }
                try (var producer = clusterInstance.producer(Map.of(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"))) {
                    assertDoesNotThrow(() -> producer.send(new ProducerRecord<>(timestampTopic, 0, entry.getValue(), "key".getBytes(), "value".getBytes())));
                }
            }
        }
    }

    // Test that producer with max.block.ms=0 can be used to send in non-blocking mode
    // where requests are failed immediately without blocking if metadata is not available
    // or buffer is full.
    @ClusterTest
    public void testNonBlockingProducer() throws InterruptedException {
        try (var producer = clusterInstance.producer(Map.of(
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 0,
            ProducerConfig.LINGER_MS_CONFIG, 15000,
            ProducerConfig.BATCH_SIZE_CONFIG, 1100,
            ProducerConfig.BUFFER_MEMORY_CONFIG, 1500
        ))) {
            Future<RecordMetadata> future0 = producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes()));
            assertTrue(future0.isDone());
            TestUtils.assertFutureThrows(TimeoutException.class, future0);

            TestUtils.waitForCondition(() -> {
                try {
                    producer.partitionsFor(topic);
                    return true;
                } catch (TimeoutException e) {
                    return false;
                }
            }, "Can't get metadata");

            producer.flush();
            byte[] value = new byte[1000];
            Future<RecordMetadata> successFuture = producer.send(new ProducerRecord<>(topic, 0, "key".getBytes(), value));
            assertFalse(successFuture.isDone());
            TestUtils.assertFutureThrows(BufferExhaustedException.class, producer.send(new ProducerRecord<>(topic, 0, "key".getBytes(), value)));
        }
    }

    @ClusterTest
    public void testSendRecordBatchWithMaxRequestSizeAndHigher() throws ExecutionException, InterruptedException {
        try (Producer<byte[], byte[]> producer = clusterInstance.producer()) {
            int keyLengthSize = 1, headerLengthSize = 1, valueLengthSize = 3;
            int overhead = Records.LOG_OVERHEAD + DefaultRecordBatch.RECORD_BATCH_OVERHEAD + DefaultRecord.MAX_RECORD_OVERHEAD + keyLengthSize + headerLengthSize + valueLengthSize;
            int valueSize = ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT - overhead;

            ProducerRecord<byte[], byte[]> record0 = new ProducerRecord<>(topic, new byte[0], new byte[valueSize]);
            assertEquals(record0.value().length, producer.send(record0).get().serializedValueSize());

            ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topic, new byte[0], new byte[valueSize + 1]);
            TestUtils.assertFutureThrows(RecordTooLargeException.class, producer.send(record1));
        }
    }

}
