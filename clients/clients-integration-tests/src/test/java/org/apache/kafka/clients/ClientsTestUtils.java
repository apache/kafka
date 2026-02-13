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
package org.apache.kafka.clients;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.kafka.clients.ClientsTestUtils.TestClusterResourceListenerDeserializer.UPDATE_CONSUMER_COUNT;
import static org.apache.kafka.clients.ClientsTestUtils.TestClusterResourceListenerSerializer.UPDATE_PRODUCER_COUNT;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientsTestUtils {

    private static final String KEY_PREFIX = "key ";
    private static final String VALUE_PREFIX = "value ";

    private ClientsTestUtils() {}

    public static <K, V> List<ConsumerRecord<K, V>> consumeRecords(
        Consumer<K, V> consumer,
        int numRecords
    ) throws InterruptedException {
        return consumeRecords(consumer, numRecords, Integer.MAX_VALUE);
    }

    public static <K, V> List<ConsumerRecord<K, V>> consumeRecords(
        Consumer<K, V> consumer,
        int numRecords,
        int maxPollRecords
    ) throws InterruptedException {
        List<ConsumerRecord<K, V>> consumedRecords = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            var records = consumer.poll(Duration.ofMillis(100));
            records.forEach(consumedRecords::add);
            assertTrue(records.count() <= maxPollRecords);
            return consumedRecords.size() >= numRecords;
        }, 60000, "Timed out before consuming expected " + numRecords + " records.");

        return consumedRecords;
    }

    public static void consumeAndVerifyRecords(
        Consumer<byte[], byte[]> consumer,
        TopicPartition tp,
        int numRecords,
        int startingOffset,
        int startingKeyAndValueIndex,
        long startingTimestamp,
        long timestampIncrement
    ) throws InterruptedException {
        consumeAndVerifyRecords(
            consumer,
            tp,
            numRecords,
            Integer.MAX_VALUE,
            startingOffset,
            startingKeyAndValueIndex,
            startingTimestamp,
            timestampIncrement
        );
    }

    public static void pollUntilTrue(
        Consumer<byte[], byte[]> consumer,
        Supplier<Boolean> testCondition,
        String msg
    ) throws InterruptedException {
        pollUntilTrue(consumer, Duration.ofMillis(100), testCondition, 15_000L, msg);
    }

    public static void pollUntilTrue(
        Consumer<byte[], byte[]> consumer,
        Supplier<Boolean> testCondition,
        long waitTimeMs,
        String msg
    ) throws InterruptedException {
        pollUntilTrue(consumer, Duration.ofMillis(100), testCondition, waitTimeMs, msg);
    }

    public static void pollUntilTrue(
        Consumer<byte[], byte[]> consumer,
        Duration timeout,
        Supplier<Boolean> testCondition,
        long waitTimeMs, 
        String msg
    ) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            consumer.poll(timeout);
            return testCondition.get();
        }, waitTimeMs, msg);
    }

    public static void consumeAndVerifyRecordsWithTimeTypeLogAppend(
        Consumer<byte[], byte[]> consumer,
        TopicPartition tp,
        int numRecords,
        long startingTimestamp
    ) throws InterruptedException {
        var records = consumeRecords(consumer, numRecords, Integer.MAX_VALUE);
        var now = System.currentTimeMillis();
        for (var i = 0; i < numRecords; i++) {
            var record = records.get(i);
            assertEquals(tp.topic(), record.topic());
            assertEquals(tp.partition(), record.partition());

            assertTrue(record.timestamp() >= startingTimestamp && record.timestamp() <= now,
                "Got unexpected timestamp " + record.timestamp() + ". Timestamp should be between [" + startingTimestamp + ", " + now + "]");

            assertEquals(i, record.offset());
            assertEquals(KEY_PREFIX + i, new String(record.key()));
            assertEquals(VALUE_PREFIX + i, new String(record.value()));
            // this is true only because K and V are byte arrays
            assertEquals((KEY_PREFIX + i).length(), record.serializedKeySize());
            assertEquals((VALUE_PREFIX + i).length(), record.serializedValueSize());
        }
    }

    public static void consumeAndVerifyRecords(
        Consumer<byte[], byte[]> consumer,
        TopicPartition tp,
        int numRecords,
        int maxPollRecords,
        int startingOffset,
        int startingKeyAndValueIndex,
        long startingTimestamp,
        long timestampIncrement
    ) throws InterruptedException {
        var records = consumeRecords(consumer, numRecords, maxPollRecords);
        for (var i = 0; i < numRecords; i++) {
            var record = records.get(i);
            var offset = startingOffset + i;

            assertEquals(tp.topic(), record.topic());
            assertEquals(tp.partition(), record.partition());

            assertEquals(TimestampType.CREATE_TIME, record.timestampType());
            var timestamp = startingTimestamp + i * (timestampIncrement > 0 ? timestampIncrement : 1);
            assertEquals(timestamp, record.timestamp());

            assertEquals(offset, record.offset());
            var keyAndValueIndex = startingKeyAndValueIndex + i;
            assertEquals(KEY_PREFIX + keyAndValueIndex, new String(record.key()));
            assertEquals(VALUE_PREFIX + keyAndValueIndex, new String(record.value()));
            // this is true only because K and V are byte arrays
            assertEquals((KEY_PREFIX + keyAndValueIndex).length(), record.serializedKeySize());
            assertEquals((VALUE_PREFIX + keyAndValueIndex).length(), record.serializedValueSize());
        }
    }

    public static void consumeAndVerifyRecords(
        Consumer<byte[], byte[]> consumer,
        TopicPartition tp,
        int numRecords,
        int startingOffset,
        int startingKeyAndValueIndex,
        long startingTimestamp
    ) throws InterruptedException {
        consumeAndVerifyRecords(consumer, tp, numRecords, startingOffset, startingKeyAndValueIndex, startingTimestamp, -1);
    }

    public static void consumeAndVerifyRecords(
        Consumer<byte[], byte[]> consumer,
        TopicPartition tp,
        int numRecords,
        int startingOffset
    ) throws InterruptedException {
        consumeAndVerifyRecords(consumer, tp, numRecords, startingOffset, 0, 0, -1);
    }

    public static void sendRecords(
        ClusterInstance cluster,
        TopicPartition tp,
        int numRecords,
        long startingTimestamp,
        long timestampIncrement
    ) {
        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            for (var i = 0; i < numRecords; i++) {
                sendRecord(producer, tp, startingTimestamp, i, timestampIncrement);
            }
            producer.flush();
        }
    }

    public static void sendRecords(
        ClusterInstance cluster,
        TopicPartition tp,
        int numRecords,
        long startingTimestamp
    ) {
        sendRecords(cluster, tp, numRecords, startingTimestamp, -1);
    }

    public static void sendRecords(
        ClusterInstance cluster,
        TopicPartition tp,
        int numRecords
    ) {
        sendRecords(cluster, tp, numRecords, System.currentTimeMillis());
    }

    public static List<ProducerRecord<byte[], byte[]>> sendRecords(
        Producer<byte[], byte[]> producer,
        TopicPartition tp,
        int numRecords,
        long startingTimestamp,
        long timestampIncrement
    ) {
        List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (var i = 0; i < numRecords; i++) {
            var record = sendRecord(producer, tp, startingTimestamp, i, timestampIncrement);
            records.add(record);
        }
        producer.flush();
        return records;
    }

    public static void sendRecords(
        Producer<byte[], byte[]> producer,
        TopicPartition tp,
        int numRecords,
        long startingTimestamp
    ) {
        for (var i = 0; i < numRecords; i++) {
            sendRecord(producer, tp, startingTimestamp, i, -1);
        }
        producer.flush();
    }

    public static void awaitAssignment(
        Consumer<byte[], byte[]> consumer,
        Set<TopicPartition> expectedAssignment
    ) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100));
            return consumer.assignment().equals(expectedAssignment);
        }, () -> "Timed out while awaiting expected assignment " + expectedAssignment + ". " +
                "The current assignment is " + consumer.assignment()
        );
    }

    private static ProducerRecord<byte[], byte[]> sendRecord(
        Producer<byte[], byte[]> producer,
        TopicPartition tp,
        long startingTimestamp,
        int numRecord,
        long timestampIncrement
    ) {
        var timestamp = startingTimestamp + numRecord * (timestampIncrement > 0 ? timestampIncrement : 1);
        var record = new ProducerRecord<>(
            tp.topic(),
            tp.partition(),
            timestamp,
            (KEY_PREFIX + numRecord).getBytes(),
            (VALUE_PREFIX + numRecord).getBytes()
        );
        producer.send(record);
        return record;
    }

    public static <K, V> void sendAndAwaitAsyncCommit(
        Consumer<K, V> consumer,
        Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt
    ) throws InterruptedException {

        var commitCallback = new RetryCommitCallback<>(consumer, offsetsOpt);
        sendAsyncCommit(consumer, commitCallback, offsetsOpt);

        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100));
            return commitCallback.isComplete;
        }, "Failed to observe commit callback before timeout");

        assertEquals(Optional.empty(), commitCallback.error);
    }

    public static void awaitRebalance(
        Consumer<byte[], byte[]> consumer,
        TestConsumerReassignmentListener rebalanceListener
    ) throws InterruptedException {
        var numReassignments = rebalanceListener.callsToAssigned;
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100));
            return rebalanceListener.callsToAssigned > numReassignments;
        }, "Timed out before expected rebalance completed");
    }

    public static void ensureNoRebalance(
        Consumer<byte[], byte[]> consumer,
        TestConsumerReassignmentListener rebalanceListener
    ) throws InterruptedException {
        // The best way to verify that the current membership is still active is to commit offsets.
        // This would fail if the group had rebalanced.
        var initialRevokeCalls = rebalanceListener.callsToRevoked;
        sendAndAwaitAsyncCommit(consumer, Optional.empty());
        assertEquals(initialRevokeCalls, rebalanceListener.callsToRevoked);
    }


    public static void waitForPollThrowException(
        Consumer<byte[], byte[]> consumer,
        Class<? extends Exception> exceptedException
    ) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            try {
                consumer.poll(Duration.ZERO);
                return false;
            } catch (Exception e) {
                return exceptedException.isInstance(e);
            }
        }, "Continuous poll not fail");
    }

    /**
     * This class is intended to replace the test cases in BaseConsumerTest.scala.
     * When converting tests that extend from BaseConsumerTest.scala to Java,
     * we should use the test cases provided in this class.
     */
    public static final class BaseConsumerTestcase {

        public static final int BROKER_COUNT = 3;
        public static final String TOPIC = "topic";
        public static final TopicPartition TP = new TopicPartition(TOPIC, 0);

        private BaseConsumerTestcase() {
        }

        public static void testSimpleConsumption(
            ClusterInstance cluster,
            Map<String, Object> config
        ) throws InterruptedException {
            var numRecords = 10000;
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(cluster, TP, numRecords, startingTimestamp);
            try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
                assertEquals(0, consumer.assignment().size());
                consumer.assign(List.of(TP));
                assertEquals(1, consumer.assignment().size());
                consumer.seek(TP, 0);
                consumeAndVerifyRecords(consumer, TP, numRecords, 0, 0, startingTimestamp);
                // check async commit callbacks
                sendAndAwaitAsyncCommit(consumer, Optional.empty());
            }
        }

        public static void testClusterResourceListener(
            ClusterInstance cluster,
            Map<String, Object> consumerConfig
        ) throws InterruptedException {
            var numRecords = 100;
            Map<String, Object> producerConfig = Map.of(
                KEY_SERIALIZER_CLASS_CONFIG, TestClusterResourceListenerSerializer.class,
                VALUE_SERIALIZER_CLASS_CONFIG, TestClusterResourceListenerSerializer.class
            );
            Map<String, Object> consumerConfigOverrides = new HashMap<>(consumerConfig);
            consumerConfigOverrides.put(KEY_DESERIALIZER_CLASS_CONFIG, TestClusterResourceListenerDeserializer.class);
            consumerConfigOverrides.put(VALUE_DESERIALIZER_CLASS_CONFIG, TestClusterResourceListenerDeserializer.class);
            try (Producer<byte[], byte[]> producer = cluster.producer(producerConfig);
                 Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfigOverrides)
            ) {
                var startingTimestamp = System.currentTimeMillis();
                sendRecords(producer, TP, numRecords, startingTimestamp, -1);

                consumer.subscribe(List.of(TP.topic()));
                consumeAndVerifyRecords(consumer, TP, numRecords, 0, 0, startingTimestamp);
                assertNotEquals(0, UPDATE_PRODUCER_COUNT.get());
                assertNotEquals(0, UPDATE_CONSUMER_COUNT.get());

                TestClusterResourceListenerSerializer.resetCount();
                TestClusterResourceListenerDeserializer.resetCount();
            }
        }

        public static void testCoordinatorFailover(
            ClusterInstance cluster,
            Map<String, Object> consumerConfig
        ) throws InterruptedException {
            var listener = new TestConsumerReassignmentListener();
            try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
                consumer.subscribe(List.of(TOPIC), listener);
                // the initial subscription should cause a callback execution
                awaitRebalance(consumer, listener);
                assertEquals(1, listener.callsToAssigned);

                // get metadata for the topic
                List<PartitionInfo> parts = null;
                while (parts == null) {
                    parts = consumer.partitionsFor(Topic.GROUP_METADATA_TOPIC_NAME);
                }
                assertEquals(1, parts.size());
                assertNotNull(parts.get(0).leader());

                // shutdown the coordinator
                int coordinator = parts.get(0).leader().id();
                cluster.shutdownBroker(coordinator);

                // the failover should not cause a rebalance
                ensureNoRebalance(consumer, listener);
            }
        }
    }

    public static <K, V> void sendAsyncCommit(
        Consumer<K, V> consumer,
        OffsetCommitCallback callback,
        Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt
    ) {
        offsetsOpt.ifPresentOrElse(
            offsets -> consumer.commitAsync(offsets, callback),
            () -> consumer.commitAsync(callback)
        );
    }

    public static class TestClusterResourceListenerSerializer implements Serializer<byte[]>, ClusterResourceListener {

        public static final AtomicInteger UPDATE_PRODUCER_COUNT = new AtomicInteger();

        @Override
        public void onUpdate(ClusterResource clusterResource) {
            UPDATE_PRODUCER_COUNT.incrementAndGet();
        }

        @Override
        public byte[] serialize(String topic, byte[] data) {
            return data;
        }
        
        public static void resetCount() {
            UPDATE_PRODUCER_COUNT.set(0);
        }
    }

    public static class TestClusterResourceListenerDeserializer implements Deserializer<byte[]>, ClusterResourceListener {

        public static final AtomicInteger UPDATE_CONSUMER_COUNT = new AtomicInteger();

        @Override
        public void onUpdate(ClusterResource clusterResource) {
            UPDATE_CONSUMER_COUNT.incrementAndGet();
        }

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            return data;
        }

        public static void resetCount() {
            UPDATE_CONSUMER_COUNT.set(0);
        }
    }

    private static class RetryCommitCallback<K, V> implements OffsetCommitCallback {
        boolean isComplete = false;
        Optional<Exception> error = Optional.empty();
        Consumer<K, V> consumer;
        Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt;

        public RetryCommitCallback(
            Consumer<K, V> consumer,
            Optional<Map<TopicPartition, OffsetAndMetadata>> offsetsOpt
        ) {
            this.consumer = consumer;
            this.offsetsOpt = offsetsOpt;
        }

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception instanceof RetriableCommitFailedException) {
                sendAsyncCommit(consumer, this, offsetsOpt);
            } else {
                isComplete = true;
                error = Optional.ofNullable(exception);
            }
        }
    }

    public static class TestConsumerReassignmentListener implements ConsumerRebalanceListener {
        public int callsToAssigned = 0;
        public int callsToRevoked = 0;

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            callsToAssigned += 1;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            callsToRevoked += 1;
        }
    }
}
