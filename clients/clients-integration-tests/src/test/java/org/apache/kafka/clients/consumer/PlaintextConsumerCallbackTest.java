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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.GroupProtocol.CLASSIC;
import static org.apache.kafka.clients.consumer.GroupProtocol.CONSUMER;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3
)
public class PlaintextConsumerCallbackTest {

    private final ClusterInstance cluster;
    private final String topic = "topic";
    private final TopicPartition tp = new TopicPartition(topic, 0);

    public PlaintextConsumerCallbackTest(ClusterInstance clusterInstance) {
        this.cluster = clusterInstance;
    }

    @ClusterTest
    public void testClassicConsumerRebalanceListenerAssignOnPartitionsAssigned() throws InterruptedException {
        testRebalanceListenerAssignOnPartitionsAssigned(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerRebalanceListenerAssignOnPartitionsAssigned() throws InterruptedException {
        testRebalanceListenerAssignOnPartitionsAssigned(CONSUMER);
    }

    private void testRebalanceListenerAssignOnPartitionsAssigned(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol)) {
            triggerOnPartitionsAssigned(tp, consumer, (executeConsumer, partitions) -> {
                var e = assertThrows(IllegalStateException.class, () -> executeConsumer.assign(List.of(tp)));
                assertEquals("Subscription to topics, partitions and pattern are mutually exclusive", e.getMessage());
            });
        }
    }

    @ClusterTest
    public void testClassicConsumerRebalanceListenerAssignmentOnPartitionsAssigned() throws InterruptedException {
        testRebalanceListenerAssignmentOnPartitionsAssigned(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerRebalanceListenerAssignmentOnPartitionsAssigned() throws InterruptedException {
        testRebalanceListenerAssignmentOnPartitionsAssigned(CONSUMER);
    }

    private void testRebalanceListenerAssignmentOnPartitionsAssigned(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol)) {
            triggerOnPartitionsAssigned(tp, consumer,
                (executeConsumer, partitions) -> assertTrue(executeConsumer.assignment().contains(tp))
            );
        }
    }

    @ClusterTest
    public void testClassicConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned() throws InterruptedException {
        testRebalanceListenerBeginningOffsetsOnPartitionsAssigned(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned() throws InterruptedException {
        testRebalanceListenerBeginningOffsetsOnPartitionsAssigned(CONSUMER);
    }

    private void testRebalanceListenerBeginningOffsetsOnPartitionsAssigned(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol)) {
            triggerOnPartitionsAssigned(tp, consumer, (executeConsumer, partitions) -> {
                var map = executeConsumer.beginningOffsets(List.of(tp));
                assertTrue(map.containsKey(tp));
                assertEquals(0L, map.get(tp));
            });
        }
    }

    @ClusterTest
    public void testClassicConsumerRebalanceListenerAssignOnPartitionsRevoked() throws InterruptedException {
        testRebalanceListenerAssignOnPartitionsRevoked(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerRebalanceListenerAssignOnPartitionsRevoked() throws InterruptedException {
        testRebalanceListenerAssignOnPartitionsRevoked(CONSUMER);
    }

    private void testRebalanceListenerAssignOnPartitionsRevoked(GroupProtocol groupProtocol) throws InterruptedException {
        triggerOnPartitionsRevoked(tp, groupProtocol, (consumer, partitions) -> {
            var e = assertThrows(IllegalStateException.class, () -> consumer.assign(List.of(tp)));
            assertEquals("Subscription to topics, partitions and pattern are mutually exclusive", e.getMessage());
        });
    }

    @ClusterTest
    public void testClassicConsumerRebalanceListenerAssignmentOnPartitionsRevoked() throws InterruptedException {
        triggerOnPartitionsRevoked(tp, CLASSIC,
            (consumer, partitions) -> assertTrue(consumer.assignment().contains(tp))
        );
    }

    @ClusterTest
    public void testAsyncConsumerRebalanceListenerAssignmentOnPartitionsRevoked() throws InterruptedException {
        triggerOnPartitionsRevoked(tp, CONSUMER,
            (consumer, partitions) -> assertTrue(consumer.assignment().contains(tp))
        );
    }

    @ClusterTest
    public void testClassicConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked() throws InterruptedException {
        testRebalanceListenerBeginningOffsetsOnPartitionsRevoked(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked() throws InterruptedException {
        testRebalanceListenerBeginningOffsetsOnPartitionsRevoked(CONSUMER);
    }

    private void testRebalanceListenerBeginningOffsetsOnPartitionsRevoked(GroupProtocol groupProtocol) throws InterruptedException {
        triggerOnPartitionsRevoked(tp, groupProtocol, (consumer, partitions) -> {
            var map = consumer.beginningOffsets(List.of(tp));
            assertTrue(map.containsKey(tp));
            assertEquals(0L, map.get(tp));
        });
    }

    @ClusterTest
    public void testClassicConsumerGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback() throws InterruptedException {
        testGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback() throws InterruptedException {
        testGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback(CONSUMER);
    }

    private void testGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol)) {
            triggerOnPartitionsAssigned(tp, consumer,
                (executeConsumer, partitions) -> assertDoesNotThrow(() -> executeConsumer.position(tp))
            );
        }
    }

    @ClusterTest
    public void testClassicConsumerSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback() throws InterruptedException {
        testSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback() throws InterruptedException {
        testSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback(CONSUMER);
    }

    private void testSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol)) {
            var startingOffset = 100L;
            var totalRecords = 120;
            var startingTimestamp = 0L;

            sendRecords(totalRecords, startingTimestamp);

            triggerOnPartitionsAssigned(tp, consumer, (executeConsumer, partitions) -> {
                executeConsumer.seek(tp, startingOffset);
                executeConsumer.pause(List.of(tp));
            });

            assertTrue(consumer.paused().contains(tp));
            consumer.resume(List.of(tp));
            consumeAndVerifyRecords(
                consumer,
                (int) (totalRecords - startingOffset),
                (int) startingOffset,
                (int) startingOffset,
                startingOffset
            );
        }
    }

    private void triggerOnPartitionsAssigned(
        TopicPartition tp,
        Consumer<byte[], byte[]> consumer,
        BiConsumer<Consumer<byte[], byte[]>, Collection<TopicPartition>> execute
    ) throws InterruptedException {
        var partitionsAssigned = new AtomicBoolean(false);
        consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // Make sure the partition used in the test is actually assigned before continuing.
                if (partitions.contains(tp)) {
                    execute.accept(consumer, partitions);
                    partitionsAssigned.set(true);
                }
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // noop
            }
        });
        TestUtils.waitForCondition(
            () -> {
                consumer.poll(Duration.ofMillis(100));
                return partitionsAssigned.get();
            },
            "Timed out before expected rebalance completed"
        );
    }

    private void triggerOnPartitionsRevoked(
        TopicPartition tp,
        GroupProtocol protocol,
        BiConsumer<Consumer<byte[], byte[]>, Collection<TopicPartition>> execute
    ) throws InterruptedException {
        var partitionsAssigned = new AtomicBoolean(false);
        var partitionsRevoked = new AtomicBoolean(false);
        try (var consumer = createConsumer(protocol)) {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // Make sure the partition used in the test is actually assigned before continuing.
                    if (partitions.contains(tp)) {
                        partitionsAssigned.set(true);
                    }
                }

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Make sure the partition used in the test is actually revoked before continuing.
                    if (partitions.contains(tp)) {
                        execute.accept(consumer, partitions);
                        partitionsRevoked.set(true);
                    }
                }
            });
            TestUtils.waitForCondition(
                () -> {
                    consumer.poll(Duration.ofMillis(100));
                    return partitionsAssigned.get();
                },
                "Timed out before expected rebalance completed"
            );
        }
        assertTrue(partitionsRevoked.get());
    }

    private Consumer<byte[], byte[]> createConsumer(GroupProtocol protocol) {
        return cluster.consumer(Map.of(
            GROUP_PROTOCOL_CONFIG, protocol.name().toLowerCase(Locale.ROOT),
            ENABLE_AUTO_COMMIT_CONFIG, "false"
        ));
    }

    private void sendRecords(int numRecords, long startingTimestamp) {
        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            for (var i = 0; i < numRecords; i++) {
                var timestamp = startingTimestamp + i;
                var record = new ProducerRecord<>(
                    tp.topic(),
                    tp.partition(),
                    timestamp,
                    ("key " + i).getBytes(),
                    ("value " + i).getBytes()
                );
                producer.send(record);
            }
            producer.flush();
        }
    }

    protected void consumeAndVerifyRecords(
        Consumer<byte[], byte[]> consumer,
        int numRecords,
        int startingOffset,
        int startingKeyAndValueIndex,
        long startingTimestamp
    ) throws InterruptedException {
        var records = consumeRecords(consumer, numRecords);
        for (var i = 0; i < numRecords; i++) {
            var record = records.get(i);
            var offset = startingOffset + i;

            assertEquals(tp.topic(), record.topic());
            assertEquals(tp.partition(), record.partition());

            assertEquals(TimestampType.CREATE_TIME, record.timestampType());
            var timestamp = startingTimestamp + i;
            assertEquals(timestamp, record.timestamp());

            assertEquals(offset, record.offset());
            var keyAndValueIndex = startingKeyAndValueIndex + i;
            assertEquals("key " + keyAndValueIndex, new String(record.key()));
            assertEquals("value " + keyAndValueIndex, new String(record.value()));
            // this is true only because K and V are byte arrays
            assertEquals(("key " + keyAndValueIndex).length(), record.serializedKeySize());
            assertEquals(("value " + keyAndValueIndex).length(), record.serializedValueSize());
        }
    }

    protected <K, V> List<ConsumerRecord<K, V>> consumeRecords(
        Consumer<K, V> consumer,
        int numRecords
    ) throws InterruptedException {
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100)).forEach(records::add);
            return records.size() >= numRecords;
        }, 60000, "Timed out before consuming expected " + numRecords + " records.");

        return records;
    }
}
