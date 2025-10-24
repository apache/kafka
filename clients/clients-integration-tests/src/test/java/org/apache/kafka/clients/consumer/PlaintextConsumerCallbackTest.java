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

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.apache.kafka.clients.ClientsTestUtils.consumeAndVerifyRecords;
import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
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
    public void testOnPartitionsAssignedCalledWithNewPartitionsOnlyForClassicCooperative() throws InterruptedException {
        try (var consumer = createClassicConsumerCooperativeProtocol()) {
            testOnPartitionsAssignedCalledWithExpectedPartitions(consumer, true);
        }
    }

    @ClusterTest
    public void testOnPartitionsAssignedCalledWithNewPartitionsOnlyForAsyncConsumer() throws InterruptedException {
        try (var consumer = createConsumer(CONSUMER)) {
            testOnPartitionsAssignedCalledWithExpectedPartitions(consumer, true);
        }
    }

    @ClusterTest
    public void testOnPartitionsAssignedCalledWithNewPartitionsOnlyForClassicEager() throws InterruptedException {
        try (var consumer = createConsumer(CLASSIC)) {
            testOnPartitionsAssignedCalledWithExpectedPartitions(consumer, false);
        }
    }

    private void testOnPartitionsAssignedCalledWithExpectedPartitions(
            Consumer<byte[], byte[]> consumer,
            boolean expectNewPartitionsOnlyInCallback) throws InterruptedException {
        subscribeAndExpectOnPartitionsAssigned(consumer, List.of(topic), List.of(tp));
        assertEquals(Set.of(tp), consumer.assignment());

        // Add a new partition assignment while keeping the previous one
        String newTopic = "newTopic";
        TopicPartition addedPartition = new TopicPartition(newTopic, 0);
        List<TopicPartition> expectedPartitionsInCallback;
        if (expectNewPartitionsOnlyInCallback) {
            expectedPartitionsInCallback = List.of(addedPartition);
        } else {
            expectedPartitionsInCallback = List.of(tp, addedPartition);
        }

        // Change subscription to keep the previous one and add a new topic. Assignment should be updated
        // to contain partitions from both topics, but the onPartitionsAssigned parameters may containing
        // the full new assignment or just the newly added partitions depending on the case.
        subscribeAndExpectOnPartitionsAssigned(
                consumer,
                List.of(topic, newTopic),
                expectedPartitionsInCallback);
        assertEquals(Set.of(tp, addedPartition), consumer.assignment());
    }

    private void subscribeAndExpectOnPartitionsAssigned(Consumer<byte[], byte[]> consumer, List<String> topics, Collection<TopicPartition> expectedPartitionsInCallback) throws InterruptedException {
        var partitionsAssigned = new AtomicBoolean(false);
        AtomicReference<Collection<TopicPartition>> partitionsFromCallback = new AtomicReference<>();
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (partitions.containsAll(expectedPartitionsInCallback)) {
                    partitionsFromCallback.set(partitions);
                    partitionsAssigned.set(true);
                }
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // noop
            }
        });
        ClientsTestUtils.pollUntilTrue(
                consumer,
                partitionsAssigned::get,
                "Timed out before expected rebalance completed"
        );
        // These are different types, so comparing values instead
        assertTrue(expectedPartitionsInCallback.containsAll(partitionsFromCallback.get()) && partitionsFromCallback.get().containsAll(expectedPartitionsInCallback),
                "Expected partitions " + expectedPartitionsInCallback + " as parameter for onPartitionsAssigned, but got " + partitionsFromCallback.get());
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

            sendRecords(cluster, tp, totalRecords, startingTimestamp);

            triggerOnPartitionsAssigned(tp, consumer, (executeConsumer, partitions) -> {
                executeConsumer.seek(tp, startingOffset);
                executeConsumer.pause(List.of(tp));
            });

            assertTrue(consumer.paused().contains(tp));
            consumer.resume(List.of(tp));
            consumeAndVerifyRecords(
                consumer,
                tp,
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
        ClientsTestUtils.pollUntilTrue(
            consumer, 
            partitionsAssigned::get, 
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
            ClientsTestUtils.pollUntilTrue(
                consumer,
                partitionsAssigned::get,
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

    private Consumer<byte[], byte[]> createClassicConsumerCooperativeProtocol() {
        return cluster.consumer(Map.of(
                GROUP_PROTOCOL_CONFIG, CLASSIC.name.toLowerCase(Locale.ROOT),
                ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
        ));
    }
}
