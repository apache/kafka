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
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        consumer.subscribe(topics);
        consumer.setRebalanceListener(new RebalanceListener() {
            @Override
            public void onPartitionsAssigned(
                    Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                if (partitions.containsAll(expectedPartitionsInCallback)) {
                    partitionsFromCallback.set(partitions);
                    partitionsAssigned.set(true);
                }
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
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

    @ClusterTest
    public void testClassicConsumerAwareSeekAndCommitOnPartitionsAssigned() throws InterruptedException {
        testConsumerAwareSeekAndCommitOnPartitionsAssigned(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAwareSeekAndCommitOnPartitionsAssigned() throws InterruptedException {
        testConsumerAwareSeekAndCommitOnPartitionsAssigned(CONSUMER);
    }

    private void testConsumerAwareSeekAndCommitOnPartitionsAssigned(GroupProtocol groupProtocol) throws InterruptedException {
        var startingOffset = 100L;
        var totalRecords = 120;
        var startingTimestamp = 0L;

        sendRecords(cluster, tp, totalRecords, startingTimestamp);

        try (var consumer = createConsumer(groupProtocol)) {
            triggerOnPartitionsAssignedConsumerAware(tp, consumer, (rebalanceConsumer, partitions) -> {
                rebalanceConsumer.seek(tp, startingOffset);
                rebalanceConsumer.pause(List.of(tp));
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

    @ClusterTest
    public void testClassicConsumerAwarePositionOnPartitionsAssigned() throws InterruptedException {
        testConsumerAwarePositionOnPartitionsAssigned(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAwarePositionOnPartitionsAssigned() throws InterruptedException {
        testConsumerAwarePositionOnPartitionsAssigned(CONSUMER);
    }

    private void testConsumerAwarePositionOnPartitionsAssigned(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol)) {
            triggerOnPartitionsAssignedConsumerAware(tp, consumer, (rebalanceConsumer, partitions) ->
                    assertDoesNotThrow(() -> rebalanceConsumer.position(tp))
            );
        }
    }

    @ClusterTest
    public void testClassicConsumerAwareAssignmentVisibleOnPartitionsAssigned() throws InterruptedException {
        testConsumerAwareAssignmentVisibleOnPartitionsAssigned(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAwareAssignmentVisibleOnPartitionsAssigned() throws InterruptedException {
        testConsumerAwareAssignmentVisibleOnPartitionsAssigned(CONSUMER);
    }

    private void testConsumerAwareAssignmentVisibleOnPartitionsAssigned(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol)) {
            triggerOnPartitionsAssignedConsumerAware(tp, consumer, (rebalanceConsumer, partitions) ->
                    assertTrue(rebalanceConsumer.assignment().contains(tp))
            );
        }
    }

    @ClusterTest
    public void testClassicConsumerAwarePauseStatePersistsAfterAssigned() throws InterruptedException {
        testConsumerAwarePauseStatePersistsAfterAssigned(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAwarePauseStatePersistsAfterAssigned() throws InterruptedException {
        testConsumerAwarePauseStatePersistsAfterAssigned(CONSUMER);
    }

    private void testConsumerAwarePauseStatePersistsAfterAssigned(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol)) {
            triggerOnPartitionsAssignedConsumerAware(tp, consumer, (rebalanceConsumer, partitions) -> {
                rebalanceConsumer.pause(List.of(tp));
                assertTrue(rebalanceConsumer.paused().contains(tp));
            });

            // Verify pause state persists after callback completes
            assertTrue(consumer.paused().contains(tp));
            consumer.resume(List.of(tp));
        }
    }

    @ClusterTest
    public void testClassicConsumerAwareBeginningOffsetsOnPartitionsAssigned() throws InterruptedException {
        testConsumerAwareBeginningOffsetsOnPartitionsAssigned(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAwareBeginningOffsetsOnPartitionsAssigned() throws InterruptedException {
        testConsumerAwareBeginningOffsetsOnPartitionsAssigned(CONSUMER);
    }

    private void testConsumerAwareBeginningOffsetsOnPartitionsAssigned(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol)) {
            triggerOnPartitionsAssignedConsumerAware(tp, consumer, (rebalanceConsumer, partitions) -> {
                var offsets = rebalanceConsumer.beginningOffsets(List.of(tp));
                assertTrue(offsets.containsKey(tp));
                assertEquals(0L, offsets.get(tp));
            });
        }
    }

    @ClusterTest
    public void testClassicConsumerAwareCommitOnPartitionsRevoked() throws InterruptedException {
        testConsumerAwareCommitOnPartitionsRevoked(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAwareCommitOnPartitionsRevoked() throws InterruptedException {
        testConsumerAwareCommitOnPartitionsRevoked(CONSUMER);
    }

    private void testConsumerAwareCommitOnPartitionsRevoked(GroupProtocol groupProtocol) throws InterruptedException {
        triggerOnPartitionsRevokedConsumerAware(tp, groupProtocol, (rebalanceConsumer, partitions) ->
                assertDoesNotThrow(() -> rebalanceConsumer.commitSync())
        );
    }

    @ClusterTest
    public void testClassicRebalanceConsumerExpiredAfterAssignedCallback() throws InterruptedException {
        testRebalanceConsumerExpiredAfterAssignedCallback(CLASSIC);
    }

    @ClusterTest
    public void testAsyncRebalanceConsumerExpiredAfterAssignedCallback() throws InterruptedException {
        testRebalanceConsumerExpiredAfterAssignedCallback(CONSUMER);
    }

    private void testRebalanceConsumerExpiredAfterAssignedCallback(GroupProtocol groupProtocol) throws InterruptedException {
        AtomicReference<RebalanceConsumer> captured = new AtomicReference<>();
        try (var consumer = createConsumer(groupProtocol)) {
            var partitionsAssigned = new AtomicBoolean(false);
            consumer.subscribe(List.of(topic));
            consumer.setRebalanceListener(new RebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                    if (partitions.contains(tp)) {
                        captured.set(rc);
                        partitionsAssigned.set(true);
                    }
                }

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                }
            });
            ClientsTestUtils.pollUntilTrue(
                    consumer,
                    partitionsAssigned::get,
                    "Timed out before expected rebalance completed"
            );
        }

        assertNotNull(captured.get());
        assertThrows(IllegalStateException.class, () -> captured.get().assignment());
    }

    @ClusterTest
    public void testClassicConsumerAwareGroupMetadataOnPartitionsAssigned() throws InterruptedException {
        testConsumerAwareGroupMetadataOnPartitionsAssigned(CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAwareGroupMetadataOnPartitionsAssigned() throws InterruptedException {
        testConsumerAwareGroupMetadataOnPartitionsAssigned(CONSUMER);
    }

    private void testConsumerAwareGroupMetadataOnPartitionsAssigned(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol)) {
            triggerOnPartitionsAssignedConsumerAware(tp, consumer, (rebalanceConsumer, partitions) -> {
                var metadata = rebalanceConsumer.groupMetadata();
                assertNotNull(metadata);
                assertNotNull(metadata.groupId());
            });
        }
    }

    private void triggerOnPartitionsAssignedConsumerAware(
            TopicPartition tp,
            Consumer<byte[], byte[]> consumer,
            BiConsumer<RebalanceConsumer, Collection<TopicPartition>> execute
    ) throws InterruptedException {
        var partitionsAssigned = new AtomicBoolean(false);
        consumer.subscribe(List.of(topic));
        consumer.setRebalanceListener(new RebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                if (partitions.contains(tp)) {
                    execute.accept(rc, partitions);
                    partitionsAssigned.set(true);
                }
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
            }
        });
        ClientsTestUtils.pollUntilTrue(
                consumer,
                partitionsAssigned::get,
                "Timed out before expected rebalance completed"
        );
    }

    private void triggerOnPartitionsRevokedConsumerAware(
            TopicPartition tp,
            GroupProtocol protocol,
            BiConsumer<RebalanceConsumer, Collection<TopicPartition>> execute
    ) throws InterruptedException {
        var partitionsAssigned = new AtomicBoolean(false);
        var partitionsRevoked = new AtomicBoolean(false);
        try (var consumer = createConsumer(protocol)) {
            consumer.setRebalanceListener(new RebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                    if (partitions.contains(tp)) {
                        partitionsAssigned.set(true);
                    }
                }

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                    if (partitions.contains(tp)) {
                        execute.accept(rc, partitions);
                        partitionsRevoked.set(true);
                    }
                }
            });
            consumer.subscribe(List.of(topic));
            ClientsTestUtils.pollUntilTrue(
                    consumer,
                    partitionsAssigned::get,
                    "Timed out before expected rebalance completed"
            );
        }
        assertTrue(partitionsRevoked.get());
    }

    private void triggerOnPartitionsAssigned(
        TopicPartition tp,
        Consumer<byte[], byte[]> consumer,
        BiConsumer<Consumer<byte[], byte[]>, Collection<TopicPartition>> execute
    ) throws InterruptedException {
        var partitionsAssigned = new AtomicBoolean(false);
        consumer.setRebalanceListener(new RebalanceListener() {
            @Override
            public void onPartitionsAssigned(
                    Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                // Make sure the partition used in the test is actually assigned before continuing.
                if (partitions.contains(tp)) {
                    execute.accept(consumer, partitions);
                    partitionsAssigned.set(true);
                }
            }

            @Override
            public void onPartitionsRevoked(
                    Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                // noop
            }
        });
        consumer.subscribe(List.of(topic));

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
            consumer.subscribe(List.of(topic));
            consumer.setRebalanceListener(new RebalanceListener() {
                @Override
                public void onPartitionsAssigned(
                        Collection<TopicPartition> partitions, RebalanceConsumer rebalanceConsumer) {
                    // Make sure the partition used in the test is actually assigned before continuing.
                    if (partitions.contains(tp)) {
                        partitionsAssigned.set(true);
                    }
                }

                @Override
                public void onPartitionsRevoked(
                        Collection<TopicPartition> partitions, RebalanceConsumer rebalanceConsumer) {
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