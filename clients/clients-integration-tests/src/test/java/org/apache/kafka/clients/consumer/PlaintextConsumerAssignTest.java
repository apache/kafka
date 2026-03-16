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
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the consumer that covers logic related to manual assignment.
 */
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = PlaintextConsumerAssignTest.BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "3"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "10"),
    }
)
public class PlaintextConsumerAssignTest {

    public static final int BROKER_COUNT = 3;

    private final ClusterInstance clusterInstance;
    private final String topic = "topic";
    private final int partition = 0;
    TopicPartition tp = new TopicPartition(topic, partition);

    PlaintextConsumerAssignTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        clusterInstance.createTopic(topic, 2, (short) BROKER_COUNT);
    }

    @ClusterTest
    public void testClassicAssignAndCommitAsyncNotCommitted() throws Exception {
        testAssignAndCommitAsyncNotCommitted(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncAssignAndCommitAsyncNotCommitted() throws Exception {
        testAssignAndCommitAsyncNotCommitted(GroupProtocol.CONSUMER);
    }

    private void testAssignAndCommitAsyncNotCommitted(GroupProtocol groupProtocol) throws InterruptedException {
        int numRecords = 10000;
        long startingTimestamp = System.currentTimeMillis();
        CountConsumerCommitCallback cb = new CountConsumerCommitCallback();

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {
            ClientsTestUtils.sendRecords(clusterInstance, tp, numRecords, startingTimestamp);
            consumer.assign(List.of(tp));
            consumer.commitAsync(cb);
            ClientsTestUtils.pollUntilTrue(consumer, () -> cb.successCount >= 1 || cb.lastError.isPresent(),
                    10000, "Failed to observe commit callback before timeout");
            Map<TopicPartition, OffsetAndMetadata> committedOffset = consumer.committed(Set.of(tp));
            assertNotNull(committedOffset);
            // No valid fetch position due to the absence of consumer.poll; and therefore no offset was committed to
            // tp. The committed offset should be null. This is intentional.
            assertNull(committedOffset.get(tp));
            assertTrue(consumer.assignment().contains(tp));
        }
    }

    @ClusterTest
    public void testClassicAssignAndCommitSyncNotCommitted() throws Exception {
        testAssignAndCommitSyncNotCommitted(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncAssignAndCommitSyncNotCommitted() {
        testAssignAndCommitSyncNotCommitted(GroupProtocol.CONSUMER);
    }

    private void testAssignAndCommitSyncNotCommitted(GroupProtocol groupProtocol) {
        int numRecords = 10000;
        long startingTimestamp = System.currentTimeMillis();

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {
            ClientsTestUtils.sendRecords(clusterInstance, tp, numRecords, startingTimestamp);
            consumer.assign(List.of(tp));
            consumer.commitSync();
            Map<TopicPartition, OffsetAndMetadata> committedOffset = consumer.committed(Set.of(tp));
            assertNotNull(committedOffset);
            // No valid fetch position due to the absence of consumer.poll; and therefore no offset was committed to
            // tp. The committed offset should be null. This is intentional.
            assertNull(committedOffset.get(tp));
            assertTrue(consumer.assignment().contains(tp));
        }
    }

    @ClusterTest
    public void testClassicAssignAndCommitSyncAllConsumed() throws Exception {
        testAssignAndCommitSyncAllConsumed(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncAssignAndCommitSyncAllConsumed() throws Exception {
        testAssignAndCommitSyncAllConsumed(GroupProtocol.CONSUMER);
    }

    private void testAssignAndCommitSyncAllConsumed(GroupProtocol groupProtocol) throws InterruptedException {
        int numRecords = 10000;

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {
            long startingTimestamp = System.currentTimeMillis();
            ClientsTestUtils.sendRecords(clusterInstance, tp, numRecords, startingTimestamp);
            consumer.assign(List.of(tp));
            consumer.seek(tp, 0);
            ClientsTestUtils.consumeAndVerifyRecords(consumer, tp, numRecords, 0, 0, startingTimestamp);

            consumer.commitSync();
            Map<TopicPartition, OffsetAndMetadata> committedOffset = consumer.committed(Set.of(tp));
            assertNotNull(committedOffset);
            assertNotNull(committedOffset.get(tp));
            assertEquals(numRecords, committedOffset.get(tp).offset());
        }
    }

    @ClusterTest
    public void testClassicAssignAndConsume() throws InterruptedException {
        testAssignAndConsume(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncAssignAndConsume() throws InterruptedException {
        testAssignAndConsume(GroupProtocol.CONSUMER);
    }

    private void testAssignAndConsume(GroupProtocol groupProtocol) throws InterruptedException {
        int numRecords = 10;

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {
            long startingTimestamp = System.currentTimeMillis();
            ClientsTestUtils.sendRecords(clusterInstance, tp, numRecords, startingTimestamp);
            consumer.assign(List.of(tp));
            ClientsTestUtils.consumeAndVerifyRecords(consumer, tp, numRecords, 0, 0, startingTimestamp);

            assertEquals(numRecords, consumer.position(tp));
        }
    }

    @ClusterTest
    public void testClassicAssignAndConsumeSkippingPosition() throws InterruptedException {
        testAssignAndConsumeSkippingPosition(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncAssignAndConsumeSkippingPosition() throws InterruptedException {
        testAssignAndConsumeSkippingPosition(GroupProtocol.CONSUMER);
    }

    private void testAssignAndConsumeSkippingPosition(GroupProtocol groupProtocol) throws InterruptedException {
        int numRecords = 10;

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {
            long startingTimestamp = System.currentTimeMillis();
            ClientsTestUtils.sendRecords(clusterInstance, tp, numRecords, startingTimestamp);
            consumer.assign(List.of(tp));
            int offset = 1;
            consumer.seek(tp, offset);
            ClientsTestUtils.consumeAndVerifyRecords(consumer, tp, numRecords - offset, offset, offset, startingTimestamp + offset);

            assertEquals(numRecords, consumer.position(tp));
        }
    }

    @ClusterTest
    public void testClassicAssignAndFetchCommittedOffsets() throws InterruptedException {
        testAssignAndFetchCommittedOffsets(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncAssignAndFetchCommittedOffsets() throws InterruptedException {
        testAssignAndFetchCommittedOffsets(GroupProtocol.CONSUMER);
    }

    private void testAssignAndFetchCommittedOffsets(GroupProtocol groupProtocol) throws InterruptedException {
        int numRecords = 100;
        long startingTimestamp = System.currentTimeMillis();

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name, GROUP_ID_CONFIG, "group1"))) {
            ClientsTestUtils.sendRecords(clusterInstance, tp, numRecords, startingTimestamp);
            consumer.assign(List.of(tp));
            // First consumer consumes and commits offsets
            consumer.seek(tp, 0);
            ClientsTestUtils.consumeAndVerifyRecords(consumer, tp, numRecords, 0, 0, startingTimestamp);
            consumer.commitSync();
            assertEquals(numRecords, consumer.committed(Set.of(tp)).get(tp).offset());
        }

        // We should see the committed offsets from another consumer
        try (Consumer<byte[], byte[]> anotherConsumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name, GROUP_ID_CONFIG, "group1"))) {
            anotherConsumer.assign(List.of(tp));
            assertEquals(numRecords, anotherConsumer.committed(Set.of(tp)).get(tp).offset());
        }
    }

    @ClusterTest
    public void testClassicAssignAndConsumeFromCommittedOffsets() throws InterruptedException {
        testAssignAndConsumeFromCommittedOffsets(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncAssignAndConsumeFromCommittedOffsets() throws InterruptedException {
        testAssignAndConsumeFromCommittedOffsets(GroupProtocol.CONSUMER);
    }

    private void testAssignAndConsumeFromCommittedOffsets(GroupProtocol groupProtocol) throws InterruptedException {
        int numRecords = 100;
        int offset = 10;
        long startingTimestamp = System.currentTimeMillis();
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name, GROUP_ID_CONFIG, "group1");

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(config)) {
            ClientsTestUtils.sendRecords(clusterInstance, tp, numRecords, startingTimestamp);
            consumer.assign(List.of(tp));
            consumer.commitSync(Map.of(tp, new OffsetAndMetadata(offset)));
            assertEquals(offset, consumer.committed(Set.of(tp)).get(tp).offset());
        }

        // We should see the committed offsets from another consumer
        try (Consumer<byte[], byte[]> anotherConsumer = clusterInstance.consumer(config)) {
            assertEquals(offset, anotherConsumer.committed(Set.of(tp)).get(tp).offset());
            anotherConsumer.assign(List.of(tp));
            ClientsTestUtils.consumeAndVerifyRecords(anotherConsumer, tp, numRecords - offset, offset, offset, startingTimestamp + offset);
        }
    }

    @ClusterTest
    public void testClassicAssignAndRetrievingCommittedOffsetsMultipleTimes() throws InterruptedException {
        testAssignAndRetrievingCommittedOffsetsMultipleTimes(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncAssignAndRetrievingCommittedOffsetsMultipleTimes() throws InterruptedException {
        testAssignAndRetrievingCommittedOffsetsMultipleTimes(GroupProtocol.CONSUMER);
    }

    private void testAssignAndRetrievingCommittedOffsetsMultipleTimes(GroupProtocol groupProtocol) throws InterruptedException {
        int numRecords = 100;
        long startingTimestamp = System.currentTimeMillis();

        try (Consumer<byte[], byte[]> consumer = clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name))) {
            ClientsTestUtils.sendRecords(clusterInstance, tp, numRecords, startingTimestamp);
            consumer.assign(List.of(tp));

            // Consume and commit offsets
            consumer.seek(tp, 0);
            ClientsTestUtils.consumeAndVerifyRecords(consumer, tp, numRecords, 0, 0, startingTimestamp);
            consumer.commitSync();

            // Check committed offsets twice with same consumer
            assertEquals(numRecords, consumer.committed(Set.of(tp)).get(tp).offset());
            assertEquals(numRecords, consumer.committed(Set.of(tp)).get(tp).offset());
        }
    }

    private static class CountConsumerCommitCallback implements OffsetCommitCallback {
        int successCount = 0;
        int failCount = 0;
        Optional<Exception> lastError = Optional.empty();

        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception == null) {
                successCount += 1;
            } else {
                failCount += 1;
                lastError = Optional.of(exception);
            }
        }
    }
}
