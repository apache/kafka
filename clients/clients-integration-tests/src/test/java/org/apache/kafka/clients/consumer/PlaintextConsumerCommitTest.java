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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.MockConsumerInterceptor;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.clients.ClientsTestUtils.awaitAssignment;
import static org.apache.kafka.clients.ClientsTestUtils.consumeAndVerifyRecords;
import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.consumer.PlaintextConsumerCommitTest.BROKER_COUNT;
import static org.apache.kafka.clients.consumer.PlaintextConsumerCommitTest.OFFSETS_TOPIC_PARTITIONS;
import static org.apache.kafka.clients.consumer.PlaintextConsumerCommitTest.OFFSETS_TOPIC_REPLICATION;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = OFFSETS_TOPIC_PARTITIONS),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = OFFSETS_TOPIC_REPLICATION),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
    }
)
public class PlaintextConsumerCommitTest {

    public static final int BROKER_COUNT = 3;
    public static final String OFFSETS_TOPIC_PARTITIONS = "1";
    public static final String OFFSETS_TOPIC_REPLICATION = "3";
    private final ClusterInstance cluster;
    private final String topic = "topic";
    private final TopicPartition tp = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);

    public PlaintextConsumerCommitTest(ClusterInstance clusterInstance) {
        this.cluster = clusterInstance;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        cluster.createTopic(topic, 2, (short) BROKER_COUNT);
    }

    @ClusterTest
    public void testClassicConsumerAutoCommitOnClose() throws InterruptedException {
        testAutoCommitOnClose(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAutoCommitOnClose() throws InterruptedException {
        testAutoCommitOnClose(GroupProtocol.CONSUMER);
    }

    private void testAutoCommitOnClose(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol, true)) {
            sendRecords(cluster, tp, 1000);

            consumer.subscribe(List.of(topic));
            awaitAssignment(consumer, Set.of(tp, tp1));
            // should auto-commit sought positions before closing
            consumer.seek(tp, 300);
            consumer.seek(tp1, 500);
        }

        // now we should see the committed positions from another consumer
        try (var anotherConsumer = createConsumer(groupProtocol, true)) {
            assertEquals(300, anotherConsumer.committed(Set.of(tp)).get(tp).offset());
            assertEquals(500, anotherConsumer.committed(Set.of(tp1)).get(tp1).offset());
        }
    }

    @ClusterTest
    public void testClassicConsumerAutoCommitOnCloseAfterWakeup() throws InterruptedException {
        testAutoCommitOnCloseAfterWakeup(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAutoCommitOnCloseAfterWakeup() throws InterruptedException {
        testAutoCommitOnCloseAfterWakeup(GroupProtocol.CONSUMER);
    }

    private void testAutoCommitOnCloseAfterWakeup(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol, true)) {
            sendRecords(cluster, tp, 1000);

            consumer.subscribe(List.of(topic));
            awaitAssignment(consumer, Set.of(tp, tp1));

            // should auto-commit sought positions before closing
            consumer.seek(tp, 300);
            consumer.seek(tp1, 500);

            // wakeup the consumer before closing to simulate trying to break a poll
            // loop from another thread
            consumer.wakeup();
        }

        // now we should see the committed positions from another consumer
        try (var anotherConsumer = createConsumer(groupProtocol, true)) {
            assertEquals(300, anotherConsumer.committed(Set.of(tp)).get(tp).offset());
            assertEquals(500, anotherConsumer.committed(Set.of(tp1)).get(tp1).offset());
        }
    }

    @ClusterTest
    public void testClassicConsumerCommitMetadata() throws InterruptedException {
        testCommitMetadata(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerCommitMetadata() throws InterruptedException {
        testCommitMetadata(GroupProtocol.CONSUMER);
    }

    private void testCommitMetadata(GroupProtocol groupProtocol) throws InterruptedException {
        try (var consumer = createConsumer(groupProtocol, true)) {
            consumer.assign(List.of(tp));
            // sync commit
            var syncMetadata = new OffsetAndMetadata(5, Optional.of(15), "foo");
            consumer.commitSync(Map.of(tp, syncMetadata));
            assertEquals(syncMetadata, consumer.committed(Set.of(tp)).get(tp));

            // async commit
            var asyncMetadata = new OffsetAndMetadata(10, "bar");
            sendAndAwaitAsyncCommit(consumer, Map.of(tp, asyncMetadata));
            assertEquals(asyncMetadata, consumer.committed(Set.of(tp)).get(tp));

            // handle null metadata
            var nullMetadata = new OffsetAndMetadata(5, null);
            consumer.commitSync(Map.of(tp, nullMetadata));
            assertEquals(nullMetadata, consumer.committed(Set.of(tp)).get(tp));
        }
    }

    @ClusterTest
    public void testClassicConsumerNoCommittedOffsets() {
        testNoCommittedOffsets(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerNoCommittedOffsets() {
        testNoCommittedOffsets(GroupProtocol.CONSUMER);
    }

    private void testNoCommittedOffsets(GroupProtocol groupProtocol) {
        try (var consumer = createConsumer(groupProtocol, true)) {
            consumer.assign(List.of(tp));
            // commit for one partition
            var metadata = new OffsetAndMetadata(5, Optional.of(15), "foo");
            consumer.commitSync(Map.of(tp, metadata));

            TopicPartition tp2 = new TopicPartition(topic, 2);
            // fetch offset for three partitions:
            // 1. tp: exists and has committed offset
            // 2. tp1: exists but has NO committed offset
            // 3. tp2: does not exist
            var committed = consumer.committed(Set.of(tp, tp1, tp2));
            assertEquals(3, committed.size());
            assertEquals(metadata, committed.get(tp));
            assertNull(committed.get(tp1));
            assertNull(committed.get(tp2));
        }
    }

    @ClusterTest
    public void testClassicConsumerAsyncCommit() throws InterruptedException {
        testAsyncCommit(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAsyncCommit() throws InterruptedException {
        testAsyncCommit(GroupProtocol.CONSUMER);
    }

    private void testAsyncCommit(GroupProtocol groupProtocol) throws InterruptedException {
        // Ensure the __consumer_offsets topic is created to prevent transient issues,
        // such as RetriableCommitFailedException during async offset commits.
        cluster.createTopic(
            Topic.GROUP_METADATA_TOPIC_NAME,
            Integer.parseInt(OFFSETS_TOPIC_PARTITIONS),
            Short.parseShort(OFFSETS_TOPIC_REPLICATION)
        );
        try (var consumer = createConsumer(groupProtocol, false)) {
            consumer.assign(List.of(tp));

            var callback = new CountConsumerCommitCallback();
            var count = 5;
            for (var i = 1; i <= count; i++)
                consumer.commitAsync(Map.of(tp, new OffsetAndMetadata(i)), callback);

            ClientsTestUtils.pollUntilTrue(
                consumer,
                () -> callback.successCount >= count || callback.lastError.isPresent(),
                "Failed to observe commit callback before timeout"
            );

            assertEquals(Optional.empty(), callback.lastError);
            assertEquals(count, callback.successCount);
            assertEquals(new OffsetAndMetadata(count), consumer.committed(Set.of(tp)).get(tp));
        }
    }

    @ClusterTest
    public void testClassicConsumerAutoCommitIntercept() throws InterruptedException {
        testAutoCommitIntercept(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAutoCommitIntercept() throws InterruptedException {
        testAutoCommitIntercept(GroupProtocol.CONSUMER);
    }

    private void testAutoCommitIntercept(GroupProtocol groupProtocol) throws InterruptedException {
        var topic2 = "topic2";
        cluster.createTopic(topic2, 2, (short) 3);
        var numRecords = 100;
        try (var producer = cluster.producer();
             // create consumer with interceptor
             Consumer<byte[], byte[]> consumer = cluster.consumer(Map.of(
                 GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
                 ENABLE_AUTO_COMMIT_CONFIG, "true",
                 INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.test.MockConsumerInterceptor"
             ))
        ) {
            // produce records
            for (var i = 0; i < numRecords; i++) {
                producer.send(new ProducerRecord<>(tp.topic(), tp.partition(), ("key " + i).getBytes(), ("value " + i).getBytes()));
            }

            var rebalanceListener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // keep partitions paused in this test so that we can verify the commits based on specific seeks
                    consumer.pause(partitions);
                }
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // No-op
                }
            };

            changeConsumerSubscriptionAndValidateAssignment(
                consumer,
                List.of(topic),
                Set.of(tp, tp1),
                rebalanceListener
            );
            consumer.seek(tp, 10);
            consumer.seek(tp1, 20);

            // change subscription to trigger rebalance
            var commitCountBeforeRebalance = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue();
            var expectedAssignment = Set.of(tp, tp1, new TopicPartition(topic2, 0), new TopicPartition(topic2, 1));
            changeConsumerSubscriptionAndValidateAssignment(
                consumer,
                List.of(topic, topic2),
                expectedAssignment,
                rebalanceListener
            );

            // after rebalancing, we should have reset to the committed positions
            var committed1 = consumer.committed(Set.of(tp));
            assertEquals(10, committed1.get(tp).offset());
            var committed2 = consumer.committed(Set.of(tp1));
            assertEquals(20, committed2.get(tp1).offset());

            // In both CLASSIC and CONSUMER protocols, interceptors are executed in poll and close.
            // However, in the CONSUMER protocol, the assignment may be changed outside a poll, so
            // we need to poll once to ensure the interceptor is called.
            if (groupProtocol == GroupProtocol.CONSUMER) {
                consumer.poll(Duration.ZERO);
            }

            assertTrue(MockConsumerInterceptor.ON_COMMIT_COUNT.intValue() > commitCountBeforeRebalance);

            // verify commits are intercepted on close
            var commitCountBeforeClose = MockConsumerInterceptor.ON_COMMIT_COUNT.intValue();
            consumer.close();
            assertTrue(MockConsumerInterceptor.ON_COMMIT_COUNT.intValue() > commitCountBeforeClose);
            producer.close();
            // cleanup
            MockConsumerInterceptor.resetCounters();
        }
    }

    @ClusterTest
    public void testClassicConsumerCommitSpecifiedOffsets() throws InterruptedException {
        testCommitSpecifiedOffsets(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerCommitSpecifiedOffsets() throws InterruptedException {
        testCommitSpecifiedOffsets(GroupProtocol.CONSUMER);
    }

    private void testCommitSpecifiedOffsets(GroupProtocol groupProtocol) throws InterruptedException {
        try (Producer<byte[], byte[]> producer = cluster.producer();
             var consumer = createConsumer(groupProtocol, false)
        ) {
            sendRecords(producer, tp, 5, System.currentTimeMillis());
            sendRecords(producer, tp1, 7, System.currentTimeMillis());

            consumer.assign(List.of(tp, tp1));

            var pos1 = consumer.position(tp);
            var pos2 = consumer.position(tp1);

            consumer.commitSync(Map.of(tp, new OffsetAndMetadata(3L)));

            assertEquals(3, consumer.committed(Set.of(tp)).get(tp).offset());
            assertNull(consumer.committed(Collections.singleton(tp1)).get(tp1));

            // Positions should not change
            assertEquals(pos1, consumer.position(tp));
            assertEquals(pos2, consumer.position(tp1));

            consumer.commitSync(Map.of(tp1, new OffsetAndMetadata(5L)));

            assertEquals(3, consumer.committed(Set.of(tp)).get(tp).offset());
            assertEquals(5, consumer.committed(Set.of(tp1)).get(tp1).offset());

            // Using async should pick up the committed changes after commit completes
            sendAndAwaitAsyncCommit(consumer, Map.of(tp1, new OffsetAndMetadata(7L)));
            assertEquals(7, consumer.committed(Collections.singleton(tp1)).get(tp1).offset());
        }
    }

    @ClusterTest
    public void testClassicConsumerAutoCommitOnRebalance() throws InterruptedException {
        testAutoCommitOnRebalance(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerAutoCommitOnRebalance() throws InterruptedException {
        testAutoCommitOnRebalance(GroupProtocol.CONSUMER);
    }

    private void testAutoCommitOnRebalance(GroupProtocol groupProtocol) throws InterruptedException {
        var topic2 = "topic2";
        cluster.createTopic(topic2, 2, (short) BROKER_COUNT);
        try (var consumer = createConsumer(groupProtocol, true)) {
            sendRecords(cluster, tp, 1000);

            var rebalanceListener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // keep partitions paused in this test so that we can verify the commits based on specific seeks
                    consumer.pause(partitions);
                }

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

                }
            };

            consumer.subscribe(List.of(topic), rebalanceListener);
            awaitAssignment(consumer, Set.of(tp, tp1));

            consumer.seek(tp, 300);
            consumer.seek(tp1, 500);
            // change subscription to trigger rebalance
            consumer.subscribe(List.of(topic, topic2), rebalanceListener);

            var newAssignment = Set.of(tp, tp1, new TopicPartition(topic2, 0), new TopicPartition(topic2, 1));
            awaitAssignment(consumer, newAssignment);

            // after rebalancing, we should have reset to the committed positions
            assertEquals(300, consumer.committed(Set.of(tp)).get(tp).offset());
            assertEquals(500, consumer.committed(Set.of(tp1)).get(tp1).offset());
        }
    }

    @ClusterTest
    public void testClassicConsumerSubscribeAndCommitSync() throws InterruptedException {
        testSubscribeAndCommitSync(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerSubscribeAndCommitSync() throws InterruptedException {
        testSubscribeAndCommitSync(GroupProtocol.CONSUMER);
    }

    private void testSubscribeAndCommitSync(GroupProtocol groupProtocol) throws InterruptedException {
        // This test ensure that the member ID is propagated from the group coordinator when the
        // assignment is received into a subsequent offset commit
        try (var consumer = createConsumer(groupProtocol, false)) {
            assertEquals(0, consumer.assignment().size());
            consumer.subscribe(List.of(topic));
            awaitAssignment(consumer, Set.of(tp, tp1));

            consumer.seek(tp, 0);
            consumer.commitSync();
        }
    }

    @ClusterTest
    public void testClassicConsumerPositionAndCommit() throws InterruptedException {
        testPositionAndCommit(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerPositionAndCommit() throws InterruptedException {
        testPositionAndCommit(GroupProtocol.CONSUMER);
    }

    private void testPositionAndCommit(GroupProtocol groupProtocol) throws InterruptedException {
        try (Producer<byte[], byte[]> producer = cluster.producer();
             var consumer = createConsumer(groupProtocol, false);
             var otherConsumer = createConsumer(groupProtocol, false)
        ) {
            var startingTimestamp = System.currentTimeMillis();
            sendRecords(producer, tp, 5, startingTimestamp);

            var topicPartition = new TopicPartition(topic, 15);
            assertNull(consumer.committed(Collections.singleton(topicPartition)).get(topicPartition));

            // position() on a partition that we aren't subscribed to throws an exception
            assertThrows(IllegalStateException.class, () -> consumer.position(topicPartition));

            consumer.assign(List.of(tp));

            assertEquals(0L, consumer.position(tp), "position() on a partition that we are subscribed to should reset the offset");
            consumer.commitSync();
            assertEquals(0L, consumer.committed(Set.of(tp)).get(tp).offset());
            consumeAndVerifyRecords(consumer, tp, 5, 0, 0, startingTimestamp);
            assertEquals(5L, consumer.position(tp), "After consuming 5 records, position should be 5");
            consumer.commitSync();
            assertEquals(5L, consumer.committed(Set.of(tp)).get(tp).offset(), "Committed offset should be returned");

            startingTimestamp = System.currentTimeMillis();
            sendRecords(producer, tp, 1, startingTimestamp);

            // another consumer in the same group should get the same position
            otherConsumer.assign(List.of(tp));
            consumeAndVerifyRecords(otherConsumer, tp, 1, 5, 0, startingTimestamp);
        }
    }

    /**
     * This is testing when closing the consumer but commit request has already been sent.
     * During the closing, the consumer won't find the coordinator anymore.
     */
    @ClusterTest
    public void testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose() throws InterruptedException {
        try (Producer<byte[], byte[]> producer = cluster.producer();
             var consumer = createConsumer(GroupProtocol.CONSUMER, false)
        ) {
            sendRecords(producer, tp, 3, System.currentTimeMillis());
            consumer.assign(List.of(tp));

            var callback = new CountConsumerCommitCallback();

            // Close the coordinator before committing because otherwise the commit will fail to find the coordinator.
            cluster.brokerIds().forEach(cluster::shutdownBroker);

            TestUtils.waitForCondition(() -> cluster.aliveBrokers().isEmpty(), "All brokers should be shut down");

            consumer.poll(Duration.ofMillis(500));
            consumer.commitAsync(Map.of(tp, new OffsetAndMetadata(1L)), callback);

            long startTime = System.currentTimeMillis();
            consumer.close(CloseOptions.timeout(Duration.ofMillis(500)));
            long closeDuration = System.currentTimeMillis() - startTime;

            assertTrue(closeDuration < 1000, "The closing process for the consumer was too long: " + closeDuration + " ms");
            assertTrue(callback.lastError.isPresent());
            assertEquals(CommitFailedException.class, callback.lastError.get().getClass());
            assertEquals("Failed to commit offsets: Coordinator unknown and consumer is closing", callback.lastError.get().getMessage());
            assertEquals(1, callback.exceptionCount);
        }
    }

    // TODO: This only works in the new consumer, but should be fixed for the old consumer as well
    @ClusterTest
    public void testCommitAsyncCompletedBeforeConsumerCloses() throws InterruptedException {
        // This is testing the contract that asynchronous offset commit are completed before the consumer
        // is closed, even when no commit sync is performed as part of the close (due to auto-commit
        // disabled, or simply because there are no consumed offsets).

        // Create offsets topic to ensure coordinator is available during close
        cluster.createTopic(Topic.GROUP_METADATA_TOPIC_NAME, Integer.parseInt(OFFSETS_TOPIC_PARTITIONS), Short.parseShort(OFFSETS_TOPIC_REPLICATION));

        try (Producer<byte[], byte[]> producer = cluster.producer(Map.of(ProducerConfig.ACKS_CONFIG, "all"));
             var consumer = createConsumer(GroupProtocol.CONSUMER, false)
        ) {
            sendRecords(producer, tp, 3, System.currentTimeMillis());
            sendRecords(producer, tp1, 3, System.currentTimeMillis());
            consumer.assign(List.of(tp, tp1));

            // Try without looking up the coordinator first
            var cb = new CountConsumerCommitCallback();
            consumer.commitAsync(Map.of(tp, new OffsetAndMetadata(1L)), cb);
            consumer.commitAsync(Map.of(tp1, new OffsetAndMetadata(1L)), cb);

            consumer.close();
            assertEquals(2, cb.successCount);
        }
    }

    // TODO: This only works in the new consumer, but should be fixed for the old consumer as well
    @ClusterTest
    public void testCommitAsyncCompletedBeforeCommitSyncReturns() {
        // This is testing the contract that asynchronous offset commits sent previously with the
        // `commitAsync` are guaranteed to have their callbacks invoked prior to completion of
        // `commitSync` (given that it does not time out).
        try (Producer<byte[], byte[]> producer = cluster.producer();
             var consumer = createConsumer(GroupProtocol.CONSUMER, false)
        ) {
            sendRecords(producer, tp, 3, System.currentTimeMillis());
            sendRecords(producer, tp1, 3, System.currentTimeMillis());

            consumer.assign(List.of(tp, tp1));

            // Try without looking up the coordinator first
            var cb = new CountConsumerCommitCallback();
            consumer.commitAsync(Map.of(tp, new OffsetAndMetadata(1L)), cb);
            consumer.commitSync(Map.of());

            assertEquals(1, consumer.committed(Set.of(tp)).get(tp).offset());
            assertEquals(1, cb.successCount);

            // Try with coordinator known
            consumer.commitAsync(Map.of(tp, new OffsetAndMetadata(2L)), cb);
            consumer.commitSync(Map.of(tp1, new OffsetAndMetadata(2L)));

            assertEquals(2, consumer.committed(Set.of(tp)).get(tp).offset());
            assertEquals(2, consumer.committed(Set.of(tp1)).get(tp1).offset());
            assertEquals(2, cb.successCount);

            // Try with empty sync commit
            consumer.commitAsync(Map.of(tp, new OffsetAndMetadata(3L)), cb);
            consumer.commitSync(Map.of());

            assertEquals(3, consumer.committed(Set.of(tp)).get(tp).offset());
            assertEquals(2, consumer.committed(Set.of(tp1)).get(tp1).offset());
            assertEquals(3, cb.successCount);
        }
    }

    private Consumer<byte[], byte[]> createConsumer(GroupProtocol protocol, boolean enableAutoCommit) {
        return cluster.consumer(Map.of(
            GROUP_ID_CONFIG, "test-group",
            GROUP_PROTOCOL_CONFIG, protocol.name().toLowerCase(Locale.ROOT),
            ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit
        ));
    }

    private void sendAndAwaitAsyncCommit(
        Consumer<byte[], byte[]> consumer,
        Map<TopicPartition, OffsetAndMetadata> offsetsOpt
    ) throws InterruptedException {
        var commitCallback = new RetryCommitCallback(consumer, offsetsOpt);

        commitCallback.sendAsyncCommit();
        ClientsTestUtils.pollUntilTrue(
            consumer,
            () -> commitCallback.isComplete,
            "Failed to observe commit callback before timeout"
        );

        assertEquals(Optional.empty(), commitCallback.error);
    }

    private static class RetryCommitCallback implements OffsetCommitCallback {
        private boolean isComplete = false;
        private Optional<Exception> error = Optional.empty();

        private final Consumer<byte[], byte[]> consumer;
        private final Map<TopicPartition, OffsetAndMetadata> offsetsOpt;

        public RetryCommitCallback(
            Consumer<byte[], byte[]> consumer,
            Map<TopicPartition, OffsetAndMetadata> offsetsOpt
        ) {
            this.consumer = consumer;
            this.offsetsOpt = offsetsOpt;
        }

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception instanceof RetriableCommitFailedException) {
                sendAsyncCommit();
            } else {
                isComplete = true;
                error = Optional.ofNullable(exception);
            }
        }

        void sendAsyncCommit() {
            consumer.commitAsync(offsetsOpt, this);
        }
    }

    private static class CountConsumerCommitCallback implements OffsetCommitCallback {
        private int successCount = 0;
        private int exceptionCount = 0;
        private Optional<Exception> lastError = Optional.empty();

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception == null) {
                successCount += 1;
            } else {
                exceptionCount += 1;
                lastError = Optional.of(exception);
            }
        }
    }

    private void changeConsumerSubscriptionAndValidateAssignment(
        Consumer<byte[], byte[]> consumer,
        List<String> topicsToSubscribe,
        Set<TopicPartition> expectedAssignment,
        ConsumerRebalanceListener rebalanceListener
    ) throws InterruptedException {
        consumer.subscribe(topicsToSubscribe, rebalanceListener);
        awaitAssignment(consumer, expectedAssignment);
    }
}
