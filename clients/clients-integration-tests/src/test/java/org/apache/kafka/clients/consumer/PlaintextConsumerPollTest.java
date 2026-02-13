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
import org.apache.kafka.clients.ClientsTestUtils.TestConsumerReassignmentListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.ClientsTestUtils.awaitRebalance;
import static org.apache.kafka.clients.ClientsTestUtils.consumeAndVerifyRecords;
import static org.apache.kafka.clients.ClientsTestUtils.ensureNoRebalance;
import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.clients.ClientsTestUtils.waitForPollThrowException;
import static org.apache.kafka.clients.CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = PlaintextConsumerPollTest.BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "3"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
        @ClusterConfigProperty(key = CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "10"),
    }
)
public class PlaintextConsumerPollTest {

    public static final int BROKER_COUNT = 3;
    public static final double EPSILON = 0.1;
    public static final long GROUP_MAX_SESSION_TIMEOUT_MS = 60000L;
    private final ClusterInstance cluster;
    private final String topic = "topic";
    private final TopicPartition tp = new TopicPartition(topic, 0);
    private final TopicPartition tp2 = new TopicPartition(topic, 1);

    public PlaintextConsumerPollTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        cluster.createTopic(topic, 2, (short) BROKER_COUNT);
    }

    @ClusterTest
    public void testClassicConsumerMaxPollRecords() throws InterruptedException {
        testMaxPollRecords(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerMaxPollRecords() throws InterruptedException {
        testMaxPollRecords(GroupProtocol.CONSUMER);
    }

    private void testMaxPollRecords(GroupProtocol groupProtocol) throws InterruptedException {
        var maxPollRecords = 100;
        var numRecords = 5000;
        Map<String, Object> config = Map.of(
            MAX_POLL_RECORDS_CONFIG, maxPollRecords,
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT)
        );
        var startingTimestamp = System.currentTimeMillis();
        sendRecords(cluster, tp, numRecords, startingTimestamp);
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            consumer.assign(List.of(tp));
            consumeAndVerifyRecords(
                consumer,
                tp,
                numRecords,
                maxPollRecords,
                0,
                0,
                startingTimestamp,
                -1
            );
        }
    }

    @ClusterTest
    public void testClassicConsumerMaxPollIntervalMs() throws InterruptedException {
        testMaxPollIntervalMs(Map.of(
            MAX_POLL_INTERVAL_MS_CONFIG, 1000,
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            HEARTBEAT_INTERVAL_MS_CONFIG, 500,
            SESSION_TIMEOUT_MS_CONFIG, 2000
        ));
    }

    @ClusterTest
    public void testAsyncConsumerMaxPollIntervalMs() throws InterruptedException {
        testMaxPollIntervalMs(Map.of(
            MAX_POLL_INTERVAL_MS_CONFIG, 1000,
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testMaxPollIntervalMs(Map<String, Object> config) throws InterruptedException {
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var listener = new TestConsumerReassignmentListener();
            consumer.subscribe(List.of(topic), listener);

            // rebalance to get the initial assignment
            awaitRebalance(consumer, listener);
            assertEquals(1, listener.callsToAssigned);
            assertEquals(0, listener.callsToRevoked);

            // after we extend longer than max.poll a rebalance should be triggered
            // NOTE we need to have a relatively much larger value than max.poll to let heartbeat expired for sure
            TimeUnit.MILLISECONDS.sleep(3000);

            awaitRebalance(consumer, listener);
            assertEquals(2, listener.callsToAssigned);
            assertEquals(1, listener.callsToRevoked);
        }
    }

    @ClusterTest
    public void testClassicConsumerMaxPollIntervalMsDelayInRevocation() throws InterruptedException {
        testMaxPollIntervalMsDelayInRevocation(Map.of(
            MAX_POLL_INTERVAL_MS_CONFIG, 5000,
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            HEARTBEAT_INTERVAL_MS_CONFIG, 500,
            SESSION_TIMEOUT_MS_CONFIG, 1000,
            ENABLE_AUTO_COMMIT_CONFIG, false
        ));
    }

    @ClusterTest
    public void testAsyncConsumerMaxPollIntervalMsDelayInRevocation() throws InterruptedException {
        testMaxPollIntervalMsDelayInRevocation(Map.of(
            MAX_POLL_INTERVAL_MS_CONFIG, 5000,
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            ENABLE_AUTO_COMMIT_CONFIG, false
        ));
    }

    private void testMaxPollIntervalMsDelayInRevocation(Map<String, Object> config) throws InterruptedException {
        var commitCompleted = new AtomicBoolean(false);
        var committedPosition = new AtomicLong(-1);

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var listener = new TestConsumerReassignmentListener() {
                @Override
                public void onPartitionsLost(Collection<TopicPartition> partitions) {
                    // no op
                }

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    if (!partitions.isEmpty() && partitions.contains(tp)) {
                        // on the second rebalance (after we have joined the group initially), sleep longer
                        // than session timeout and then try a commit. We should still be in the group,
                        // so the commit should succeed
                        Utils.sleep(1500);
                        committedPosition.set(consumer.position(tp));
                        var offsets = Map.of(tp, new OffsetAndMetadata(committedPosition.get()));
                        consumer.commitSync(offsets);
                        commitCompleted.set(true);
                    }
                    super.onPartitionsRevoked(partitions);
                }
            };
            consumer.subscribe(List.of(topic), listener);

            // rebalance to get the initial assignment
            awaitRebalance(consumer, listener);

            // force a rebalance to trigger an invocation of the revocation callback while in the group
            consumer.subscribe(List.of("otherTopic"), listener);
            awaitRebalance(consumer, listener);

            assertEquals(0, committedPosition.get());
            assertTrue(commitCompleted.get());
        }
    }

    @ClusterTest
    public void testClassicConsumerMaxPollIntervalMsDelayInAssignment() throws InterruptedException {
        testMaxPollIntervalMsDelayInAssignment(Map.of(
            MAX_POLL_INTERVAL_MS_CONFIG, 5000,
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            HEARTBEAT_INTERVAL_MS_CONFIG, 500,
            SESSION_TIMEOUT_MS_CONFIG, 1000,
            ENABLE_AUTO_COMMIT_CONFIG, false
        ));
    }

    @ClusterTest
    public void testAsyncConsumerMaxPollIntervalMsDelayInAssignment() throws InterruptedException {
        testMaxPollIntervalMsDelayInAssignment(Map.of(
            MAX_POLL_INTERVAL_MS_CONFIG, 5000,
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            ENABLE_AUTO_COMMIT_CONFIG, false
        ));
    }

    private void testMaxPollIntervalMsDelayInAssignment(Map<String, Object> config) throws InterruptedException {
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var listener = new TestConsumerReassignmentListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // sleep longer than the session timeout, we should still be in the group after invocation
                    Utils.sleep(1500);
                    super.onPartitionsAssigned(partitions);
                }
            };
            consumer.subscribe(List.of(topic), listener);
            // rebalance to get the initial assignment
            awaitRebalance(consumer, listener);
            // We should still be in the group after this invocation
            ensureNoRebalance(consumer, listener);
        }
    }

    @ClusterTest
    public void testClassicConsumerMaxPollIntervalMsShorterThanPollTimeout() throws InterruptedException {
        testMaxPollIntervalMsShorterThanPollTimeout(Map.of(
            MAX_POLL_INTERVAL_MS_CONFIG, 1000,
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            HEARTBEAT_INTERVAL_MS_CONFIG, 500
        ));
    }

    @ClusterTest
    public void testAsyncConsumerMaxPollIntervalMsShorterThanPollTimeout() throws InterruptedException {
        testMaxPollIntervalMsShorterThanPollTimeout(Map.of(
            MAX_POLL_INTERVAL_MS_CONFIG, 1000,
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)
        ));
    }

    private void testMaxPollIntervalMsShorterThanPollTimeout(Map<String, Object> config) throws InterruptedException {
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var listener = new TestConsumerReassignmentListener();
            consumer.subscribe(List.of(topic), listener);

            // rebalance to get the initial assignment
            awaitRebalance(consumer, listener);
            var callsToAssignedAfterFirstRebalance = listener.callsToAssigned;

            consumer.poll(Duration.ofMillis(2000));
            // If the poll above times out, it would trigger a rebalance.
            // Leave some time for the rebalance to happen and check for the rebalance event.
            consumer.poll(Duration.ofMillis(500));
            consumer.poll(Duration.ofMillis(500));

            assertEquals(callsToAssignedAfterFirstRebalance, listener.callsToAssigned);
        }
    }

    @ClusterTest
    public void testClassicConsumerPerPartitionLeadWithMaxPollRecords() throws InterruptedException {
        testPerPartitionLeadWithMaxPollRecords(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerPerPartitionLeadWithMaxPollRecords() throws InterruptedException {
        testPerPartitionLeadWithMaxPollRecords(GroupProtocol.CONSUMER);
    }

    private void testPerPartitionLeadWithMaxPollRecords(GroupProtocol groupProtocol) throws InterruptedException {
        int numMessages = 1000;
        int maxPollRecords = 10;
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, "testPerPartitionLeadWithMaxPollRecords",
            CLIENT_ID_CONFIG, "testPerPartitionLeadWithMaxPollRecords",
            MAX_POLL_RECORDS_CONFIG, maxPollRecords
        );

        sendRecords(cluster, tp, numMessages);
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            consumer.assign(List.of(tp));
            awaitNonEmptyRecords(consumer, tp, 100);

            var tags = Map.of(
                "client-id", "testPerPartitionLeadWithMaxPollRecords",
                "topic", tp.topic(),
                "partition", String.valueOf(tp.partition())
            );
            var lead = consumer.metrics()
                    .get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags));
            assertEquals(maxPollRecords, (Double) lead.metricValue(), "The lead should be " + maxPollRecords);
        }
    }

    @ClusterTest
    public void testClassicConsumerPerPartitionLagWithMaxPollRecords() throws InterruptedException {
        testPerPartitionLagWithMaxPollRecords(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerPerPartitionLagWithMaxPollRecords() throws InterruptedException {
        testPerPartitionLagWithMaxPollRecords(GroupProtocol.CONSUMER);
    }

    private void testPerPartitionLagWithMaxPollRecords(GroupProtocol groupProtocol) throws InterruptedException {
        int numMessages = 1000;
        int maxPollRecords = 10;
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, "testPerPartitionLagWithMaxPollRecords",
            CLIENT_ID_CONFIG, "testPerPartitionLagWithMaxPollRecords",
            MAX_POLL_RECORDS_CONFIG, maxPollRecords
        );
        sendRecords(cluster, tp, numMessages);
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            consumer.assign(List.of(tp));
            var records = awaitNonEmptyRecords(consumer, tp, 100);

            var tags = Map.of(
                "client-id", "testPerPartitionLagWithMaxPollRecords",
                "topic", tp.topic(),
                "partition", String.valueOf(tp.partition())
            );
            var lag = consumer.metrics()
                    .get(new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags));

            // Count the number of records received
            var recordCount = records.count();
            assertEquals(
                numMessages - recordCount,
                (Double) lag.metricValue(),
                EPSILON,
                "The lag should be " + (numMessages - recordCount)
            );
        }
    }

    @ClusterTest
    public void runCloseClassicConsumerMultiConsumerSessionTimeoutTest() throws InterruptedException {
        runMultiConsumerSessionTimeoutTest(GroupProtocol.CLASSIC, true);
    }

    @ClusterTest
    public void runClassicConsumerMultiConsumerSessionTimeoutTest() throws InterruptedException {
        runMultiConsumerSessionTimeoutTest(GroupProtocol.CLASSIC, false);
    }

    @ClusterTest
    public void runCloseAsyncConsumerMultiConsumerSessionTimeoutTest() throws InterruptedException {
        runMultiConsumerSessionTimeoutTest(GroupProtocol.CONSUMER, true);
    }

    @ClusterTest
    public void runAsyncConsumerMultiConsumerSessionTimeoutTest() throws InterruptedException {
        runMultiConsumerSessionTimeoutTest(GroupProtocol.CONSUMER, false);
    }

    private void runMultiConsumerSessionTimeoutTest(GroupProtocol groupProtocol, boolean closeConsumer) throws InterruptedException {
        String topic1 = "topic1";
        int partitions = 6;
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            GROUP_ID_CONFIG, "test-group",
            MAX_POLL_INTERVAL_MS_CONFIG, 100
        );
        // use consumers defined in this class plus one additional consumer
        // Use topic defined in this class + one additional topic
        try (Producer<byte[], byte[]> producer = cluster.producer();
             // create one more consumer and add it to the group; we will time out this consumer
             Consumer<byte[], byte[]> consumer1 = cluster.consumer(config);
             Consumer<byte[], byte[]> consumer2 = cluster.consumer(config);
             Consumer<byte[], byte[]> timeoutConsumer = cluster.consumer(config)
        ) {
            sendRecords(producer, tp, 100, System.currentTimeMillis(), -1);
            sendRecords(producer, tp2, 100, System.currentTimeMillis(), -1);

            Set<TopicPartition> subscriptions = new HashSet<>();
            subscriptions.add(tp);
            subscriptions.add(tp2);

            cluster.createTopic(topic1, partitions, (short) BROKER_COUNT);
            IntStream.range(0, partitions).forEach(partition -> {
                TopicPartition topicPartition = new TopicPartition(topic1, partition);
                sendRecords(producer, topicPartition, 100, System.currentTimeMillis(), -1);
                subscriptions.add(topicPartition);
            });

            // first subscribe consumers that are defined in this class
            List<ConsumerAssignmentPoller> consumerPollers = new ArrayList<>();
            try {
                consumerPollers.add(subscribeConsumerAndStartPolling(consumer1, List.of(topic, topic1)));
                consumerPollers.add(subscribeConsumerAndStartPolling(consumer2, List.of(topic, topic1)));

                ConsumerAssignmentPoller timeoutPoller = subscribeConsumerAndStartPolling(timeoutConsumer, List.of(topic, topic1));
                consumerPollers.add(timeoutPoller);

                // validate the initial assignment
                validateGroupAssignment(consumerPollers, subscriptions, null);

                // stop polling and close one of the consumers, should trigger partition re-assignment among alive consumers
                timeoutPoller.shutdown();
                consumerPollers.remove(timeoutPoller);
                if (closeConsumer)
                    timeoutConsumer.close();

                validateGroupAssignment(consumerPollers, subscriptions,
                        "Did not get valid assignment for partitions " + subscriptions + " after one consumer left");
            } finally {
                // done with pollers and consumers
                for (ConsumerAssignmentPoller poller : consumerPollers)
                    poller.shutdown();
            }
        }
    }

    @ClusterTest
    public void testClassicConsumerPollEventuallyReturnsRecordsWithZeroTimeout() throws InterruptedException {
        testPollEventuallyReturnsRecordsWithZeroTimeout(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerPollEventuallyReturnsRecordsWithZeroTimeout() throws InterruptedException {
        testPollEventuallyReturnsRecordsWithZeroTimeout(GroupProtocol.CONSUMER);
    }

    private void testPollEventuallyReturnsRecordsWithZeroTimeout(GroupProtocol groupProtocol) throws InterruptedException {
        int numMessages = 100;
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        sendRecords(cluster, tp, numMessages);

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            consumer.subscribe(List.of(topic));
            var records = awaitNonEmptyRecords(consumer, tp, 0L);
            assertEquals(numMessages, records.count());
        }

    }

    @ClusterTest
    public void testClassicConsumerNoOffsetForPartitionExceptionOnPollZero() throws InterruptedException {
        testNoOffsetForPartitionExceptionOnPollZero(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerNoOffsetForPartitionExceptionOnPollZero() throws InterruptedException {
        testNoOffsetForPartitionExceptionOnPollZero(GroupProtocol.CONSUMER);
    }

    private void testNoOffsetForPartitionExceptionOnPollZero(GroupProtocol groupProtocol) throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "none"
        );
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            consumer.assign(List.of(tp));

            // continuous poll should eventually fail because there is no offset reset strategy set
            // (fail only when resetting positions after coordinator is known)
            waitForPollThrowException(consumer, NoOffsetForPartitionException.class);
        }
    }

    @ClusterTest
    public void testClassicConsumerRecoveryOnPollAfterDelayedRebalance() throws InterruptedException {
        testConsumerRecoveryOnPollAfterDelayedRebalance(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerRecoveryOnPollAfterDelayedRebalance() throws InterruptedException {
        testConsumerRecoveryOnPollAfterDelayedRebalance(GroupProtocol.CONSUMER);
    }

    public void testConsumerRecoveryOnPollAfterDelayedRebalance(GroupProtocol groupProtocol) throws InterruptedException {
        var rebalanceTimeout = 1000;
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            MAX_POLL_INTERVAL_MS_CONFIG, 1000,
            ENABLE_AUTO_COMMIT_CONFIG, false
        );
        try (Producer<byte[], byte[]> producer = cluster.producer();
             // Subscribe consumer that will reconcile in time on the first rebalance, but will
             // take longer than the allowed timeout in the second rebalance (onPartitionsRevoked) to get fenced by the broker.
             // The consumer should recover after being fenced (automatically rejoin the group on the next call to poll)
             Consumer<byte[], byte[]> consumer = cluster.consumer(config)
        ) {
            var numMessages = 10;
            var otherTopic = "otherTopic";
            var tpOther = new TopicPartition(otherTopic, 0);
            cluster.createTopic(otherTopic, 1, (short) BROKER_COUNT);
            sendRecords(producer, tpOther, numMessages, System.currentTimeMillis(), -1);
            sendRecords(producer, tp, numMessages, System.currentTimeMillis(), -1);

            var rebalanceTimeoutExceeded = new AtomicBoolean(false);

            var listener = new TestConsumerReassignmentListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    if (!partitions.isEmpty() && partitions.contains(tp)) {
                        // on the second rebalance (after we have joined the group initially), sleep longer
                        // than rebalance timeout to get fenced.
                        Utils.sleep(rebalanceTimeout + 500);
                        rebalanceTimeoutExceeded.set(true);
                    }
                    super.onPartitionsRevoked(partitions);
                }
            };
            // Subscribe to get first assignment (no delays) and verify consumption
            consumer.subscribe(List.of(topic), listener);
            var records = awaitNonEmptyRecords(consumer, tp, 0L);
            assertEquals(numMessages, records.count());

            // Subscribe to different topic. This will trigger the delayed revocation exceeding rebalance timeout and get fenced
            consumer.subscribe(List.of(otherTopic), listener);
            ClientsTestUtils.pollUntilTrue(
                consumer,
                rebalanceTimeoutExceeded::get,
                "Timeout waiting for delayed callback to complete"
            );

            // Verify consumer recovers after being fenced, being able to continue consuming.
            // (The member should automatically rejoin on the next poll, with the new topic as subscription)
            records = awaitNonEmptyRecords(consumer, tpOther, 0L);
            assertEquals(numMessages, records.count());
        }
    }

    /**
     * Subscribes consumer 'consumer' to a given list of topics 'topicsToSubscribe', creates
     * consumer poller and starts polling.
     * Assumes that the consumer is not subscribed to any topics yet
     *
     * @param topicsToSubscribe topics that this consumer will subscribe to
     * @return consumer poller for the given consumer
     */
    private ConsumerAssignmentPoller subscribeConsumerAndStartPolling(
        Consumer<byte[], byte[]> consumer,
        List<String> topicsToSubscribe
    ) {
        assertEquals(0, consumer.assignment().size());
        ConsumerAssignmentPoller consumerPoller;
        consumerPoller = new ConsumerAssignmentPoller(consumer, topicsToSubscribe);
        consumerPoller.start();
        return consumerPoller;
    }

    /**
     * Check whether partition assignment is valid
     * Assumes partition assignment is valid iff
     * 1. Every consumer got assigned at least one partition
     * 2. Each partition is assigned to only one consumer
     * 3. Every partition is assigned to one of the consumers
     *
     * @param assignments set of consumer assignments; one per each consumer
     * @param partitions set of partitions that consumers subscribed to
     * @return true if partition assignment is valid
     */
    private boolean isPartitionAssignmentValid(
        List<Set<TopicPartition>> assignments,
        Set<TopicPartition> partitions
    ) {
        // check that all consumers got at least one partition
        var allNonEmptyAssignments = assignments
            .stream()
            .noneMatch(Set::isEmpty);

        if (!allNonEmptyAssignments) {
            // at least one consumer got empty assignment
            return false;
        }

        // make sure that sum of all partitions to all consumers equals total number of partitions
        var totalPartitionsInAssignments = 0;
        for (var assignment : assignments) {
            totalPartitionsInAssignments += assignment.size();
        }

        if (totalPartitionsInAssignments != partitions.size()) {
            // either same partitions got assigned to more than one consumer or some
            // partitions were not assigned
            return false;
        }

        // The above checks could miss the case where one or more partitions were assigned to more
        // than one consumer and the same number of partitions were missing from assignments.
        // Make sure that all unique assignments are the same as 'partitions'
        var uniqueAssignedPartitions = new HashSet<>();
        for (var assignment : assignments) {
            uniqueAssignedPartitions.addAll(assignment);
        }

        return uniqueAssignedPartitions.equals(partitions);
    }

    /**
     * Wait for consumers to get partition assignment and validate it.
     *
     * @param consumerPollers consumer pollers corresponding to the consumer group we are testing
     * @param subscriptions   set of all topic partitions
     * @param msg             message to print when waiting for/validating assignment fails
     */
    private void validateGroupAssignment(
        List<ConsumerAssignmentPoller> consumerPollers,
        Set<TopicPartition> subscriptions,
        String msg
    ) throws InterruptedException {
        List<Set<TopicPartition>> assignments = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            assignments.clear();
            for (ConsumerAssignmentPoller poller : consumerPollers) {
                assignments.add(poller.consumerAssignment());
            }
            return isPartitionAssignmentValid(assignments, subscriptions);
        }, GROUP_MAX_SESSION_TIMEOUT_MS * 3,
                () -> msg != null ? msg : "Did not get valid assignment for partitions " + subscriptions + ". Instead, got " + assignments
        );
    }

    private ConsumerRecords<byte[], byte[]> awaitNonEmptyRecords(
        Consumer<byte[], byte[]> consumer,
        TopicPartition partition,
        long pollTimeoutMs
    ) throws InterruptedException {
        List<ConsumerRecords<byte[], byte[]>> result = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            var records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
            result.add(records);
            return !records.records(partition).isEmpty();
        }, "Consumer did not consume any messages for partition " + partition + " before timeout.");
        return result.get(result.size() - 1);
    }
}
