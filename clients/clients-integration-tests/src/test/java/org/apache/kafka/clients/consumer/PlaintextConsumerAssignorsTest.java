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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.AutoStart;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for client-side and server-side consumer assignors.
 */
@Timeout(600)
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3"),
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "group.min.session.timeout.ms", value = "100"),
        @ClusterConfigProperty(key = "group.max.session.timeout.ms", value = "60000"),
        @ClusterConfigProperty(key = "group.initial.rebalance.delay.ms", value = "10")
    }
)
public class PlaintextConsumerAssignorsTest {
    private final ClusterInstance cluster;
    private final List<Consumer<byte[], byte[]>> consumers = new ArrayList<>();
    private final List<ConsumerAssignmentPoller> pollers = new ArrayList<>();

    public PlaintextConsumerAssignorsTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(autoStart = AutoStart.NO)
    public void testAssignmentValidation() {
        var first = new TopicPartition("topic", 0);
        var second = new TopicPartition("topic", 1);
        var partitions = Set.of(first, second);
        assertFalse(isPartitionAssignmentValid(List.of(Set.of(first), partitions), partitions, List.of()));
        assertFalse(isPartitionAssignmentValid(List.of(Set.of(first), Set.of(first)), partitions, List.of()));
        assertFalse(isPartitionAssignmentValid(List.of(partitions, Set.of()), partitions, List.of()));
        assertFalse(isPartitionAssignmentValid(List.of(Set.of(first), Set.of(second)), partitions,
            List.of(partitions)));
        assertTrue(isPartitionAssignmentValid(List.of(Set.of(second), Set.of(first)), partitions,
            List.of(Set.of(first), Set.of(second))));
    }

    private boolean isPartitionAssignmentValid(List<Set<TopicPartition>> assignments,
                                               Set<TopicPartition> partitions,
                                               List<Set<TopicPartition>> expectedAssignments) {
        if (assignments.stream().anyMatch(Set::isEmpty)
            || assignments.stream().mapToInt(Set::size).sum() != partitions.size()) {
            return false;
        }
        var assigned = new HashSet<TopicPartition>();
        assignments.forEach(assigned::addAll);
        return assigned.equals(partitions)
            && (expectedAssignments.isEmpty()
                || (assignments.size() == expectedAssignments.size()
                    && new HashSet<>(assignments).equals(new HashSet<>(expectedAssignments))));
    }

    @ClusterTest
    public void testRoundRobinAssignment() throws Exception {
        testSubscriptionChanges(Map.of(
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name,
            ConsumerConfig.GROUP_ID_CONFIG, "roundrobin-group",
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName()
        ));
    }

    @ClusterTest
    public void testRemoteAssignorRange() throws Exception {
        testSubscriptionChanges(Map.of(
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name,
            ConsumerConfig.GROUP_ID_CONFIG, "range-group",
            ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "range",
            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "30000"
        ));
    }

    private void testSubscriptionChanges(Map<String, Object> configs) throws Exception {
        var consumer = createConsumer(configs);
        var expected = createTopicAndSendRecords("topic1", 2);
        expected.addAll(createTopicAndSendRecords("topic2", 2));
        assertTrue(consumer.assignment().isEmpty());
        consumer.subscribe(List.of("topic1", "topic2"));
        ClientsTestUtils.awaitAssignment(consumer, expected);

        var expanded = new HashSet<>(expected);
        expanded.addAll(createTopicAndSendRecords("topic3", 2));
        consumer.subscribe(List.of("topic1", "topic2", "topic3"));
        ClientsTestUtils.awaitAssignment(consumer, expanded);
        consumer.subscribe(List.of("topic1", "topic2"));
        ClientsTestUtils.awaitAssignment(consumer, expected);
        consumer.unsubscribe();
        assertTrue(consumer.assignment().isEmpty());
    }

    @ClusterTest
    public void testRemoteAssignorInvalid() throws Exception {
        var consumer = createConsumer(Map.of(
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name,
            ConsumerConfig.GROUP_ID_CONFIG, "invalid-assignor-group",
            ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "invalid"
        ));
        createTopicAndSendRecords("topic1", 2);
        assertTrue(consumer.assignment().isEmpty());
        consumer.subscribe(List.of("topic1"));
        var failure = new AtomicReference<UnsupportedAssignorException>();
        // Assert inside the retry loop so the polling exception is not swallowed by the wait helper.
        TestUtils.waitForCondition(() -> {
            failure.set(assertThrows(UnsupportedAssignorException.class,
                () -> consumer.poll(Duration.ofMillis(100))));
            return true;
        }, "Consumer did not reject the invalid remote assignor");
        assertTrue(failure.get().getMessage().startsWith(
            "ServerAssignor invalid is not supported. Supported assignors: "));
    }

    @ClusterTest
    public void testMultiConsumerRoundRobinAssignor() throws Exception {
        var configs = Map.<String, Object>of(
            ConsumerConfig.GROUP_ID_CONFIG, "roundrobin-group",
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName()
        );
        var partitions = createTopicAndSendRecords("topic1", 5);
        partitions.addAll(createTopicAndSendRecords("topic2", 8));
        var topics = List.of("topic1", "topic2");
        addConsumers(10, topics, configs);
        awaitGroupAssignment(partitions);
        addConsumers(1, topics, configs);
        awaitGroupAssignment(partitions);
    }

    @ClusterTest
    public void testMultiConsumerStickyAssignor() throws Exception {
        var configs = Map.<String, Object>of(
            ConsumerConfig.GROUP_ID_CONFIG, "sticky-group",
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName()
        );
        // Adding the tenth consumer should move exactly one tenth of the partitions.
        int partitionsPerConsumer = ThreadLocalRandom.current().nextInt(1, 11);
        var partitions = createTopicAndSendRecords("single-topic", partitionsPerConsumer * 10);
        var topics = List.of("single-topic");
        addConsumers(9, topics, configs);
        awaitGroupAssignment(partitions);
        var previousOwners = partitionOwners();
        addConsumers(1, topics, configs);
        awaitGroupAssignment(partitions);
        var newOwners = partitionOwners();
        long changes = partitions.stream()
            .filter(partition -> previousOwners.get(partition) != newOwners.get(partition)).count();
        assertEquals(partitionsPerConsumer, changes,
            "Only the partitions needed by the tenth consumer should change owners");
    }

    private Map<TopicPartition, ConsumerAssignmentPoller> partitionOwners() {
        var owners = new HashMap<TopicPartition, ConsumerAssignmentPoller>();
        pollers.forEach(poller -> poller.consumerAssignment().forEach(partition -> owners.put(partition, poller)));
        return owners;
    }

    @ClusterTest
    public void testMultiConsumerDefaultAssignorAndVerifyAssignment() throws Exception {
        cluster.createTopic("topic1", 3, (short) 3);
        cluster.createTopic("topic2", 3, (short) 3);
        var first = Set.of(new TopicPartition("topic1", 0), new TopicPartition("topic1", 1),
            new TopicPartition("topic2", 0), new TopicPartition("topic2", 1));
        var second = Set.of(new TopicPartition("topic1", 2), new TopicPartition("topic2", 2));
        var partitions = new HashSet<>(first);
        partitions.addAll(second);
        // Leave the assignment strategy unset to exercise the default Range assignor.
        addConsumers(2, List.of("topic1", "topic2"), Map.of());
        awaitGroupAssignment(partitions, List.of(first, second));
    }

    @ClusterTest
    public void testMultiConsumerDefaultAssignor() throws Exception {
        var partitions = createTopicAndSendRecords("topic", 2);
        partitions.addAll(createTopicAndSendRecords("topic1", 5));
        var topics = List.of("topic", "topic1");
        addConsumers(2, topics, Map.of());
        awaitGroupAssignment(partitions);
        addConsumers(2, topics, Map.of());
        awaitGroupAssignment(partitions);
        var expanded = new HashSet<>(partitions);
        expanded.addAll(createTopicAndSendRecords("topic2", 3));
        changeSubscriptions(List.of("topic", "topic1", "topic2"), expanded);
        changeSubscriptions(topics, partitions);
    }

    @ClusterTest
    public void testRebalanceAndRejoinWithRangeAssignor() throws Exception {
        testRebalanceAndRejoin(RangeAssignor.class.getName(), 1);
    }

    @ClusterTest
    public void testRebalanceAndRejoinWithCooperativeStickyAssignor() throws Exception {
        testRebalanceAndRejoin(CooperativeStickyAssignor.class.getName(), 2);
    }

    private void testRebalanceAndRejoin(String strategy, int expectedGenerations) throws Exception {
        var configs = Map.<String, Object>of(
            ConsumerConfig.GROUP_ID_CONFIG, "rebalance-and-rejoin-group",
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, strategy,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"
        );
        var consumer1 = createConsumer(configs);
        var consumer2 = createConsumer(configs);
        var partitions = createTopicAndSendRecords("topic1", 2);
        assertTrue(consumer1.assignment().isEmpty());
        assertTrue(consumer2.assignment().isEmpty());
        var metadata = new AtomicReference<ConsumerGroupMetadata>();
        var listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
                metadata.set(consumer1.groupMetadata());
            }
        };
        var poller1 = startPolling(consumer1, List.of("topic1"), listener);
        TestUtils.waitForCondition(() -> poller1.consumerAssignment().equals(partitions) && metadata.get() != null,
            "First consumer did not complete its initial assignment");
        var stableMetadata = metadata.get();
        var poller2 = startPolling(consumer2, List.of("topic1"), null);
        TestUtils.waitForCondition(() -> poller1.consumerAssignment().size() == 1
                && poller2.consumerAssignment().size() == 1
                && metadata.get().generationId() >= stableMetadata.generationId() + expectedGenerations,
            "Consumers did not complete the expected rebalance");
        assertEquals(stableMetadata.generationId() + expectedGenerations, metadata.get().generationId());
        assertEquals(stableMetadata.memberId(), metadata.get().memberId());
    }

    private Consumer<byte[], byte[]> createConsumer(Map<String, Object> overrides) {
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "my-test");
        configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        configs.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100");
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "6000");
        configs.putAll(overrides);
        Consumer<byte[], byte[]> consumer = cluster.consumer(configs);
        consumers.add(consumer);
        return consumer;
    }

    private Set<TopicPartition> createTopicAndSendRecords(String topic, int partitionCount) throws Exception {
        cluster.createTopic(topic, partitionCount, (short) 3);
        var partitions = new HashSet<TopicPartition>();
        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            for (int partition = 0; partition < partitionCount; partition++) {
                var tp = new TopicPartition(topic, partition);
                partitions.add(tp);
                ClientsTestUtils.sendRecords(producer, tp, 100, System.currentTimeMillis());
            }
        }
        return partitions;
    }

    private void addConsumers(int count, List<String> topics, Map<String, Object> configs) {
        for (int i = 0; i < count; i++) {
            startPolling(createConsumer(configs), topics, null);
        }
    }

    private ConsumerAssignmentPoller startPolling(Consumer<byte[], byte[]> consumer, List<String> topics,
                                                  ConsumerRebalanceListener listener) {
        var poller = new ConsumerAssignmentPoller(consumer, topics, Set.of(), listener);
        pollers.add(poller);
        poller.start();
        return poller;
    }

    private void changeSubscriptions(List<String> topics, Set<TopicPartition> partitions) throws InterruptedException {
        pollers.forEach(poller -> poller.subscribe(topics));
        TestUtils.waitForCondition(() -> pollers.stream().allMatch(ConsumerAssignmentPoller::isSubscribeRequestProcessed),
            "Not all consumers processed the subscription change");
        awaitGroupAssignment(partitions);
    }

    private void awaitGroupAssignment(Set<TopicPartition> partitions) throws InterruptedException {
        awaitGroupAssignment(partitions, List.of());
    }

    private void awaitGroupAssignment(Set<TopicPartition> partitions,
                                      List<Set<TopicPartition>> expectedAssignments) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            var assignments = new ArrayList<Set<TopicPartition>>();
            for (var poller : pollers) {
                poller.getThrownException().ifPresent(exception -> {
                    throw new AssertionError("Consumer poller failed", exception);
                });
                assignments.add(poller.consumerAssignment());
            }
            return isPartitionAssignmentValid(assignments, partitions, expectedAssignments);
        }, "Consumers did not reach the expected assignment for " + partitions);
    }

    @AfterEach
    public void tearDown() {
        try {
            assertAll(pollers.stream().map(poller -> poller::shutdown));
        } finally {
            assertAll(consumers.stream().map(consumer -> consumer::close));
        }
    }
}
