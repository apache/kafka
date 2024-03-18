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
package org.apache.kafka.clients.consumer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.RackConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.TEST_NAME_WITH_RACK_CONFIG;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.TEST_NAME_WITH_CONSUMER_RACK;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.nullRacks;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.racks;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.verifyRackAssignment;
import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractStickyAssignorTest {
    protected AbstractStickyAssignor assignor;
    protected String consumerId = "consumer";
    protected String consumer1 = "consumer1";
    protected String consumer2 = "consumer2";
    protected String consumer3 = "consumer3";
    protected String consumer4 = "consumer4";
    protected Map<String, Subscription> subscriptions;
    protected String topic = "topic";
    protected String topic1 = "topic1";
    protected String topic2 = "topic2";
    protected String topic3 = "topic3";
    protected TopicPartition tp0 = tp(topic, 0);
    protected TopicPartition tp1 = tp(topic, 1);
    protected TopicPartition tp2 = tp(topic, 2);
    protected String groupId = "group";
    protected int generationId = 1;

    protected int numBrokerRacks;
    protected boolean hasConsumerRack;
    protected int replicationFactor = 2;
    private int nextPartitionIndex;

    protected abstract AbstractStickyAssignor createAssignor();

    // simulate ConsumerProtocolSubscription V0 protocol
    protected abstract Subscription buildSubscriptionV0(List<String> topics, List<TopicPartition> partitions, int generationId, int consumerIndex);

    // simulate ConsumerProtocolSubscription V1 protocol
    protected abstract Subscription buildSubscriptionV1(List<String> topics, List<TopicPartition> partitions, int generationId, int consumerIndex);

    // simulate ConsumerProtocolSubscription V2 or above protocol
    protected abstract Subscription buildSubscriptionV2Above(List<String> topics, List<TopicPartition> partitions, int generation, int consumerIndex);

    protected abstract ByteBuffer generateUserData(List<String> topics, List<TopicPartition> partitions, int generation);

    @BeforeEach
    public void setUp() {
        assignor = createAssignor();

        if (subscriptions != null) {
            subscriptions.clear();
        } else {
            subscriptions = new HashMap<>();
        }
    }

    @Test
    public void testMemberData() {
        List<String> topics = topics(topic);
        List<TopicPartition> ownedPartitions = partitions(tp(topic1, 0), tp(topic2, 1));
        List<Subscription> subscriptions = new ArrayList<>();
        // add subscription in all ConsumerProtocolSubscription versions
        subscriptions.add(buildSubscriptionV0(topics, ownedPartitions, generationId, 0));
        subscriptions.add(buildSubscriptionV1(topics, ownedPartitions, generationId, 1));
        subscriptions.add(buildSubscriptionV2Above(topics, ownedPartitions, generationId, 2));
        for (Subscription subscription : subscriptions) {
            if (subscription != null) {
                AbstractStickyAssignor.MemberData memberData = assignor.memberData(subscription);
                assertEquals(ownedPartitions, memberData.partitions, "subscription: " + subscription + " doesn't have expected owned partition");
                assertEquals(generationId, memberData.generation.orElse(-1), "subscription: " + subscription + " doesn't have expected generation id");
            }
        }
    }

    @ParameterizedTest(name = TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = {false, true})
    public void testOneConsumerNoTopic(boolean hasConsumerRack) {
        initializeRacks(hasConsumerRack ? RackConfig.BROKER_AND_CONSUMER_RACK : RackConfig.NO_CONSUMER_RACK);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        subscriptions = Collections.singletonMap(consumerId, subscription(Collections.emptyList(), 0));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = {false, true})
    public void testOneConsumerNonexistentTopic(boolean hasConsumerRack) {
        initializeRacks(hasConsumerRack ? RackConfig.BROKER_AND_CONSUMER_RACK : RackConfig.NO_CONSUMER_RACK);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, Collections.emptyList());
        subscriptions = Collections.singletonMap(consumerId, subscription(topics(topic), 0));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testOneConsumerOneTopic(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));
        subscriptions = Collections.singletonMap(consumerId, subscription(topics(topic), 0));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic, 0), tp(topic, 1), tp(topic, 2)), assignment.get(consumerId));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testOnlyAssignsPartitionsFromSubscribedTopics(RackConfig rackConfig) {
        String otherTopic = "other";

        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 2));
        subscriptions = mkMap(
                mkEntry(consumerId, buildSubscriptionV2Above(
                        topics(topic),
                        Arrays.asList(tp(topic, 0), tp(topic, 1), tp(otherTopic, 0), tp(otherTopic, 1)),
                        generationId, 0)
                )
        );

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic, 0), tp(topic, 1)), assignment.get(consumerId));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testOneConsumerMultipleTopics(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 1));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 2));
        subscriptions = Collections.singletonMap(consumerId, subscription(topics(topic1, topic2), 0));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic1, 0), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumerId));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testTwoConsumersOneTopicOnePartition(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 1));

        subscriptions.put(consumer1, subscription(topics(topic), 0));
        subscriptions.put(consumer2, subscription(topics(topic), 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testTwoConsumersOneTopicTwoPartitions(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 2));

        subscriptions.put(consumer1, subscription(topics(topic), 0));
        subscriptions.put(consumer2, subscription(topics(topic), 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic, 0)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic, 1)), assignment.get(consumer2));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testMultipleConsumersMixedTopicSubscriptions(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 3));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 2));

        subscriptions.put(consumer1, subscription(topics(topic1), 0));
        subscriptions.put(consumer2, subscription(topics(topic1, topic2), 1));
        subscriptions.put(consumer3, subscription(topics(topic1), 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic1, 0), tp(topic1, 2)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer2));
        assertEquals(partitions(tp(topic1, 1)), assignment.get(consumer3));
        assertNull(assignor.partitionsTransferringOwnership);

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testTwoConsumersTwoTopicsSixPartitions(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 3));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 3));

        subscriptions.put(consumer1, subscription(topics(topic1, topic2), 0));
        subscriptions.put(consumer2, subscription(topics(topic1, topic2), 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic1, 0), tp(topic1, 2), tp(topic2, 1)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic1, 1), tp(topic2, 0), tp(topic2, 2)), assignment.get(consumer2));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    /**
     * This unit test is testing consumer owned minQuota partitions, and expected to have maxQuota partitions situation
     */
    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testConsumerOwningMinQuotaExpectedMaxQuota(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 2));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 3));

        List<String> subscribedTopics = topics(topic1, topic2);

        subscriptions.put(consumer1,
            buildSubscriptionV2Above(subscribedTopics, partitions(tp(topic1, 0), tp(topic2, 1)), generationId, 0));
        subscriptions.put(consumer2,
            buildSubscriptionV2Above(subscribedTopics, partitions(tp(topic1, 1), tp(topic2, 2)), generationId, 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic1, 0), tp(topic2, 1), tp(topic2, 0)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic1, 1), tp(topic2, 2)), assignment.get(consumer2));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    /**
     * This unit test is testing consumers owned maxQuota partitions are more than numExpectedMaxCapacityMembers situation
     */
    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testMaxQuotaConsumerMoreThanNumExpectedMaxCapacityMembers(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 2));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 2));

        List<String> subscribedTopics = topics(topic1, topic2);

        subscriptions.put(consumer1,
            buildSubscriptionV2Above(subscribedTopics, partitions(tp(topic1, 0), tp(topic2, 0)), generationId, 0));
        subscriptions.put(consumer2,
            buildSubscriptionV2Above(subscribedTopics, partitions(tp(topic1, 1), tp(topic2, 1)), generationId, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(subscribedTopics, Collections.emptyList(), generationId, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(Collections.singletonMap(tp(topic2, 0), consumer3), assignor.partitionsTransferringOwnership);

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertEquals(partitions(tp(topic1, 0)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic1, 1), tp(topic2, 1)), assignment.get(consumer2));
        assertEquals(partitions(tp(topic2, 0)), assignment.get(consumer3));

        assertTrue(isFullyBalanced(assignment));
    }

    /**
     * This unit test is testing all consumers owned less than minQuota partitions situation
     */
    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testAllConsumersAreUnderMinQuota(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 2));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 3));

        List<String> subscribedTopics = topics(topic1, topic2);

        subscriptions.put(consumer1,
            buildSubscriptionV2Above(subscribedTopics, partitions(tp(topic1, 0)), generationId, 0));
        subscriptions.put(consumer2,
            buildSubscriptionV2Above(subscribedTopics, partitions(tp(topic1, 1)), generationId, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(subscribedTopics, Collections.emptyList(), generationId, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertEquals(partitions(tp(topic1, 0), tp(topic2, 1)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic1, 1), tp(topic2, 2)), assignment.get(consumer2));
        assertEquals(partitions(tp(topic2, 0)), assignment.get(consumer3));

        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testAddRemoveConsumerOneTopic(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));
        subscriptions.put(consumer1, subscription(topics(topic), 0));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic, 0), tp(topic, 1), tp(topic, 2)), assignment.get(consumer1));

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic), assignment.get(consumer1), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic), Collections.emptyList(), generationId, 1));
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(Collections.singletonMap(tp(topic, 2), consumer2), assignor.partitionsTransferringOwnership);

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertEquals(partitions(tp(topic, 0), tp(topic, 1)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic, 2)), assignment.get(consumer2));
        assertTrue(isFullyBalanced(assignment));

        subscriptions.remove(consumer1);
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic), assignment.get(consumer2), generationId, 1));
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(new HashSet<>(partitions(tp(topic, 2), tp(topic, 1), tp(topic, 0))),
            new HashSet<>(assignment.get(consumer2)));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testTopicBalanceAfterReassignment(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        List<String> allTopics = topics(topic1, topic2);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 12));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 12));
        subscriptions.put(consumer1, subscription(allTopics, 0));
        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assignment.forEach((consumer, tps) -> assertEquals(12, tps.stream().filter(tp -> tp.topic().equals(topic1)).count()));
        assignment.forEach((consumer, tps) -> assertEquals(12, tps.stream().filter(tp -> tp.topic().equals(topic2)).count()));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        assertTrue(isFullyBalanced(assignment));

        // Add another consumer
        subscriptions.put(consumer1, buildSubscriptionV2Above(allTopics, assignment.get(consumer1), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(allTopics, Collections.emptyList(), generationId, 1));
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assignment.forEach((consumer, tps) -> assertEquals(6, tps.stream().filter(tp -> tp.topic().equals(topic1)).count()));
        assignment.forEach((consumer, tps) -> assertEquals(6, tps.stream().filter(tp -> tp.topic().equals(topic2)).count()));
        assertTrue(isFullyBalanced(assignment));

        // Add two more consumers
        subscriptions.put(consumer1, buildSubscriptionV2Above(allTopics, assignment.get(consumer1), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(allTopics, assignment.get(consumer2), generationId, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(allTopics, Collections.emptyList(), generationId, 2));
        subscriptions.put(consumer4, buildSubscriptionV2Above(allTopics, Collections.emptyList(), generationId, 3));
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assignment.forEach((consumer, tps) -> assertEquals(3, tps.stream().filter(tp -> tp.topic().equals(topic1)).count()));
        assignment.forEach((consumer, tps) -> assertEquals(3, tps.stream().filter(tp -> tp.topic().equals(topic2)).count()));
        assertTrue(isFullyBalanced(assignment));

        // remove 2 consumers
        subscriptions.remove(consumer1);
        subscriptions.remove(consumer2);
        subscriptions.put(consumer3, buildSubscriptionV2Above(allTopics, assignment.get(consumer3), generationId, 2));
        subscriptions.put(consumer4, buildSubscriptionV2Above(allTopics, assignment.get(consumer4), generationId, 3));
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assignment.forEach((consumer, tps) -> assertEquals(6, tps.stream().filter(tp -> tp.topic().equals(topic1)).count()));
        assignment.forEach((consumer, tps) -> assertEquals(6, tps.stream().filter(tp -> tp.topic().equals(topic2)).count()));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testAddRemoveTwoConsumersTwoTopics(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        List<String> allTopics = topics(topic1, topic2);

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 3));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 4));
        subscriptions.put(consumer1, subscription(allTopics, 0));
        subscriptions.put(consumer2, subscription(allTopics, 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic1, 0), tp(topic1, 2), tp(topic2, 1), tp(topic2, 3)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic1, 1), tp(topic2, 0), tp(topic2, 2)), assignment.get(consumer2));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        // add 2 consumers
        subscriptions.put(consumer1, buildSubscriptionV2Above(allTopics, assignment.get(consumer1), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(allTopics, assignment.get(consumer2), generationId, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(allTopics, Collections.emptyList(), generationId, 2));
        subscriptions.put(consumer4, buildSubscriptionV2Above(allTopics, Collections.emptyList(), generationId, 3));
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);

        Map<TopicPartition, String> expectedPartitionsTransferringOwnership = new HashMap<>();
        expectedPartitionsTransferringOwnership.put(tp(topic1, 2), consumer3);
        expectedPartitionsTransferringOwnership.put(tp(topic2, 3), consumer3);
        expectedPartitionsTransferringOwnership.put(tp(topic2, 2), consumer4);
        assertEquals(expectedPartitionsTransferringOwnership, assignor.partitionsTransferringOwnership);

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertEquals(partitions(tp(topic1, 0), tp(topic2, 1)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic2, 0), tp(topic1, 1)), assignment.get(consumer2));
        assertEquals(partitions(tp(topic1, 2), tp(topic2, 3)), assignment.get(consumer3));
        assertEquals(partitions(tp(topic2, 2)), assignment.get(consumer4));
        assertTrue(isFullyBalanced(assignment));

        // remove 2 consumers
        subscriptions.remove(consumer1);
        subscriptions.remove(consumer2);
        subscriptions.put(consumer3, buildSubscriptionV2Above(allTopics, assignment.get(consumer3), generationId, 2));
        subscriptions.put(consumer4, buildSubscriptionV2Above(allTopics, assignment.get(consumer4), generationId, 3));
        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic1, 2), tp(topic2, 3), tp(topic1, 0), tp(topic2, 1)), assignment.get(consumer3));
        assertEquals(partitions(tp(topic2, 2), tp(topic1, 1), tp(topic2, 0)), assignment.get(consumer4));

        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    /**
     * This unit test performs sticky assignment for a scenario that round robin assignor handles poorly.
     * Topics (partitions per topic): topic1 (2), topic2 (1), topic3 (2), topic4 (1), topic5 (2)
     * Subscriptions:
     *  - consumer1: topic1, topic2, topic3, topic4, topic5
     *  - consumer2: topic1, topic3, topic5
     *  - consumer3: topic1, topic3, topic5
     *  - consumer4: topic1, topic2, topic3, topic4, topic5
     * Round Robin Assignment Result:
     *  - consumer1: topic1-0, topic3-0, topic5-0
     *  - consumer2: topic1-1, topic3-1, topic5-1
     *  - consumer3:
     *  - consumer4: topic2-0, topic4-0
     * Sticky Assignment Result:
     *  - consumer1: topic2-0, topic3-0
     *  - consumer2: topic1-0, topic3-1
     *  - consumer3: topic1-1, topic5-0
     *  - consumer4: topic4-0, topic5-1
     */
    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testPoorRoundRobinAssignmentScenario(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i <= 5; i++) {
            String topicName = String.format("topic%d", i);
            partitionsPerTopic.put(topicName, partitionInfos(topicName, (i % 2) + 1));
        }

        subscriptions.put("consumer1",
            subscription(topics("topic1", "topic2", "topic3", "topic4", "topic5"), 0));
        subscriptions.put("consumer2",
            subscription(topics("topic1", "topic3", "topic5"), 1));
        subscriptions.put("consumer3",
            subscription(topics("topic1", "topic3", "topic5"), 2));
        subscriptions.put("consumer4",
            subscription(topics("topic1", "topic2", "topic3", "topic4", "topic5"), 3));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testAddRemoveTopicTwoConsumers(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));
        subscriptions.put(consumer1, subscription(topics(topic), 0));
        subscriptions.put(consumer2, subscription(topics(topic), 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        // verify balance
        assertTrue(isFullyBalanced(assignment));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        // verify stickiness
        List<TopicPartition> consumer1Assignment1 = assignment.get(consumer1);
        List<TopicPartition> consumer2Assignment1 = assignment.get(consumer2);
        assertTrue((consumer1Assignment1.size() == 1 && consumer2Assignment1.size() == 2) ||
            (consumer1Assignment1.size() == 2 && consumer2Assignment1.size() == 1));

        partitionsPerTopic.put(topic2, partitionInfos(topic2, 3));
        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic, topic2), assignment.get(consumer1), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic, topic2), assignment.get(consumer2), generationId, 1));

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        // verify balance
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
        // verify stickiness
        List<TopicPartition> consumer1assignment = assignment.get(consumer1);
        List<TopicPartition> consumer2assignment = assignment.get(consumer2);
        assertTrue(consumer1assignment.size() == 3 && consumer2assignment.size() == 3);
        assertTrue(consumer1assignment.containsAll(consumer1Assignment1));
        assertTrue(consumer2assignment.containsAll(consumer2Assignment1));

        partitionsPerTopic.remove(topic);
        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic2), assignment.get(consumer1), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic2), assignment.get(consumer2), generationId, 1));

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        // verify balance
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
        // verify stickiness
        List<TopicPartition> consumer1Assignment3 = assignment.get(consumer1);
        List<TopicPartition> consumer2Assignment3 = assignment.get(consumer2);
        assertTrue((consumer1Assignment3.size() == 1 && consumer2Assignment3.size() == 2) ||
            (consumer1Assignment3.size() == 2 && consumer2Assignment3.size() == 1));
        assertTrue(consumer1assignment.containsAll(consumer1Assignment3));
        assertTrue(consumer2assignment.containsAll(consumer2Assignment3));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testReassignmentAfterOneConsumerLeaves(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i < 20; i++) {
            String topicName = getTopicName(i, 20);
            partitionsPerTopic.put(topicName, partitionInfos(topicName, i));
        }

        for (int i = 1; i < 20; i++) {
            List<String> topics = new ArrayList<>();
            for (int j = 1; j <= i; j++)
                topics.add(getTopicName(j, 20));
            subscriptions.put(getConsumerName(i, 20), subscription(topics, i));
        }

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);

        for (int i = 1; i < 20; i++) {
            String consumer = getConsumerName(i, 20);
            subscriptions.put(consumer,
                buildSubscriptionV2Above(subscriptions.get(consumer).topics(), assignment.get(consumer), generationId, i));
        }
        subscriptions.remove("consumer10");

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(assignor.isSticky());
    }


    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testReassignmentAfterOneConsumerAdded(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put("topic", partitionInfos("topic", 20));

        for (int i = 1; i < 10; i++)
            subscriptions.put(getConsumerName(i, 10),
                subscription(topics("topic"), i));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);

        // add a new consumer
        subscriptions.put(getConsumerName(10, 10), subscription(topics("topic"), 10));

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testSameSubscriptions(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i < 15; i++) {
            String topicName = getTopicName(i, 15);
            partitionsPerTopic.put(topicName, partitionInfos(topicName, i));
        }

        for (int i = 1; i < 9; i++) {
            List<String> topics = new ArrayList<>();
            for (int j = 1; j <= partitionsPerTopic.size(); j++)
                topics.add(getTopicName(j, 15));
            subscriptions.put(getConsumerName(i, 9), subscription(topics, i));
        }

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);

        for (int i = 1; i < 9; i++) {
            String consumer = getConsumerName(i, 9);
            subscriptions.put(consumer,
                buildSubscriptionV2Above(subscriptions.get(consumer).topics(), assignment.get(consumer), generationId, i));
        }
        subscriptions.remove(getConsumerName(5, 9));

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
    }

    @Timeout(30)
    @ParameterizedTest(name = TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = {false, true})
    public void testLargeAssignmentAndGroupWithUniformSubscription(boolean hasConsumerRack) {
        initializeRacks(hasConsumerRack ? RackConfig.BROKER_AND_CONSUMER_RACK : RackConfig.NO_CONSUMER_RACK);
        // 1 million partitions for non-rack-aware! For rack-aware, use smaller number of partitions to reduce test run time.
        int topicCount = hasConsumerRack ? 50 : 500;
        int partitionCount = 2_000;
        int consumerCount = 2_000;

        List<String> topics = new ArrayList<>();
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        for (int i = 0; i < topicCount; i++) {
            String topicName = getTopicName(i, topicCount);
            topics.add(topicName);
            partitionsPerTopic.put(topicName, partitionInfos(topicName, partitionCount));
        }

        for (int i = 0; i < consumerCount; i++) {
            subscriptions.put(getConsumerName(i, consumerCount), subscription(topics, i));
        }

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);

        for (int i = 1; i < consumerCount; i++) {
            String consumer = getConsumerName(i, consumerCount);
            subscriptions.put(consumer, buildSubscriptionV2Above(topics, assignment.get(consumer), generationId, i));
        }

        assignor.assignPartitions(partitionsPerTopic, subscriptions);
    }

    @Timeout(90)
    @ParameterizedTest(name = TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = {false, true})
    public void testLargeAssignmentAndGroupWithNonEqualSubscription(boolean hasConsumerRack) {
        initializeRacks(hasConsumerRack ? RackConfig.BROKER_AND_CONSUMER_RACK : RackConfig.NO_CONSUMER_RACK);
        // 1 million partitions for non-rack-aware! For rack-aware, use smaller number of partitions to reduce test run time.
        int topicCount = hasConsumerRack ? 50 : 500;
        int partitionCount = 2_000;
        int consumerCount = 2_000;

        List<String> topics = new ArrayList<>();
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        for (int i = 0; i < topicCount; i++) {
            String topicName = getTopicName(i, topicCount);
            topics.add(topicName);
            partitionsPerTopic.put(topicName, partitionInfos(topicName, partitionCount));
        }
        for (int i = 0; i < consumerCount; i++) {
            if (i == consumerCount - 1) {
                subscriptions.put(getConsumerName(i, consumerCount), subscription(topics.subList(0, 1), i));
            } else {
                subscriptions.put(getConsumerName(i, consumerCount), subscription(topics, i));
            }
        }

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);

        for (int i = 1; i < consumerCount; i++) {
            String consumer = getConsumerName(i, consumerCount);
            if (i == consumerCount - 1) {
                subscriptions.put(consumer, buildSubscriptionV2Above(topics.subList(0, 1), assignment.get(consumer), generationId, i));
            } else {
                subscriptions.put(consumer, buildSubscriptionV2Above(topics, assignment.get(consumer), generationId, i));
            }
        }

        assignor.assignPartitions(partitionsPerTopic, subscriptions);
    }

    @Timeout(90)
    @ParameterizedTest(name = TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = {false, true})
    public void testAssignmentAndGroupWithNonEqualSubscriptionNotTimeout(boolean hasConsumerRack) {
        initializeRacks(hasConsumerRack ? RackConfig.BROKER_AND_CONSUMER_RACK : RackConfig.NO_CONSUMER_RACK);
        int topicCount = hasConsumerRack ? 50 : 100;
        int partitionCount = 2_00;
        int consumerCount = 5_00;

        List<String> topics = new ArrayList<>();
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        for (int i = 0; i < topicCount; i++) {
            String topicName = getTopicName(i, topicCount);
            topics.add(topicName);
            partitionsPerTopic.put(topicName, partitionInfos(topicName, partitionCount));
        }
        for (int i = 0; i < consumerCount; i++) {
            if (i % 4 == 0) {
                subscriptions.put(getConsumerName(i, consumerCount),
                        subscription(topics.subList(0, topicCount / 2), i));
            } else {
                subscriptions.put(getConsumerName(i, consumerCount),
                        subscription(topics.subList(topicCount / 2, topicCount), i));
            }
        }

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);

        for (int i = 1; i < consumerCount; i++) {
            String consumer = getConsumerName(i, consumerCount);
            if (i % 4 == 0) {
                subscriptions.put(
                        consumer,
                        buildSubscriptionV2Above(topics.subList(0, topicCount / 2),
                        assignment.get(consumer), generationId, i)
                );
            } else {
                subscriptions.put(
                        consumer,
                        buildSubscriptionV2Above(topics.subList(topicCount / 2, topicCount),
                        assignment.get(consumer), generationId, i)
                );
            }
        }

        assignor.assignPartitions(partitionsPerTopic, subscriptions);
    }

    @Test
    public void testSubscriptionNotEqualAndAssignSamePartitionWith3Generation() {
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 6));
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 1));
        int[][] sequence = new int[][]{{1, 2, 3}, {1, 3, 2}, {2, 1, 3}, {2, 3, 1}, {3, 1, 2}, {3, 2, 1}};
        for (int[] ints : sequence) {
            subscriptions.put(
                    consumer1,
                    buildSubscriptionV2Above(topics(topic),
                    partitions(tp(topic, 0), tp(topic, 2)), ints[0], 0)
            );
            subscriptions.put(
                    consumer2,
                    buildSubscriptionV2Above(topics(topic),
                    partitions(tp(topic, 1), tp(topic, 2), tp(topic, 3)), ints[1], 1)
            );
            subscriptions.put(
                    consumer3,
                    buildSubscriptionV2Above(topics(topic),
                    partitions(tp(topic, 2), tp(topic, 4), tp(topic, 5)), ints[2], 2)
            );
            subscriptions.put(
                    consumer4,
                    buildSubscriptionV2Above(topics(topic1),
                    partitions(tp(topic1, 0)), 2, 3)
            );

            Map<String, List<TopicPartition>> assign = assignor.assignPartitions(partitionsPerTopic, subscriptions);
            assertEquals(assign.values().stream().mapToInt(List::size).sum(),
                    assign.values().stream().flatMap(List::stream).collect(Collectors.toSet()).size());
            for (List<TopicPartition> list: assign.values()) {
                assertTrue(list.size() >= 1 && list.size() <= 2);
            }
        }
    }

    @Timeout(60)
    @ParameterizedTest(name = TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = {false, true})
    public void testLargeAssignmentWithMultipleConsumersLeavingAndRandomSubscription(boolean hasConsumerRack) {
        initializeRacks(hasConsumerRack ? RackConfig.BROKER_AND_CONSUMER_RACK : RackConfig.NO_CONSUMER_RACK);
        Random rand = new Random();
        int topicCount = 40;
        int consumerCount = 200;

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        for (int i = 0; i < topicCount; i++) {
            String topicName = getTopicName(i, topicCount);
            partitionsPerTopic.put(topicName, partitionInfos(topicName, rand.nextInt(10) + 1));
        }

        for (int i = 0; i < consumerCount; i++) {
            List<String> topics = new ArrayList<>();
            for (int j = 0; j < rand.nextInt(20); j++)
                topics.add(getTopicName(rand.nextInt(topicCount), topicCount));
            subscriptions.put(getConsumerName(i, consumerCount), subscription(topics, i));
        }

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);

        for (int i = 1; i < consumerCount; i++) {
            String consumer = getConsumerName(i, consumerCount);
            subscriptions.put(consumer,
                buildSubscriptionV2Above(subscriptions.get(consumer).topics(), assignment.get(consumer), generationId, i));
        }
        for (int i = 0; i < 50; ++i) {
            String c = getConsumerName(rand.nextInt(consumerCount), consumerCount);
            subscriptions.remove(c);
        }

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(assignor.isSticky());
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testNewSubscription(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i < 5; i++) {
            String topicName = getTopicName(i, 5);
            partitionsPerTopic.put(topicName, partitionInfos(topicName, 1));
        }

        for (int i = 0; i < 3; i++) {
            List<String> topics = new ArrayList<>();
            for (int j = i; j <= 3 * i - 2; j++)
                topics.add(getTopicName(j, 5));
            subscriptions.put(getConsumerName(i, 3), subscription(topics, i));
        }

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);

        subscriptions.get(getConsumerName(0, 3)).topics().add(getTopicName(1, 5));

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(assignor.isSticky());
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testMoveExistingAssignments(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        String topic4 = "topic4";
        String topic5 = "topic5";
        String topic6 = "topic6";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i <= 6; i++) {
            String topicName = String.format("topic%d", i);
            partitionsPerTopic.put(topicName, partitionInfos(topicName, 1));
        }

        subscriptions.put(consumer1,
            buildSubscriptionV2Above(topics(topic1, topic2),
                partitions(tp(topic1, 0)), generationId, 0));
        subscriptions.put(consumer2,
            buildSubscriptionV2Above(topics(topic1, topic2, topic3, topic4),
                partitions(tp(topic2, 0), tp(topic3, 0)), generationId, 1));
        subscriptions.put(consumer3,
            buildSubscriptionV2Above(topics(topic2, topic3, topic4, topic5, topic6),
                partitions(tp(topic4, 0), tp(topic5, 0), tp(topic6, 0)), generationId, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertNull(assignor.partitionsTransferringOwnership);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testStickiness(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 3));

        subscriptions.put(consumer1, subscription(topics(topic1), 0));
        subscriptions.put(consumer2, subscription(topics(topic1), 1));
        subscriptions.put(consumer3, subscription(topics(topic1), 2));
        subscriptions.put(consumer4, subscription(topics(topic1), 3));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        Map<String, TopicPartition> partitionsAssigned = new HashMap<>();

        Set<Map.Entry<String, List<TopicPartition>>> assignments = assignment.entrySet();
        for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
            String consumer = entry.getKey();
            List<TopicPartition> topicPartitions = entry.getValue();
            int size = topicPartitions.size();
            assertTrue(size <= 1, "Consumer " + consumer + " is assigned more topic partitions than expected.");
            if (size == 1)
                partitionsAssigned.put(consumer, topicPartitions.get(0));
        }

        // removing the potential group leader
        subscriptions.remove(consumer1);
        subscriptions.put(consumer2,
            buildSubscriptionV2Above(topics(topic1), assignment.get(consumer2), generationId, 1));
        subscriptions.put(consumer3,
            buildSubscriptionV2Above(topics(topic1), assignment.get(consumer3), generationId, 2));
        subscriptions.put(consumer4,
            buildSubscriptionV2Above(topics(topic1), assignment.get(consumer4), generationId, 3));


        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);

        assignments = assignment.entrySet();
        for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
            String consumer = entry.getKey();
            List<TopicPartition> topicPartitions = entry.getValue();
            assertEquals(1, topicPartitions.size(), "Consumer " + consumer + " is assigned more topic partitions than expected.");
            assertTrue((!partitionsAssigned.containsKey(consumer)) || (assignment.get(consumer).contains(partitionsAssigned.get(consumer))),
                "Stickiness was not honored for consumer " + consumer);
        }
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testAssignmentUpdatedForDeletedTopic(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 1));
        partitionsPerTopic.put(topic3, partitionInfos(topic3, 100));
        subscriptions = Collections.singletonMap(consumerId, subscription(topics(topic1, topic2, topic3), 0));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        assertEquals(assignment.values().stream().mapToInt(List::size).sum(), 1 + 100);
        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testNoExceptionThrownWhenOnlySubscribedTopicDeleted(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));
        subscriptions.put(consumerId, subscription(topics(topic), 0));
        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        subscriptions.put(consumerId, buildSubscriptionV2Above(topics(topic), assignment.get(consumerId), generationId, 0));

        assignment = assignor.assign(Collections.emptyMap(), subscriptions);
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());
        assertEquals(assignment.size(), 1);
        assertTrue(assignment.get(consumerId).isEmpty());
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testReassignmentWithRandomSubscriptionsAndChanges(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        final int minNumConsumers = 20;
        final int maxNumConsumers = 40;
        final int minNumTopics = 10;
        final int maxNumTopics = 20;

        for (int round = 1; round <= 100; ++round) {
            int numTopics = minNumTopics + new Random().nextInt(maxNumTopics - minNumTopics);

            ArrayList<String> topics = new ArrayList<>();

            Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
            for (int i = 0; i < numTopics; ++i) {
                String topicName = getTopicName(i, maxNumTopics);
                topics.add(topicName);
                partitionsPerTopic.put(topicName, partitionInfos(topicName, i + 1));
            }

            int numConsumers = minNumConsumers + new Random().nextInt(maxNumConsumers - minNumConsumers);

            for (int i = 0; i < numConsumers; ++i) {
                List<String> sub = Utils.sorted(getRandomSublist(topics));
                subscriptions.put(getConsumerName(i, maxNumConsumers), subscription(sub, i));
            }

            assignor = createAssignor();

            Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
            verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);

            subscriptions.clear();
            for (int i = 0; i < numConsumers; ++i) {
                List<String> sub = Utils.sorted(getRandomSublist(topics));
                String consumer = getConsumerName(i, maxNumConsumers);
                subscriptions.put(consumer, buildSubscriptionV2Above(sub, assignment.get(consumer), generationId, i));
            }

            assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
            verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
            assertTrue(assignor.isSticky());
        }
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testAllConsumersReachExpectedQuotaAndAreConsideredFilled(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 4));

        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic), partitions(tp(topic, 0), tp(topic, 1)), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic), partitions(tp(topic, 2)), generationId, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(topics(topic), Collections.emptyList(), generationId, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic, 0), tp(topic, 1)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic, 2)), assignment.get(consumer2));
        assertEquals(partitions(tp(topic, 3)), assignment.get(consumer3));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testOwnedPartitionsAreInvalidatedForConsumerWithStaleGeneration(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 3));

        int currentGeneration = 10;

        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic, topic2), partitions(tp(topic, 0), tp(topic, 2), tp(topic2, 1)), currentGeneration, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic, topic2), partitions(tp(topic, 0), tp(topic, 2), tp(topic2, 1)), currentGeneration - 1, 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(new HashSet<>(partitions(tp(topic, 0), tp(topic, 2), tp(topic2, 1))), new HashSet<>(assignment.get(consumer1)));
        assertEquals(new HashSet<>(partitions(tp(topic, 1), tp(topic2, 0), tp(topic2, 2))), new HashSet<>(assignment.get(consumer2)));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testOwnedPartitionsAreInvalidatedForConsumerWithNoGeneration(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 3));

        int currentGeneration = 10;

        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic, topic2), partitions(tp(topic, 0), tp(topic, 2), tp(topic2, 1)), currentGeneration, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic, topic2), partitions(tp(topic, 0), tp(topic, 2), tp(topic2, 1)), DEFAULT_GENERATION, 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(new HashSet<>(partitions(tp(topic, 0), tp(topic, 2), tp(topic2, 1))), new HashSet<>(assignment.get(consumer1)));
        assertEquals(new HashSet<>(partitions(tp(topic, 1), tp(topic2, 0), tp(topic2, 2))), new HashSet<>(assignment.get(consumer2)));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testPartitionsTransferringOwnershipIncludeThePartitionClaimedByMultipleConsumersInSameGeneration(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));

        // partition topic-0 is owned by multiple consumer
        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic), partitions(tp(topic, 0), tp(topic, 1)), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic), partitions(tp(topic, 0), tp(topic, 2)), generationId, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(topics(topic), emptyList(), generationId, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        // we should include the partitions claimed by multiple consumers in partitionsTransferringOwnership
        assertEquals(Collections.singletonMap(tp(topic, 0), consumer3), assignor.partitionsTransferringOwnership);

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertEquals(partitions(tp(topic, 1)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic, 2)), assignment.get(consumer2));
        assertEquals(partitions(tp(topic, 0)), assignment.get(consumer3));
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testEnsurePartitionsAssignedToHighestGeneration(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 3));
        partitionsPerTopic.put(topic3, partitionInfos(topic3, 3));

        int currentGeneration = 10;

        // ensure partitions are always assigned to the member with the highest generation
        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic, topic2, topic3),
            partitions(tp(topic, 0), tp(topic2, 0), tp(topic3, 0)), currentGeneration, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic, topic2, topic3),
            partitions(tp(topic, 1), tp(topic2, 1), tp(topic3, 1)), currentGeneration - 1, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(topics(topic, topic2, topic3),
            partitions(tp(topic2, 1), tp(topic3, 0), tp(topic3, 2)), currentGeneration - 2, 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(new HashSet<>(partitions(tp(topic, 0), tp(topic2, 0), tp(topic3, 0))),
            new HashSet<>(assignment.get(consumer1)));
        assertEquals(new HashSet<>(partitions(tp(topic, 1), tp(topic2, 1), tp(topic3, 1))),
            new HashSet<>(assignment.get(consumer2)));
        assertEquals(new HashSet<>(partitions(tp(topic, 2), tp(topic2, 2), tp(topic3, 2))),
            new HashSet<>(assignment.get(consumer3)));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testNoReassignmentOnCurrentMembers(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 3));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 3));
        partitionsPerTopic.put(topic3, partitionInfos(topic3, 3));

        int currentGeneration = 10;

        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic, topic2, topic3, topic1),
            partitions(), DEFAULT_GENERATION, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic, topic2, topic3, topic1),
            partitions(tp(topic, 0), tp(topic2, 0), tp(topic1, 0)), currentGeneration - 1, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(topics(topic, topic2, topic3, topic1),
            partitions(tp(topic3, 2), tp(topic2, 2), tp(topic1, 1)), currentGeneration - 2, 2));
        subscriptions.put(consumer4, buildSubscriptionV2Above(topics(topic, topic2, topic3, topic1),
            partitions(tp(topic3, 1), tp(topic, 1), tp(topic, 2)), currentGeneration - 3, 3));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        // ensure assigned partitions don't get reassigned
        assertEquals(new HashSet<>(partitions(tp(topic1, 2), tp(topic2, 1), tp(topic3, 0))),
                new HashSet<>(assignment.get(consumer1)));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testOwnedPartitionsAreInvalidatedForConsumerWithMultipleGeneration(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 3));

        int currentGeneration = 10;

        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic, topic2),
            partitions(tp(topic, 0), tp(topic2, 1), tp(topic, 1)), currentGeneration, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic, topic2),
            partitions(tp(topic, 0), tp(topic2, 1), tp(topic2, 2)), currentGeneration - 2, 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(new HashSet<>(partitions(tp(topic, 0), tp(topic2, 1), tp(topic, 1))),
            new HashSet<>(assignment.get(consumer1)));
        assertEquals(new HashSet<>(partitions(tp(topic, 2), tp(topic2, 2), tp(topic2, 0))),
            new HashSet<>(assignment.get(consumer2)));
        assertTrue(assignor.partitionsTransferringOwnership.isEmpty());

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testRackAwareAssignmentWithUniformSubscription() {
        Map<String, Integer> topics = mkMap(mkEntry("t1", 6), mkEntry("t2", 7), mkEntry("t3", 2));
        List<String> allTopics = asList("t1", "t2", "t3");
        List<List<String>> consumerTopics = asList(allTopics, allTopics, allTopics);
        List<String> nonRackAwareAssignment = asList(
                "t1-0, t1-3, t2-0, t2-3, t2-6",
                "t1-1, t1-4, t2-1, t2-4, t3-0",
                "t1-2, t1-5, t2-2, t2-5, t3-1"
        );
        verifyUniformSubscription(assignor, topics, 3, nullRacks(3), racks(3), consumerTopics, nonRackAwareAssignment, -1);
        verifyUniformSubscription(assignor, topics, 3, racks(3), nullRacks(3), consumerTopics, nonRackAwareAssignment, -1);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
        verifyUniformSubscription(assignor, topics, 3, racks(3), racks(3), consumerTopics, nonRackAwareAssignment, 0);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
        verifyUniformSubscription(assignor, topics, 4, racks(4), racks(3), consumerTopics, nonRackAwareAssignment, 0);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, false);
        verifyUniformSubscription(assignor, topics, 3, racks(3), racks(3), consumerTopics, nonRackAwareAssignment, 0);
        verifyUniformSubscription(assignor, topics, 3, racks(3), asList("d", "e", "f"), consumerTopics, nonRackAwareAssignment, -1);
        verifyUniformSubscription(assignor, topics, 3, racks(3), asList(null, "e", "f"), consumerTopics, nonRackAwareAssignment, -1);

        // Verify assignment is rack-aligned for lower replication factor where brokers have a subset of partitions
        List<String> assignment = asList("t1-0, t1-3, t2-0, t2-3, t2-6", "t1-1, t1-4, t2-1, t2-4, t3-0", "t1-2, t1-5, t2-2, t2-5, t3-1");
        verifyUniformSubscription(assignor, topics, 1, racks(3), racks(3), consumerTopics, assignment, 0);
        assignment = asList("t1-0, t1-3, t2-0, t2-3, t2-6", "t1-1, t1-4, t2-1, t2-4, t3-0", "t1-2, t1-5, t2-2, t2-5, t3-1");
        verifyUniformSubscription(assignor, topics, 2, racks(3), racks(3), consumerTopics, assignment, 0);

        // One consumer on a rack with no partitions. We allocate with misaligned rack to this consumer to maintain balance.
        assignment = asList("t1-0, t1-3, t2-0, t2-3, t2-6", "t1-1, t1-4, t2-1, t2-4, t3-0", "t1-2, t1-5, t2-2, t2-5, t3-1");
        verifyUniformSubscription(assignor, topics, 3, racks(2), racks(3), consumerTopics, assignment, 5);

        // Verify that rack-awareness is improved if already owned partitions are misaligned
        assignment = asList("t1-0, t1-3, t2-0, t2-3, t2-6", "t1-1, t1-4, t2-1, t2-4, t3-0", "t1-2, t1-5, t2-2, t2-5, t3-1");
        List<String> owned = asList("t1-0, t1-1, t1-2, t1-3, t1-4", "t1-5, t2-0, t2-1, t2-2, t2-3", "t2-4, t2-5, t2-6, t3-0, t3-1");
        verifyRackAssignment(assignor, topics, 1, racks(3), racks(3), consumerTopics, owned, assignment, 0);

        // Verify that stickiness is retained when racks match
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
        verifyRackAssignment(assignor, topics, 3, racks(3), racks(3), consumerTopics, assignment, assignment, 0);
    }

    private void verifyUniformSubscription(AbstractStickyAssignor assignor,
                                           Map<String, Integer> numPartitionsPerTopic,
                                           int replicationFactor,
                                           List<String> brokerRacks,
                                           List<String> consumerRacks,
                                           List<List<String>> consumerTopics,
                                           List<String> expectedAssignments,
                                           int numPartitionsWithRackMismatch) {
        verifyRackAssignment(assignor, numPartitionsPerTopic, replicationFactor, brokerRacks, consumerRacks,
                consumerTopics, null, expectedAssignments, numPartitionsWithRackMismatch);
        verifyRackAssignment(assignor, numPartitionsPerTopic, replicationFactor, brokerRacks, consumerRacks,
                consumerTopics, expectedAssignments, expectedAssignments, numPartitionsWithRackMismatch);
    }

    @Test
    public void testRackAwareAssignmentWithNonEqualSubscription() {
        Map<String, Integer> topics = mkMap(mkEntry("t1", 6), mkEntry("t2", 7), mkEntry("t3", 2));
        List<String> allTopics = asList("t1", "t2", "t3");
        List<List<String>> consumerTopics = asList(allTopics, allTopics, asList("t1", "t3"));
        List<String> nonRackAwareAssignment = asList(
                "t1-5, t2-0, t2-2, t2-4, t2-6",
                "t1-3, t2-1, t2-3, t2-5, t3-0",
                "t1-0, t1-1, t1-2, t1-4, t3-1"
        );
        verifyNonEqualSubscription(assignor, topics, 3, nullRacks(3), racks(3), consumerTopics, nonRackAwareAssignment, -1);
        verifyNonEqualSubscription(assignor, topics, 3, racks(3), nullRacks(3), consumerTopics, nonRackAwareAssignment, -1);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
        verifyNonEqualSubscription(assignor, topics, 3, racks(3), racks(3), consumerTopics, nonRackAwareAssignment, 0);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
        verifyNonEqualSubscription(assignor, topics, 4, racks(4), racks(3), consumerTopics, nonRackAwareAssignment, 0);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, false);
        verifyNonEqualSubscription(assignor, topics, 3, racks(3), racks(3), consumerTopics, nonRackAwareAssignment, 0);
        verifyNonEqualSubscription(assignor, topics, 3, racks(3), asList("d", "e", "f"), consumerTopics, nonRackAwareAssignment, -1);
        verifyNonEqualSubscription(assignor, topics, 3, racks(3), asList(null, "e", "f"), consumerTopics, nonRackAwareAssignment, -1);

        // Verify assignment is rack-aligned for lower replication factor where brokers have a subset of partitions
        // Rack-alignment is best-effort, misalignments can occur when number of rack choices is low.
        List<String> assignment = asList("t1-3, t2-0, t2-2, t2-3, t2-6", "t1-4, t2-1, t2-4, t2-5, t3-0", "t1-0, t1-1, t1-2, t1-5, t3-1");
        verifyNonEqualSubscription(assignor, topics, 1, racks(3), racks(3), consumerTopics, assignment, 4);
        assignment = asList("t1-3, t2-0, t2-2, t2-5, t2-6", "t1-0, t2-1, t2-3, t2-4, t3-0", "t1-1, t1-2, t1-4, t1-5, t3-1");
        verifyNonEqualSubscription(assignor, topics, 2, racks(3), racks(3), consumerTopics, assignment, 0);

        // One consumer on a rack with no partitions. We allocate with misaligned rack to this consumer to maintain balance.
        verifyNonEqualSubscription(assignor, topics, 3, racks(2), racks(3), consumerTopics,
                asList("t1-5, t2-0, t2-2, t2-4, t2-6", "t1-3, t2-1, t2-3, t2-5, t3-0", "t1-0, t1-1, t1-2, t1-4, t3-1"), 5);

        // Verify that rack-awareness is improved if already owned partitions are misaligned.
        // Rack alignment is attempted, but not guaranteed.
        List<String> owned = asList("t1-0, t1-1, t1-2, t1-3, t1-4", "t1-5, t2-0, t2-1, t2-2, t2-3", "t2-4, t2-5, t2-6, t3-0, t3-1");
        if (assignor instanceof StickyAssignor) {
            assignment = asList("t1-3, t2-0, t2-2, t2-3, t2-6", "t1-4, t2-1, t2-4, t2-5, t3-0", "t1-0, t1-1, t1-2, t1-5, t3-1");
            verifyRackAssignment(assignor, topics, 1, racks(3), racks(3), consumerTopics, owned, assignment, 4);
        } else {
            List<String> intermediate = asList("t1-3", "t2-1", "t3-1");
            verifyRackAssignment(assignor, topics, 1, racks(3), racks(3), consumerTopics, owned, intermediate, 0);
            assignment = asList("t1-3, t2-0, t2-2, t2-3, t2-6", "t1-4, t2-1, t2-4, t2-5, t3-0", "t1-0, t1-1, t1-2, t1-5, t3-1");
            verifyRackAssignment(assignor, topics, 1, racks(3), racks(3), consumerTopics, intermediate, assignment, 4);
        }

        // Verify that result is same as non-rack-aware assignment if all racks match
        if (assignor instanceof StickyAssignor) {
            assignment = asList("t1-5, t2-0, t2-2, t2-4, t2-6", "t1-3, t2-1, t2-3, t2-5, t3-0", "t1-0, t1-1, t1-2, t1-4, t3-1");
            AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, false);
            verifyRackAssignment(assignor, topics, 3, racks(3), racks(3), consumerTopics, owned, assignment, 0);
            AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
            verifyRackAssignment(assignor, topics, 3, racks(3), racks(3), consumerTopics, owned, assignment, 0);
        } else {
            assignment = asList("t1-2, t1-3, t1-4, t2-4, t2-5", "t2-0, t2-1, t2-2, t2-3, t2-6", "t1-0, t1-1, t1-5, t3-0, t3-1");
            List<String> intermediate = asList("t1-2, t1-3, t1-4", "t2-0, t2-1, t2-2, t2-3", "t3-0, t3-1");
            AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, false);
            verifyRackAssignment(assignor, topics, 3, racks(3), racks(3), consumerTopics, owned, intermediate, 0);
            verifyRackAssignment(assignor, topics, 3, racks(3), racks(3), consumerTopics, intermediate, assignment, 0);
            AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
            verifyRackAssignment(assignor, topics, 3, racks(3), racks(3), consumerTopics, owned, intermediate, 0);
            verifyRackAssignment(assignor, topics, 3, racks(3), racks(3), consumerTopics, intermediate, assignment, 0);
        }
    }

    private void verifyNonEqualSubscription(AbstractStickyAssignor assignor,
                                            Map<String, Integer> numPartitionsPerTopic,
                                            int replicationFactor,
                                            List<String> brokerRacks,
                                            List<String> consumerRacks,
                                            List<List<String>> consumerTopics,
                                            List<String> expectedAssignments,
                                            int numPartitionsWithRackMismatch) {
        verifyRackAssignment(assignor, numPartitionsPerTopic, replicationFactor, brokerRacks,
                consumerRacks, consumerTopics, null, expectedAssignments, numPartitionsWithRackMismatch);
        verifyRackAssignment(assignor, numPartitionsPerTopic, replicationFactor, brokerRacks,
                consumerRacks, consumerTopics, expectedAssignments, expectedAssignments, numPartitionsWithRackMismatch);
    }

    private String getTopicName(int i, int maxNum) {
        return getCanonicalName("t", i, maxNum);
    }

    private String getConsumerName(int i, int maxNum) {
        return getCanonicalName("c", i, maxNum);
    }

    private String getCanonicalName(String str, int i, int maxNum) {
        return str + pad(i, Integer.toString(maxNum).length());
    }

    private String pad(int num, int digits) {
        StringBuilder sb = new StringBuilder();
        int iDigits = Integer.toString(num).length();

        for (int i = 1; i <= digits - iDigits; ++i)
            sb.append("0");

        sb.append(num);
        return sb.toString();
    }

    protected static List<String> topics(String... topics) {
        return Arrays.asList(topics);
    }

    protected static List<TopicPartition> partitions(TopicPartition... partitions) {
        return Arrays.asList(partitions);
    }

    protected static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

    protected Optional<String> consumerRackId(int consumerIndex) {
        int numRacks = numBrokerRacks > 0 ? numBrokerRacks : AbstractPartitionAssignorTest.ALL_RACKS.length;
        return Optional.ofNullable(hasConsumerRack ? AbstractPartitionAssignorTest.ALL_RACKS[consumerIndex % numRacks] : null);
    }

    protected Subscription subscription(List<String> topics, int consumerIndex) {
        return new Subscription(topics, null, Collections.emptyList(), DEFAULT_GENERATION, consumerRackId(consumerIndex));
    }

    protected static boolean isFullyBalanced(Map<String, List<TopicPartition>> assignment) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (List<TopicPartition> topicPartitions: assignment.values()) {
            int size = topicPartitions.size();
            if (size < min)
                min = size;
            if (size > max)
                max = size;
        }
        return max - min <= 1;
    }

    protected static List<String> getRandomSublist(ArrayList<String> list) {
        List<String> selectedItems = new ArrayList<>(list);
        int len = list.size();
        Random random = new Random();
        int howManyToRemove = random.nextInt(len);

        for (int i = 1; i <= howManyToRemove; ++i)
            selectedItems.remove(random.nextInt(selectedItems.size()));

        return selectedItems;
    }

    /**
     * Verifies that the given assignment is valid with respect to the given subscriptions
     * Validity requirements:
     * - each consumer is subscribed to topics of all partitions assigned to it, and
     * - each partition is assigned to no more than one consumer
     * Balance requirements:
     * - the assignment is fully balanced (the numbers of topic partitions assigned to consumers differ by at most one), or
     * - there is no topic partition that can be moved from one consumer to another with 2+ fewer topic partitions
     *
     * @param subscriptions: topic subscriptions of each consumer
     * @param assignments: given assignment for balance check
     * @param partitionsPerTopic: number of partitions per topic
     */
    protected void verifyValidityAndBalance(Map<String, Subscription> subscriptions,
                                            Map<String, List<TopicPartition>> assignments,
                                            Map<String, List<PartitionInfo>> partitionsPerTopic) {
        int size = subscriptions.size();
        assert size == assignments.size();

        List<String> consumers = Utils.sorted(assignments.keySet());

        for (int i = 0; i < size; ++i) {
            String consumer = consumers.get(i);
            List<TopicPartition> partitions = assignments.get(consumer);
            for (TopicPartition partition: partitions)
                assertTrue(subscriptions.get(consumer).topics().contains(partition.topic()),
                    "Error: Partition " + partition + "is assigned to c" + i + ", but it is not subscribed to Topic t" +
                    partition.topic() + "\nSubscriptions: " + subscriptions + "\nAssignments: " + assignments);

            if (i == size - 1)
                continue;

            for (int j = i + 1; j < size; ++j) {
                String otherConsumer = consumers.get(j);
                List<TopicPartition> otherPartitions = assignments.get(otherConsumer);

                Set<TopicPartition> intersection = new HashSet<>(partitions);
                intersection.retainAll(otherPartitions);
                assertTrue(intersection.isEmpty(),
                    "Error: Consumers c" + i + " and c" + j + " have common partitions assigned to them: " + intersection +
                    "\nSubscriptions: " + subscriptions + "\nAssignments: " + assignments);

                int len = partitions.size();
                int otherLen = otherPartitions.size();

                if (Math.abs(len - otherLen) <= 1)
                    continue;

                Map<String, List<Integer>> map = CollectionUtils.groupPartitionsByTopic(partitions);
                Map<String, List<Integer>> otherMap = CollectionUtils.groupPartitionsByTopic(otherPartitions);

                int moreLoaded = len > otherLen ? i : j;
                int lessLoaded = len > otherLen ? j : i;

                // If there's any overlap in the subscribed topics, we should have been able to balance partitions
                for (String topic: map.keySet()) {
                    assertFalse(otherMap.containsKey(topic),
                        "Error: Some partitions can be moved from c" + moreLoaded + " to c" + lessLoaded + " to achieve a better balance" +
                        "\nc" + i + " has " + len + " partitions, and c" + j + " has " + otherLen + " partitions." +
                        "\nSubscriptions: " + subscriptions +
                        "\nAssignments: " + assignments);
                }
            }
        }
    }


    protected AbstractStickyAssignor.MemberData memberData(Subscription subscription) {
        return assignor.memberData(subscription);
    }

    protected List<PartitionInfo> partitionInfos(String topic, int numberOfPartitions) {
        int nextIndex = nextPartitionIndex;
        nextPartitionIndex += 1;
        return AbstractPartitionAssignorTest.partitionInfos(topic, numberOfPartitions,
                replicationFactor, numBrokerRacks, nextIndex);
    }

    protected void initializeRacks(RackConfig rackConfig) {
        this.replicationFactor = 3;
        this.numBrokerRacks = rackConfig != RackConfig.NO_BROKER_RACK ? 3 : 0;
        this.hasConsumerRack = rackConfig != RackConfig.NO_CONSUMER_RACK;
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
    }
}
