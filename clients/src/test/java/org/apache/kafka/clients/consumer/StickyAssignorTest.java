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

import static org.apache.kafka.clients.consumer.StickyAssignor.serializeTopicPartitionAssignment;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.TEST_NAME_WITH_RACK_CONFIG;
import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.RackConfig;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.MemberData;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignorTest;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import static java.util.Collections.emptyList;

public class StickyAssignorTest extends AbstractStickyAssignorTest {

    @Override
    public AbstractStickyAssignor createAssignor() {
        return new StickyAssignor();
    }

    @Override
    public Subscription buildSubscriptionV0(List<String> topics, List<TopicPartition> partitions, int generationId, int consumerIndex) {
        return new Subscription(topics, serializeTopicPartitionAssignment(new MemberData(partitions, Optional.of(generationId))),
            Collections.emptyList(), DEFAULT_GENERATION, consumerRackId(consumerIndex));
    }

    @Override
    public Subscription buildSubscriptionV1(List<String> topics, List<TopicPartition> partitions, int generationId, int consumerIndex) {
        return new Subscription(topics, serializeTopicPartitionAssignment(new MemberData(partitions, Optional.of(generationId))),
            partitions, DEFAULT_GENERATION, Optional.empty());
    }

    @Override
    public Subscription buildSubscriptionV2Above(List<String> topics, List<TopicPartition> partitions, int generationId, int consumerIndex) {
        return new Subscription(topics, serializeTopicPartitionAssignment(new MemberData(partitions, Optional.of(generationId))),
            partitions, generationId, Optional.empty());
    }

    @Override
    public ByteBuffer generateUserData(List<String> topics, List<TopicPartition> partitions, int generation) {
        return serializeTopicPartitionAssignment(new MemberData(partitions, Optional.of(generation)));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testAllConsumersHaveOwnedPartitionInvalidatedWhenClaimedByMultipleConsumersInSameGenerationWithEqualPartitionsPerConsumer(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));

        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic), partitions(tp(topic, 0), tp(topic, 1)), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic), partitions(tp(topic, 0), tp(topic, 2)), generationId, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(topics(topic), emptyList(), generationId, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic, 1)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic, 2)), assignment.get(consumer2));
        assertEquals(partitions(tp(topic, 0)), assignment.get(consumer3));

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testAllConsumersHaveOwnedPartitionInvalidatedWhenClaimedByMultipleConsumersInSameGenerationWithUnequalPartitionsPerConsumer(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 4));

        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic), partitions(tp(topic, 0), tp(topic, 1)), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(topics(topic), partitions(tp(topic, 0), tp(topic, 2)), generationId, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(topics(topic), emptyList(), generationId, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic, 1), tp(topic, 3)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic, 2)), assignment.get(consumer2));
        assertEquals(partitions(tp(topic, 0)), assignment.get(consumer3));

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = "{displayName}.rackConfig = {0}, isAllSubscriptionsEqual = {1}")
    @MethodSource("rackAndSubscriptionCombinations")
    public void testAssignmentWithMultipleGenerations1(RackConfig rackConfig, boolean isAllSubscriptionsEqual) {
        initializeRacks(rackConfig);
        List<String> allTopics = topics(topic, topic2);
        List<String> consumer2SubscribedTopics = isAllSubscriptionsEqual ? allTopics : topics(topic);

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 6));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 6));
        subscriptions.put(consumer1, subscription(allTopics, 0));
        subscriptions.put(consumer2, subscription(consumer2SubscribedTopics, 1));
        subscriptions.put(consumer3, subscription(allTopics, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        List<TopicPartition> r1partitions1 = assignment.get(consumer1);
        List<TopicPartition> r1partitions2 = assignment.get(consumer2);
        List<TopicPartition> r1partitions3 = assignment.get(consumer3);
        assertTrue(r1partitions1.size() == 4 && r1partitions2.size() == 4 && r1partitions3.size() == 4);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        subscriptions.put(consumer1, buildSubscriptionV2Above(allTopics, r1partitions1, generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(consumer2SubscribedTopics, r1partitions2, generationId, 1));
        subscriptions.remove(consumer3);

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        List<TopicPartition> r2partitions1 = assignment.get(consumer1);
        List<TopicPartition> r2partitions2 = assignment.get(consumer2);
        assertTrue(r2partitions1.size() == 6 && r2partitions2.size() == 6);
        if (isAllSubscriptionsEqual) {
            // only true in all subscription equal case
            assertTrue(r2partitions1.containsAll(r1partitions1));
        }
        assertTrue(r2partitions2.containsAll(r1partitions2));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
        assertFalse(Collections.disjoint(r2partitions2, r1partitions3));

        subscriptions.remove(consumer1);
        subscriptions.put(consumer2, buildSubscriptionV2Above(consumer2SubscribedTopics, r2partitions2, 2, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(allTopics, r1partitions3, 1, 2));

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        List<TopicPartition> r3partitions2 = assignment.get(consumer2);
        List<TopicPartition> r3partitions3 = assignment.get(consumer3);
        assertTrue(r3partitions2.size() == 6 && r3partitions3.size() == 6);
        assertTrue(Collections.disjoint(r3partitions2, r3partitions3));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = "{displayName}.rackConfig = {0}, isAllSubscriptionsEqual = {1}")
    @MethodSource("rackAndSubscriptionCombinations")
    public void testAssignmentWithMultipleGenerations2(RackConfig rackConfig, boolean isAllSubscriptionsEqual) {
        initializeRacks(rackConfig);
        List<String> allTopics = topics(topic, topic2, topic3);
        List<String> consumer1SubscribedTopics = isAllSubscriptionsEqual ? allTopics : topics(topic);
        List<String> consumer3SubscribedTopics = isAllSubscriptionsEqual ? allTopics : topics(topic, topic2);

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 4));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 4));
        partitionsPerTopic.put(topic3, partitionInfos(topic3, 4));
        subscriptions.put(consumer1, subscription(consumer1SubscribedTopics, 0));
        subscriptions.put(consumer2, subscription(allTopics, 1));
        subscriptions.put(consumer3, subscription(consumer3SubscribedTopics, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        List<TopicPartition> r1partitions1 = assignment.get(consumer1);
        List<TopicPartition> r1partitions2 = assignment.get(consumer2);
        List<TopicPartition> r1partitions3 = assignment.get(consumer3);
        assertTrue(r1partitions1.size() == 4 && r1partitions2.size() == 4 && r1partitions3.size() == 4);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        subscriptions.remove(consumer1);
        subscriptions.put(consumer2, buildSubscriptionV2Above(allTopics, r1partitions2, 1, 1));
        subscriptions.remove(consumer3);

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        List<TopicPartition> r2partitions2 = assignment.get(consumer2);
        assertEquals(12, r2partitions2.size());
        assertTrue(r2partitions2.containsAll(r1partitions2));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        subscriptions.put(consumer1, buildSubscriptionV2Above(consumer1SubscribedTopics, r1partitions1, 1, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(allTopics, r2partitions2, 2, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(consumer3SubscribedTopics, r1partitions3, 1, 2));

        assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        List<TopicPartition> r3partitions1 = assignment.get(consumer1);
        List<TopicPartition> r3partitions2 = assignment.get(consumer2);
        List<TopicPartition> r3partitions3 = assignment.get(consumer3);
        assertTrue(r3partitions1.size() == 4 && r3partitions2.size() == 4 && r3partitions3.size() == 4);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = "{displayName}.rackConfig = {0}, isAllSubscriptionsEqual = {1}")
    @MethodSource("rackAndSubscriptionCombinations")
    public void testAssignmentWithConflictingPreviousGenerations(RackConfig rackConfig, boolean isAllSubscriptionsEqual) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 4));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, 4));
        partitionsPerTopic.put(topic3, partitionInfos(topic3, 4));

        List<String> allTopics = topics(topic, topic2, topic3);
        List<String> consumer1SubscribedTopics = isAllSubscriptionsEqual ? allTopics : topics(topic);
        List<String> consumer2SubscribedTopics = isAllSubscriptionsEqual ? allTopics : topics(topic, topic2);

        subscriptions.put(consumer1, subscription(consumer1SubscribedTopics, 0));
        subscriptions.put(consumer2, subscription(consumer2SubscribedTopics, 1));
        subscriptions.put(consumer3, subscription(allTopics, 2));

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        TopicPartition tp2 = new TopicPartition(topic, 2);
        TopicPartition tp3 = new TopicPartition(topic, 3);
        TopicPartition t2p0 = new TopicPartition(topic2, 0);
        TopicPartition t2p1 = new TopicPartition(topic2, 1);
        TopicPartition t2p2 = new TopicPartition(topic2, 2);
        TopicPartition t2p3 = new TopicPartition(topic2, 3);
        TopicPartition t3p0 = new TopicPartition(topic3, 0);
        TopicPartition t3p1 = new TopicPartition(topic3, 1);
        TopicPartition t3p2 = new TopicPartition(topic3, 2);
        TopicPartition t3p3 = new TopicPartition(topic3, 3);

        List<TopicPartition> c1partitions0 = isAllSubscriptionsEqual ? partitions(tp0, tp1, tp2, t2p2, t2p3, t3p0) :
            partitions(tp0, tp1, tp2, tp3);
        List<TopicPartition> c2partitions0 = partitions(tp0, tp1, t2p0, t2p1, t2p2, t2p3);
        List<TopicPartition> c3partitions0 = partitions(tp2, tp3, t3p0, t3p1, t3p2, t3p3);
        subscriptions.put(consumer1, buildSubscriptionV2Above(consumer1SubscribedTopics, c1partitions0, 1, 0));
        subscriptions.put(consumer2, buildSubscriptionV2Above(consumer2SubscribedTopics, c2partitions0, 2, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(allTopics, c3partitions0, 2, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        List<TopicPartition> c1partitions = assignment.get(consumer1);
        List<TopicPartition> c2partitions = assignment.get(consumer2);
        List<TopicPartition> c3partitions = assignment.get(consumer3);

        assertTrue(c1partitions.size() == 4 && c2partitions.size() == 4 && c3partitions.size() == 4);
        assertTrue(c2partitions0.containsAll(c2partitions));
        assertTrue(c3partitions0.containsAll(c3partitions));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testSchemaBackwardCompatibility(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, partitionInfos(topic, 3));
        subscriptions.put(consumer1, subscription(topics(topic), 0));
        subscriptions.put(consumer2, subscription(topics(topic), 1));
        subscriptions.put(consumer3, subscription(topics(topic), 2));

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        TopicPartition tp2 = new TopicPartition(topic, 2);

        List<TopicPartition> c1partitions0 = partitions(tp0, tp2);
        List<TopicPartition> c2partitions0 = partitions(tp1);
        subscriptions.put(consumer1, buildSubscriptionV2Above(topics(topic), c1partitions0, 1, 0));
        subscriptions.put(consumer2, buildSubscriptionWithOldSchema(topics(topic), c2partitions0, 1));
        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        List<TopicPartition> c1partitions = assignment.get(consumer1);
        List<TopicPartition> c2partitions = assignment.get(consumer2);
        List<TopicPartition> c3partitions = assignment.get(consumer3);

        assertTrue(c1partitions.size() == 1 && c2partitions.size() == 1 && c3partitions.size() == 1);
        assertTrue(c1partitions0.containsAll(c1partitions));
        assertTrue(c2partitions0.containsAll(c2partitions));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testMemberDataWithInconsistentData(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        List<TopicPartition> ownedPartitionsInUserdata = partitions(tp1);
        List<TopicPartition> ownedPartitionsInSubscription = partitions(tp0);

        assignor.onAssignment(new ConsumerPartitionAssignor.Assignment(ownedPartitionsInUserdata), new ConsumerGroupMetadata(groupId, generationId, consumer1, Optional.empty()));
        ByteBuffer userDataWithHigherGenerationId = assignor.subscriptionUserData(new HashSet<>(topics(topic)));

        Subscription subscription = new Subscription(topics(topic), userDataWithHigherGenerationId, ownedPartitionsInSubscription);
        AbstractStickyAssignor.MemberData memberData = memberData(subscription);

        // In StickyAssignor, we'll serialize owned partition in assignment into userData and always honor userData
        assertEquals(ownedPartitionsInUserdata, memberData.partitions, "subscription: " + subscription + " doesn't have expected owned partition");
        assertEquals(generationId, memberData.generation.orElse(-1), "subscription: " + subscription + " doesn't have expected generation id");
    }

    @Test
    public void testMemberDataWillHonorUserData() {
        List<String> topics = topics(topic);
        List<TopicPartition> ownedPartitions = partitions(tp(topic1, 0), tp(topic2, 1));
        int generationIdInUserData = generationId - 1;

        Subscription subscription = new Subscription(topics, generateUserData(topics, ownedPartitions, generationIdInUserData), Collections.emptyList(), generationId, Optional.empty());
        AbstractStickyAssignor.MemberData memberData = memberData(subscription);
        // in StickyAssignor with eager rebalance protocol, we'll always honor data in user data
        assertEquals(ownedPartitions, memberData.partitions, "subscription: " + subscription + " doesn't have expected owned partition");
        assertEquals(generationIdInUserData, memberData.generation.orElse(-1), "subscription: " + subscription + " doesn't have expected generation id");
    }

    @Test
    public void testAssignorWithOldVersionSubscriptions() {
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 3));

        List<String> subscribedTopics = topics(topic1);

        subscriptions.put(consumer1, buildSubscriptionV0(subscribedTopics, partitions(tp(topic1, 0)), generationId, 0));
        subscriptions.put(consumer2, buildSubscriptionV1(subscribedTopics, partitions(tp(topic1, 1)), generationId, 1));
        subscriptions.put(consumer3, buildSubscriptionV2Above(subscribedTopics, Collections.emptyList(), generationId, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptions);
        assertEquals(partitions(tp(topic1, 0)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic1, 1)), assignment.get(consumer2));
        assertEquals(partitions(tp(topic1, 2)), assignment.get(consumer3));

        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    private Subscription buildSubscriptionWithOldSchema(List<String> topics, List<TopicPartition> partitions, int consumerIndex) {
        Struct struct = new Struct(StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0);
        List<Struct> topicAssignments = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> topicEntry : CollectionUtils.groupPartitionsByTopic(partitions).entrySet()) {
            Struct topicAssignment = new Struct(StickyAssignor.TOPIC_ASSIGNMENT);
            topicAssignment.set(StickyAssignor.TOPIC_KEY_NAME, topicEntry.getKey());
            topicAssignment.set(StickyAssignor.PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
            topicAssignments.add(topicAssignment);
        }
        struct.set(StickyAssignor.TOPIC_PARTITIONS_KEY_NAME, topicAssignments.toArray());
        ByteBuffer buffer = ByteBuffer.allocate(StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0.sizeOf(struct));
        StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0.write(buffer, struct);
        buffer.flip();

        return new Subscription(topics, buffer, Collections.emptyList(), DEFAULT_GENERATION, consumerRackId(consumerIndex));
    }

    public static Collection<Arguments> rackAndSubscriptionCombinations() {
        return Arrays.asList(
                Arguments.of(RackConfig.NO_BROKER_RACK, true),
                Arguments.of(RackConfig.NO_CONSUMER_RACK, true),
                Arguments.of(RackConfig.BROKER_AND_CONSUMER_RACK, true),
                Arguments.of(RackConfig.NO_BROKER_RACK, false),
                Arguments.of(RackConfig.NO_CONSUMER_RACK, false),
                Arguments.of(RackConfig.BROKER_AND_CONSUMER_RACK, false));
    }
}
