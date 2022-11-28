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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor.MemberInfo;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.RackConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.TEST_NAME_WITH_CONSUMER_RACK;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.TEST_NAME_WITH_RACK_CONFIG;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.partitionsPerTopic;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.racks;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.nullRacks;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.verifyRackAwareWithNonEqualSubscription;
import static org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignorTest.verifyRackAwareWithUniformSubscription;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangeAssignorTest {

    private final RangeAssignor assignor = new RangeAssignor();

    // For plural tests
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final String consumer1 = "consumer1";
    private final String instance1 = "instance1";
    private final String consumer2 = "consumer2";
    private final String instance2 = "instance2";
    private final String consumer3 = "consumer3";
    private final String instance3 = "instance3";

    private int numBrokerRacks;
    private boolean hasConsumerRack;

    private List<MemberInfo> staticMemberInfos;
    private int replicationFactor = 3;

    @BeforeEach
    public void setUp() {
        staticMemberInfos = new ArrayList<>();
        staticMemberInfos.add(new MemberInfo(consumer1, Optional.of(instance1)));
        staticMemberInfos.add(new MemberInfo(consumer2, Optional.of(instance2)));
        staticMemberInfos.add(new MemberInfo(consumer3, Optional.of(instance3)));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = {true, false})
    public void testOneConsumerNoTopic(boolean hasConsumerRack) {
        initializeRacks(hasConsumerRack ? RackConfig.BROKER_AND_CONSUMER_RACK : RackConfig.NO_CONSUMER_RACK);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic,
                Collections.singletonMap(consumer1, subscription(Collections.emptyList(), 0)));

        assertEquals(Collections.singleton(consumer1), assignment.keySet());
        assertTrue(assignment.get(consumer1).isEmpty());
    }

    @ParameterizedTest(name = TEST_NAME_WITH_CONSUMER_RACK)
    @ValueSource(booleans = {true, false})
    public void testOneConsumerNonexistentTopic(boolean hasConsumerRack) {
        initializeRacks(hasConsumerRack ? RackConfig.BROKER_AND_CONSUMER_RACK : RackConfig.NO_CONSUMER_RACK);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic,
                Collections.singletonMap(consumer1, subscription(topics(topic1), 0)));
        assertEquals(Collections.singleton(consumer1), assignment.keySet());
        assertTrue(assignment.get(consumer1).isEmpty());
    }

    @ParameterizedTest(name = "rackConfig = {0}")
    @EnumSource(RackConfig.class)
    public void testOneConsumerOneTopic(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 3));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic,
                Collections.singletonMap(consumer1, subscription(topics(topic1), 0)));

        assertEquals(Collections.singleton(consumer1), assignment.keySet());
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic1, 2)), assignment.get(consumer1));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testOnlyAssignsPartitionsFromSubscribedTopics(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        String otherTopic = "other";

        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 3));
        partitionsPerTopic.put(otherTopic, partitionInfos(otherTopic, 3));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic,
                Collections.singletonMap(consumer1, subscription(topics(topic1), 0)));
        assertEquals(Collections.singleton(consumer1), assignment.keySet());
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic1, 2)), assignment.get(consumer1));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testOneConsumerMultipleTopics(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(1, 2);

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic,
                Collections.singletonMap(consumer1, subscription(topics(topic1, topic2), 0)));

        assertEquals(Collections.singleton(consumer1), assignment.keySet());
        assertAssignment(partitions(tp(topic1, 0), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer1));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testTwoConsumersOneTopicOnePartition(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 1));

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, subscription(topics(topic1), 0));
        consumers.put(consumer2, subscription(topics(topic1), 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0)), assignment.get(consumer1));
        assertAssignment(Collections.emptyList(), assignment.get(consumer2));
    }


    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testTwoConsumersOneTopicTwoPartitions(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, 2));

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, subscription(topics(topic1), 0));
        consumers.put(consumer2, subscription(topics(topic1), 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0)), assignment.get(consumer1));
        assertAssignment(partitions(tp(topic1, 1)), assignment.get(consumer2));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testMultipleConsumersMixedTopics(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(3, 2);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, subscription(topics(topic1), 0));
        consumers.put(consumer2, subscription(topics(topic1, topic2), 1));
        consumers.put(consumer3, subscription(topics(topic1), 2));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0)), assignment.get(consumer1));
        assertAssignment(partitions(tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer2));
        assertAssignment(partitions(tp(topic1, 2)), assignment.get(consumer3));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testTwoConsumersTwoTopicsSixPartitions(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, List<PartitionInfo>> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(3, 3);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, subscription(topics(topic1, topic2), 0));
        consumers.put(consumer2, subscription(topics(topic1, topic2), 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer1));
        assertAssignment(partitions(tp(topic1, 2), tp(topic2, 2)), assignment.get(consumer2));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testTwoStaticConsumersTwoTopicsSixPartitions(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        // although consumer high has a higher rank than consumer low, the comparison happens on
        // instance id level.
        String consumerIdLow = "consumer-b";
        String consumerIdHigh = "consumer-a";

        Map<String, List<PartitionInfo>> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(3, 3);

        Map<String, Subscription> consumers = new HashMap<>();
        Subscription consumerLowSubscription = subscription(topics(topic1, topic2), 0);
        consumerLowSubscription.setGroupInstanceId(Optional.of(instance1));
        consumers.put(consumerIdLow, consumerLowSubscription);
        Subscription consumerHighSubscription = subscription(topics(topic1, topic2), 1);
        consumerHighSubscription.setGroupInstanceId(Optional.of(instance2));
        consumers.put(consumerIdHigh, consumerHighSubscription);
        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumerIdLow));
        assertAssignment(partitions(tp(topic1, 2), tp(topic2, 2)), assignment.get(consumerIdHigh));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testOneStaticConsumerAndOneDynamicConsumerTwoTopicsSixPartitions(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        // although consumer high has a higher rank than low, consumer low will win the comparison
        // because it has instance id while consumer 2 doesn't.
        String consumerIdLow = "consumer-b";
        String consumerIdHigh = "consumer-a";

        Map<String, List<PartitionInfo>> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(3, 3);

        Map<String, Subscription> consumers = new HashMap<>();

        Subscription consumerLowSubscription = subscription(topics(topic1, topic2), 0);
        consumerLowSubscription.setGroupInstanceId(Optional.of(instance1));
        consumers.put(consumerIdLow, consumerLowSubscription);
        consumers.put(consumerIdHigh, subscription(topics(topic1, topic2), 1));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumerIdLow));
        assertAssignment(partitions(tp(topic1, 2), tp(topic2, 2)), assignment.get(consumerIdHigh));
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testStaticMemberRangeAssignmentPersistent(RackConfig rackConfig) {
        initializeRacks(rackConfig, 5);
        Map<String, List<PartitionInfo>> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(5, 4);

        Map<String, Subscription> consumers = new HashMap<>();
        int consumerIndex = 0;
        for (MemberInfo m : staticMemberInfos) {
            Subscription subscription = subscription(topics(topic1, topic2), consumerIndex++);
            subscription.setGroupInstanceId(m.groupInstanceId);
            consumers.put(m.memberId, subscription);
        }
        // Consumer 4 is a dynamic member.
        String consumer4 = "consumer4";
        consumers.put(consumer4, subscription(topics(topic1, topic2), consumerIndex++));

        Map<String, List<TopicPartition>> expectedAssignment = new HashMap<>();
        // Have 3 static members instance1, instance2, instance3 to be persistent
        // across generations. Their assignment shall be the same.
        expectedAssignment.put(consumer1, partitions(tp(topic1, 0), tp(topic1, 1), tp(topic2, 0)));
        expectedAssignment.put(consumer2, partitions(tp(topic1, 2), tp(topic2, 1)));
        expectedAssignment.put(consumer3, partitions(tp(topic1, 3), tp(topic2, 2)));
        expectedAssignment.put(consumer4, partitions(tp(topic1, 4), tp(topic2, 3)));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, consumers);
        assertEquals(expectedAssignment, assignment);

        // Replace dynamic member 4 with a new dynamic member 5.
        consumers.remove(consumer4);
        String consumer5 = "consumer5";
        consumers.put(consumer5, subscription(topics(topic1, topic2), consumerIndex++));

        expectedAssignment.remove(consumer4);
        expectedAssignment.put(consumer5, partitions(tp(topic1, 4), tp(topic2, 3)));
        assignment = assignor.assignPartitions(partitionsPerTopic, consumers);
        assertEquals(expectedAssignment, assignment);
    }

    @ParameterizedTest(name = TEST_NAME_WITH_RACK_CONFIG)
    @EnumSource(RackConfig.class)
    public void testStaticMemberRangeAssignmentPersistentAfterMemberIdChanges(RackConfig rackConfig) {
        initializeRacks(rackConfig);
        Map<String, List<PartitionInfo>> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(5, 5);

        Map<String, Subscription> consumers = new HashMap<>();
        int consumerIndex = 0;
        for (MemberInfo m : staticMemberInfos) {
            Subscription subscription = subscription(topics(topic1, topic2), consumerIndex++);
            subscription.setGroupInstanceId(m.groupInstanceId);
            consumers.put(m.memberId, subscription);
        }
        Map<String, List<TopicPartition>> expectedInstanceAssignment = new HashMap<>();
        expectedInstanceAssignment.put(instance1,
                                       partitions(tp(topic1, 0), tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)));
        expectedInstanceAssignment.put(instance2,
                                       partitions(tp(topic1, 2), tp(topic1, 3), tp(topic2, 2), tp(topic2, 3)));
        expectedInstanceAssignment.put(instance3,
                                       partitions(tp(topic1, 4), tp(topic2, 4)));

        Map<String, List<TopicPartition>> staticAssignment =
            checkStaticAssignment(assignor, partitionsPerTopic, consumers);
        assertEquals(expectedInstanceAssignment, staticAssignment);

        // Now switch the member.id fields for each member info, the assignment should
        // stay the same as last time.
        String consumer4 = "consumer4";
        String consumer5 = "consumer5";
        consumers.put(consumer4, consumers.get(consumer3));
        consumers.remove(consumer3);
        consumers.put(consumer5, consumers.get(consumer2));
        consumers.remove(consumer2);

        Map<String, List<TopicPartition>> newStaticAssignment =
            checkStaticAssignment(assignor, partitionsPerTopic, consumers);
        assertEquals(staticAssignment, newStaticAssignment);
    }

    @Test
    public void testRackAwareAssignmentWithUniformSubscription() {
        List<String> nonRackAwareAssignment = asList(
                "t1-0, t1-1, t2-0, t2-1, t2-2, t3-0",
                "t1-2, t1-3, t2-3, t2-4, t3-1",
                "t1-4, t1-5, t2-5, t2-6"
        );
        verifyRackAwareWithUniformSubscription(assignor, 3, nullRacks(3), racks(3), null, nonRackAwareAssignment, -1);
        verifyRackAwareWithUniformSubscription(assignor, 3, racks(3), nullRacks(3), null, nonRackAwareAssignment, -1);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
        verifyRackAwareWithUniformSubscription(assignor, 3, racks(3), racks(3), null, nonRackAwareAssignment, 0);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
        verifyRackAwareWithUniformSubscription(assignor, 4, racks(4), racks(3), null, nonRackAwareAssignment, 0);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, false);
        verifyRackAwareWithUniformSubscription(assignor, 3, racks(3), racks(3), null, nonRackAwareAssignment, 0);
        verifyRackAwareWithUniformSubscription(assignor, 3, racks(3), asList("d", "e", "f"), null, nonRackAwareAssignment, -1);
        verifyRackAwareWithUniformSubscription(assignor, 3, racks(3), asList(null, "e", "f"), null, nonRackAwareAssignment, -1);

        // Verify assignment is rack-aligned for lower replication factor where brokers have a subset of partitions
        verifyRackAwareWithUniformSubscription(assignor, 1, racks(3), racks(3), null,
                asList("t1-0, t1-3, t2-0, t2-3, t2-6", "t1-1, t1-4, t2-1, t2-4, t3-0", "t1-2, t1-5, t2-2, t2-5, t3-1"), 0);
        verifyRackAwareWithUniformSubscription(assignor, 2, racks(3), racks(3), null,
                asList("t1-0, t1-2, t2-0, t2-3, t3-1", "t1-1, t1-3, t2-5, t2-6, t3-0", "t1-4, t1-5, t2-1, t2-2, t2-4"), 1);

        // One consumer on a rack with no partitions
        verifyRackAwareWithUniformSubscription(assignor, 3, racks(2), racks(3), null,
                asList("t1-0, t1-1, t2-0, t2-1, t2-2, t3-0", "t1-2, t1-3, t2-3, t2-4, t3-1", "t1-4, t1-5, t2-5, t2-6"), 4);
    }

    @Test
    public void testRackAwareAssignmentWithNonEqualSubscription() {
        List<String> nonRackAwareAssignment = asList(
            "t1-0, t1-1, t2-0, t2-1, t2-2, t2-3, t3-0", "t1-2, t1-3, t2-4, t2-5, t2-6, t3-1", "t1-4, t1-5"
        );
        verifyRackAwareWithNonEqualSubscription(assignor, 3, nullRacks(3), racks(3), null, nonRackAwareAssignment, -1);
        verifyRackAwareWithNonEqualSubscription(assignor, 3, racks(3), nullRacks(3), null, nonRackAwareAssignment, -1);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
        verifyRackAwareWithNonEqualSubscription(assignor, 3, racks(3), racks(3), null, nonRackAwareAssignment, 0);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
        verifyRackAwareWithNonEqualSubscription(assignor, 4, racks(4), racks(3), null, nonRackAwareAssignment, 0);
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, false);
        verifyRackAwareWithNonEqualSubscription(assignor, 3, racks(3), racks(3), null, nonRackAwareAssignment, 0);
        verifyRackAwareWithNonEqualSubscription(assignor, 3, racks(3), asList("d", "e", "f"), null, nonRackAwareAssignment, -1);
        verifyRackAwareWithNonEqualSubscription(assignor, 3, racks(3), asList(null, "e", "f"), null, nonRackAwareAssignment, -1);

        // Verify assignment is rack-aligned for lower replication factor where brokers have a subset of partitions
        verifyRackAwareWithNonEqualSubscription(assignor, 1, racks(3), racks(3), null,
                asList("t1-0, t1-3, t2-0, t2-2, t2-3, t2-6", "t1-1, t1-4, t2-1, t2-4, t2-5, t3-0", "t1-2, t1-5, t3-1"), 2);
        verifyRackAwareWithNonEqualSubscription(assignor, 2, racks(3), racks(3), null,
                asList("t1-0, t1-2, t2-0, t2-2, t2-3, t2-5, t3-1", "t1-1, t1-3, t2-1, t2-4, t2-6, t3-0", "t1-4, t1-5"), 0);

        // One consumer on a rack with no partitions
        verifyRackAwareWithNonEqualSubscription(assignor, 3, racks(2), racks(3), null,
                asList("t1-0, t1-1, t2-0, t2-1, t2-2, t2-3, t3-0", "t1-2, t1-3, t2-4, t2-5, t2-6, t3-1", "t1-4, t1-5"), 2);
    }

    static Map<String, List<TopicPartition>> checkStaticAssignment(AbstractPartitionAssignor assignor,
                                                                   Map<String, List<PartitionInfo>> partitionsPerTopic,
                                                                   Map<String, Subscription> consumers) {
        Map<String, List<TopicPartition>> assignmentByMemberId = assignor.assignPartitions(partitionsPerTopic, consumers);
        Map<String, List<TopicPartition>> assignmentByInstanceId = new HashMap<>();
        for (Map.Entry<String, Subscription> entry : consumers.entrySet()) {
            String memberId = entry.getKey();
            Optional<String> instanceId = entry.getValue().groupInstanceId();
            instanceId.ifPresent(id -> assignmentByInstanceId.put(id, assignmentByMemberId.get(memberId)));
        }
        return assignmentByInstanceId;
    }

    private void assertAssignment(List<TopicPartition> expected, List<TopicPartition> actual) {
        // order doesn't matter for assignment, so convert to a set
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }

    private Map<String, List<PartitionInfo>> setupPartitionsPerTopicWithTwoTopics(int numberOfPartitions1, int numberOfPartitions2) {
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, partitionInfos(topic1, numberOfPartitions1));
        partitionsPerTopic.put(topic2, partitionInfos(topic2, numberOfPartitions2));
        return partitionsPerTopic;
    }

    private List<PartitionInfo> partitionInfos(String topic, int numberOfPartitions) {
        return partitionsPerTopic(topic, numberOfPartitions, replicationFactor, numBrokerRacks, 0);
    }

    private Subscription subscription(List<String> topics, int consumerIndex) {
        int numRacks = numBrokerRacks > 0 ? numBrokerRacks : AbstractPartitionAssignorTest.ALL_RACKS.length;
        Optional<String> rackId = Optional.ofNullable(hasConsumerRack ? AbstractPartitionAssignorTest.ALL_RACKS[consumerIndex % numRacks] : null);
        return new Subscription(topics, null, Collections.emptyList(), -1, rackId);
    }

    private static List<String> topics(String... topics) {
        return Arrays.asList(topics);
    }

    private static List<TopicPartition> partitions(TopicPartition... partitions) {
        return Arrays.asList(partitions);
    }

    private static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

    void initializeRacks(RackConfig rackConfig) {
        initializeRacks(rackConfig, 3);
    }

    void initializeRacks(RackConfig rackConfig, int maxConsumers) {
        this.replicationFactor = maxConsumers;
        this.numBrokerRacks = rackConfig != RackConfig.NO_BROKER_RACK ? maxConsumers : 0;
        this.hasConsumerRack = rackConfig != RackConfig.NO_CONSUMER_RACK;
        // Rack and consumer ordering are the same in all the tests, so we can verify
        // rack-aware logic using the same tests.
        AbstractPartitionAssignorTest.preferRackAwareLogic(assignor, true);
    }
}
