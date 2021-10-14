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
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangeAssignorTest {

    private RangeAssignor assignor = new RangeAssignor();

    // For plural tests
    private String topic1 = "topic1";
    private String topic2 = "topic2";
    private final String consumer1 = "consumer1";
    private final String instance1 = "instance1";
    private final String consumer2 = "consumer2";
    private final String instance2 = "instance2";
    private final String consumer3 = "consumer3";
    private final String instance3 = "instance3";

    private List<MemberInfo> staticMemberInfos;

    @BeforeEach
    public void setUp() {
        staticMemberInfos = new ArrayList<>();
        staticMemberInfos.add(new MemberInfo(consumer1, Optional.of(instance1)));
        staticMemberInfos.add(new MemberInfo(consumer2, Optional.of(instance2)));
        staticMemberInfos.add(new MemberInfo(consumer3, Optional.of(instance3)));
    }

    @Test
    public void testOneConsumerNoTopic() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumer1, new Subscription(Collections.emptyList())));

        assertEquals(Collections.singleton(consumer1), assignment.keySet());
        assertTrue(assignment.get(consumer1).isEmpty());
    }

    @Test
    public void testOneConsumerNonexistentTopic() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumer1, new Subscription(topics(topic1))));
        assertEquals(Collections.singleton(consumer1), assignment.keySet());
        assertTrue(assignment.get(consumer1).isEmpty());
    }

    @Test
    public void testOneConsumerOneTopic() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumer1, new Subscription(topics(topic1))));

        assertEquals(Collections.singleton(consumer1), assignment.keySet());
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic1, 2)), assignment.get(consumer1));
    }

    @Test
    public void testOnlyAssignsPartitionsFromSubscribedTopics() {
        String otherTopic = "other";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(otherTopic, 3);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumer1, new Subscription(topics(topic1))));
        assertEquals(Collections.singleton(consumer1), assignment.keySet());
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic1, 2)), assignment.get(consumer1));
    }

    @Test
    public void testOneConsumerMultipleTopics() {
        Map<String, Integer> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(1, 2);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumer1, new Subscription(topics(topic1, topic2))));

        assertEquals(Collections.singleton(consumer1), assignment.keySet());
        assertAssignment(partitions(tp(topic1, 0), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer1));
    }

    @Test
    public void testTwoConsumersOneTopicOnePartition() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 1);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic1)));
        consumers.put(consumer2, new Subscription(topics(topic1)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0)), assignment.get(consumer1));
        assertAssignment(Collections.emptyList(), assignment.get(consumer2));
    }


    @Test
    public void testTwoConsumersOneTopicTwoPartitions() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 2);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic1)));
        consumers.put(consumer2, new Subscription(topics(topic1)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0)), assignment.get(consumer1));
        assertAssignment(partitions(tp(topic1, 1)), assignment.get(consumer2));
    }

    @Test
    public void testMultipleConsumersMixedTopics() {
        Map<String, Integer> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(3, 2);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic1)));
        consumers.put(consumer2, new Subscription(topics(topic1, topic2)));
        consumers.put(consumer3, new Subscription(topics(topic1)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0)), assignment.get(consumer1));
        assertAssignment(partitions(tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer2));
        assertAssignment(partitions(tp(topic1, 2)), assignment.get(consumer3));
    }

    @Test
    public void testTwoConsumersTwoTopicsSixPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(3, 3);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic1, topic2)));
        consumers.put(consumer2, new Subscription(topics(topic1, topic2)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer1));
        assertAssignment(partitions(tp(topic1, 2), tp(topic2, 2)), assignment.get(consumer2));
    }

    @Test
    public void testTwoStaticConsumersTwoTopicsSixPartitions() {
        // although consumer high has a higher rank than consumer low, the comparison happens on
        // instance id level.
        String consumerIdLow = "consumer-b";
        String consumerIdHigh = "consumer-a";

        Map<String, Integer> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(3, 3);

        Map<String, Subscription> consumers = new HashMap<>();
        Subscription consumerLowSubscription = new Subscription(topics(topic1, topic2),
                                                              null,
                                                              Collections.emptyList());
        consumerLowSubscription.setGroupInstanceId(Optional.of(instance1));
        consumers.put(consumerIdLow, consumerLowSubscription);
        Subscription consumerHighSubscription = new Subscription(topics(topic1, topic2),
                                                              null,
                                                              Collections.emptyList());
        consumerHighSubscription.setGroupInstanceId(Optional.of(instance2));
        consumers.put(consumerIdHigh, consumerHighSubscription);
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumerIdLow));
        assertAssignment(partitions(tp(topic1, 2), tp(topic2, 2)), assignment.get(consumerIdHigh));
    }

    @Test
    public void testOneStaticConsumerAndOneDynamicConsumerTwoTopicsSixPartitions() {
        // although consumer high has a higher rank than low, consumer low will win the comparison
        // because it has instance id while consumer 2 doesn't.
        String consumerIdLow = "consumer-b";
        String consumerIdHigh = "consumer-a";

        Map<String, Integer> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(3, 3);

        Map<String, Subscription> consumers = new HashMap<>();

        Subscription consumerLowSubscription = new Subscription(topics(topic1, topic2),
                                                              null,
                                                              Collections.emptyList());
        consumerLowSubscription.setGroupInstanceId(Optional.of(instance1));
        consumers.put(consumerIdLow, consumerLowSubscription);
        consumers.put(consumerIdHigh, new Subscription(topics(topic1, topic2)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(partitions(tp(topic1, 0), tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumerIdLow));
        assertAssignment(partitions(tp(topic1, 2), tp(topic2, 2)), assignment.get(consumerIdHigh));
    }

    @Test
    public void testStaticMemberRangeAssignmentPersistent() {
        Map<String, Integer> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(5, 4);

        Map<String, Subscription> consumers = new HashMap<>();
        for (MemberInfo m : staticMemberInfos) {
            Subscription subscription = new Subscription(topics(topic1, topic2),
                                                         null,
                                                         Collections.emptyList());
            subscription.setGroupInstanceId(m.groupInstanceId);
            consumers.put(m.memberId, subscription);
        }
        // Consumer 4 is a dynamic member.
        String consumer4 = "consumer4";
        consumers.put(consumer4, new Subscription(topics(topic1, topic2)));

        Map<String, List<TopicPartition>> expectedAssignment = new HashMap<>();
        // Have 3 static members instance1, instance2, instance3 to be persistent
        // across generations. Their assignment shall be the same.
        expectedAssignment.put(consumer1, partitions(tp(topic1, 0), tp(topic1, 1), tp(topic2, 0)));
        expectedAssignment.put(consumer2, partitions(tp(topic1, 2), tp(topic2, 1)));
        expectedAssignment.put(consumer3, partitions(tp(topic1, 3), tp(topic2, 2)));
        expectedAssignment.put(consumer4, partitions(tp(topic1, 4), tp(topic2, 3)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(expectedAssignment, assignment);

        // Replace dynamic member 4 with a new dynamic member 5.
        consumers.remove(consumer4);
        String consumer5 = "consumer5";
        consumers.put(consumer5, new Subscription(topics(topic1, topic2)));

        expectedAssignment.remove(consumer4);
        expectedAssignment.put(consumer5, partitions(tp(topic1, 4), tp(topic2, 3)));
        assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(expectedAssignment, assignment);
    }

    @Test
    public void testStaticMemberRangeAssignmentPersistentAfterMemberIdChanges() {
        Map<String, Integer> partitionsPerTopic = setupPartitionsPerTopicWithTwoTopics(5, 5);

        Map<String, Subscription> consumers = new HashMap<>();
        for (MemberInfo m : staticMemberInfos) {
            Subscription subscription = new Subscription(topics(topic1, topic2),
                                                         null,
                                                         Collections.emptyList());
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

    static Map<String, List<TopicPartition>> checkStaticAssignment(AbstractPartitionAssignor assignor,
                                                                   Map<String, Integer> partitionsPerTopic,
                                                                   Map<String, Subscription> consumers) {
        Map<String, List<TopicPartition>> assignmentByMemberId = assignor.assign(partitionsPerTopic, consumers);
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

    private Map<String, Integer> setupPartitionsPerTopicWithTwoTopics(int numberOfPartitions1, int numberOfPartitions2) {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, numberOfPartitions1);
        partitionsPerTopic.put(topic2, numberOfPartitions2);
        return partitionsPerTopic;
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
}
