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

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor.MemberInfo;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RoundRobinAssignorTest {

    private RoundRobinAssignor assignor = new RoundRobinAssignor();
    private String topic = "topic";
    private String consumerId = "consumer";

    @Test
    public void testOneConsumerNoTopic() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(Collections.emptyList())));
        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());
    }

    @Test
    public void testOneConsumerNonexistentTopic() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(topics(topic))));

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());
    }

    @Test
    public void testOneConsumerOneTopic() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(topics(topic))));
        assertEquals(partitions(tp(topic, 0), tp(topic, 1), tp(topic, 2)), assignment.get(consumerId));
    }

    @Test
    public void testOnlyAssignsPartitionsFromSubscribedTopics() {
        String otherTopic = "other";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        partitionsPerTopic.put(otherTopic, 3);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(topics(topic))));
        assertEquals(partitions(tp(topic, 0), tp(topic, 1), tp(topic, 2)), assignment.get(consumerId));
    }

    @Test
    public void testOneConsumerMultipleTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 1);
        partitionsPerTopic.put(topic2, 2);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(topics(topic1, topic2))));
        assertEquals(partitions(tp(topic1, 0), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumerId));
    }

    @Test
    public void testTwoConsumersOneTopicOnePartition() {
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 1);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic)));
        consumers.put(consumer2, new Subscription(topics(topic)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(partitions(tp(topic, 0)), assignment.get(consumer1));
        assertEquals(Collections.<TopicPartition>emptyList(), assignment.get(consumer2));
    }

    @Test
    public void testTwoConsumersOneTopicTwoPartitions() {
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 2);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic)));
        consumers.put(consumer2, new Subscription(topics(topic)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(partitions(tp(topic, 0)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic, 1)), assignment.get(consumer2));
    }

    @Test
    public void testMultipleConsumersMixedTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 2);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic1)));
        consumers.put(consumer2, new Subscription(topics(topic1, topic2)));
        consumers.put(consumer3, new Subscription(topics(topic1)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(partitions(tp(topic1, 0)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic1, 1), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer2));
        assertEquals(partitions(tp(topic1, 2)), assignment.get(consumer3));
    }

    @Test
    public void testTwoDynamicConsumersTwoTopicsSixPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 3);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(topics(topic1, topic2)));
        consumers.put(consumer2, new Subscription(topics(topic1, topic2)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(partitions(tp(topic1, 0), tp(topic1, 2), tp(topic2, 1)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic1, 1), tp(topic2, 0), tp(topic2, 2)), assignment.get(consumer2));
    }

    @Test
    public void testTwoStaticConsumersTwoTopicsSixPartitions() {
        // although consumer 2 has a higher rank than 1, the comparison happens on
        // instance id level.
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer-b";
        String instance1 = "instance1";
        String consumer2 = "consumer-a";
        String instance2 = "instance2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 3);

        Map<String, Subscription> consumers = new HashMap<>();
        Subscription consumer1Subscription = new Subscription(topics(topic1, topic2),
                                                              null,
                                                              Collections.emptyList());
        consumer1Subscription.setGroupInstanceId(Optional.of(instance1));
        consumers.put(consumer1, consumer1Subscription);
        Subscription consumer2Subscription = new Subscription(topics(topic1, topic2),
                                                              null,
                                                              Collections.emptyList());
        consumer2Subscription.setGroupInstanceId(Optional.of(instance2));
        consumers.put(consumer2, consumer2Subscription);
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(partitions(tp(topic1, 0), tp(topic1, 2), tp(topic2, 1)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic1, 1), tp(topic2, 0), tp(topic2, 2)), assignment.get(consumer2));
    }

    @Test
    public void testOneStaticConsumerAndOneDynamicConsumerTwoTopicsSixPartitions() {
        // although consumer 2 has a higher rank than 1, consumer 1 will win the comparison
        // because it has instance id while consumer 2 doesn't.
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer-b";
        String instance1 = "instance1";
        String consumer2 = "consumer-a";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 3);

        Map<String, Subscription> consumers = new HashMap<>();

        Subscription consumer1Subscription = new Subscription(topics(topic1, topic2),
                                                              null,
                                                              Collections.emptyList());
        consumer1Subscription.setGroupInstanceId(Optional.of(instance1));
        consumers.put(consumer1, consumer1Subscription);
        consumers.put(consumer2, new Subscription(topics(topic1, topic2)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(partitions(tp(topic1, 0), tp(topic1, 2), tp(topic2, 1)), assignment.get(consumer1));
        assertEquals(partitions(tp(topic1, 1), tp(topic2, 0), tp(topic2, 2)), assignment.get(consumer2));
    }

    @Test
    public void testStaticMemberAssignmentPersistent() {
        // Have 3 static members instance1, instance2, instance3 to be persistent
        // across generations. Their assignment shall be the same.
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String instance1 = "instance1";
        String consumer2 = "consumer2";
        String instance2 = "instance2";
        String consumer3 = "consumer3";
        String instance3 = "instance3";

        List<MemberInfo> staticMemberInfos = new ArrayList<>();
        staticMemberInfos.add(new MemberInfo(consumer1, Optional.of(instance1)));
        staticMemberInfos.add(new MemberInfo(consumer2, Optional.of(instance2)));
        staticMemberInfos.add(new MemberInfo(consumer3, Optional.of(instance3)));

        // Consumer 4 is a dynamic member.
        String consumer4 = "consumer4";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 3);
        Map<String, Subscription> consumers = new HashMap<>();
        for (MemberInfo m : staticMemberInfos) {
            Subscription subscription = new Subscription(topics(topic1, topic2),
                                                         null,
                                                         Collections.emptyList());
            subscription.setGroupInstanceId(m.groupInstanceId);
            consumers.put(m.memberId, subscription);
        }
        consumers.put(consumer4, new Subscription(topics(topic1, topic2)));

        Map<String, List<TopicPartition>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(consumer1, partitions(tp(topic1, 0), tp(topic2, 1)));
        expectedAssignment.put(consumer2, partitions(tp(topic1, 1), tp(topic2, 2)));
        expectedAssignment.put(consumer3, partitions(tp(topic1, 2)));
        expectedAssignment.put(consumer4, partitions(tp(topic2, 0)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(expectedAssignment, assignment);

        // Replace dynamic member 4 with a new dynamic member 5.
        consumers.remove(consumer4);
        String consumer5 = "consumer5";
        consumers.put(consumer5, new Subscription(topics(topic1, topic2)));

        expectedAssignment.remove(consumer4);
        expectedAssignment.put(consumer5, partitions(tp(topic2, 0)));
        assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(expectedAssignment, assignment);
    }

    @Test
    public void testStaticMemberAssignmentPersistentAfterMemberIdChanges() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String instance1 = "instance1";
        String consumer2 = "consumer2";
        String instance2 = "instance2";
        String consumer3 = "consumer3";
        String instance3 = "instance3";
        Map<String, String> memberIdToInstanceId = new HashMap<>();
        memberIdToInstanceId.put(consumer1, instance1);
        memberIdToInstanceId.put(consumer2, instance2);
        memberIdToInstanceId.put(consumer3, instance3);

        List<String> memberIdList = Arrays.asList(consumer1, consumer2, consumer3);
        Map<String, List<TopicPartition>> staticAssignment =
            checkStaticAssignment(topic1, topic2, memberIdList, memberIdToInstanceId);
        memberIdToInstanceId.clear();

        // Now switch the member.id fields for each member info, the assignment should
        // stay the same as last time.
        String consumer4 = "consumer4";
        String consumer5 = "consumer5";
        memberIdToInstanceId.put(consumer4, instance1);
        memberIdToInstanceId.put(consumer5, instance2);
        memberIdToInstanceId.put(consumer1, instance3);
        memberIdList = Arrays.asList(consumer4, consumer5, consumer1);
        Map<String, List<TopicPartition>> newStaticAssignment =
            checkStaticAssignment(topic1, topic2, memberIdList, memberIdToInstanceId);

        assertEquals(staticAssignment, newStaticAssignment);
    }

    private Map<String, List<TopicPartition>> checkStaticAssignment(String topic1,
                                                                    String topic2,
                                                                    List<String> memberIdList,
                                                                    Map<String, String> memberIdToInstanceId) {
        List<MemberInfo> staticMemberInfos = new ArrayList<>();
        for (Map.Entry<String, String> entry : memberIdToInstanceId.entrySet()) {
            staticMemberInfos.add(new MemberInfo(entry.getKey(), Optional.of(entry.getValue())));
        }

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 3);
        Map<String, Subscription> consumers = new HashMap<>();
        for (MemberInfo m : staticMemberInfos) {
            Subscription subscription = new Subscription(topics(topic1, topic2),
                                                         null,
                                                         Collections.emptyList());
            subscription.setGroupInstanceId(m.groupInstanceId);
            consumers.put(m.memberId, subscription);
        }

        Map<String, List<TopicPartition>> expectedInstanceAssignment = new HashMap<>();
        for (int i = 0; i < memberIdList.size(); i++) {
            expectedInstanceAssignment.put(memberIdToInstanceId.get(memberIdList.get(i)),
                                           partitions(tp(topic1, i), tp(topic2, i)));
        }

        Map<String, List<TopicPartition>> assignmentByMemberId =
            assignor.assign(partitionsPerTopic, consumers);
        Map<String, List<TopicPartition>> assignmentByInstanceId = new HashMap<>();
        for (String memberId : memberIdList) {
            assignmentByInstanceId.put(memberIdToInstanceId.get(memberId),
                                       assignmentByMemberId.get(memberId));
        }
        assertEquals(expectedInstanceAssignment, assignmentByInstanceId);
        return assignmentByInstanceId;
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
