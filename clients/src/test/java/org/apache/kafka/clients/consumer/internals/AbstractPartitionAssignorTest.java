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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor.MemberInfo;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractPartitionAssignorTest {

    public static final String TEST_NAME_WITH_RACK_CONFIG = "{displayName}.rackConfig = {0}";
    public static final String TEST_NAME_WITH_CONSUMER_RACK = "{displayName}.hasConsumerRack = {0}";
    public static final String[] ALL_RACKS = {"a", "b", "c", "d", "e", "f"};

    public enum RackConfig {
        NO_BROKER_RACK,
        NO_CONSUMER_RACK,
        BROKER_AND_CONSUMER_RACK
    }

    @Test
    public void testMemberInfoSortingWithoutGroupInstanceId() {
        MemberInfo m1 = new MemberInfo("a", Optional.empty());
        MemberInfo m2 = new MemberInfo("b", Optional.empty());
        MemberInfo m3 = new MemberInfo("c", Optional.empty());

        List<MemberInfo> memberInfoList = Arrays.asList(m1, m2, m3);
        assertEquals(memberInfoList, Utils.sorted(memberInfoList));
    }

    @Test
    public void testMemberInfoSortingWithAllGroupInstanceId() {
        MemberInfo m1 = new MemberInfo("a", Optional.of("y"));
        MemberInfo m2 = new MemberInfo("b", Optional.of("z"));
        MemberInfo m3 = new MemberInfo("c", Optional.of("x"));

        List<MemberInfo> memberInfoList = Arrays.asList(m1, m2, m3);
        assertEquals(Arrays.asList(m3, m1, m2), Utils.sorted(memberInfoList));
    }

    @Test
    public void testMemberInfoSortingSomeGroupInstanceId() {
        MemberInfo m1 = new MemberInfo("a", Optional.empty());
        MemberInfo m2 = new MemberInfo("b", Optional.of("y"));
        MemberInfo m3 = new MemberInfo("c", Optional.of("x"));

        List<MemberInfo> memberInfoList = Arrays.asList(m1, m2, m3);
        assertEquals(Arrays.asList(m3, m2, m1), Utils.sorted(memberInfoList));
    }

    @Test
    public void testMergeSortManyMemberInfo() {
        Random rand = new Random();
        int bound = 2;
        List<MemberInfo> memberInfoList = new ArrayList<>();
        List<MemberInfo> staticMemberList = new ArrayList<>();
        List<MemberInfo> dynamicMemberList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            // Need to make sure all the ids are defined as 3-digits otherwise
            // the comparison result will break.
            String id = Integer.toString(i + 100);
            Optional<String> groupInstanceId = rand.nextInt(bound) < bound / 2 ?
                                                       Optional.of(id) : Optional.empty();
            MemberInfo m = new MemberInfo(id, groupInstanceId);
            memberInfoList.add(m);
            if (m.groupInstanceId.isPresent()) {
                staticMemberList.add(m);
            } else {
                dynamicMemberList.add(m);
            }
        }
        staticMemberList.addAll(dynamicMemberList);
        Collections.shuffle(memberInfoList);
        assertEquals(staticMemberList, Utils.sorted(memberInfoList));
    }

    @Test
    public void testUseRackAwareAssignment() {
        AbstractPartitionAssignor assignor = new RangeAssignor();
        String[] racks = new String[] {"a", "b", "c"};
        Set<String> allRacks = Set.of(racks);
        Set<String> twoRacks = Set.of("a", "b");
        Map<TopicPartition, Set<String>> partitionsOnAllRacks = new HashMap<>();
        Map<TopicPartition, Set<String>> partitionsOnSubsetOfRacks = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            TopicPartition tp = new TopicPartition("topic", i);
            partitionsOnAllRacks.put(tp, allRacks);
            partitionsOnSubsetOfRacks.put(tp, Set.of(racks[i % racks.length]));
        }
        assertFalse(assignor.useRackAwareAssignment(Collections.emptySet(), Collections.emptySet(), partitionsOnAllRacks));
        assertFalse(assignor.useRackAwareAssignment(Collections.emptySet(), allRacks, partitionsOnAllRacks));
        assertFalse(assignor.useRackAwareAssignment(allRacks, Collections.emptySet(), Collections.emptyMap()));
        assertFalse(assignor.useRackAwareAssignment(Set.of("d"), allRacks, partitionsOnAllRacks));
        assertFalse(assignor.useRackAwareAssignment(allRacks, allRacks, partitionsOnAllRacks));
        assertFalse(assignor.useRackAwareAssignment(twoRacks, allRacks, partitionsOnAllRacks));
        assertFalse(assignor.useRackAwareAssignment(Set.of("a", "d"), allRacks, partitionsOnAllRacks));
        assertTrue(assignor.useRackAwareAssignment(allRacks, allRacks, partitionsOnSubsetOfRacks));
        assertTrue(assignor.useRackAwareAssignment(twoRacks, allRacks, partitionsOnSubsetOfRacks));
        assertTrue(assignor.useRackAwareAssignment(Set.of("a", "d"), allRacks, partitionsOnSubsetOfRacks));

        assignor.preferRackAwareLogic = true;
        assertFalse(assignor.useRackAwareAssignment(Collections.emptySet(), Collections.emptySet(), partitionsOnAllRacks));
        assertFalse(assignor.useRackAwareAssignment(Collections.emptySet(), allRacks, partitionsOnAllRacks));
        assertFalse(assignor.useRackAwareAssignment(allRacks, Collections.emptySet(), Collections.emptyMap()));
        assertFalse(assignor.useRackAwareAssignment(Set.of("d"), allRacks, partitionsOnAllRacks));
        assertTrue(assignor.useRackAwareAssignment(allRacks, allRacks, partitionsOnAllRacks));
        assertTrue(assignor.useRackAwareAssignment(twoRacks, allRacks, partitionsOnAllRacks));
        assertTrue(assignor.useRackAwareAssignment(allRacks, allRacks, partitionsOnSubsetOfRacks));
        assertTrue(assignor.useRackAwareAssignment(twoRacks, allRacks, partitionsOnSubsetOfRacks));
    }

    public static List<String> racks(int numRacks) {
        List<String> racks = new ArrayList<>(numRacks);
        for (int i = 0; i < numRacks; i++)
            racks.add(ALL_RACKS[i % ALL_RACKS.length]);
        return racks;
    }

    public static List<String> nullRacks(int numRacks) {
        return Arrays.asList(new String[numRacks]);
    }

    public static void verifyRackAssignment(AbstractPartitionAssignor assignor,
                                            Map<String, Integer> numPartitionsPerTopic,
                                            int replicationFactor,
                                            List<String> brokerRacks,
                                            List<String> consumerRacks,
                                            List<List<String>> consumerTopics,
                                            List<String> expectedAssignments,
                                            int numPartitionsWithRackMismatch) {
        verifyRackAssignment(assignor, numPartitionsPerTopic, replicationFactor,
                brokerRacks, consumerRacks, consumerTopics, Collections.emptyList(),
                expectedAssignments, numPartitionsWithRackMismatch);

    }

    public static void verifyRackAssignment(AbstractPartitionAssignor assignor,
                                            Map<String, Integer> numPartitionsPerTopic,
                                            int replicationFactor,
                                            List<String> brokerRacks,
                                            List<String> consumerRacks,
                                            List<List<String>> consumerTopics,
                                            List<String> consumerOwnedPartitions,
                                            List<String> expectedAssignments,
                                            int numPartitionsWithRackMismatch) {
        List<String> consumers = IntStream.range(0, consumerRacks.size()).mapToObj(i -> "consumer" + i).collect(Collectors.toList());
        List<Subscription> subscriptions = subscriptions(consumerTopics, consumerRacks, consumerOwnedPartitions);
        Map<String, List<PartitionInfo>> partitionsPerTopic = partitionsPerTopic(numPartitionsPerTopic, replicationFactor, brokerRacks);

        Map<String, Subscription> subscriptionsByConsumer = new HashMap<>(consumers.size());
        for (int i = 0; i < subscriptions.size(); i++)
            subscriptionsByConsumer.put(consumers.get(i), subscriptions.get(i));

        Map<String, String> expectedAssignment = new HashMap<>(consumers.size());
        for (int i = 0; i < consumers.size(); i++)
            expectedAssignment.put(consumers.get(i), expectedAssignments.get(i));

        Map<String, List<TopicPartition>> assignment = assignor.assignPartitions(partitionsPerTopic, subscriptionsByConsumer);
        Map<String, String> actualAssignment = assignment.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> toSortedString(e.getValue())));
        assertEquals(expectedAssignment, actualAssignment);

        if (numPartitionsWithRackMismatch >= 0) {
            List<TopicPartition> numMismatched = new ArrayList<>();
            for (int i = 0; i < consumers.size(); i++) {
                String rack = consumerRacks.get(i);
                if (rack != null) {
                    List<TopicPartition> partitions = assignment.get(consumers.get(i));
                    for (TopicPartition tp : partitions) {
                        PartitionInfo partitionInfo = partitionsPerTopic.get(tp.topic()).stream()
                                .filter(p -> p.topic().equals(tp.topic()) && p.partition() == tp.partition())
                                .findFirst().get();
                        if (Arrays.stream(partitionInfo.replicas()).noneMatch(n -> rack.equals(n.rack())))
                            numMismatched.add(tp);
                    }
                }
            }
            assertEquals(numPartitionsWithRackMismatch, numMismatched.size(), "Partitions with rack mismatch " + numMismatched);
        }
    }

    private static String toSortedString(List<?> partitions) {
        return partitions.stream().map(Object::toString).sorted().collect(Collectors.joining(", "));
    }

    private static List<Subscription> subscriptions(List<List<String>> consumerTopics,
                                                    List<String> consumerRacks,
                                                    List<String> consumerOwnedPartitions) {
        List<List<TopicPartition>> ownedPartitions = ownedPartitions(consumerOwnedPartitions, consumerTopics.size());
        List<Subscription> subscriptions = new ArrayList<>(consumerTopics.size());
        for (int i = 0; i < consumerTopics.size(); i++) {
            subscriptions.add(new Subscription(consumerTopics.get(i), null, ownedPartitions.get(i),
                    DEFAULT_GENERATION, Optional.ofNullable(consumerRacks.get(i))));
        }
        return subscriptions;
    }

    private static List<List<TopicPartition>> ownedPartitions(List<String> consumerOwnedPartitions, int numConsumers) {
        List<List<TopicPartition>> owedPartitions = new ArrayList<>(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            List<TopicPartition> owned = Collections.emptyList();
            if (consumerOwnedPartitions == null || consumerOwnedPartitions.size() <= i)
                owedPartitions.add(owned);
            else {
                String[] partitions = consumerOwnedPartitions.get(i).split(", ");
                List<TopicPartition> topicPartitions = new ArrayList<>(partitions.length);
                for (String partition : partitions) {
                    String topic = partition.substring(0, partition.lastIndexOf('-'));
                    int p = Integer.parseInt(partition.substring(partition.lastIndexOf('-') + 1));
                    topicPartitions.add(new TopicPartition(topic, p));
                }
                owedPartitions.add(topicPartitions);
            }
        }
        return owedPartitions;
    }

    private static Map<String, List<PartitionInfo>> partitionsPerTopic(Map<String, Integer> numPartitionsPerTopic,
                                                                       int replicationFactor,
                                                                       List<String> brokerRacks) {
        Map<String, List<PartitionInfo>> partitionsPerTopic = new HashMap<>();
        int nextIndex = 0;
        for (Map.Entry<String, Integer> entry : numPartitionsPerTopic.entrySet()) {
            String topic = entry.getKey();
            int numPartitions = entry.getValue();
            partitionsPerTopic.put(topic, partitionInfos(topic, numPartitions, replicationFactor, brokerRacks, nextIndex));
            nextIndex += numPartitions;
        }
        return partitionsPerTopic;
    }

    private static List<PartitionInfo> partitionInfos(String topic, int numberOfPartitions, int replicationFactor, List<String> brokerRacks, int nextNodeIndex) {
        int numBrokers = brokerRacks.size();
        List<Node> nodes = new ArrayList<>(numBrokers);
        for (int i = 0; i < brokerRacks.size(); i++) {
            nodes.add(new Node(i, "", i, brokerRacks.get(i)));
        }
        List<PartitionInfo> partitionInfos = new ArrayList<>(numberOfPartitions);
        for (int i = 0; i < numberOfPartitions; i++) {
            Node[] replicas = new Node[replicationFactor];
            for (int j = 0; j < replicationFactor; j++) {
                replicas[j] = nodes.get((i + j + nextNodeIndex) % nodes.size());
            }
            partitionInfos.add(new PartitionInfo(topic, i, replicas[0], replicas, replicas));
        }
        return partitionInfos;
    }

    public static List<PartitionInfo> partitionInfos(String topic, int numberOfPartitions, int replicationFactor, int numBrokerRacks, int nextNodeIndex) {
        int numBrokers = numBrokerRacks <= 0 ? replicationFactor : numBrokerRacks * replicationFactor;
        List<String> brokerRacks = new ArrayList<>(numBrokers);
        for (int i = 0; i < numBrokers; i++) {
            brokerRacks.add(numBrokerRacks <= 0 ? null : ALL_RACKS[i % numBrokerRacks]);
        }
        return partitionInfos(topic, numberOfPartitions, replicationFactor, brokerRacks, nextNodeIndex);
    }

    public static void preferRackAwareLogic(AbstractPartitionAssignor assignor, boolean value) {
        assignor.preferRackAwareLogic = value;
    }
}
