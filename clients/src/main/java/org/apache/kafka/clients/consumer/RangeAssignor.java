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

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <p>The range assignor works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumers in lexicographic order. We then divide the number of partitions by the total number of
 * consumers to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition.
 *
 * <p>For example, suppose there are two consumers <code>C0</code> and <code>C1</code>, two topics <code>t0</code> and
 * <code>t1</code>, and each topic has 3 partitions, resulting in partitions <code>t0p0</code>, <code>t0p1</code>,
 * <code>t0p2</code>, <code>t1p0</code>, <code>t1p1</code>, and <code>t1p2</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t0p1, t1p0, t1p1]</code></li>
 * <li><code>C1: [t0p2, t1p2]</code></li>
 * </ul>
 *
 * Since the introduction of static membership, we could leverage <code>group.instance.id</code> to make the assignment behavior more sticky.
 * For the above example, after one rolling bounce, group coordinator will attempt to assign new <code>member.id</code> towards consumers,
 * for example <code>C0</code> -&gt; <code>C3</code> <code>C1</code> -&gt; <code>C2</code>.
 *
 * <p>The assignment could be completely shuffled to:
 * <ul>
 * <li><code>C3 (was C0): [t0p2, t1p2] (before was [t0p0, t0p1, t1p0, t1p1])</code>
 * <li><code>C2 (was C1): [t0p0, t0p1, t1p0, t1p1] (before was [t0p2, t1p2])</code>
 * </ul>
 *
 * The assignment change was caused by the change of <code>member.id</code> relative order, and
 * can be avoided by setting the group.instance.id.
 * Consumers will have individual instance ids <code>I1</code>, <code>I2</code>. As long as
 * 1. Number of members remain the same across generation
 * 2. Static members' identities persist across generation
 * 3. Subscription pattern doesn't change for any member
 *
 * <p>The assignment will always be:
 * <ul>
 * <li><code>I0: [t0p0, t0p1, t1p0, t1p1]</code>
 * <li><code>I1: [t0p2, t1p2]</code>
 * </ul>
 * <p>
 * If racks are specified for consumers, we attempt to match consumer racks with partition replica
 * racks on a best-effort basis, prioritizing balanced assignment over rack alignment. We use
 * rack-aware assignment if both consumer and partition racks are available and some partitions
 * have replicas only on a subset of racks. In this case, we apply the standard range assignor
 * algorithm on each rack with rack matching first, up to the limit per consumer per topic.
 * Remaining partitions are then allocated using the same algorithm without rack matching.
 */
public class RangeAssignor extends AbstractPartitionAssignor {
    public static final String RANGE_ASSIGNOR_NAME = "range";

    @Override
    public String name() {
        return RANGE_ASSIGNOR_NAME;
    }

    private Map<String, List<MemberInfo>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<MemberInfo>> topicToConsumers = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            Subscription subscription = subscriptionEntry.getValue();
            MemberInfo memberInfo = new MemberInfo(consumerId, subscription.groupInstanceId(), subscription.rackId());
            for (String topic : subscription.topics()) {
                put(topicToConsumers, topic, memberInfo);
            }
        }
        return topicToConsumers;
    }

    @Override
    public Map<String, List<TopicPartition>> assignPartitions(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                                              Map<String, Subscription> subscriptions) {
        Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);
        Map<String, TopicAssignmentState> topicAssignmentStates = partitionsPerTopic.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new TopicAssignmentState(e.getValue(), consumersPerTopic.get(e.getKey()))));

        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<>());

        for (Map.Entry<String, TopicAssignmentState> topicEntry : topicAssignmentStates.entrySet()) {
            String topic = topicEntry.getKey();
            TopicAssignmentState topicAssignmentState = topicEntry.getValue();

            List<PartitionInfo> partitionsForTopic = partitionsPerTopic.get(topic);
            if (partitionsForTopic == null || partitionsForTopic.isEmpty())
                continue;

            // Assign based on racks first, but limit to each consumer's quota since we
            // prioritize balanced assignment over locality.
            for (String rackId : topicAssignmentState.racks) {
                List<String> consumersForRack = topicAssignmentState.consumersByRack.get(rackId);
                List<TopicPartition> partitionsForRack = topicAssignmentState.partitionsByRack.get(rackId);
                doAssign(consumersForRack, partitionsForRack, assignment, topicAssignmentState);
            }

            // Assign any remaining partitions without matching racks, still maintaining balanced assignment
            doAssign(topicAssignmentState.consumers, new ArrayList<>(topicAssignmentState.unassignedPartitions), assignment, topicAssignmentState);
        }
        return assignment;
    }

    private void doAssign(List<String> consumers,
                         List<TopicPartition> assignablePartitions,
                         Map<String, List<TopicPartition>> assignment,
                         TopicAssignmentState assignmentState) {
        assignablePartitions.removeIf(tp -> !assignmentState.unassignedPartitions.contains(tp));
        if (consumers.isEmpty() || assignmentState.unassignedPartitions.isEmpty() || assignablePartitions.isEmpty())
            return;

        int start = 0;
        for (String consumer : consumers) {
            List<TopicPartition> consumerAssignment = assignment.get(consumer);
            int numAssignable = Math.min(assignmentState.maxAssignable(consumer), assignablePartitions.size());
            if (numAssignable <= 0)
                continue;

            List<TopicPartition> partitionsToAssign = assignablePartitions.subList(start, start + numAssignable);
            consumerAssignment.addAll(partitionsToAssign);
            assignmentState.onAssigned(consumer, partitionsToAssign);
            start += numAssignable;
            if (start >= assignablePartitions.size())
                break;
        }
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        return assignPartitions(partitionInfosWithoutRacks(partitionsPerTopic), subscriptions);
    }

    private class TopicAssignmentState {
        private final List<String> consumers;
        private final List<String> racks;
        private final Map<String, List<TopicPartition>> partitionsByRack;
        private final Map<String, List<String>> consumersByRack;

        private final List<TopicPartition> unassignedPartitions;
        private final Map<String, Integer> numAssignedByConsumer;
        private final int numPartitionsPerConsumer;
        private final int numConsumersWithExtraPartition;

        public TopicAssignmentState(List<PartitionInfo> partitionInfos, List<MemberInfo> membersOrNull) {
            List<MemberInfo> members = membersOrNull == null ? Collections.emptyList() : membersOrNull;
            Collections.sort(members);
            consumers = members.stream().map(c -> c.memberId).collect(Collectors.toList());

            this.unassignedPartitions = partitionInfos.stream().map(p -> new TopicPartition(p.topic(), p.partition()))
                    .collect(Collectors.toCollection(LinkedList::new));
            this.numAssignedByConsumer = consumers.stream().collect(Collectors.toMap(Function.identity(), c -> 0));
            numPartitionsPerConsumer = consumers.isEmpty() ? 0 : partitionInfos.size() / consumers.size();
            numConsumersWithExtraPartition = consumers.isEmpty() ? 0 : partitionInfos.size() % consumers.size();

            Map<String, List<String>> consumersByRack = new HashMap<>();
            members.forEach(consumer -> put(consumersByRack, consumer.rackId.orElse(""), consumer.memberId));
            consumersByRack.remove("");
            Map<String, List<TopicPartition>> partitionsByRack = consumersByRack.isEmpty() ? Collections.emptyMap() : partitionsByRack(partitionInfos, null);
            consumersByRack.keySet().removeIf(r -> !partitionsByRack.containsKey(r));
            partitionsByRack.keySet().removeIf(r -> !consumersByRack.containsKey(r));

            boolean useRackAwareAssignment = useRackAwareAssignment(consumersByRack, partitionsByRack);
            if (useRackAwareAssignment) {
                racks = new ArrayList<>(consumersByRack.keySet());
                this.consumersByRack = consumersByRack;
                this.partitionsByRack = partitionsByRack;
                racks.sort(Comparator.comparing(this::partitionsPerConsumerOnRack));
            } else {
                racks = Collections.emptyList();
                this.consumersByRack = Collections.emptyMap();
                this.partitionsByRack = Collections.emptyMap();
            }
        }

        private double partitionsPerConsumerOnRack(String rackId) {
            int partitions = partitionsByRack.containsKey(rackId) ? partitionsByRack.get(rackId).size() : 0;
            int consumers = consumersByRack.containsKey(rackId) ? consumersByRack.get(rackId).size() : 1;
            return (double) partitions / consumers;

        }

        int maxAssignable(String consumer) {
            boolean hasExtra = numAssignedByConsumer.values().stream().filter(p -> p > numPartitionsPerConsumer).count() < numConsumersWithExtraPartition;
            int maxForConsumer = numPartitionsPerConsumer + (hasExtra ? 1 : 0) - numAssignedByConsumer.get(consumer);
            return Math.max(0, maxForConsumer);
        }

        void onAssigned(String consumer, List<TopicPartition> newlyAssignedPartitions) {
            unassignedPartitions.removeAll(newlyAssignedPartitions);
            numAssignedByConsumer.compute(consumer, (c, n) -> n + newlyAssignedPartitions.size());
        }

        @Override
        public String toString() {
            return "TopicAssignmentState(" +
                    "consumersByRack=" + consumersByRack +
                    ", partitionsByRack=" + partitionsByRack +
                    ", unassignedPartitions=" + unassignedPartitions +
                    ")";
        }
    }
}
