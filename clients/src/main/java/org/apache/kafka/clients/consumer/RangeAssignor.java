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
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
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
 * Rack-aware assignment is used if both consumer and partition replica racks are available and
 * some partitions have replicas only on a subset of racks. We attempt to match consumer racks with
 * partition replica racks on a best-effort basis, prioritizing balanced assignment over rack alignment.
 * Topics with equal partition count and same set of subscribers guarantee co-partitioning by prioritizing
 * co-partitioning over rack alignment. In this case, aligning partition replicas of these topics on the
 * same racks will improve locality for consumers. For example, if partitions 0 of all topics have a replica
 * on rack 'a', partition 1 on rack 'b' etc., partition 0 of all topics can be assigned to a consumer
 * on rack 'a', partition 1 to a consumer on rack 'b' and so on.
 * <p>
 * Note that rack-aware assignment currently takes all replicas into account, including any offline replicas
 * and replicas that are not in the ISR. This is based on the assumption that these replicas are likely
 * to join the ISR relatively soon. Since consumers don't rebalance on ISR change, this avoids unnecessary
 * cross-rack traffic for long durations after replicas rejoin the ISR. In the future, we may consider
 * rebalancing when replicas are added or removed to improve consumer rack alignment.
 * </p>
 */
public class RangeAssignor extends AbstractPartitionAssignor {
    public static final String RANGE_ASSIGNOR_NAME = "range";
    private static final TopicPartitionComparator PARTITION_COMPARATOR = new TopicPartitionComparator();

    @Override
    public String name() {
        return RANGE_ASSIGNOR_NAME;
    }

    private Map<String, List<MemberInfo>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<MemberInfo>> topicToConsumers = new HashMap<>();
        consumerMetadata.forEach((consumerId, subscription) -> {
            MemberInfo memberInfo = new MemberInfo(consumerId, subscription.groupInstanceId(), subscription.rackId());
            subscription.topics().forEach(topic -> put(topicToConsumers, topic, memberInfo));
        });
        return topicToConsumers;
    }

    /**
     * Performs range assignment of the specified partitions for the consumers with the provided subscriptions.
     * If rack-awareness is enabled for one or more consumers, we perform rack-aware assignment first to assign
     * the subset of partitions that can be aligned on racks, while retaining the same co-partitioning and
     * per-topic balancing guarantees as non-rack-aware range assignment. The remaining partitions are assigned
     * using standard non-rack-aware range assignment logic, which may result in mis-aligned racks.
     */
    @Override
    public Map<String, List<TopicPartition>> assignPartitions(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                                              Map<String, Subscription> subscriptions) {
        Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);
        Map<String, String> consumerRacks = consumerRacks(subscriptions);
        List<TopicAssignmentState> topicAssignmentStates = partitionsPerTopic.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .map(e -> new TopicAssignmentState(e.getKey(), e.getValue(), consumersPerTopic.get(e.getKey()), consumerRacks))
                .collect(Collectors.toList());

        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        subscriptions.keySet().forEach(memberId -> assignment.put(memberId, new ArrayList<>()));

        boolean useRackAware = topicAssignmentStates.stream().anyMatch(t -> t.needsRackAwareAssignment);
        if (useRackAware)
            assignWithRackMatching(topicAssignmentStates, assignment);

        topicAssignmentStates.forEach(t -> assignRanges(t, (c, tp) -> true, assignment));

        if (useRackAware)
            assignment.values().forEach(list -> list.sort(PARTITION_COMPARATOR));
        return assignment;
    }

    // This method is not used, but retained for compatibility with any custom assignors that extend this class.
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        return assignPartitions(partitionInfosWithoutRacks(partitionsPerTopic), subscriptions);
    }

    private void assignRanges(TopicAssignmentState assignmentState,
                              BiFunction<String, TopicPartition, Boolean> mayAssign,
                              Map<String, List<TopicPartition>> assignment) {
        for (String consumer : assignmentState.consumers.keySet()) {
            if (assignmentState.unassignedPartitions.isEmpty())
                break;
            List<TopicPartition> assignablePartitions = assignmentState.unassignedPartitions.stream()
                    .filter(tp -> mayAssign.apply(consumer, tp))
                    .limit(assignmentState.maxAssignable(consumer))
                    .collect(Collectors.toList());
            if (assignablePartitions.isEmpty())
                continue;

            assign(consumer, assignablePartitions, assignmentState, assignment);
        }
    }

    private void assignWithRackMatching(Collection<TopicAssignmentState> assignmentStates,
                                        Map<String, List<TopicPartition>> assignment) {

        assignmentStates.stream().collect(Collectors.groupingBy(t -> t.consumers)).forEach((consumers, states) -> {
            states.stream().collect(Collectors.groupingBy(t -> t.partitionRacks.size())).forEach((numPartitions, coPartitionedStates) -> {
                if (coPartitionedStates.size() > 1)
                    assignCoPartitionedWithRackMatching(consumers, numPartitions, coPartitionedStates, assignment);
                else {
                    TopicAssignmentState state = coPartitionedStates.get(0);
                    if (state.needsRackAwareAssignment)
                        assignRanges(state, state::racksMatch, assignment);
                }
            });
        });
    }

    private void assignCoPartitionedWithRackMatching(LinkedHashMap<String, Optional<String>> consumers,
                                                     int numPartitions,
                                                     Collection<TopicAssignmentState> assignmentStates,
                                                     Map<String, List<TopicPartition>> assignment) {

        Set<String> remainingConsumers = new LinkedHashSet<>(consumers.keySet());
        for (int i = 0; i < numPartitions; i++) {
            int p = i;

            Optional<String> matchingConsumer = remainingConsumers.stream()
                    .filter(c -> assignmentStates.stream().allMatch(t -> t.racksMatch(c, new TopicPartition(t.topic, p)) && t.maxAssignable(c) > 0))
                    .findFirst();
            if (matchingConsumer.isPresent()) {
                String consumer = matchingConsumer.get();
                assignmentStates.forEach(t -> assign(consumer, Collections.singletonList(new TopicPartition(t.topic, p)), t, assignment));

                if (assignmentStates.stream().noneMatch(t -> t.maxAssignable(consumer) > 0)) {
                    remainingConsumers.remove(consumer);
                    if (remainingConsumers.isEmpty())
                        break;
                }
            }
        }
    }

    private void assign(String consumer, List<TopicPartition> partitions, TopicAssignmentState assignmentState, Map<String, List<TopicPartition>> assignment) {
        assignment.get(consumer).addAll(partitions);
        assignmentState.onAssigned(consumer, partitions);
    }

    private Map<String, String> consumerRacks(Map<String, Subscription> subscriptions) {
        Map<String, String> consumerRacks = new HashMap<>(subscriptions.size());
        subscriptions.forEach((memberId, subscription) ->
                subscription.rackId().filter(r -> !r.isEmpty()).ifPresent(rackId -> consumerRacks.put(memberId, rackId)));
        return consumerRacks;
    }

    private class TopicAssignmentState {
        private final String topic;
        private final LinkedHashMap<String, Optional<String>> consumers;
        private final boolean needsRackAwareAssignment;
        private final Map<TopicPartition, Set<String>> partitionRacks;

        private final Set<TopicPartition> unassignedPartitions;
        private final Map<String, Integer> numAssignedByConsumer;
        private final int numPartitionsPerConsumer;
        private int remainingConsumersWithExtraPartition;

        public TopicAssignmentState(String topic, List<PartitionInfo> partitionInfos, List<MemberInfo> membersOrNull, Map<String, String> consumerRacks) {
            this.topic = topic;
            List<MemberInfo> members = membersOrNull == null ? Collections.emptyList() : membersOrNull;
            Collections.sort(members);
            consumers = members.stream().map(c -> c.memberId)
                    .collect(Collectors.toMap(Function.identity(), c -> Optional.ofNullable(consumerRacks.get(c)), (a, b) -> a, LinkedHashMap::new));

            this.unassignedPartitions = partitionInfos.stream().map(p -> new TopicPartition(p.topic(), p.partition()))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            this.numAssignedByConsumer = consumers.keySet().stream().collect(Collectors.toMap(Function.identity(), c -> 0));
            numPartitionsPerConsumer = consumers.isEmpty() ? 0 : partitionInfos.size() / consumers.size();
            remainingConsumersWithExtraPartition = consumers.isEmpty() ? 0 : partitionInfos.size() % consumers.size();

            Set<String> allConsumerRacks = new HashSet<>();
            Set<String> allPartitionRacks = new HashSet<>();
            members.stream().map(m -> m.memberId).filter(consumerRacks::containsKey)
                    .forEach(memberId -> allConsumerRacks.add(consumerRacks.get(memberId)));
            if (!allConsumerRacks.isEmpty()) {
                partitionRacks = new HashMap<>(partitionInfos.size());
                partitionInfos.forEach(p -> {
                    TopicPartition tp = new TopicPartition(p.topic(), p.partition());
                    Set<String> racks = Arrays.stream(p.replicas())
                            .map(Node::rack)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet());
                    partitionRacks.put(tp, racks);
                    allPartitionRacks.addAll(racks);
                });
            } else {
                partitionRacks = Collections.emptyMap();
            }

            needsRackAwareAssignment = useRackAwareAssignment(allConsumerRacks, allPartitionRacks, partitionRacks);
        }

        boolean racksMatch(String consumer, TopicPartition tp) {
            Optional<String> consumerRack = consumers.get(consumer);
            Set<String> replicaRacks = partitionRacks.get(tp);
            return !consumerRack.isPresent() || (replicaRacks != null && replicaRacks.contains(consumerRack.get()));
        }

        int maxAssignable(String consumer) {
            int maxForConsumer = numPartitionsPerConsumer + (remainingConsumersWithExtraPartition > 0 ? 1 : 0) - numAssignedByConsumer.get(consumer);
            return Math.max(0, maxForConsumer);
        }

        void onAssigned(String consumer, List<TopicPartition> newlyAssignedPartitions) {
            int numAssigned = numAssignedByConsumer.compute(consumer, (c, n) -> n + newlyAssignedPartitions.size());
            if (numAssigned > numPartitionsPerConsumer)
                remainingConsumersWithExtraPartition--;
            unassignedPartitions.removeAll(newlyAssignedPartitions);
        }

        @Override
        public String toString() {
            return "TopicAssignmentState(" +
                    "topic=" + topic +
                    ", consumers=" + consumers +
                    ", partitionRacks=" + partitionRacks +
                    ", unassignedPartitions=" + unassignedPartitions +
                    ")";
        }
    }
}
