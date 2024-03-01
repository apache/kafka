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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.internals.Utils.PartitionComparator;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sticky assignment implementation used by {@link org.apache.kafka.clients.consumer.StickyAssignor} and
 * {@link org.apache.kafka.clients.consumer.CooperativeStickyAssignor}. Sticky assignors are rack-aware.
 * If racks are specified for consumers, we attempt to match consumer racks with partition replica
 * racks on a best-effort basis, prioritizing balanced assignment over rack alignment. Previously
 * owned partitions may be reassigned to improve rack locality. We use rack-aware assignment if both
 * consumer and partition racks are available and some partitions have replicas only on a subset of racks.
 */
public abstract class AbstractStickyAssignor extends AbstractPartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(AbstractStickyAssignor.class);

    public static final int DEFAULT_GENERATION = -1;
    private int maxGeneration = DEFAULT_GENERATION;

    private PartitionMovements partitionMovements;

    // Keep track of the partitions being migrated from one consumer to another during assignment
    // so the cooperative assignor can adjust the assignment
    protected Map<TopicPartition, String> partitionsTransferringOwnership = new HashMap<>();

    static final class ConsumerGenerationPair {
        final String consumer;
        final int generation;
        ConsumerGenerationPair(String consumer, int generation) {
            this.consumer = consumer;
            this.generation = generation;
        }
    }

    public static final class MemberData {
        public final List<TopicPartition> partitions;
        public final Optional<Integer> generation;
        public final Optional<String> rackId;

        public MemberData(List<TopicPartition> partitions, Optional<Integer> generation, Optional<String> rackId) {
            this.partitions = partitions;
            this.generation = generation;
            this.rackId = rackId;
        }

        public MemberData(List<TopicPartition> partitions, Optional<Integer> generation) {
            this(partitions, generation, Optional.empty());
        }
    }

    abstract protected MemberData memberData(Subscription subscription);

    @Override
    public Map<String, List<TopicPartition>> assignPartitions(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                                              Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> consumerToOwnedPartitions = new HashMap<>();
        Set<TopicPartition> partitionsWithMultiplePreviousOwners = new HashSet<>();

        List<PartitionInfo> allPartitions = new ArrayList<>();
        partitionsPerTopic.values().forEach(allPartitions::addAll);
        RackInfo rackInfo = new RackInfo(allPartitions, subscriptions);

        AbstractAssignmentBuilder assignmentBuilder;
        if (allSubscriptionsEqual(partitionsPerTopic.keySet(), subscriptions, consumerToOwnedPartitions, partitionsWithMultiplePreviousOwners)) {
            log.debug("Detected that all consumers were subscribed to same set of topics, invoking the "
                          + "optimized assignment algorithm");
            partitionsTransferringOwnership = new HashMap<>();
            assignmentBuilder = new ConstrainedAssignmentBuilder(partitionsPerTopic, rackInfo, consumerToOwnedPartitions, partitionsWithMultiplePreviousOwners);
        } else {
            log.debug("Detected that not all consumers were subscribed to same set of topics, falling back to the "
                          + "general case assignment algorithm");
            // we must set this to null for the general case so the cooperative assignor knows to compute it from scratch
            partitionsTransferringOwnership = null;
            assignmentBuilder = new GeneralAssignmentBuilder(partitionsPerTopic, rackInfo, consumerToOwnedPartitions, subscriptions);
        }
        return assignmentBuilder.build();
    }

    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        return assignPartitions(partitionInfosWithoutRacks(partitionsPerTopic), subscriptions);
    }

    public int maxGeneration() {
        return maxGeneration;
    }

    /**
     * Returns true iff all consumers have an identical subscription. Also fills out the passed in
     * {@code consumerToOwnedPartitions} with each consumer's previously owned and still-subscribed partitions,
     * and the {@code partitionsWithMultiplePreviousOwners} with any partitions claimed by multiple previous owners
     */
    private boolean allSubscriptionsEqual(Set<String> allTopics,
                                          Map<String, Subscription> subscriptions,
                                          Map<String, List<TopicPartition>> consumerToOwnedPartitions,
                                          Set<TopicPartition> partitionsWithMultiplePreviousOwners) {
        boolean isAllSubscriptionsEqual = true;

        Set<String> subscribedTopics = new HashSet<>();

        // keep track of all previously owned partitions so we can invalidate them if invalid input is
        // detected, eg two consumers somehow claiming the same partition in the same/current generation
        Map<TopicPartition, String> allPreviousPartitionsToOwner = new HashMap<>();

        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            final String consumer = subscriptionEntry.getKey();
            final Subscription subscription = subscriptionEntry.getValue();

            // initialize the subscribed topics set if this is the first subscription
            if (subscribedTopics.isEmpty()) {
                subscribedTopics.addAll(subscription.topics());
            } else if (isAllSubscriptionsEqual && !(subscription.topics().size() == subscribedTopics.size()
                && subscribedTopics.containsAll(subscription.topics()))) {
                isAllSubscriptionsEqual = false;
            }

            MemberData memberData = memberData(subscription);
            final int memberGeneration = memberData.generation.orElse(DEFAULT_GENERATION);
            maxGeneration = Math.max(maxGeneration, memberGeneration);

            List<TopicPartition> ownedPartitions = new ArrayList<>();
            consumerToOwnedPartitions.put(consumer, ownedPartitions);

            // the member has a valid generation, so we can consider its owned partitions if it has the highest
            // generation amongst
            for (final TopicPartition tp : memberData.partitions) {
                if (allTopics.contains(tp.topic())) {
                    String otherConsumer = allPreviousPartitionsToOwner.get(tp);
                    if (otherConsumer == null) {
                        // this partition is not owned by other consumer in the same generation
                        ownedPartitions.add(tp);
                        allPreviousPartitionsToOwner.put(tp, consumer);
                    } else {
                        final int otherMemberGeneration = subscriptions.get(otherConsumer).generationId().orElse(DEFAULT_GENERATION);

                        if (memberGeneration == otherMemberGeneration) {
                            // if two members of the same generation own the same partition, revoke the partition
                            log.error("Found multiple consumers {} and {} claiming the same TopicPartition {} in the "
                                            + "same generation {}, this will be invalidated and removed from their previous assignment.",
                                    consumer, otherConsumer, tp, memberGeneration);
                            partitionsWithMultiplePreviousOwners.add(tp);
                            consumerToOwnedPartitions.get(otherConsumer).remove(tp);
                            allPreviousPartitionsToOwner.put(tp, consumer);
                        } else if (memberGeneration > otherMemberGeneration) {
                            // move partition from the member with an older generation to the member with the newer generation
                            ownedPartitions.add(tp);
                            consumerToOwnedPartitions.get(otherConsumer).remove(tp);
                            allPreviousPartitionsToOwner.put(tp, consumer);
                            log.warn("Consumer {} in generation {} and consumer {} in generation {} claiming the same " +
                                            "TopicPartition {} in different generations. The topic partition will be " +
                                            "assigned to the member with the higher generation {}.",
                                    consumer, memberGeneration,
                                    otherConsumer, otherMemberGeneration,
                                    tp,
                                    memberGeneration);
                        } else {
                            // let the other member continue to own the topic partition
                            log.warn("Consumer {} in generation {} and consumer {} in generation {} claiming the same " +
                                            "TopicPartition {} in different generations. The topic partition will be " +
                                            "assigned to the member with the higher generation {}.",
                                    consumer, memberGeneration,
                                    otherConsumer, otherMemberGeneration,
                                    tp,
                                    otherMemberGeneration);
                        }
                    }
                }
            }
        }
        return isAllSubscriptionsEqual;
    }

    public boolean isSticky() {
        return partitionMovements.isSticky();
    }

    private static class TopicComparator implements Comparator<String>, Serializable {
        private static final long serialVersionUID = 1L;
        private final Map<String, List<String>> map;

        TopicComparator(Map<String, List<String>> map) {
            this.map = map;
        }

        @Override
        public int compare(String o1, String o2) {
            int ret = map.get(o1).size() - map.get(o2).size();
            if (ret == 0) {
                ret = o1.compareTo(o2);
            }
            return ret;
        }
    }

    private static class SubscriptionComparator implements Comparator<String>, Serializable {
        private static final long serialVersionUID = 1L;
        private final Map<String, List<TopicPartition>> map;

        SubscriptionComparator(Map<String, List<TopicPartition>> map) {
            this.map = map;
        }

        @Override
        public int compare(String o1, String o2) {
            int ret = map.get(o1).size() - map.get(o2).size();
            if (ret == 0)
                ret = o1.compareTo(o2);
            return ret;
        }
    }

    /**
     * This class maintains some data structures to simplify lookup of partition movements among consumers. At each point of
     * time during a partition rebalance it keeps track of partition movements corresponding to each topic, and also possible
     * movement (in form a <code>ConsumerPair</code> object) for each partition.
     */
    private static class PartitionMovements {
        private final Map<String, Map<ConsumerPair, Set<TopicPartition>>> partitionMovementsByTopic = new HashMap<>();
        private final Map<TopicPartition, ConsumerPair> partitionMovements = new HashMap<>();

        private ConsumerPair removeMovementRecordOfPartition(TopicPartition partition) {
            ConsumerPair pair = partitionMovements.remove(partition);

            String topic = partition.topic();
            Map<ConsumerPair, Set<TopicPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            partitionMovementsForThisTopic.get(pair).remove(partition);
            if (partitionMovementsForThisTopic.get(pair).isEmpty())
                partitionMovementsForThisTopic.remove(pair);
            if (partitionMovementsByTopic.get(topic).isEmpty())
                partitionMovementsByTopic.remove(topic);

            return pair;
        }

        private void addPartitionMovementRecord(TopicPartition partition, ConsumerPair pair) {
            partitionMovements.put(partition, pair);

            String topic = partition.topic();
            if (!partitionMovementsByTopic.containsKey(topic))
                partitionMovementsByTopic.put(topic, new HashMap<>());

            Map<ConsumerPair, Set<TopicPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            if (!partitionMovementsForThisTopic.containsKey(pair))
                partitionMovementsForThisTopic.put(pair, new HashSet<>());

            partitionMovementsForThisTopic.get(pair).add(partition);
        }

        private void movePartition(TopicPartition partition, String oldConsumer, String newConsumer) {
            ConsumerPair pair = new ConsumerPair(oldConsumer, newConsumer);

            if (partitionMovements.containsKey(partition)) {
                // this partition has previously moved
                ConsumerPair existingPair = removeMovementRecordOfPartition(partition);
                assert existingPair.dstMemberId.equals(oldConsumer);
                if (!existingPair.srcMemberId.equals(newConsumer)) {
                    // the partition is not moving back to its previous consumer
                    // return new ConsumerPair2(existingPair.src, newConsumer);
                    addPartitionMovementRecord(partition, new ConsumerPair(existingPair.srcMemberId, newConsumer));
                }
            } else
                addPartitionMovementRecord(partition, pair);
        }

        private TopicPartition getTheActualPartitionToBeMoved(TopicPartition partition, String oldConsumer, String newConsumer) {
            String topic = partition.topic();

            if (!partitionMovementsByTopic.containsKey(topic))
                return partition;

            if (partitionMovements.containsKey(partition)) {
                // this partition has previously moved
                assert oldConsumer.equals(partitionMovements.get(partition).dstMemberId);
                oldConsumer = partitionMovements.get(partition).srcMemberId;
            }

            Map<ConsumerPair, Set<TopicPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            ConsumerPair reversePair = new ConsumerPair(newConsumer, oldConsumer);
            if (!partitionMovementsForThisTopic.containsKey(reversePair))
                return partition;

            return partitionMovementsForThisTopic.get(reversePair).iterator().next();
        }

        private boolean isLinked(String src, String dst, Set<ConsumerPair> pairs, List<String> currentPath) {
            if (src.equals(dst))
                return false;

            if (pairs.isEmpty())
                return false;

            if (new ConsumerPair(src, dst).in(pairs)) {
                currentPath.add(src);
                currentPath.add(dst);
                return true;
            }

            for (ConsumerPair pair: pairs)
                if (pair.srcMemberId.equals(src)) {
                    Set<ConsumerPair> reducedSet = new HashSet<>(pairs);
                    reducedSet.remove(pair);
                    currentPath.add(pair.srcMemberId);
                    return isLinked(pair.dstMemberId, dst, reducedSet, currentPath);
                }

            return false;
        }

        private boolean in(List<String> cycle, Set<List<String>> cycles) {
            List<String> superCycle = new ArrayList<>(cycle);
            superCycle.remove(superCycle.size() - 1);
            superCycle.addAll(cycle);
            for (List<String> foundCycle: cycles) {
                if (foundCycle.size() == cycle.size() && Collections.indexOfSubList(superCycle, foundCycle) != -1)
                    return true;
            }
            return false;
        }

        private boolean hasCycles(Set<ConsumerPair> pairs) {
            Set<List<String>> cycles = new HashSet<>();
            for (ConsumerPair pair: pairs) {
                Set<ConsumerPair> reducedPairs = new HashSet<>(pairs);
                reducedPairs.remove(pair);
                List<String> path = new ArrayList<>(Collections.singleton(pair.srcMemberId));
                if (isLinked(pair.dstMemberId, pair.srcMemberId, reducedPairs, path) && !in(path, cycles)) {
                    cycles.add(new ArrayList<>(path));
                    log.error("A cycle of length {} was found: {}", path.size() - 1, path);
                }
            }

            // for now we want to make sure there is no partition movements of the same topic between a pair of consumers.
            // the odds of finding a cycle among more than two consumers seem to be very low (according to various randomized
            // tests with the given sticky algorithm) that it should not worth the added complexity of handling those cases.
            for (List<String> cycle: cycles)
                if (cycle.size() == 3) // indicates a cycle of length 2
                    return true;
            return false;
        }

        private boolean isSticky() {
            for (Map.Entry<String, Map<ConsumerPair, Set<TopicPartition>>> topicMovements: this.partitionMovementsByTopic.entrySet()) {
                Set<ConsumerPair> topicMovementPairs = topicMovements.getValue().keySet();
                if (hasCycles(topicMovementPairs)) {
                    log.error("Stickiness is violated for topic {}"
                        + "\nPartition movements for this topic occurred among the following consumer pairs:"
                        + "\n{}", topicMovements.getKey(), topicMovements.getValue().toString());
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * <code>ConsumerPair</code> represents a pair of Kafka consumer ids involved in a partition reassignment. Each
     * <code>ConsumerPair</code> object, which contains a source (<code>src</code>) and a destination (<code>dst</code>)
     * element, normally corresponds to a particular partition or topic, and indicates that the particular partition or some
     * partition of the particular topic was moved from the source consumer to the destination consumer during the rebalance.
     * This class is used, through the <code>PartitionMovements</code> class, by the sticky assignor and helps in determining
     * whether a partition reassignment results in cycles among the generated graph of consumer pairs.
     */
    private static class ConsumerPair {
        private final String srcMemberId;
        private final String dstMemberId;

        ConsumerPair(String srcMemberId, String dstMemberId) {
            this.srcMemberId = srcMemberId;
            this.dstMemberId = dstMemberId;
        }

        public String toString() {
            return this.srcMemberId + "->" + this.dstMemberId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.srcMemberId == null) ? 0 : this.srcMemberId.hashCode());
            result = prime * result + ((this.dstMemberId == null) ? 0 : this.dstMemberId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;

            if (!getClass().isInstance(obj))
                return false;

            ConsumerPair otherPair = (ConsumerPair) obj;
            return this.srcMemberId.equals(otherPair.srcMemberId) && this.dstMemberId.equals(otherPair.dstMemberId);
        }

        private boolean in(Set<ConsumerPair> pairs) {
            for (ConsumerPair pair: pairs)
                if (this.equals(pair))
                    return true;
            return false;
        }
    }

    private class RackInfo {
        private final Map<String, String> consumerRacks;
        private final Map<TopicPartition, Set<String>> partitionRacks;
        private final Map<TopicPartition, Integer> numConsumersByPartition;

        public RackInfo(List<PartitionInfo> partitionInfos, Map<String, Subscription> subscriptions) {
            List<Subscription> consumers = new ArrayList<>(subscriptions.values());

            Map<String, List<String>> consumersByRack = new HashMap<>();
            subscriptions.forEach((memberId, subscription) ->
                    subscription.rackId().filter(r -> !r.isEmpty()).ifPresent(rackId -> put(consumersByRack, rackId, memberId)));

            Map<String, List<TopicPartition>> partitionsByRack;
            Map<TopicPartition, Set<String>> partitionRacks;
            if (consumersByRack.isEmpty()) {
                partitionsByRack = Collections.emptyMap();
                partitionRacks = Collections.emptyMap();
            } else {
                partitionRacks = new HashMap<>(partitionInfos.size());
                partitionsByRack = new HashMap<>();
                partitionInfos.forEach(p -> {
                    TopicPartition tp = new TopicPartition(p.topic(), p.partition());
                    Set<String> racks = new HashSet<>(p.replicas().length);
                    partitionRacks.put(tp, racks);
                    Arrays.stream(p.replicas())
                            .map(Node::rack)
                            .filter(Objects::nonNull)
                            .distinct()
                            .forEach(rackId -> {
                                put(partitionsByRack, rackId, tp);
                                racks.add(rackId);
                            });
                });
            }

            if (useRackAwareAssignment(consumersByRack.keySet(), partitionsByRack.keySet(), partitionRacks)) {
                this.consumerRacks = new HashMap<>(consumers.size());
                consumersByRack.forEach((rack, rackConsumers) -> rackConsumers.forEach(c -> consumerRacks.put(c, rack)));
                this.partitionRacks = partitionRacks;
            } else {
                this.consumerRacks = Collections.emptyMap();
                this.partitionRacks = Collections.emptyMap();
            }
            numConsumersByPartition = partitionRacks.entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().stream()
                        .map(r -> consumersByRack.getOrDefault(r, Collections.emptyList()).size())
                        .reduce(0, Integer::sum)));
        }

        private boolean racksMismatch(String consumer, TopicPartition tp) {
            String consumerRack = consumerRacks.get(consumer);
            Set<String> replicaRacks = partitionRacks.get(tp);
            return consumerRack != null && (replicaRacks == null || !replicaRacks.contains(consumerRack));
        }

        private List<TopicPartition> sortPartitionsByRackConsumers(List<TopicPartition> partitions) {
            if (numConsumersByPartition.isEmpty())
                return partitions;
            // Return a sorted linked list of partitions to enable fast updates during rack-aware assignment
            List<TopicPartition> sortedPartitions = new LinkedList<>(partitions);
            sortedPartitions.sort(Comparator.comparing(tp -> numConsumersByPartition.getOrDefault(tp, 0)));
            return sortedPartitions;
        }

        private int nextRackConsumer(TopicPartition tp, List<String> consumerList, int firstIndex) {
            Set<String> racks = partitionRacks.get(tp);
            if (racks == null || racks.isEmpty())
                return -1;
            for (int i = 0; i < consumerList.size(); i++) {
                int index = (firstIndex + i) % consumerList.size();
                String consumer = consumerList.get(index);
                String consumerRack = consumerRacks.get(consumer);
                if (consumerRack != null && racks.contains(consumerRack))
                    return index;
            }
            return -1;
        }

        @Override
        public String toString() {
            return "RackInfo(" +
                    "consumerRacks=" + consumerRacks +
                    ", partitionRacks=" + partitionRacks +
                    ")";
        }
    }

    private abstract class AbstractAssignmentBuilder {

        final Map<String, List<PartitionInfo>> partitionsPerTopic;
        final RackInfo rackInfo;
        final Map<String, List<TopicPartition>> currentAssignment;
        final int totalPartitionsCount;

        AbstractAssignmentBuilder(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                  RackInfo rackInfo,
                                  Map<String, List<TopicPartition>> currentAssignment) {
            this.partitionsPerTopic = partitionsPerTopic;
            this.currentAssignment = currentAssignment;
            this.rackInfo = rackInfo;
            this.totalPartitionsCount = partitionsPerTopic.values().stream().map(List::size).reduce(0, Integer::sum);
        }

        /**
         * Builds the assignment.
         *
         * @return Map from each member to the list of partitions assigned to them.
         */
        abstract Map<String, List<TopicPartition>> build();

        protected List<TopicPartition> getAllTopicPartitions(List<String> sortedAllTopics) {
            List<TopicPartition> allPartitions = new ArrayList<>(totalPartitionsCount);

            for (String topic : sortedAllTopics) {
                partitionsPerTopic.get(topic).forEach(p -> allPartitions.add(new TopicPartition(p.topic(), p.partition())));
            }
            return allPartitions;
        }
    }

    /**
     * This constrained assignment optimizes the assignment algorithm when all consumers were subscribed to same set of topics.
     * The method includes the following steps:
     *
     * 1. Reassign previously owned partitions:
     *   a. if owned less than minQuota partitions, just assign all owned partitions, and put the member into unfilled member list.
     *   b. if owned maxQuota or more, and we're still under the number of expected max capacity members, assign maxQuota partitions
     *   c. if owned at least "minQuota" of partitions, assign minQuota partitions, and put the member into unfilled member list if
     *     we're still under the number of expected max capacity members
     *   If using rack-aware algorithm, only owned partitions with matching racks are allocated in this step.
     * 2. Fill remaining members with rack matching up to the expected numbers of maxQuota partitions, otherwise, to minQuota partitions.
     *    Partitions that cannot be aligned on racks within the quota are not assigned in this step. This step is only used if rack-aware.
     * 3. Fill remaining members up to the expected numbers of maxQuota partitions, otherwise, to minQuota partitions.
     *    For rack-aware algorithm, these are partitions that could not be aligned on racks within the balancing constraints.
     *
     */
    private class ConstrainedAssignmentBuilder extends AbstractAssignmentBuilder {

        private final Set<TopicPartition> partitionsWithMultiplePreviousOwners;
        private final Set<TopicPartition> allRevokedPartitions;

        // the consumers which may still be assigned one or more partitions to reach expected capacity
        private final List<String> unfilledMembersWithUnderMinQuotaPartitions;
        private final LinkedList<String> unfilledMembersWithExactlyMinQuotaPartitions;

        private final int minQuota;
        private final int maxQuota;
        // the expected number of members receiving more than minQuota partitions (zero when minQuota == maxQuota)
        private final int expectedNumMembersWithOverMinQuotaPartitions;
        // the current number of members receiving more than minQuota partitions (zero when minQuota == maxQuota)
        private int currentNumMembersWithOverMinQuotaPartitions;

        private final Map<String, List<TopicPartition>> assignment;
        private final List<TopicPartition> assignedPartitions;

        /**
         * Constructs a constrained assignment builder.
         *
         * @param partitionsPerTopic                   The partitions for each subscribed topic
         * @param rackInfo                             Rack information for consumers and racks
         * @param consumerToOwnedPartitions            Each consumer's previously owned and still-subscribed partitions
         * @param partitionsWithMultiplePreviousOwners The partitions being claimed in the previous assignment of multiple consumers
         */
        ConstrainedAssignmentBuilder(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                     RackInfo rackInfo,
                                     Map<String, List<TopicPartition>> consumerToOwnedPartitions,
                                     Set<TopicPartition> partitionsWithMultiplePreviousOwners) {
            super(partitionsPerTopic, rackInfo, consumerToOwnedPartitions);

            this.partitionsWithMultiplePreviousOwners = partitionsWithMultiplePreviousOwners;
            allRevokedPartitions = new HashSet<>();
            unfilledMembersWithUnderMinQuotaPartitions = new LinkedList<>();
            unfilledMembersWithExactlyMinQuotaPartitions = new LinkedList<>();

            int numberOfConsumers = consumerToOwnedPartitions.size();

            minQuota = (int) Math.floor(((double) totalPartitionsCount) / numberOfConsumers);
            maxQuota = (int) Math.ceil(((double) totalPartitionsCount) / numberOfConsumers);
            expectedNumMembersWithOverMinQuotaPartitions = totalPartitionsCount % numberOfConsumers;
            currentNumMembersWithOverMinQuotaPartitions = 0;

            // initialize the assignment map with an empty array of size maxQuota for all members
            assignment = new HashMap<>(consumerToOwnedPartitions.keySet().stream()
                    .collect(Collectors.toMap(c -> c, c -> new ArrayList<>(maxQuota))));
            assignedPartitions = new ArrayList<>();
        }

        @Override
        Map<String, List<TopicPartition>> build() {
            if (log.isDebugEnabled()) {
                log.debug("Performing constrained assign with partitionsPerTopic: {}, currentAssignment: {}, rackInfo {}.",
                        partitionsPerTopic, currentAssignment, rackInfo);
            }

            assignOwnedPartitions();

            List<TopicPartition> unassignedPartitions = getUnassignedPartitions(assignedPartitions);

            if (log.isDebugEnabled()) {
                log.debug("After reassigning previously owned partitions, unfilled members: {}, unassigned partitions: {}, " +
                        "current assignment: {}", unfilledMembersWithUnderMinQuotaPartitions, unassignedPartitions, assignment);
            }

            Collections.sort(unfilledMembersWithUnderMinQuotaPartitions);
            Collections.sort(unfilledMembersWithExactlyMinQuotaPartitions);
            unassignedPartitions = rackInfo.sortPartitionsByRackConsumers(unassignedPartitions);

            assignRackAwareRoundRobin(unassignedPartitions);
            assignRoundRobin(unassignedPartitions);
            verifyUnfilledMembers();

            log.info("Final assignment of partitions to consumers: \n{}", assignment);

            return assignment;
        }

        // Reassign previously owned partitions, up to the expected number of partitions per consumer
        private void assignOwnedPartitions() {

            for (Map.Entry<String, List<TopicPartition>> consumerEntry : currentAssignment.entrySet()) {
                String consumer = consumerEntry.getKey();
                List<TopicPartition> ownedPartitions = consumerEntry.getValue().stream()
                        .filter(tp -> !rackInfo.racksMismatch(consumer, tp))
                        .sorted(Comparator.comparing(TopicPartition::partition).thenComparing(TopicPartition::topic))
                        .collect(Collectors.toList());

                List<TopicPartition> consumerAssignment = assignment.get(consumer);

                for (TopicPartition doublyClaimedPartition : partitionsWithMultiplePreviousOwners) {
                    if (ownedPartitions.contains(doublyClaimedPartition)) {
                        log.error("Found partition {} still claimed as owned by consumer {}, despite being claimed by multiple "
                                        + "consumers already in the same generation. Removing it from the ownedPartitions",
                                doublyClaimedPartition, consumer);
                        ownedPartitions.remove(doublyClaimedPartition);
                    }
                }

                if (ownedPartitions.size() < minQuota) {
                    // the expected assignment size is more than this consumer has now, so keep all the owned partitions
                    // and put this member into the unfilled member list
                    if (ownedPartitions.size() > 0) {
                        consumerAssignment.addAll(ownedPartitions);
                        assignedPartitions.addAll(ownedPartitions);
                    }
                    unfilledMembersWithUnderMinQuotaPartitions.add(consumer);
                } else if (ownedPartitions.size() >= maxQuota && currentNumMembersWithOverMinQuotaPartitions < expectedNumMembersWithOverMinQuotaPartitions) {
                    // consumer owned the "maxQuota" of partitions or more, and we're still under the number of expected members
                    // with more than the minQuota partitions, so keep "maxQuota" of the owned partitions, and revoke the rest of the partitions
                    currentNumMembersWithOverMinQuotaPartitions++;
                    if (currentNumMembersWithOverMinQuotaPartitions == expectedNumMembersWithOverMinQuotaPartitions) {
                        unfilledMembersWithExactlyMinQuotaPartitions.clear();
                    }
                    List<TopicPartition> maxQuotaPartitions = ownedPartitions.subList(0, maxQuota);
                    consumerAssignment.addAll(maxQuotaPartitions);
                    assignedPartitions.addAll(maxQuotaPartitions);
                    allRevokedPartitions.addAll(ownedPartitions.subList(maxQuota, ownedPartitions.size()));
                } else {
                    // consumer owned at least "minQuota" of partitions
                    // so keep "minQuota" of the owned partitions, and revoke the rest of the partitions
                    List<TopicPartition> minQuotaPartitions = ownedPartitions.subList(0, minQuota);
                    consumerAssignment.addAll(minQuotaPartitions);
                    assignedPartitions.addAll(minQuotaPartitions);
                    allRevokedPartitions.addAll(ownedPartitions.subList(minQuota, ownedPartitions.size()));
                    // this consumer is potential maxQuota candidate since we're still under the number of expected members
                    // with more than the minQuota partitions. Note, if the number of expected members with more than
                    // the minQuota partitions is 0, it means minQuota == maxQuota, and there are no potentially unfilled
                    if (currentNumMembersWithOverMinQuotaPartitions < expectedNumMembersWithOverMinQuotaPartitions) {
                        unfilledMembersWithExactlyMinQuotaPartitions.add(consumer);
                    }
                }
            }
        }

        // Round-Robin filling within racks for remaining members up to the expected numbers of maxQuota,
        // otherwise, to minQuota
        private void assignRackAwareRoundRobin(List<TopicPartition> unassignedPartitions) {
            if (rackInfo.consumerRacks.isEmpty())
                return;
            int nextUnfilledConsumerIndex = 0;
            Iterator<TopicPartition> unassignedIter = unassignedPartitions.iterator();
            while (unassignedIter.hasNext()) {
                TopicPartition unassignedPartition = unassignedIter.next();
                String consumer = null;
                int nextIndex = rackInfo.nextRackConsumer(unassignedPartition, unfilledMembersWithUnderMinQuotaPartitions, nextUnfilledConsumerIndex);
                if (nextIndex >= 0) {
                    consumer = unfilledMembersWithUnderMinQuotaPartitions.get(nextIndex);
                    int assignmentCount = assignment.get(consumer).size() + 1;
                    if (assignmentCount >= minQuota) {
                        unfilledMembersWithUnderMinQuotaPartitions.remove(consumer);
                        if (assignmentCount < maxQuota)
                            unfilledMembersWithExactlyMinQuotaPartitions.add(consumer);
                    } else {
                        nextIndex++;
                    }
                    nextUnfilledConsumerIndex = unfilledMembersWithUnderMinQuotaPartitions.isEmpty() ? 0 : nextIndex % unfilledMembersWithUnderMinQuotaPartitions.size();
                } else if (!unfilledMembersWithExactlyMinQuotaPartitions.isEmpty()) {
                    int firstIndex = rackInfo.nextRackConsumer(unassignedPartition, unfilledMembersWithExactlyMinQuotaPartitions, 0);
                    if (firstIndex >= 0) {
                        consumer = unfilledMembersWithExactlyMinQuotaPartitions.get(firstIndex);
                        if (assignment.get(consumer).size() + 1 == maxQuota)
                            unfilledMembersWithExactlyMinQuotaPartitions.remove(firstIndex);
                    }
                }

                if (consumer != null) {
                    assignNewPartition(unassignedPartition, consumer);
                    unassignedIter.remove();
                }
            }
        }

        private void assignRoundRobin(List<TopicPartition> unassignedPartitions) {

            Iterator<String> unfilledConsumerIter = unfilledMembersWithUnderMinQuotaPartitions.iterator();
            // Round-Robin filling remaining members up to the expected numbers of maxQuota, otherwise, to minQuota
            for (TopicPartition unassignedPartition : unassignedPartitions) {
                String consumer;
                if (unfilledConsumerIter.hasNext()) {
                    consumer = unfilledConsumerIter.next();
                } else {
                    if (unfilledMembersWithUnderMinQuotaPartitions.isEmpty() && unfilledMembersWithExactlyMinQuotaPartitions.isEmpty()) {
                        // Should not enter here since we have calculated the exact number to assign to each consumer.
                        // This indicates issues in the assignment algorithm
                        int currentPartitionIndex = unassignedPartitions.indexOf(unassignedPartition);
                        log.error("No more unfilled consumers to be assigned. The remaining unassigned partitions are: {}",
                                unassignedPartitions.subList(currentPartitionIndex, unassignedPartitions.size()));
                        throw new IllegalStateException("No more unfilled consumers to be assigned.");
                    } else if (unfilledMembersWithUnderMinQuotaPartitions.isEmpty()) {
                        consumer = unfilledMembersWithExactlyMinQuotaPartitions.poll();
                    } else {
                        unfilledConsumerIter = unfilledMembersWithUnderMinQuotaPartitions.iterator();
                        consumer = unfilledConsumerIter.next();
                    }
                }

                int currentAssignedCount = assignNewPartition(unassignedPartition, consumer);

                if (currentAssignedCount == minQuota) {
                    unfilledConsumerIter.remove();
                    unfilledMembersWithExactlyMinQuotaPartitions.add(consumer);
                } else if (currentAssignedCount == maxQuota) {
                    currentNumMembersWithOverMinQuotaPartitions++;
                    if (currentNumMembersWithOverMinQuotaPartitions == expectedNumMembersWithOverMinQuotaPartitions) {
                        // We only start to iterate over the "potentially unfilled" members at minQuota after we've filled
                        // all members up to at least minQuota, so once the last minQuota member reaches maxQuota, we
                        // should be done. But in case of some algorithmic error, just log a warning and continue to
                        // assign any remaining partitions within the assignment constraints
                        if (unassignedPartitions.indexOf(unassignedPartition) != unassignedPartitions.size() - 1) {
                            log.error("Filled the last member up to maxQuota but still had partitions remaining to assign, "
                                    + "will continue but this indicates a bug in the assignment.");
                        }
                    }
                }
            }
        }

        private int assignNewPartition(TopicPartition unassignedPartition, String consumer) {
            List<TopicPartition> consumerAssignment = assignment.get(consumer);
            consumerAssignment.add(unassignedPartition);

            // We already assigned all possible ownedPartitions, so we know this must be newly assigned to this consumer
            // or else the partition was actually claimed by multiple previous owners and had to be invalidated from all
            // members claimed ownedPartitions
            if (allRevokedPartitions.contains(unassignedPartition) || partitionsWithMultiplePreviousOwners.contains(unassignedPartition))
                partitionsTransferringOwnership.put(unassignedPartition, consumer);

            return consumerAssignment.size();
        }

        private void verifyUnfilledMembers() {

            if (!unfilledMembersWithUnderMinQuotaPartitions.isEmpty()) {
                // we expected all the remaining unfilled members have minQuota partitions and we're already at the expected number
                // of members with more than the minQuota partitions. Otherwise, there must be error here.
                if (currentNumMembersWithOverMinQuotaPartitions != expectedNumMembersWithOverMinQuotaPartitions) {
                    log.error("Current number of members with more than the minQuota partitions: {}, is less than the expected number " +
                                    "of members with more than the minQuota partitions: {}, and no more partitions to be assigned to the remaining unfilled consumers: {}",
                            currentNumMembersWithOverMinQuotaPartitions, expectedNumMembersWithOverMinQuotaPartitions, unfilledMembersWithUnderMinQuotaPartitions);
                    throw new IllegalStateException("We haven't reached the expected number of members with " +
                            "more than the minQuota partitions, but no more partitions to be assigned");
                } else {
                    for (String unfilledMember : unfilledMembersWithUnderMinQuotaPartitions) {
                        int assignedPartitionsCount = assignment.get(unfilledMember).size();
                        if (assignedPartitionsCount != minQuota) {
                            log.error("Consumer: [{}] should have {} partitions, but got {} partitions, and no more partitions " +
                                    "to be assigned. The remaining unfilled consumers are: {}", unfilledMember, minQuota, assignedPartitionsCount, unfilledMembersWithUnderMinQuotaPartitions);
                            throw new IllegalStateException(String.format("Consumer: [%s] doesn't reach minQuota partitions, " +
                                    "and no more partitions to be assigned", unfilledMember));
                        } else {
                            log.trace("skip over this unfilled member: [{}] because we've reached the expected number of " +
                                    "members with more than the minQuota partitions, and this member already has minQuota partitions", unfilledMember);
                        }
                    }
                }
            }
        }

        /**
         * get the unassigned partition list by computing the difference set of all sorted partitions
         * and sortedAssignedPartitions. If no assigned partitions, we'll just return all sorted topic partitions.
         *
         * To compute the difference set, we use two pointers technique here:
         *
         * We loop through the all sorted topics, and then iterate all partitions the topic has,
         * compared with the ith element in sortedAssignedPartitions(i starts from 0):
         *   - if not equal to the ith element, add to unassignedPartitions
         *   - if equal to the ith element, get next element from sortedAssignedPartitions
         *
         * @param sortedAssignedPartitions  sorted partitions, all are included in the sortedPartitions
         * @return                          the partitions not yet assigned to any consumers
         */
        private List<TopicPartition> getUnassignedPartitions(List<TopicPartition> sortedAssignedPartitions) {
            List<String> sortedAllTopics = new ArrayList<>(partitionsPerTopic.keySet());
            // sort all topics first, then we can have sorted all topic partitions by adding partitions starting from 0
            Collections.sort(sortedAllTopics);

            if (sortedAssignedPartitions.isEmpty()) {
                // no assigned partitions means all partitions are unassigned partitions
                return getAllTopicPartitions(sortedAllTopics);
            }

            List<TopicPartition> unassignedPartitions = new ArrayList<>(totalPartitionsCount - sortedAssignedPartitions.size());

            Collections.sort(sortedAssignedPartitions, Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));

            boolean shouldAddDirectly = false;
            Iterator<TopicPartition> sortedAssignedPartitionsIter = sortedAssignedPartitions.iterator();
            TopicPartition nextAssignedPartition = sortedAssignedPartitionsIter.next();

            for (String topic : sortedAllTopics) {
                int partitionCount = partitionsPerTopic.get(topic).size();
                for (int i = 0; i < partitionCount; i++) {
                    if (shouldAddDirectly || !(nextAssignedPartition.topic().equals(topic) && nextAssignedPartition.partition() == i)) {
                        unassignedPartitions.add(new TopicPartition(topic, i));
                    } else {
                        // this partition is in assignedPartitions, don't add to unassignedPartitions, just get next assigned partition
                        if (sortedAssignedPartitionsIter.hasNext()) {
                            nextAssignedPartition = sortedAssignedPartitionsIter.next();
                        } else {
                            // add the remaining directly since there is no more sortedAssignedPartitions
                            shouldAddDirectly = true;
                        }
                    }
                }
            }

            return unassignedPartitions;
        }
    }

    /**
     * This general assignment algorithm guarantees the assignment that is as balanced as possible.
     * This method includes the following steps:
     *
     * 1. Preserving all the existing partition assignments. If rack-aware algorithm is used, only assignments
     *    within racks are preserved.
     * 2. Removing all the partition assignments that have become invalid due to the change that triggers the reassignment.
     *    Partition assignments with mismatched racks are also removed.
     * 3. Assigning the unassigned partitions in a way that balances out the overall assignments of partitions to consumers.
     *    while preserving rack-alignment. This step is used only for rack-aware assignment.
     * 4. Assigning the remaining unassigned partitions in a way that balances out the overall assignments of partitions to consumers.
     *    For rack-aware algorithm, these are partitions that could not be aligned on racks within the balancing constraints.
     * 5. Further balancing out the resulting assignment by finding the partitions that can be reassigned
     *    to another consumer towards an overall more balanced assignment. For rack-aware algorithm, attempt
     *    to retain rack alignment if possible.
     *
     */
    private class GeneralAssignmentBuilder extends AbstractAssignmentBuilder {
        private final Map<String, Subscription> subscriptions;

        // a mapping of all topics to all consumers that can be assigned to them
        private final Map<String, List<String>> topic2AllPotentialConsumers;
        // a mapping of all consumers to all potential topics that can be assigned to them
        private final Map<String, List<String>> consumer2AllPotentialTopics;
        // a mapping of partition to current consumer
        private final Map<TopicPartition, String> currentPartitionConsumer;
        private final List<TopicPartition> sortedAllPartitions;
        // an ascending sorted set of consumers based on how many topic partitions are already assigned to them
        private final TreeSet<String> sortedCurrentSubscriptions;
        private boolean revocationRequired;

        /**
         * Constructs a general assignment builder.
         *
         * @param partitionsPerTopic         The partitions for each subscribed topic.
         * @param subscriptions              Map from the member id to their respective topic subscription
         * @param currentAssignment          Each consumer's previously owned and still-subscribed partitions
         * @param rackInfo                   Rack information for consumers and partitions
         */
        GeneralAssignmentBuilder(Map<String, List<PartitionInfo>> partitionsPerTopic,
                                 RackInfo rackInfo,
                                 Map<String, List<TopicPartition>> currentAssignment,
                                 Map<String, Subscription> subscriptions) {
            super(partitionsPerTopic, rackInfo, currentAssignment);
            this.subscriptions = subscriptions;

            topic2AllPotentialConsumers = new HashMap<>(partitionsPerTopic.keySet().size());
            consumer2AllPotentialTopics = new HashMap<>(subscriptions.keySet().size());

            // initialize topic2AllPotentialConsumers and consumer2AllPotentialTopics
            partitionsPerTopic.keySet().forEach(
                    topicName -> topic2AllPotentialConsumers.put(topicName, new ArrayList<>()));

            subscriptions.forEach((consumerId, subscription) -> {
                List<String> subscribedTopics = new ArrayList<>(subscription.topics().size());
                consumer2AllPotentialTopics.put(consumerId, subscribedTopics);
                subscription.topics().stream().filter(topic -> partitionsPerTopic.get(topic) != null).forEach(topic -> {
                    subscribedTopics.add(topic);
                    topic2AllPotentialConsumers.get(topic).add(consumerId);
                });

                // add this consumer to currentAssignment (with an empty topic partition assignment) if it does not already exist
                if (!currentAssignment.containsKey(consumerId))
                    currentAssignment.put(consumerId, new ArrayList<>());
            });

            currentPartitionConsumer = new HashMap<>();
            for (Map.Entry<String, List<TopicPartition>> entry: currentAssignment.entrySet())
                for (TopicPartition topicPartition: entry.getValue())
                    currentPartitionConsumer.put(topicPartition, entry.getKey());

            List<String> sortedAllTopics = new ArrayList<>(topic2AllPotentialConsumers.keySet());
            Collections.sort(sortedAllTopics, new TopicComparator(topic2AllPotentialConsumers));
            sortedAllPartitions = getAllTopicPartitions(sortedAllTopics);

            sortedCurrentSubscriptions = new TreeSet<>(new SubscriptionComparator(currentAssignment));
        }

        @Override
        Map<String, List<TopicPartition>> build() {
            if (log.isDebugEnabled()) {
                log.debug("performing general assign. partitionsPerTopic: {}, subscriptions: {}, currentAssignment: {}, rackInfo: {}",
                        partitionsPerTopic, subscriptions, currentAssignment, rackInfo);
            }

            Map<TopicPartition, ConsumerGenerationPair> prevAssignment = new HashMap<>();
            partitionMovements = new PartitionMovements();
            prepopulateCurrentAssignments(prevAssignment);

            // the partitions already assigned in current assignment
            List<TopicPartition> assignedPartitions = assignOwnedPartitions();

            // all partitions that still need to be assigned
            List<TopicPartition> unassignedPartitions = getUnassignedPartitions(assignedPartitions);

            if (log.isDebugEnabled()) {
                log.debug("unassigned Partitions: {}", unassignedPartitions);
            }

            // at this point we have preserved all valid topic partition to consumer assignments and removed
            // all invalid topic partitions and invalid consumers. Now we need to assign unassignedPartitions
            // to consumers so that the topic partition assignments are as balanced as possible.
            sortedCurrentSubscriptions.addAll(currentAssignment.keySet());

            balance(prevAssignment, unassignedPartitions);

            log.info("Final assignment of partitions to consumers: \n{}", currentAssignment);

            return currentAssignment;
        }

        private List<TopicPartition> assignOwnedPartitions() {
            List<TopicPartition> assignedPartitions = new ArrayList<>();
            for (Iterator<Entry<String, List<TopicPartition>>> it = currentAssignment.entrySet().iterator(); it.hasNext();) {
                Map.Entry<String, List<TopicPartition>> entry = it.next();
                String consumer = entry.getKey();
                Subscription consumerSubscription = subscriptions.get(consumer);
                if (consumerSubscription == null) {
                    // if a consumer that existed before (and had some partition assignments) is now removed, remove it from currentAssignment
                    for (TopicPartition topicPartition: entry.getValue())
                        currentPartitionConsumer.remove(topicPartition);
                    it.remove();
                } else {
                    // otherwise (the consumer still exists)
                    for (Iterator<TopicPartition> partitionIter = entry.getValue().iterator(); partitionIter.hasNext();) {
                        TopicPartition partition = partitionIter.next();
                        if (!topic2AllPotentialConsumers.containsKey(partition.topic())) {
                            // if this topic partition of this consumer no longer exists, remove it from currentAssignment of the consumer
                            partitionIter.remove();
                            currentPartitionConsumer.remove(partition);
                        } else if (!consumerSubscription.topics().contains(partition.topic()) || rackInfo.racksMismatch(consumer, partition)) {
                            // if the consumer is no longer subscribed to its topic or if racks don't match for rack-aware assignment,
                            // remove it from currentAssignment of the consumer
                            partitionIter.remove();
                            revocationRequired = true;
                        } else {
                            // otherwise, remove the topic partition from those that need to be assigned only if
                            // its current consumer is still subscribed to its topic (because it is already assigned
                            // and we would want to preserve that assignment as much as possible)
                            assignedPartitions.add(partition);
                        }
                    }
                }
            }
            return assignedPartitions;
        }

        /**
         * get the unassigned partition list by computing the difference set of the sortedPartitions(all partitions)
         * and sortedAssignedPartitions. If no assigned partitions, we'll just return all sorted topic partitions.
         *
         * We loop the sortedPartition, and compare the ith element in sortedAssignedPartitions(i start from 0):
         *   - if not equal to the ith element, add to unassignedPartitions
         *   - if equal to the ith element, get next element from sortedAssignedPartitions
         *
         * @param sortedAssignedPartitions:     sorted partitions, all are included in the sortedPartitions
         * @return                              partitions that aren't assigned to any current consumer
         */
        private List<TopicPartition> getUnassignedPartitions(List<TopicPartition> sortedAssignedPartitions) {
            if (sortedAssignedPartitions.isEmpty()) {
                return sortedAllPartitions;
            }

            List<TopicPartition> unassignedPartitions = new ArrayList<>();

            Collections.sort(sortedAssignedPartitions, new PartitionComparator(topic2AllPotentialConsumers));

            boolean shouldAddDirectly = false;
            Iterator<TopicPartition> sortedAssignedPartitionsIter = sortedAssignedPartitions.iterator();
            TopicPartition nextAssignedPartition = sortedAssignedPartitionsIter.next();

            for (TopicPartition topicPartition : sortedAllPartitions) {
                if (shouldAddDirectly || !nextAssignedPartition.equals(topicPartition)) {
                    unassignedPartitions.add(topicPartition);
                } else {
                    // this partition is in assignedPartitions, don't add to unassignedPartitions, just get next assigned partition
                    if (sortedAssignedPartitionsIter.hasNext()) {
                        nextAssignedPartition = sortedAssignedPartitionsIter.next();
                    } else {
                        // add the remaining directly since there is no more sortedAssignedPartitions
                        shouldAddDirectly = true;
                    }
                }
            }
            return unassignedPartitions;
        }

        /**
         * update the prevAssignment with the partitions, consumer and generation in parameters
         *
         * @param partitions:       The partitions to be updated the prevAssignment
         * @param consumer:         The consumer Id
         * @param prevAssignment:   The assignment contains the assignment with the 2nd largest generation
         * @param generation:       The generation of this assignment (partitions)
         */
        private void updatePrevAssignment(Map<TopicPartition, ConsumerGenerationPair> prevAssignment,
                                          List<TopicPartition> partitions,
                                          String consumer,
                                          int generation) {
            for (TopicPartition partition: partitions) {
                if (prevAssignment.containsKey(partition)) {
                    // only keep the latest previous assignment
                    if (generation > prevAssignment.get(partition).generation) {
                        prevAssignment.put(partition, new ConsumerGenerationPair(consumer, generation));
                    }
                } else {
                    prevAssignment.put(partition, new ConsumerGenerationPair(consumer, generation));
                }
            }
        }

        /**
         * filling in the prevAssignment from the subscriptions.
         *
         * @param prevAssignment: The assignment contains the assignment with the 2nd largest generation
         */
        private void prepopulateCurrentAssignments(Map<TopicPartition, ConsumerGenerationPair> prevAssignment) {
            // we need to process subscriptions' user data with each consumer's reported generation in mind
            // higher generations overwrite lower generations in case of a conflict
            // note that a conflict could exist only if user data is for different generations

            for (Map.Entry<String, Subscription> subscriptionEntry: subscriptions.entrySet()) {
                String consumer = subscriptionEntry.getKey();
                Subscription subscription = subscriptionEntry.getValue();
                if (subscription.userData() != null) {
                    // since this is our 2nd time to deserialize memberData, rewind userData is necessary
                    subscription.userData().rewind();
                }

                MemberData memberData = memberData(subscription);

                // we already have the maxGeneration info, so just compare the current generation of memberData, and put into prevAssignment
                if (memberData.generation.isPresent() && memberData.generation.get() < maxGeneration) {
                    // if the current member's generation is lower than maxGeneration, put into prevAssignment if needed
                    updatePrevAssignment(prevAssignment, memberData.partitions, consumer, memberData.generation.get());
                } else if (!memberData.generation.isPresent() && maxGeneration > DEFAULT_GENERATION) {
                    // if maxGeneration is larger than DEFAULT_GENERATION
                    // put all (no generation) partitions as DEFAULT_GENERATION into prevAssignment if needed
                    updatePrevAssignment(prevAssignment, memberData.partitions, consumer, DEFAULT_GENERATION);
                }
            }
        }

        /**
         * determine if the current assignment is a balanced one
         *
         * @return true if the given assignment is balanced; false otherwise
         */
        private boolean isBalanced() {
            int min = currentAssignment.get(sortedCurrentSubscriptions.first()).size();
            int max = currentAssignment.get(sortedCurrentSubscriptions.last()).size();
            if (min >= max - 1)
                // if minimum and maximum numbers of partitions assigned to consumers differ by at most one return true
                return true;

            // create a mapping from partitions to the consumer assigned to them
            final Map<TopicPartition, String> allPartitions = new HashMap<>();
            Set<Entry<String, List<TopicPartition>>> assignments = currentAssignment.entrySet();
            for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
                List<TopicPartition> topicPartitions = entry.getValue();
                for (TopicPartition topicPartition: topicPartitions) {
                    if (allPartitions.containsKey(topicPartition))
                        log.error("{} is assigned to more than one consumer.", topicPartition);
                    allPartitions.put(topicPartition, entry.getKey());
                }
            }

            // for each consumer that does not have all the topic partitions it can get make sure none of the topic partitions it
            // could but did not get cannot be moved to it (because that would break the balance)
            for (String consumer: sortedCurrentSubscriptions) {
                List<TopicPartition> consumerPartitions = currentAssignment.get(consumer);
                int consumerPartitionCount = consumerPartitions.size();

                // skip if this consumer already has all the topic partitions it can get
                List<String> allSubscribedTopics = consumer2AllPotentialTopics.get(consumer);
                int maxAssignmentSize = getMaxAssignmentSize(allSubscribedTopics);

                if (consumerPartitionCount == maxAssignmentSize)
                    continue;

                // otherwise make sure it cannot get any more
                for (String topic: allSubscribedTopics) {
                    int partitionCount = partitionsPerTopic.get(topic).size();
                    for (int i = 0; i < partitionCount; i++) {
                        TopicPartition topicPartition = new TopicPartition(topic, i);
                        if (!currentAssignment.get(consumer).contains(topicPartition)) {
                            String otherConsumer = allPartitions.get(topicPartition);
                            int otherConsumerPartitionCount = currentAssignment.get(otherConsumer).size();
                            if (consumerPartitionCount + 1 < otherConsumerPartitionCount) {
                                log.debug("{} can be moved from consumer {} to consumer {} for a more balanced assignment.",
                                        topicPartition, otherConsumer, consumer);
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        }

        /**
         * get the maximum assigned partition size of the {@code allSubscribedTopics}
         *
         * @param allSubscribedTopics           the subscribed topics of a consumer
         * @return                              maximum assigned partition size
         */
        private int getMaxAssignmentSize(List<String> allSubscribedTopics) {
            int maxAssignmentSize;
            if (allSubscribedTopics.size() == partitionsPerTopic.size()) {
                maxAssignmentSize = totalPartitionsCount;
            } else {
                maxAssignmentSize = allSubscribedTopics.stream().map(partitionsPerTopic::get).map(List::size).reduce(0, Integer::sum);
            }
            return maxAssignmentSize;
        }

        /**
         * @return the balance score of the given assignment, as the sum of assigned partitions size difference of all consumer pairs.
         * A perfectly balanced assignment (with all consumers getting the same number of partitions) has a balance score of 0.
         * Lower balance score indicates a more balanced assignment.
         */
        private int getBalanceScore(Map<String, List<TopicPartition>> assignment) {
            int score = 0;

            Map<String, Integer> consumer2AssignmentSize = new HashMap<>();
            for (Entry<String, List<TopicPartition>> entry: assignment.entrySet())
                consumer2AssignmentSize.put(entry.getKey(), entry.getValue().size());

            Iterator<Entry<String, Integer>> it = consumer2AssignmentSize.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, Integer> entry = it.next();
                int consumerAssignmentSize = entry.getValue();
                it.remove();
                for (Entry<String, Integer> otherEntry: consumer2AssignmentSize.entrySet())
                    score += Math.abs(consumerAssignmentSize - otherEntry.getValue());
            }

            return score;
        }

        /**
         * The assignment should improve the overall balance of the partition assignments to consumers.
         */
        private boolean maybeAssignPartition(TopicPartition partition, RackInfo rackInfo) {
            for (String consumer: sortedCurrentSubscriptions) {
                if (consumer2AllPotentialTopics.get(consumer).contains(partition.topic()) && (rackInfo == null || !rackInfo.racksMismatch(consumer, partition))) {
                    sortedCurrentSubscriptions.remove(consumer);
                    currentAssignment.get(consumer).add(partition);
                    currentPartitionConsumer.put(partition, consumer);
                    sortedCurrentSubscriptions.add(consumer);
                    return true;
                }
            }
            return false;
        }

        /**
         * attempt to assign all unassigned partitions
         *
         * @param unassignedPartitions partitions that are still unassigned
         * @param rackInfo             rack information used to match racks. If null, no rack-matching is performed
         * @param removeAssigned       flag that indicates if assigned partitions should be removed from `unassignedPartitions`
         */
        private void maybeAssign(List<TopicPartition> unassignedPartitions, RackInfo rackInfo, boolean removeAssigned) {
            // assign all unassigned partitions
            for (Iterator<TopicPartition> iter = unassignedPartitions.iterator(); iter.hasNext();) {
                TopicPartition partition = iter.next();
                // skip if there is no potential consumer for the topic
                if (topic2AllPotentialConsumers.get(partition.topic()).isEmpty())
                    continue;

                if (maybeAssignPartition(partition, rackInfo) && removeAssigned)
                    iter.remove();
            }
        }

        private boolean canTopicParticipateInReassignment(String topic) {
            // if a topic has two or more potential consumers it is subject to reassignment.
            return topic2AllPotentialConsumers.get(topic).size() >= 2;
        }

        private boolean canConsumerParticipateInReassignment(String consumer) {
            List<TopicPartition> currentPartitions = currentAssignment.get(consumer);
            int currentAssignmentSize = currentPartitions.size();
            List<String> allSubscribedTopics = consumer2AllPotentialTopics.get(consumer);
            int maxAssignmentSize = getMaxAssignmentSize(allSubscribedTopics);

            if (currentAssignmentSize > maxAssignmentSize)
                log.error("The consumer {} is assigned more partitions than the maximum possible.", consumer);

            if (currentAssignmentSize < maxAssignmentSize)
                // if a consumer is not assigned all its potential partitions it is subject to reassignment
                return true;

            for (TopicPartition partition: currentPartitions)
                // if any of the partitions assigned to a consumer is subject to reassignment the consumer itself
                // is subject to reassignment
                if (canTopicParticipateInReassignment(partition.topic()))
                    return true;

            return false;
        }

        /**
         * Balance the current assignment using the data structures created in the assignPartitions(...) method above.
         */
        private void balance(Map<TopicPartition, ConsumerGenerationPair> prevAssignment,
                             List<TopicPartition> unassignedPartitions) {
            boolean initializing = currentAssignment.get(sortedCurrentSubscriptions.last()).isEmpty();

            // First assign with rack matching and then assign any remaining without rack matching
            List<TopicPartition> partitionsToAssign = unassignedPartitions;
            if (!rackInfo.consumerRacks.isEmpty()) {
                partitionsToAssign = new LinkedList<>(unassignedPartitions);
                maybeAssign(partitionsToAssign, rackInfo, true);
            }
            maybeAssign(partitionsToAssign, null, false);

            // narrow down the reassignment scope to only those partitions that can actually be reassigned
            Set<TopicPartition> fixedPartitions = new HashSet<>();
            for (String topic: topic2AllPotentialConsumers.keySet())
                if (!canTopicParticipateInReassignment(topic)) {
                    for (int i = 0; i < partitionsPerTopic.get(topic).size(); i++) {
                        fixedPartitions.add(new TopicPartition(topic, i));
                    }
                }
            sortedAllPartitions.removeAll(fixedPartitions);
            unassignedPartitions.removeAll(fixedPartitions);

            // narrow down the reassignment scope to only those consumers that are subject to reassignment
            Map<String, List<TopicPartition>> fixedAssignments = new HashMap<>();
            for (String consumer: consumer2AllPotentialTopics.keySet())
                if (!canConsumerParticipateInReassignment(consumer)) {
                    sortedCurrentSubscriptions.remove(consumer);
                    fixedAssignments.put(consumer, currentAssignment.remove(consumer));
                }

            // create a deep copy of the current assignment so we can revert to it if we do not get a more balanced assignment later
            Map<String, List<TopicPartition>> preBalanceAssignment = deepCopy(currentAssignment);
            Map<TopicPartition, String> preBalancePartitionConsumers = new HashMap<>(currentPartitionConsumer);

            // if we don't already need to revoke something due to subscription changes, first try to balance by only moving newly added partitions
            if (!revocationRequired) {
                performReassignments(unassignedPartitions, prevAssignment);
            }

            boolean reassignmentPerformed = performReassignments(sortedAllPartitions, prevAssignment);

            // if we are not preserving existing assignments and we have made changes to the current assignment
            // make sure we are getting a more balanced assignment; otherwise, revert to previous assignment
            if (!initializing && reassignmentPerformed && getBalanceScore(currentAssignment) >= getBalanceScore(preBalanceAssignment)) {
                deepCopy(preBalanceAssignment, currentAssignment);
                currentPartitionConsumer.clear();
                currentPartitionConsumer.putAll(preBalancePartitionConsumers);
            }

            // add the fixed assignments (those that could not change) back
            for (Entry<String, List<TopicPartition>> entry: fixedAssignments.entrySet()) {
                String consumer = entry.getKey();
                currentAssignment.put(consumer, entry.getValue());
                sortedCurrentSubscriptions.add(consumer);
            }

            fixedAssignments.clear();
        }

        private boolean performReassignments(List<TopicPartition> reassignablePartitions,
                                             Map<TopicPartition, ConsumerGenerationPair> prevAssignment) {
            boolean reassignmentPerformed = false;
            boolean modified;

            // repeat reassignment until no partition can be moved to improve the balance
            do {
                modified = false;
                // reassign all reassignable partitions (starting from the partition with least potential consumers and if needed)
                // until the full list is processed or a balance is achieved
                Iterator<TopicPartition> partitionIterator = reassignablePartitions.iterator();
                while (partitionIterator.hasNext() && !isBalanced()) {
                    TopicPartition partition = partitionIterator.next();

                    // the partition must have at least two consumers
                    if (topic2AllPotentialConsumers.get(partition.topic()).size() <= 1)
                        log.error("Expected more than one potential consumer for partition '{}'", partition);

                    // the partition must have a current consumer
                    String consumer = currentPartitionConsumer.get(partition);
                    if (consumer == null)
                        log.error("Expected partition '{}' to be assigned to a consumer", partition);

                    if (prevAssignment.containsKey(partition) &&
                            currentAssignment.get(consumer).size() > currentAssignment.get(prevAssignment.get(partition).consumer).size() + 1) {
                        reassignPartition(partition, prevAssignment.get(partition).consumer);
                        reassignmentPerformed = true;
                        modified = true;
                        continue;
                    }

                    // check if a better-suited consumer exist for the partition; if so, reassign it
                    // Use consumer within rack if possible
                    String consumerRack = rackInfo.consumerRacks.get(consumer);
                    Set<String> partitionRacks = rackInfo.partitionRacks.get(partition);
                    boolean foundRackConsumer = false;
                    if (consumerRack != null && !partitionRacks.isEmpty() && partitionRacks.contains(consumerRack)) {
                        for (String otherConsumer : topic2AllPotentialConsumers.get(partition.topic())) {
                            String otherConsumerRack = rackInfo.consumerRacks.get(otherConsumer);
                            if (otherConsumerRack == null || !partitionRacks.contains(otherConsumerRack))
                                continue;
                            if (currentAssignment.get(consumer).size() > currentAssignment.get(otherConsumer).size() + 1) {
                                reassignPartition(partition);
                                reassignmentPerformed = true;
                                modified = true;
                                foundRackConsumer = true;
                                break;
                            }
                        }
                    }
                    if (!foundRackConsumer) {
                        for (String otherConsumer : topic2AllPotentialConsumers.get(partition.topic())) {
                            if (currentAssignment.get(consumer).size() > currentAssignment.get(otherConsumer).size() + 1) {
                                reassignPartition(partition);
                                reassignmentPerformed = true;
                                modified = true;
                                break;
                            }
                        }
                    }
                }
            } while (modified);

            return reassignmentPerformed;
        }

        private void reassignPartition(TopicPartition partition) {
            // find the new consumer
            String newConsumer = null;
            for (String anotherConsumer: sortedCurrentSubscriptions) {
                if (consumer2AllPotentialTopics.get(anotherConsumer).contains(partition.topic())) {
                    newConsumer = anotherConsumer;
                    break;
                }
            }

            assert newConsumer != null;

            reassignPartition(partition, newConsumer);
        }

        private void reassignPartition(TopicPartition partition, String newConsumer) {
            String consumer = currentPartitionConsumer.get(partition);
            // find the correct partition movement considering the stickiness requirement
            TopicPartition partitionToBeMoved = partitionMovements.getTheActualPartitionToBeMoved(partition, consumer, newConsumer);
            processPartitionMovement(partitionToBeMoved, newConsumer);
        }

        private void processPartitionMovement(TopicPartition partition, String newConsumer) {
            String oldConsumer = currentPartitionConsumer.get(partition);

            sortedCurrentSubscriptions.remove(oldConsumer);
            sortedCurrentSubscriptions.remove(newConsumer);

            partitionMovements.movePartition(partition, oldConsumer, newConsumer);

            currentAssignment.get(oldConsumer).remove(partition);
            currentAssignment.get(newConsumer).add(partition);
            currentPartitionConsumer.put(partition, newConsumer);
            sortedCurrentSubscriptions.add(newConsumer);
            sortedCurrentSubscriptions.add(oldConsumer);
        }

        private void deepCopy(Map<String, List<TopicPartition>> source, Map<String, List<TopicPartition>> dest) {
            dest.clear();
            for (Entry<String, List<TopicPartition>> entry: source.entrySet())
                dest.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }

        private Map<String, List<TopicPartition>> deepCopy(Map<String, List<TopicPartition>> assignment) {
            Map<String, List<TopicPartition>> copy = new HashMap<>();
            deepCopy(assignment, copy);
            return copy;
        }
    }
}
