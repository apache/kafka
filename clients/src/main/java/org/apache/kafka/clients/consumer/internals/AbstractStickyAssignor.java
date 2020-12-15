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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStickyAssignor extends AbstractPartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(AbstractStickyAssignor.class);

    public static final int DEFAULT_GENERATION = -1;

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
        public MemberData(List<TopicPartition> partitions, Optional<Integer> generation) {
            this.partitions = partitions;
            this.generation = generation;
        }
    }

    abstract protected MemberData memberData(Subscription subscription);

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> consumerToOwnedPartitions = new HashMap<>();
        if (allSubscriptionsEqual(partitionsPerTopic.keySet(), subscriptions, consumerToOwnedPartitions)) {
            log.debug("Detected that all consumers were subscribed to same set of topics, invoking the "
                          + "optimized assignment algorithm");
            partitionsTransferringOwnership = new HashMap<>();
            return constrainedAssign(partitionsPerTopic, consumerToOwnedPartitions);
        } else {
            log.debug("Detected that all not consumers were subscribed to same set of topics, falling back to the "
                          + "general case assignment algorithm");
            partitionsTransferringOwnership = null;
            return generalAssign(partitionsPerTopic, subscriptions);
        }
    }

    /**
     * Returns true iff all consumers have an identical subscription. Also fills out the passed in
     * {@code consumerToOwnedPartitions} with each consumer's previously owned and still-subscribed partitions
     */
    private boolean allSubscriptionsEqual(Set<String> allTopics,
                                          Map<String, Subscription> subscriptions,
                                          Map<String, List<TopicPartition>> consumerToOwnedPartitions) {
        Set<String> membersWithOldGeneration = new HashSet<>();
        Set<String> membersOfCurrentHighestGeneration = new HashSet<>();
        int maxGeneration = DEFAULT_GENERATION;

        Set<String> subscribedTopics = new HashSet<>();

        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            String consumer = subscriptionEntry.getKey();
            Subscription subscription = subscriptionEntry.getValue();

            // initialize the subscribed topics set if this is the first subscription
            if (subscribedTopics.isEmpty()) {
                subscribedTopics.addAll(subscription.topics());
            } else if (!(subscription.topics().size() == subscribedTopics.size()
                && subscribedTopics.containsAll(subscription.topics()))) {
                return false;
            }

            MemberData memberData = memberData(subscription);

            List<TopicPartition> ownedPartitions = new ArrayList<>();
            consumerToOwnedPartitions.put(consumer, ownedPartitions);

            // Only consider this consumer's owned partitions as valid if it is a member of the current highest
            // generation, or it's generation is not present but we have not seen any known generation so far
            if (memberData.generation.isPresent() && memberData.generation.get() >= maxGeneration
                || !memberData.generation.isPresent() && maxGeneration == DEFAULT_GENERATION) {

                // If the current member's generation is higher, all the previously owned partitions are invalid
                if (memberData.generation.isPresent() && memberData.generation.get() > maxGeneration) {
                    membersWithOldGeneration.addAll(membersOfCurrentHighestGeneration);
                    membersOfCurrentHighestGeneration.clear();
                    maxGeneration = memberData.generation.get();
                }

                membersOfCurrentHighestGeneration.add(consumer);
                for (final TopicPartition tp : memberData.partitions) {
                    // filter out any topics that no longer exist or aren't part of the current subscription
                    if (allTopics.contains(tp.topic())) {
                        ownedPartitions.add(tp);
                    }
                }
            }
        }

        for (String consumer : membersWithOldGeneration) {
            consumerToOwnedPartitions.get(consumer).clear();
        }
        return true;
    }


    /**
     * This constrainedAssign optimizes the assignment algorithm when all consumers were subscribed to same set of topics.
     * The method includes the following steps:
     *
     * 1. Reassign as many previously owned partitions as possible, up to the maxQuota
     * 2. Fill remaining members up to minQuota
     * 3. If we ran out of unassigned partitions before filling all consumers, we need to start stealing partitions
     *    from the over-full consumers at max capacity
     * 4. Otherwise we may have run out of unfilled consumers before assigning all partitions, in which case we
     *    should just distribute one partition each to all consumers at min capacity
     *
     * @param partitionsPerTopic          The number of partitions for each subscribed topic
     * @param consumerToOwnedPartitions   Each consumer's previously owned and still-subscribed partitions
     *
     * @return Map from each member to the list of partitions assigned to them.
     */
    private Map<String, List<TopicPartition>> constrainedAssign(Map<String, Integer> partitionsPerTopic,
                                                                Map<String, List<TopicPartition>> consumerToOwnedPartitions) {
        SortedSet<TopicPartition> unassignedPartitions = getTopicPartitions(partitionsPerTopic);

        Set<TopicPartition> allRevokedPartitions = new HashSet<>();

        // Each consumer should end up in exactly one of the below
        // the consumers not yet at capacity
        List<String> unfilledMembers = new LinkedList<>();
        // the members with exactly maxQuota partitions assigned
        Queue<String> maxCapacityMembers = new LinkedList<>();
        // the members with exactly minQuota partitions assigned
        Queue<String> minCapacityMembers = new LinkedList<>();

        int numberOfConsumers = consumerToOwnedPartitions.size();
        int minQuota = (int) Math.floor(((double) unassignedPartitions.size()) / numberOfConsumers);
        int maxQuota = (int) Math.ceil(((double) unassignedPartitions.size()) / numberOfConsumers);

        // initialize the assignment map with an empty array of size minQuota for all members
        Map<String, List<TopicPartition>> assignment = new HashMap<>(
            consumerToOwnedPartitions.keySet().stream().collect(Collectors.toMap(c -> c, c -> new ArrayList<>(minQuota))));

        // Reassign as many previously owned partitions as possible
        for (Map.Entry<String, List<TopicPartition>> consumerEntry : consumerToOwnedPartitions.entrySet()) {
            String consumer = consumerEntry.getKey();
            List<TopicPartition> ownedPartitions = consumerEntry.getValue();

            List<TopicPartition> consumerAssignment = assignment.get(consumer);
            int i = 0;
            // assign the first N partitions up to the max quota, and mark the remaining as being revoked
            for (TopicPartition tp : ownedPartitions) {
                if (i < maxQuota) {
                    consumerAssignment.add(tp);
                    unassignedPartitions.remove(tp);
                } else {
                    allRevokedPartitions.add(tp);
                }
                ++i;
            }

            if (ownedPartitions.size() < minQuota) {
                unfilledMembers.add(consumer);
            } else {
                // It's possible for a consumer to be at both min and max capacity if minQuota == maxQuota
                if (consumerAssignment.size() == minQuota)
                    minCapacityMembers.add(consumer);
                if (consumerAssignment.size() == maxQuota)
                    maxCapacityMembers.add(consumer);
            }
        }

        Collections.sort(unfilledMembers);
        Iterator<TopicPartition> unassignedPartitionsIter = unassignedPartitions.iterator();

        // Fill remaining members up to minQuota
        while (!unfilledMembers.isEmpty() && !unassignedPartitions.isEmpty()) {
            Iterator<String> unfilledConsumerIter = unfilledMembers.iterator();

            while (unfilledConsumerIter.hasNext()) {
                String consumer = unfilledConsumerIter.next();
                List<TopicPartition> consumerAssignment = assignment.get(consumer);

                if (unassignedPartitionsIter.hasNext()) {
                    TopicPartition tp = unassignedPartitionsIter.next();
                    consumerAssignment.add(tp);
                    unassignedPartitionsIter.remove();
                    // We already assigned all possible ownedPartitions, so we know this must be newly to this consumer
                    if (allRevokedPartitions.contains(tp))
                        partitionsTransferringOwnership.put(tp, consumer);
                } else {
                    break;
                }

                if (consumerAssignment.size() == minQuota) {
                    minCapacityMembers.add(consumer);
                    unfilledConsumerIter.remove();
                }
            }
        }

        // If we ran out of unassigned partitions before filling all consumers, we need to start stealing partitions
        // from the over-full consumers at max capacity
        for (String consumer : unfilledMembers) {
            List<TopicPartition> consumerAssignment = assignment.get(consumer);
            int remainingCapacity = minQuota - consumerAssignment.size();
            while (remainingCapacity > 0) {
                String overloadedConsumer = maxCapacityMembers.poll();
                if (overloadedConsumer == null) {
                    throw new IllegalStateException("Some consumers are under capacity but all partitions have been assigned");
                }
                TopicPartition swappedPartition = assignment.get(overloadedConsumer).remove(0);
                consumerAssignment.add(swappedPartition);
                --remainingCapacity;
                // This partition is by definition transferring ownership, the swapped partition must have come from
                // the max capacity member's owned partitions since it can only reach max capacity with owned partitions
                partitionsTransferringOwnership.put(swappedPartition, consumer);
            }
            minCapacityMembers.add(consumer);
        }

        // Otherwise we may have run out of unfilled consumers before assigning all partitions, in which case we
        // should just distribute one partition each to all consumers at min capacity
        for (TopicPartition unassignedPartition : unassignedPartitions) {
            String underCapacityConsumer = minCapacityMembers.poll();
            if (underCapacityConsumer == null) {
                throw new IllegalStateException("Some partitions are unassigned but all consumers are at maximum capacity");
            }
            // We can skip the bookkeeping of unassignedPartitions and maxCapacityMembers here since we are at the end
            assignment.get(underCapacityConsumer).add(unassignedPartition);

            if (allRevokedPartitions.contains(unassignedPartition))
                partitionsTransferringOwnership.put(unassignedPartition, underCapacityConsumer);
        }

        return assignment;
    }

    private SortedSet<TopicPartition> getTopicPartitions(Map<String, Integer> partitionsPerTopic) {
        SortedSet<TopicPartition> allPartitions =
            new TreeSet<>(Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
        for (Entry<String, Integer> entry: partitionsPerTopic.entrySet()) {
            String topic = entry.getKey();
            for (int i = 0; i < entry.getValue(); ++i) {
                allPartitions.add(new TopicPartition(topic, i));
            }
        }
        return allPartitions;
    }

    /**
     * This generalAssign algorithm guarantees the assignment that is as balanced as possible.
     * This method includes the following steps:
     *
     * 1. Preserving all the existing partition assignments
     * 2. Removing all the partition assignments that have become invalid due to the change that triggers the reassignment
     * 3. Assigning the unassigned partitions in a way that balances out the overall assignments of partitions to consumers
     * 4. Further balancing out the resulting assignment by finding the partitions that can be reassigned
     *    to another consumer towards an overall more balanced assignment.
     *
     * @param partitionsPerTopic         The number of partitions for each subscribed topic.
     * @param subscriptions              Map from the member id to their respective topic subscription
     *
     * @return Map from each member to the list of partitions assigned to them.
     */
    private Map<String, List<TopicPartition>> generalAssign(Map<String, Integer> partitionsPerTopic,
                                                            Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> currentAssignment = new HashMap<>();
        Map<TopicPartition, ConsumerGenerationPair> prevAssignment = new HashMap<>();
        partitionMovements = new PartitionMovements();

        prepopulateCurrentAssignments(subscriptions, currentAssignment, prevAssignment);

        // a mapping of all topic partitions to all consumers that can be assigned to them
        final Map<TopicPartition, List<String>> partition2AllPotentialConsumers = new HashMap<>();
        // a mapping of all consumers to all potential topic partitions that can be assigned to them
        final Map<String, List<TopicPartition>> consumer2AllPotentialPartitions = new HashMap<>();

        // initialize partition2AllPotentialConsumers and consumer2AllPotentialPartitions in the following two for loops
        for (Entry<String, Integer> entry: partitionsPerTopic.entrySet()) {
            for (int i = 0; i < entry.getValue(); ++i)
                partition2AllPotentialConsumers.put(new TopicPartition(entry.getKey(), i), new ArrayList<>());
        }

        for (Entry<String, Subscription> entry: subscriptions.entrySet()) {
            String consumerId = entry.getKey();
            consumer2AllPotentialPartitions.put(consumerId, new ArrayList<>());
            entry.getValue().topics().stream().filter(topic -> partitionsPerTopic.get(topic) != null).forEach(topic -> {
                for (int i = 0; i < partitionsPerTopic.get(topic); ++i) {
                    TopicPartition topicPartition = new TopicPartition(topic, i);
                    consumer2AllPotentialPartitions.get(consumerId).add(topicPartition);
                    partition2AllPotentialConsumers.get(topicPartition).add(consumerId);
                }
            });

            // add this consumer to currentAssignment (with an empty topic partition assignment) if it does not already exist
            if (!currentAssignment.containsKey(consumerId))
                currentAssignment.put(consumerId, new ArrayList<>());
        }

        // a mapping of partition to current consumer
        Map<TopicPartition, String> currentPartitionConsumer = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> entry: currentAssignment.entrySet())
            for (TopicPartition topicPartition: entry.getValue())
                currentPartitionConsumer.put(topicPartition, entry.getKey());

        List<TopicPartition> sortedPartitions = sortPartitions(partition2AllPotentialConsumers);

        // all partitions that need to be assigned (initially set to all partitions but adjusted in the following loop)
        List<TopicPartition> unassignedPartitions = new ArrayList<>(sortedPartitions);
        boolean revocationRequired = false;
        for (Iterator<Entry<String, List<TopicPartition>>> it = currentAssignment.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, List<TopicPartition>> entry = it.next();
            if (!subscriptions.containsKey(entry.getKey())) {
                // if a consumer that existed before (and had some partition assignments) is now removed, remove it from currentAssignment
                for (TopicPartition topicPartition: entry.getValue())
                    currentPartitionConsumer.remove(topicPartition);
                it.remove();
            } else {
                // otherwise (the consumer still exists)
                for (Iterator<TopicPartition> partitionIter = entry.getValue().iterator(); partitionIter.hasNext();) {
                    TopicPartition partition = partitionIter.next();
                    if (!partition2AllPotentialConsumers.containsKey(partition)) {
                        // if this topic partition of this consumer no longer exists remove it from currentAssignment of the consumer
                        partitionIter.remove();
                        currentPartitionConsumer.remove(partition);
                    } else if (!subscriptions.get(entry.getKey()).topics().contains(partition.topic())) {
                        // if this partition cannot remain assigned to its current consumer because the consumer
                        // is no longer subscribed to its topic remove it from currentAssignment of the consumer
                        partitionIter.remove();
                        revocationRequired = true;
                    } else
                        // otherwise, remove the topic partition from those that need to be assigned only if
                        // its current consumer is still subscribed to its topic (because it is already assigned
                        // and we would want to preserve that assignment as much as possible)
                        unassignedPartitions.remove(partition);
                }
            }
        }
        // at this point we have preserved all valid topic partition to consumer assignments and removed
        // all invalid topic partitions and invalid consumers. Now we need to assign unassignedPartitions
        // to consumers so that the topic partition assignments are as balanced as possible.

        // an ascending sorted set of consumers based on how many topic partitions are already assigned to them
        TreeSet<String> sortedCurrentSubscriptions = new TreeSet<>(new SubscriptionComparator(currentAssignment));
        sortedCurrentSubscriptions.addAll(currentAssignment.keySet());

        balance(currentAssignment, prevAssignment, sortedPartitions, unassignedPartitions, sortedCurrentSubscriptions,
            consumer2AllPotentialPartitions, partition2AllPotentialConsumers, currentPartitionConsumer, revocationRequired);
        return currentAssignment;
    }

    private void prepopulateCurrentAssignments(Map<String, Subscription> subscriptions,
                                               Map<String, List<TopicPartition>> currentAssignment,
                                               Map<TopicPartition, ConsumerGenerationPair> prevAssignment) {
        // we need to process subscriptions' user data with each consumer's reported generation in mind
        // higher generations overwrite lower generations in case of a conflict
        // note that a conflict could exists only if user data is for different generations

        // for each partition we create a sorted map of its consumers by generation
        Map<TopicPartition, TreeMap<Integer, String>> sortedPartitionConsumersByGeneration = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry: subscriptions.entrySet()) {
            String consumer = subscriptionEntry.getKey();
            MemberData memberData = memberData(subscriptionEntry.getValue());

            for (TopicPartition partition: memberData.partitions) {
                if (sortedPartitionConsumersByGeneration.containsKey(partition)) {
                    Map<Integer, String> consumers = sortedPartitionConsumersByGeneration.get(partition);
                    if (memberData.generation.isPresent() && consumers.containsKey(memberData.generation.get())) {
                        // same partition is assigned to two consumers during the same rebalance.
                        // log a warning and skip this record
                        log.warn("Partition '{}' is assigned to multiple consumers following sticky assignment generation {}.",
                            partition, memberData.generation);
                    } else
                        consumers.put(memberData.generation.orElse(DEFAULT_GENERATION), consumer);
                } else {
                    TreeMap<Integer, String> sortedConsumers = new TreeMap<>();
                    sortedConsumers.put(memberData.generation.orElse(DEFAULT_GENERATION), consumer);
                    sortedPartitionConsumersByGeneration.put(partition, sortedConsumers);
                }
            }
        }

        // prevAssignment holds the prior ConsumerGenerationPair (before current) of each partition
        // current and previous consumers are the last two consumers of each partition in the above sorted map
        for (Map.Entry<TopicPartition, TreeMap<Integer, String>> partitionConsumersEntry: sortedPartitionConsumersByGeneration.entrySet()) {
            TopicPartition partition = partitionConsumersEntry.getKey();
            TreeMap<Integer, String> consumers = partitionConsumersEntry.getValue();
            Iterator<Integer> it = consumers.descendingKeySet().iterator();

            // let's process the current (most recent) consumer first
            String consumer = consumers.get(it.next());
            currentAssignment.computeIfAbsent(consumer, k -> new ArrayList<>());
            currentAssignment.get(consumer).add(partition);

            // now update previous assignment if any
            if (it.hasNext()) {
                int generation = it.next();
                prevAssignment.put(partition, new ConsumerGenerationPair(consumers.get(generation), generation));
            }
        }
    }

    /**
     * determine if the current assignment is a balanced one
     *
     * @param currentAssignment: the assignment whose balance needs to be checked
     * @param sortedCurrentSubscriptions: an ascending sorted set of consumers based on how many topic partitions are already assigned to them
     * @param allSubscriptions: a mapping of all consumers to all potential topic partitions that can be assigned to them
     * @return true if the given assignment is balanced; false otherwise
     */
    private boolean isBalanced(Map<String, List<TopicPartition>> currentAssignment,
                               TreeSet<String> sortedCurrentSubscriptions,
                               Map<String, List<TopicPartition>> allSubscriptions) {
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
            if (consumerPartitionCount == allSubscriptions.get(consumer).size())
                continue;

            // otherwise make sure it cannot get any more
            List<TopicPartition> potentialTopicPartitions = allSubscriptions.get(consumer);
            for (TopicPartition topicPartition: potentialTopicPartitions) {
                if (!currentAssignment.get(consumer).contains(topicPartition)) {
                    String otherConsumer = allPartitions.get(topicPartition);
                    int otherConsumerPartitionCount = currentAssignment.get(otherConsumer).size();
                    if (consumerPartitionCount < otherConsumerPartitionCount) {
                        log.debug("{} can be moved from consumer {} to consumer {} for a more balanced assignment.",
                            topicPartition, otherConsumer, consumer);
                        return false;
                    }
                }
            }
        }
        return true;
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
     * Sort valid partitions so they are processed in the potential reassignment phase in the proper order
     * that causes minimal partition movement among consumers (hence honoring maximal stickiness)
     *
     * @param partition2AllPotentialConsumers a mapping of partitions to their potential consumers
     * @return  an ascending sorted list of topic partitions based on how many consumers can potentially use them
     */
    private List<TopicPartition> sortPartitions(Map<TopicPartition, List<String>> partition2AllPotentialConsumers) {
        List<TopicPartition> sortedPartitions = new ArrayList<>(partition2AllPotentialConsumers.keySet());
        Collections.sort(sortedPartitions, new PartitionComparator(partition2AllPotentialConsumers));
        return sortedPartitions;
    }

    /**
     * The assignment should improve the overall balance of the partition assignments to consumers.
     */
    private void assignPartition(TopicPartition partition,
                                 TreeSet<String> sortedCurrentSubscriptions,
                                 Map<String, List<TopicPartition>> currentAssignment,
                                 Map<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                                 Map<TopicPartition, String> currentPartitionConsumer) {
        for (String consumer: sortedCurrentSubscriptions) {
            if (consumer2AllPotentialPartitions.get(consumer).contains(partition)) {
                sortedCurrentSubscriptions.remove(consumer);
                currentAssignment.get(consumer).add(partition);
                currentPartitionConsumer.put(partition, consumer);
                sortedCurrentSubscriptions.add(consumer);
                break;
            }
        }
    }

    private boolean canParticipateInReassignment(TopicPartition partition,
                                                 Map<TopicPartition, List<String>> partition2AllPotentialConsumers) {
        // if a partition has two or more potential consumers it is subject to reassignment.
        return partition2AllPotentialConsumers.get(partition).size() >= 2;
    }

    private boolean canParticipateInReassignment(String consumer,
                                                 Map<String, List<TopicPartition>> currentAssignment,
                                                 Map<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                                                 Map<TopicPartition, List<String>> partition2AllPotentialConsumers) {
        List<TopicPartition> currentPartitions = currentAssignment.get(consumer);
        int currentAssignmentSize = currentPartitions.size();
        int maxAssignmentSize = consumer2AllPotentialPartitions.get(consumer).size();
        if (currentAssignmentSize > maxAssignmentSize)
            log.error("The consumer {} is assigned more partitions than the maximum possible.", consumer);

        if (currentAssignmentSize < maxAssignmentSize)
            // if a consumer is not assigned all its potential partitions it is subject to reassignment
            return true;

        for (TopicPartition partition: currentPartitions)
            // if any of the partitions assigned to a consumer is subject to reassignment the consumer itself
            // is subject to reassignment
            if (canParticipateInReassignment(partition, partition2AllPotentialConsumers))
                return true;

        return false;
    }

    /**
     * Balance the current assignment using the data structures created in the assign(...) method above.
     */
    private void balance(Map<String, List<TopicPartition>> currentAssignment,
                         Map<TopicPartition, ConsumerGenerationPair> prevAssignment,
                         List<TopicPartition> sortedPartitions,
                         List<TopicPartition> unassignedPartitions,
                         TreeSet<String> sortedCurrentSubscriptions,
                         Map<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                         Map<TopicPartition, List<String>> partition2AllPotentialConsumers,
                         Map<TopicPartition, String> currentPartitionConsumer,
                         boolean revocationRequired) {
        boolean initializing = currentAssignment.get(sortedCurrentSubscriptions.last()).isEmpty();
        boolean reassignmentPerformed = false;

        // assign all unassigned partitions
        for (TopicPartition partition: unassignedPartitions) {
            // skip if there is no potential consumer for the partition
            if (partition2AllPotentialConsumers.get(partition).isEmpty())
                continue;

            assignPartition(partition, sortedCurrentSubscriptions, currentAssignment,
                consumer2AllPotentialPartitions, currentPartitionConsumer);
        }

        // narrow down the reassignment scope to only those partitions that can actually be reassigned
        Set<TopicPartition> fixedPartitions = new HashSet<>();
        for (TopicPartition partition: partition2AllPotentialConsumers.keySet())
            if (!canParticipateInReassignment(partition, partition2AllPotentialConsumers))
                fixedPartitions.add(partition);
        sortedPartitions.removeAll(fixedPartitions);
        unassignedPartitions.removeAll(fixedPartitions);

        // narrow down the reassignment scope to only those consumers that are subject to reassignment
        Map<String, List<TopicPartition>> fixedAssignments = new HashMap<>();
        for (String consumer: consumer2AllPotentialPartitions.keySet())
            if (!canParticipateInReassignment(consumer, currentAssignment,
                consumer2AllPotentialPartitions, partition2AllPotentialConsumers)) {
                sortedCurrentSubscriptions.remove(consumer);
                fixedAssignments.put(consumer, currentAssignment.remove(consumer));
            }

        // create a deep copy of the current assignment so we can revert to it if we do not get a more balanced assignment later
        Map<String, List<TopicPartition>> preBalanceAssignment = deepCopy(currentAssignment);
        Map<TopicPartition, String> preBalancePartitionConsumers = new HashMap<>(currentPartitionConsumer);

        // if we don't already need to revoke something due to subscription changes, first try to balance by only moving newly added partitions
        if (!revocationRequired) {
            performReassignments(unassignedPartitions, currentAssignment, prevAssignment, sortedCurrentSubscriptions,
                consumer2AllPotentialPartitions, partition2AllPotentialConsumers, currentPartitionConsumer);
        }

        reassignmentPerformed = performReassignments(sortedPartitions, currentAssignment, prevAssignment, sortedCurrentSubscriptions,
                   consumer2AllPotentialPartitions, partition2AllPotentialConsumers, currentPartitionConsumer);

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
                                         Map<String, List<TopicPartition>> currentAssignment,
                                         Map<TopicPartition, ConsumerGenerationPair> prevAssignment,
                                         TreeSet<String> sortedCurrentSubscriptions,
                                         Map<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                                         Map<TopicPartition, List<String>> partition2AllPotentialConsumers,
                                         Map<TopicPartition, String> currentPartitionConsumer) {
        boolean reassignmentPerformed = false;
        boolean modified;

        // repeat reassignment until no partition can be moved to improve the balance
        do {
            modified = false;
            // reassign all reassignable partitions (starting from the partition with least potential consumers and if needed)
            // until the full list is processed or a balance is achieved
            Iterator<TopicPartition> partitionIterator = reassignablePartitions.iterator();
            while (partitionIterator.hasNext() && !isBalanced(currentAssignment, sortedCurrentSubscriptions, consumer2AllPotentialPartitions)) {
                TopicPartition partition = partitionIterator.next();

                // the partition must have at least two consumers
                if (partition2AllPotentialConsumers.get(partition).size() <= 1)
                    log.error("Expected more than one potential consumer for partition '{}'", partition);

                // the partition must have a current consumer
                String consumer = currentPartitionConsumer.get(partition);
                if (consumer == null)
                    log.error("Expected partition '{}' to be assigned to a consumer", partition);

                if (prevAssignment.containsKey(partition) &&
                    currentAssignment.get(consumer).size() > currentAssignment.get(prevAssignment.get(partition).consumer).size() + 1) {
                    reassignPartition(partition, currentAssignment, sortedCurrentSubscriptions, currentPartitionConsumer, prevAssignment.get(partition).consumer);
                    reassignmentPerformed = true;
                    modified = true;
                    continue;
                }

                // check if a better-suited consumer exist for the partition; if so, reassign it
                for (String otherConsumer: partition2AllPotentialConsumers.get(partition)) {
                    if (currentAssignment.get(consumer).size() > currentAssignment.get(otherConsumer).size() + 1) {
                        reassignPartition(partition, currentAssignment, sortedCurrentSubscriptions, currentPartitionConsumer, consumer2AllPotentialPartitions);
                        reassignmentPerformed = true;
                        modified = true;
                        break;
                    }
                }
            }
        } while (modified);

        return reassignmentPerformed;
    }

    private void reassignPartition(TopicPartition partition,
                                   Map<String, List<TopicPartition>> currentAssignment,
                                   TreeSet<String> sortedCurrentSubscriptions,
                                   Map<TopicPartition, String> currentPartitionConsumer,
                                   Map<String, List<TopicPartition>> consumer2AllPotentialPartitions) {
        // find the new consumer
        String newConsumer = null;
        for (String anotherConsumer: sortedCurrentSubscriptions) {
            if (consumer2AllPotentialPartitions.get(anotherConsumer).contains(partition)) {
                newConsumer = anotherConsumer;
                break;
            }
        }

        assert newConsumer != null;

        reassignPartition(partition, currentAssignment, sortedCurrentSubscriptions, currentPartitionConsumer, newConsumer);
    }

    private void reassignPartition(TopicPartition partition,
                                   Map<String, List<TopicPartition>> currentAssignment,
                                   TreeSet<String> sortedCurrentSubscriptions,
                                   Map<TopicPartition, String> currentPartitionConsumer,
                                   String newConsumer) {
        String consumer = currentPartitionConsumer.get(partition);
        // find the correct partition movement considering the stickiness requirement
        TopicPartition partitionToBeMoved = partitionMovements.getTheActualPartitionToBeMoved(partition, consumer, newConsumer);
        processPartitionMovement(partitionToBeMoved, newConsumer, currentAssignment, sortedCurrentSubscriptions, currentPartitionConsumer);
    }

    private void processPartitionMovement(TopicPartition partition,
                                          String newConsumer,
                                          Map<String, List<TopicPartition>> currentAssignment,
                                          TreeSet<String> sortedCurrentSubscriptions,
                                          Map<TopicPartition, String> currentPartitionConsumer) {
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

    public boolean isSticky() {
        return partitionMovements.isSticky();
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

    private static class PartitionComparator implements Comparator<TopicPartition>, Serializable {
        private static final long serialVersionUID = 1L;
        private Map<TopicPartition, List<String>> map;

        PartitionComparator(Map<TopicPartition, List<String>> map) {
            this.map = map;
        }

        @Override
        public int compare(TopicPartition o1, TopicPartition o2) {
            int ret = map.get(o1).size() - map.get(o2).size();
            if (ret == 0) {
                ret = o1.topic().compareTo(o2.topic());
                if (ret == 0)
                    ret = o1.partition() - o2.partition();
            }
            return ret;
        }
    }

    private static class SubscriptionComparator implements Comparator<String>, Serializable {
        private static final long serialVersionUID = 1L;
        private Map<String, List<TopicPartition>> map;

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
        private Map<String, Map<ConsumerPair, Set<TopicPartition>>> partitionMovementsByTopic = new HashMap<>();
        private Map<TopicPartition, ConsumerPair> partitionMovements = new HashMap<>();

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
                    log.error("A cycle of length {} was found: {}", path.size() - 1, path.toString());
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
}
