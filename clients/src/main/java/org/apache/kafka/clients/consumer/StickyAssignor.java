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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The sticky assignor serves two purposes. First, it guarantees an assignment that is as balanced as possible, meaning either:
 * - the numbers of topic partitions assigned to consumers differ by at most one; or
 * - each consumer that has 2+ fewer topic partitions than some other consumer cannot get any of those topic partitions transferred to it.
 * Second, it preserved as many existing assignment as possible when a reassignment occurs. This helps in saving some of the
 * overhead processing when topic partitions move from one consumer to another.
 *
 * Starting fresh it would work by distributing the partitions over consumers as evenly as possible. Even though this may sound similar to
 * how round robin assignor works, the second example below shows that it is not.
 * During a reassignment it would perform the reassignment in such a way that in the new assignment
 * 1. topic partitions are still distributed as evenly as possible, and
 * 2. topic partitions stay with their previously assigned consumers as much as possible.
 * Of course, the first goal above takes precedence over the second one.
 *
 * <b>Example 1.</b> Suppose there are three consumers <code>C0</code>, <code>C1</code>, <code>C2</code>,
 * four topics <code>t0,</code> <code>t1</code>, <code>t2</code>, <code>t3</code>, and each topic has 2 partitions,
 * resulting in partitions <code>t0p0</code>, <code>t0p1</code>, <code>t1p0</code>, <code>t1p1</code>, <code>t2p0</code>,
 * <code>t2p1</code>, <code>t3p0</code>, <code>t3p1</code>. Each consumer is subscribed to all three topics.
 *
 * The assignment with both sticky and round robin assignors will be:
 * <ul>
 * <li><code>C0: [t0p0, t1p1, t3p0]<code></li>
 * <li><code>C1: [t0p1, t2p0, t3p1]<code></li>
 * <li><code>C2: [t1p0, t2p1]<code></li>
 * </ul>
 *
 * Now, let's assume <code>C1</code> is removed and a reassignment is about to happen. The round robin assignor would produce:
 * <ul>
 * <li><code>C0: [t0p0, t1p0, t2p0, t3p0]</code></li>
 * <li><code>C2: [t0p1, t1p1, t2p1, t3p1]</code></li>
 * </ul>
 *
 * while the sticky assignor would result in:
 * <ul>
 * <li><code>C0 [t0p0, t1p1, t3p0, t2p0]</code></li>
 * <li><code>C2 [t1p0, t2p1, t0p1, t3p1]</code></li>
 * </ul>
 * preserving all the previous assignments (unlike the round robin assignor).
 *
 * <b>Example 2.</b> There are three consumers <code>C0</code>, <code>C1</code>, <code>C2</code>,
 * and three topics <code>t0</code>, <code>t1</code>, <code>t2</code>, with 1, 2, and 3 partitions respectively.
 * Therefore, the partitions are <code>t0p0</code>, <code>t1p0</code>, <code>t1p1</code>, <code>t2p0</code>,
 * <code>t2p1</code>, <code>t2p2</code>. <code>C0</code> is subscribed to <code>t0</code>; <code>C1</code> is subscribed to
 * <code>t0</code>, <code>t1</code>; and <code>C2</code> is subscribed to <code>t0</code>, <code>t1</code>, <code>t2</code>.
 *
 * The round robin assignor would come up with the following assignment:
 * <ul>
 * <li><code>C0 [t0p0]</code></li>
 * <li><code>C1 [t1p0]</code></li>
 * <li><code>C2 [t1p1, t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *
 * which is not as balanced as the assignment suggested by sticky assignor:
 * <ul>
 * <li><code>C0 [t0p0]</code></li>
 * <li><code>C1 [t1p0, t1p1]</code></li>
 * <li><code>C2 [t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *
 * Now, if consumer <code>C0</code> is removed, these two assignors would produce the following assignments.
 * Round Robin (preserves 3 partition assignments):
 * <ul>
 * <li><code>C1 [t0p0, t1p1]</code></li>
 * <li><code>C2 [t1p0, t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *
 * Sticky (preserves 5 partition assignments):
 * <ul>
 * <li><code>C1 [t1p0, t1p1, t0p0]</code></li>
 * <li><code>C2 [t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *
 * <h3>Impact on <code>ConsumerRebalanceListener</code></h3>
 * The sticky assignment strategy can provide some optimization to those consumers that have some partition cleanup code
 * in their <code>onPartitionsRevoked()</code> callback listeners. The cleanup code is placed in that callback listener
 * because the consumer has no assumption or hope of preserving any of its assigned partitions after a rebalance when it
 * is using range or round robin assignor. The listener code would look like this:
 * <code>
 * class TheOldRebalanceListener implements ConsumerRebalanceListener {
 *
 *   void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions) {
 *       commitOffsets(partition);
 *       cleanupState(partition);
 *     }
 *   }
 *
 *   void onPartitionsAssigned(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions) {
 *       initializeState(partition);
 *       initializeOffset(partition);
 *     }
 *   }
 * }
 * </code>
 *
 * As mentioned above, one advantage of the sticky assignor is that, in general, it reduces the number of partitions that
 * actually move from one consumer to another during a reassignment. Therefore, it allows consumers to do their cleanup
 * more efficiently. Of course, they still can perform the partition cleanup in the <code>onPartitionsRevoked()</code>
 * listener, but they can be more efficient and make a note of their partitions before and after the rebalance, and do the
 * cleanup after the rebalance only on the partitions they have lost (which is normally not a lot). The code snippet below
 * clarifies this point:
 * <code>
 * class TheNewRebalanceListener implements ConsumerRebalanceListener {
 *   Collection<TopicPartition> lastAssignment = Collections.emptyList();
 *
 *   void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions)
 *       commitOffsets(partition);
 *   }
 *
 *   void onPartitionsAssigned(Collection<TopicPartition> assignment) {
 *     for (TopicPartition partition: difference(lastAssignment, assignment))
 *       cleanupState(partition);
 *
 *     for (TopicPartition partition: difference(assignment, lastAssignment))
 *       initializeState(partition);
 *
 *     for (TopicPartition partition: assignment)
 *       initializeOffset(partition);
 *
 *     this.lastAssignment = assignment;
 *   }
 * }
 * </code>
 *
 * Any consumer that uses sticky assignment can leverage this listener like this:
 * <code>consumer.subscribe(topics, new TheNewRebalanceListener());</code>
 *
 */
public class StickyAssignor extends AbstractPartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(StickyAssignor.class);
    protected Map<String, List<TopicPartition>> currentAssignment = new HashMap<>();
    private List<TopicPartition> memberAssignment = null;
    private PartitionMovements partitionMovements;

    private void deepCopy(Map<String, List<TopicPartition>> source, Map<String, List<TopicPartition>> dest) {
        dest.clear();
        for (Entry<String, List<TopicPartition>> entry: source.entrySet())
            dest.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }

    protected Map<String, List<TopicPartition>> deepCopy(Map<String, List<TopicPartition>> assignment) {
        Map<String, List<TopicPartition>> copy = new HashMap<>();
        deepCopy(assignment, copy);
        return copy;
    }

    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, List<String>> subscriptions) {
        partitionMovements = new PartitionMovements();

        prepopulateCurrentAssignments();
        // make a deep copy of currentAssignment
        Map<String, List<TopicPartition>> oldAssignment = deepCopy(currentAssignment);

        // a mapping of all topic partitions to all consumers that can be assigned to them
        final HashMap<TopicPartition, List<String>> partition2AllPotentialConsumers = new HashMap<>();
        // a mapping of all consumers to all potential topic partitions that can be assigned to them
        final HashMap<String, List<TopicPartition>> consumer2AllPotentialPartitions = new HashMap<>();

        // initialize partition2AllPotentialConsumers and consumer2AllPotentialPartitions in the following two for loops
        for (Entry<String, Integer> entry: partitionsPerTopic.entrySet()) {
            for (int i = 0; i < entry.getValue(); ++i)
                partition2AllPotentialConsumers.put(new TopicPartition(entry.getKey(), i), new ArrayList<String>());
        }

        for (Entry<String, List<String>> entry: subscriptions.entrySet()) {
            String consumer = entry.getKey();
            consumer2AllPotentialPartitions.put(consumer, new ArrayList<TopicPartition>());
            for (String topic: entry.getValue()) {
                for (int i = 0; i < partitionsPerTopic.get(topic); ++i) {
                    TopicPartition topicPartition = new TopicPartition(topic, i);
                    consumer2AllPotentialPartitions.get(consumer).add(topicPartition);
                    partition2AllPotentialConsumers.get(topicPartition).add(consumer);
                }
            }

            // add this consumer to currentAssignment (with an empty topic partition assignment) if it does not already exist
            if (!currentAssignment.containsKey(consumer))
                currentAssignment.put(consumer, new ArrayList<TopicPartition>());
        }

        // a mapping of partition to current consumer
        HashMap<TopicPartition, String> currentPartitionConsumer = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> entry: currentAssignment.entrySet())
            for (TopicPartition topicPartition: entry.getValue())
                currentPartitionConsumer.put(topicPartition, entry.getKey());

        List<TopicPartition> sortedPartitions = sortPartitions(oldAssignment.isEmpty(), partition2AllPotentialConsumers, consumer2AllPotentialPartitions);

        // all partitions that need to be assigned (initially set to all partitions but adjusted in the following loop)
        List<TopicPartition> unassignedPartitions = new ArrayList<>(sortedPartitions);
        for (Iterator<Map.Entry<String, List<TopicPartition>>> it = currentAssignment.entrySet().iterator(); it.hasNext();) {
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
                    } else if (!subscriptions.get(entry.getKey()).contains(partition.topic())) {
                        // if this partition cannot remain assigned to its current consumer because the consumer
                        // is no longer subscribed to its topic remove it from currentAssignment of the consumer
                        partitionIter.remove();
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

        balance(sortedPartitions, unassignedPartitions, sortedCurrentSubscriptions, consumer2AllPotentialPartitions,
                partition2AllPotentialConsumers, oldAssignment, currentPartitionConsumer);
        return currentAssignment;
    }

    private void prepopulateCurrentAssignments() {
        Map<String, Subscription> subscriptions = getSubscriptions();
        if (subscriptions == null)
            return;

        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            ByteBuffer userData = subscriptionEntry.getValue().userData();
            if (userData != null && userData.hasRemaining())
                currentAssignment.put(subscriptionEntry.getKey(), ConsumerProtocol.deserializeTopicPartitionAssignment(userData));
        }
    }

    @Override
    public void onAssignment(Assignment assignment) {
        memberAssignment = assignment.partitions();
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        if (memberAssignment == null)
            return new Subscription(new ArrayList<>(topics));

        return new Subscription(new ArrayList<>(topics), ConsumerProtocol.serializeTopicPartitionAssignment(memberAssignment));
    }

    @Override
    public String name() {
        return "sticky";
    }

    /**
     * determine if the current assignment is a balanced one
     *
     * @param sortedCurrentSubscriptions: an ascending sorted set of consumers based on how many topic partitions are already assigned to them
     * @param allSubscriptions: a mapping of all consumers to all potential topic partitions that can be assigned to them
     * @return
     */
    protected boolean isBalanced(TreeSet<String> sortedCurrentSubscriptions, Map<String, List<TopicPartition>> allSubscriptions) {
        int min = currentAssignment.get(sortedCurrentSubscriptions.first()).size();
        int max = currentAssignment.get(sortedCurrentSubscriptions.last()).size();
        if (min >= max - 1)
            // if minimum and maximum numbers of partitions assigned to consumers differ by at most one return true
            return true;

        // create a mapping from partitions to the consumer assigned to them
        final HashMap<TopicPartition, String> allPartitions = new HashMap<>();
        Set<Entry<String, List<TopicPartition>>> assignments = currentAssignment.entrySet();
        for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
            List<TopicPartition> topicPartitions = entry.getValue();
            for (TopicPartition topicPartition: topicPartitions) {
                if (allPartitions.containsKey(topicPartition))
                    log.error(topicPartition + " is assigned to more than one consumer.");
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
                        log.debug(topicPartition + " can be moved from consumer " + otherConsumer + " to consumer " + consumer + " for a more balanced assignment.");
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
    protected int getBalanceScore(Map<String, List<TopicPartition>> assignment) {
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
     * @param isFreshAssignment whether this is a new assignment, or a reassignment of an existing one
     * @param partition2AllPotentialConsumers a mapping of partitions to their potential consumers
     * @param consumer2AllPotentialPartitions a mapping of consumers to potential partitions they can consumer from
     * @return sorted list of valid partitions
     */
    private List<TopicPartition> sortPartitions(boolean isFreshAssignment,
                                                HashMap<TopicPartition, List<String>> partition2AllPotentialConsumers,
                                                HashMap<String, List<TopicPartition>> consumer2AllPotentialPartitions) {
        List<TopicPartition> sortedPartitions = new ArrayList<>();

        if (!isFreshAssignment && areSubscriptionsIdentical(partition2AllPotentialConsumers, consumer2AllPotentialPartitions)) {
            // if this is a reassignment and the subscriptions are identical (all consumers can consumer from all topics)
            // then we just need to simply list partitions in a round robin fashion (from consumers with
            // most assigned partitions to those with least)
            Map<String, List<TopicPartition>> assignments = deepCopy(currentAssignment);
            for (Entry<String, List<TopicPartition>> entry: assignments.entrySet()) {
                List<TopicPartition> toRemove = new ArrayList<>();
                for (TopicPartition partition: entry.getValue())
                    if (!partition2AllPotentialConsumers.keySet().contains(partition))
                        toRemove.add(partition);
                for (TopicPartition partition: toRemove)
                    entry.getValue().remove(partition);
            }
            TreeSet<String> sortedConsumers = new TreeSet<>(new SubscriptionComparator(assignments));
            sortedConsumers.addAll(assignments.keySet());

            while (!sortedConsumers.isEmpty()) {
                String consumer = sortedConsumers.pollLast();
                List<TopicPartition> remainingPartitions = assignments.get(consumer);
                if (!remainingPartitions.isEmpty()) {
                    sortedPartitions.add(remainingPartitions.remove(0));
                    sortedConsumers.add(consumer);
                }
            }

            for (TopicPartition partition: partition2AllPotentialConsumers.keySet()) {
                if (!sortedPartitions.contains(partition))
                    sortedPartitions.add(partition);
            }

        } else {
            // an ascending sorted set of topic partitions based on how many consumers can potentially use them
            TreeSet<TopicPartition> sortedAllPartitions = new TreeSet<>(new PartitionComparator(partition2AllPotentialConsumers));
            sortedAllPartitions.addAll(partition2AllPotentialConsumers.keySet());

            while (!sortedAllPartitions.isEmpty())
                sortedPartitions.add(sortedAllPartitions.pollFirst());
        }

        return sortedPartitions;
    }

    /**
     * @param partition2AllPotentialConsumers a mapping of partitions to their potential consumers
     * @param consumer2AllPotentialPartitions a mapping of consumers to potential partitions they can consumer from
     * @return true if potential consumers of partitions are the same, and potential partitions consumers can
     * consumer from are the same too
     */
    private boolean areSubscriptionsIdentical(HashMap<TopicPartition, List<String>> partition2AllPotentialConsumers,
                                              HashMap<String, List<TopicPartition>> consumer2AllPotentialPartitions) {
        if (!hasIdenticalListElements(partition2AllPotentialConsumers.values()))
            return false;

        if (!hasIdenticalListElements(consumer2AllPotentialPartitions.values()))
            return false;

        return true;
    }

    /**
     * @param col a collection of elements of type list
     * @return true if all lists in the collection have the same members; false otherwise
     */
    private <T> boolean hasIdenticalListElements(Collection<List<T>> col) {
        Iterator<List<T>> it = col.iterator();
        List<T> cur = it.next();
        while (it.hasNext()) {
            List<T> next = it.next();
            if (!(cur.containsAll(next) && next.containsAll(cur)))
                return false;
            cur = next;
        }
        return true;
    }

    /**
     * @return the consumer to which the given partition is assigned. The assignment should improve the overall balance
     * of the partition assignments to consumers.
     */
    private String assignPartition(TopicPartition partition, TreeSet<String> sortedCurrentSubscriptions,
            HashMap<String, List<TopicPartition>> consumer2AllPotentialPartitions, HashMap<TopicPartition, String> currentPartitionConsumer) {
        for (String consumer: sortedCurrentSubscriptions) {
            if (consumer2AllPotentialPartitions.get(consumer).contains(partition)) {
                sortedCurrentSubscriptions.remove(consumer);
                currentAssignment.get(consumer).add(partition);
                currentPartitionConsumer.put(partition, consumer);
                sortedCurrentSubscriptions.add(consumer);
                return consumer;
            }
        }
        return null;
    }

    private boolean canParticipateInReassignment(TopicPartition partition, HashMap<TopicPartition, List<String>> partition2AllPotentialConsumers) {
        // if a partition has two or more potential consumers it is subject to reassignment.
        return partition2AllPotentialConsumers.get(partition).size() >= 2;
    }

    private boolean canParticipateInReassignment(String consumer,
                                                 HashMap<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                                                 HashMap<TopicPartition, List<String>> partition2AllPotentialConsumers) {
        List<TopicPartition> currentPartitions = currentAssignment.get(consumer);
        int currentAssignmentSize = currentPartitions.size();
        int maxAssignmentSize = consumer2AllPotentialPartitions.get(consumer).size();
        if (currentAssignmentSize > maxAssignmentSize)
            log.error("The consumer " + consumer + " is assigned more partitions than the maximum possible.");

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
    private void balance(List<TopicPartition> sortedPartitions, List<TopicPartition> unassignedPartitions, TreeSet<String> sortedCurrentSubscriptions,
                         HashMap<String, List<TopicPartition>> consumer2AllPotentialPartitions, HashMap<TopicPartition, List<String>> partition2AllPotentialConsumers,
                         Map<String, List<TopicPartition>> oldAssignment, HashMap<TopicPartition, String> currentPartitionConsumer) {
        boolean initializing = currentAssignment.get(sortedCurrentSubscriptions.last()).isEmpty();
        boolean reassignmentPerformed = false;

        // assign all unassigned partitions
        for (TopicPartition partition: unassignedPartitions) {
            // skip if there is no potential consumer for the partition
            if (partition2AllPotentialConsumers.get(partition).isEmpty())
                continue;

            assignPartition(partition, sortedCurrentSubscriptions, consumer2AllPotentialPartitions, currentPartitionConsumer);
        }

        // narrow down the reassignment scope to only those partitions that can actually be reassigned
        Set<TopicPartition> reassignablePartitions = new HashSet<>(partition2AllPotentialConsumers.keySet());
        Set<TopicPartition> fixedPartitions = new HashSet<>();
        for (TopicPartition partition: reassignablePartitions)
            if (!canParticipateInReassignment(partition, partition2AllPotentialConsumers))
                fixedPartitions.add(partition);
        reassignablePartitions.removeAll(fixedPartitions);
        sortedPartitions.removeAll(fixedPartitions);

        // narrow down the reassignment scope to only those consumers that are subject to reassignment
        Map<String, List<TopicPartition>> fixedAssignments = new HashMap<>();
        for (String consumer: consumer2AllPotentialPartitions.keySet())
            if (!canParticipateInReassignment(consumer, consumer2AllPotentialPartitions, partition2AllPotentialConsumers)) {
                sortedCurrentSubscriptions.remove(consumer);
                fixedAssignments.put(consumer, currentAssignment.remove(consumer));
            }

        // create a deep copy of the current assignment so we can revert to it if we do not get a more balanced assignment later
        Map<String, List<TopicPartition>> preBalanceAssignment = deepCopy(currentAssignment);
        HashMap<TopicPartition, String> preBalancePartitionConsumers = new HashMap<>(currentPartitionConsumer);

        reassignmentPerformed = performReassignments(sortedPartitions, sortedCurrentSubscriptions,
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

    private boolean performReassignments(List<TopicPartition> sortedPartitions, TreeSet<String> sortedCurrentSubscriptions,
                                         HashMap<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                                         HashMap<TopicPartition, List<String>> partition2AllPotentialConsumers,
                                         HashMap<TopicPartition, String> currentPartitionConsumer) {
        boolean reassignmentPerformed = false;
        boolean modified;

        // repeat reassignment until no partition can be moved to improve the balance
        do {
            modified = false;
            // reassign all reassignable partitions (starting from the partition with least potential consumers and if needed)
            // until the full list is processed or a balance is achieved
            Iterator<TopicPartition> partitionIterator = sortedPartitions.iterator();
            while (partitionIterator.hasNext() && !isBalanced(sortedCurrentSubscriptions, consumer2AllPotentialPartitions)) {
                TopicPartition partition = partitionIterator.next();

                // the partition must have at least two consumers
                if (partition2AllPotentialConsumers.get(partition).size() <= 1)
                    log.error("Expected more than one potential consumer for partition '" + partition + "'");

                // the partition must have a current consumer
                String consumer = currentPartitionConsumer.get(partition);
                if (consumer == null)
                    log.error("Expected partition '" + partition + "' to be assigned to a consumer");

                // check if a better-suited consumer exist for the partition; if so, reassign it
                for (String otherConsumer: partition2AllPotentialConsumers.get(partition)) {
                    if (currentAssignment.get(consumer).size() > currentAssignment.get(otherConsumer).size() + 1) {
                        reassignPartition(partition, sortedCurrentSubscriptions, currentPartitionConsumer, consumer2AllPotentialPartitions);
                        reassignmentPerformed = true;
                        modified = true;
                        break;
                    }
                }
            }
        } while (modified);

        return reassignmentPerformed;
    }

    private void reassignPartition(TopicPartition partition, TreeSet<String> sortedCurrentSubscriptions,
                                   HashMap<TopicPartition, String> currentPartitionConsumer,
                                   HashMap<String, List<TopicPartition>> consumer2AllPotentialPartitions) {
        String consumer = currentPartitionConsumer.get(partition);

        // find the new consumer
        String newConsumer = null;
        for (String anotherConsumer: sortedCurrentSubscriptions) {
            if (consumer2AllPotentialPartitions.get(anotherConsumer).contains(partition)) {
                newConsumer = anotherConsumer;
                break;
            }
        }

        assert newConsumer != null;

        // find the correct partition movement considering the stickiness requirement
        TopicPartition partitionToBeMoved = partitionMovements.getTheActualPartitionToBeMoved(partition, consumer, newConsumer);
        processPartitionMovement(partitionToBeMoved, newConsumer, sortedCurrentSubscriptions, currentPartitionConsumer);

        return;
    }

    private void processPartitionMovement(TopicPartition partition, String newConsumer,
                                          TreeSet<String> sortedCurrentSubscriptions,
                                          HashMap<TopicPartition, String> currentPartitionConsumer) {
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

    @SuppressWarnings("serial")
    private static class PartitionComparator implements Comparator<TopicPartition>, Serializable {
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

    @SuppressWarnings("serial")
    protected static class SubscriptionComparator implements Comparator<String>, Serializable {
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

    @SuppressWarnings("serial")
    protected static class PartitionAssignmentComparator implements Comparator<String>, Serializable {
        private Map<String, List<TopicPartition>> map;

        PartitionAssignmentComparator(Map<String, List<TopicPartition>> map) {
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

}
