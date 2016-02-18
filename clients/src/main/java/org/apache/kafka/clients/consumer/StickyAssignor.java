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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The sticky assignor serves two purposes. First, it guarantees an assignment that is as balanced as possible, meaning either:
 * - the numbers of topic partitions assigned to consumers differ by at most one; or
 * - each consumer that has 2+ fewer topic partitions than some other consumer cannot get any of those topic partitions transfered to it.
 *
 * Starting fresh it would work by distributing the partitions over consumers as evenly as possible. Even though this may sound similar to
 * how round robin assignor works, the second example below shows that it is not.
 * During a reassignment it would perform the reassignment in such a way that in the new assignment
 * 1. topic partitions are still distributed as evenly as possible, and
 * 2. topic partitions stay with their previously assigned consumers as much as possible.
 * Of course, the first goal above takes precedence over the second one.
 *
 * Example 1. Suppose there are three consumers C0, C1, C2, four topics t0, t1, t2, t3, and each topic has 2 partitions,
 * resulting in partitions t0p0, t0p1, t1p0, t1p1, t2p0, t2p1, t3p0, t3p1. Each consumer is subscribed to all three topics.
 *
 * The assignment with both sticky and round robin assignors will be:
 * C0: [t0p0, t1p1, t3p0]
 * C1: [t0p1, t2p0, t3p1]
 * C2: [t1p0, t2p1]
 *
 * Now, let's assume C1 is removed and a reassignment is about to happen. The round robin assignor would produce:
 * C0: [t0p0, t1p0, t2p0, t3p0]
 * C2: [t0p1, t1p1, t2p1, t3p1]
 *
 * while the sticky assignor would result in:
 * C0 [t0p0, t1p1, t3p0, t2p0]
 * C2 [t1p0, t2p1, t0p1, t3p1]
 * preserving all the previous assignments (unlike the round robin assignor).
 *
 * Example 2. There are three consumers C0, C1, C2, and three topics t0, t1, t2, with 1, 2, and 3 partitions respectively.
 * Therefore, the partitions are t0p0, t1p0, t1p1, t2p0, t2p1, t2p2.
 * C0 is subscribed to t0; C1 is subscribed to t0, t1; and C2 is subscribed to t0, t1, t2.
 *
 * The round robin assignor would come up with the following assignment:
 * C0 [t0p0]
 * C1 [t1p0]
 * C2 [t1p1, t2p0, t2p1, t2p2]
 *
 * which is not as balanced as the assignment suggested by sticky assignor:
 * C0 [t0p0]
 * C1 [t1p0, t1p1]
 * C2 [t2p0, t2p1, t2p2]
 *
 * Now, if consumer C0 is removed, these two assignors would produce the following assignments.
 * Round Robin (preserves 3 partition assignments):
 * C1 [t0p0, t1p1]
 * C2 [t1p0, t2p0, t2p1, t2p2]
 *
 * Sticky (preserves 5 partition assignments):
 * C1 [t1p0, t1p1, t0p0]
 * C2 [t2p0, t2p1, t2p2]
 *
 */
public class StickyAssignor extends AbstractPartitionAssignor {

    private static final Logger log = LoggerFactory.getLogger(StickyAssignor.class);
    protected Map<String, List<TopicPartition>> currentAssignment = new HashMap<>();

    private void deepCopy(Map<String, List<TopicPartition>> source, Map<String, List<TopicPartition>> dest) {
        dest.clear();
        for (Entry<String, List<TopicPartition>> entry: source.entrySet())
            dest.put(entry.getKey(), new ArrayList<TopicPartition>(entry.getValue()));
    }

    private Map<String, List<TopicPartition>> deepCopy(Map<String, List<TopicPartition>> assignment) {
        Map<String, List<TopicPartition>> copy = new HashMap<>();
        deepCopy(assignment, copy);
        return copy;
    }

    private HashMap<TopicPartition, String> deepCopy(HashMap<TopicPartition, String> source) {
        HashMap<TopicPartition, String> copy = new HashMap<>();
        copy.putAll(source);
        return copy;
    }

    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
            Map<String, List<String>> subscriptions) {
        // make a deep copy of currentAssignment
        Map<String, List<TopicPartition>> oldAssignment = deepCopy(currentAssignment);

        // a mapping of all topic partitions to all consumers that can be assigned to them
        final HashMap<TopicPartition, List<String>> partition2AllPotentialConsumers = new HashMap<>();
        // a mapping of all consumers to all potential topic partitions that can be assigned to them
        final HashMap<String, List<TopicPartition>> consumer2AllPotentialPartitions = new HashMap<>();

        // initialize partition2AllPotentialConsumers and consumer2AllPotentialPartitions in the following two for loops
        Set<String> topics = partitionsPerTopic.keySet();
        for (String topic: topics) {
            int partitions = partitionsPerTopic.get(topic);
            for (int i = 0; i < partitions; ++i)
                partition2AllPotentialConsumers.put(new TopicPartition(topic, i), new ArrayList<String>());
        }

        Set<String> consumers = subscriptions.keySet();
        for (String consumer: consumers) {
            consumer2AllPotentialPartitions.put(consumer, new ArrayList<TopicPartition>());
            List<String> consumerTopics = subscriptions.get(consumer);
            for (String topic: consumerTopics) {
                int partitions = partitionsPerTopic.get(topic);
                for (int i = 0; i < partitions; ++i) {
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
        for (String consumer: currentAssignment.keySet())
            for (TopicPartition topicPartition: currentAssignment.get(consumer))
                currentPartitionConsumer.put(topicPartition, consumer);

        // an ascending sorted set of topic partitions based on how many consumers can potentially use them
        TreeSet<TopicPartition> sortedAllPartitions = new TreeSet<>(new PartitionComparator(partition2AllPotentialConsumers));
        sortedAllPartitions.addAll(partition2AllPotentialConsumers.keySet());

        // all partitions that need to be assigned (initially set to all partitions but adjusted in the following loop)
        TreeSet<TopicPartition> unassignedPartitions = new TreeSet<>(sortedAllPartitions);
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
                    } else
                        // otherwise, remove the topic partition from those that need to be assigned (because it is already assigned
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

        balance(sortedAllPartitions, unassignedPartitions, sortedCurrentSubscriptions, consumer2AllPotentialPartitions,
                partition2AllPotentialConsumers, oldAssignment, currentPartitionConsumer);
        return currentAssignment;
    }


    @Override
    protected ByteBuffer getUserData() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(2048);
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(currentAssignment);
            return ByteBuffer.wrap(bos.toByteArray());
        } catch (Exception ioex) {
            log.error("Failed to serialize currentAssignment", ioex);
            return super.getUserData();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onAssignment(Assignment assignment) {
        try {
            byte[] array = new byte[assignment.userData().remaining()];
            assignment.userData().get(array);
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(array));
            currentAssignment = (Map<String, List<TopicPartition>>) ois.readObject();
            log.debug("Got currentAssignment = " + currentAssignment);
        } catch (IOException | ClassNotFoundException exc) {
            log.error("Failed to deserialize assignment userdata into currentAssignment", exc);
            currentAssignment.clear();
        }
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
        Set<String> consumers = assignment.keySet();
        Set<String> consumersCopy = new HashSet<>(consumers);
        for (String consumer: consumers) {
            consumersCopy.remove(consumer);
            int consumerAssignmentSize = assignment.get(consumer).size();
            for (String otherKey: consumersCopy)
                score += Math.abs(consumerAssignmentSize - assignment.get(otherKey).size());
        }
        return score;
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

    /**
     * Remove partition from partition assignments of the given consumer
     */
    private void deassignPartition(TopicPartition partition, String consumer, TreeSet<String> sortedCurrentSubscriptions, HashMap<TopicPartition, String> currentPartitionConsumer) {
        sortedCurrentSubscriptions.remove(consumer);
        currentAssignment.get(consumer).remove(partition);
        currentPartitionConsumer.remove(partition);
        sortedCurrentSubscriptions.add(consumer);
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
    private void balance(TreeSet<TopicPartition> sortedAllPartitions, TreeSet<TopicPartition> unassignedPartitions, TreeSet<String> sortedCurrentSubscriptions,
                         HashMap<String, List<TopicPartition>> consumer2AllPotentialPartitions, HashMap<TopicPartition, List<String>> partition2AllPotentialConsumers,
                         Map<String, List<TopicPartition>> oldAssignment, HashMap<TopicPartition, String> currentPartitionConsumer) {
        boolean initializing = currentAssignment.get(sortedCurrentSubscriptions.last()).isEmpty();
        boolean reassignmentPerformed = false;

        // assign all unassigned partitions
        while (!unassignedPartitions.isEmpty()) {
            TopicPartition partition = unassignedPartitions.pollFirst();

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
        sortedAllPartitions.removeAll(fixedPartitions);

        // narrow down the reassignment scope to only those consumers that are subject to reassignment
        Map<String, List<TopicPartition>> fixedAssignments = new HashMap<>();
        for (String consumer: consumer2AllPotentialPartitions.keySet())
            if (!canParticipateInReassignment(consumer, consumer2AllPotentialPartitions, partition2AllPotentialConsumers)) {
                sortedCurrentSubscriptions.remove(consumer);
                fixedAssignments.put(consumer, currentAssignment.remove(consumer));
            }

        // create a deep copy of the current assignment so we can revert to it if we do not get a more balanced assignment later
        Map<String, List<TopicPartition>> preBalanceAssignment = deepCopy(currentAssignment);
        HashMap<TopicPartition, String> preBalancePartitionConsumers = deepCopy(currentPartitionConsumer);

        // repeat reassignment until no partition can be moved to improve the balance
        boolean modified = true;
        while (modified) {
            modified = false;
            // reassign all reassignable partitions (starting from the partition with least potential consumers and if needed)
            // until the full list is processed or a balance is achieved
            Iterator<TopicPartition> partitionIterator = sortedAllPartitions.iterator();
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
                for (String otherConsumer: partition2AllPotentialConsumers.get(partition))
                    if (currentAssignment.get(consumer).size() > currentAssignment.get(otherConsumer).size() + 1) {
                        // de-assign partition from its current consumer
                        deassignPartition(partition, consumer, sortedCurrentSubscriptions, currentPartitionConsumer);
                        // reassign the partition to an eligible consumer with fewest assignments
                        assignPartition(partition, sortedCurrentSubscriptions, consumer2AllPotentialPartitions, currentPartitionConsumer);
                        reassignmentPerformed = true;
                        modified = true;
                        break;
                    }
            }
        }

        // if we are not preserving existing assignments and we have made changes to the current assignment
        // make sure we are getting a more balanced assignment; otherwise, revert to previous assignment
        if (!initializing && reassignmentPerformed && getBalanceScore(currentAssignment) >= getBalanceScore(preBalanceAssignment)) {
            deepCopy(preBalanceAssignment, currentAssignment);
            currentPartitionConsumer = preBalancePartitionConsumers;
        }

        // add the fixed assignments (those that could not change) back
        for (String consumer: fixedAssignments.keySet()) {
            currentAssignment.put(consumer, fixedAssignments.get(consumer));
            sortedCurrentSubscriptions.add(consumer);
        }
        fixedAssignments.clear();
    }

    private static class PartitionComparator implements Comparator<TopicPartition> {
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
    protected static class SubscriptionComparator implements Comparator<String> {
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
}
