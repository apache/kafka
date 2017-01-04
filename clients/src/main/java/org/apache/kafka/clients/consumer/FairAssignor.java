/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The fair assignor attempts to balance partitions across consumers such that each consumer is assigned approximately
 * the same number of partitions, even if the consumer topic subscriptions are substantially different (if they are
 * identical, then the result will be equivalent to that of the roundrobin assignor). The running total of assignments
 * per consumer is tracked as the algorithm executes in order to accomplish this.
 *
 * The algorithm starts with the topic with the fewest consumer subscriptions, and assigns its partitions in roundrobin
 * fashion. In the event of a tie for least subscriptions, the topic with the highest partition count is assigned
 * first, as this generally creates a more balanced distribution. The final tiebreaker is the topic name.
 *
 * The partitions for subsequent topics are assigned to the subscribing consumer with the fewest number of assignments.
 * In the event of a tie for least assignments, the tiebreaker is the consumer id, so that the assignment pattern is
 * deterministic and fairly similar to how the roundrobin assignor functions.
 *
 * For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions,
 * resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2. If both C0 and C1 are consuming t0, but only C1 is
 * consuming t1 then the assignment will be:
 * C0 -> [t0p0, t0p1, t0p2]
 * C1 -> [t1p0, t1p1, t1p2]
 */
public class FairAssignor extends AbstractPartitionAssignor {

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, List<String>> subscriptions) {
        List<String> consumers = Utils.sorted(subscriptions.keySet());

        // Invert topics-per-consumer map to consumers-per-topic.
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);

        // Map for tracking the total number of partitions assigned to each consumer
        Map<String, Integer> consumerAssignmentCounts = new HashMap<>();
        for (String consumer : consumers) {
            consumerAssignmentCounts.put(consumer, 0);
        }

        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<TopicPartition>());
        }

        Comparator<String> consumerComparator = new ConsumerFairness(consumerAssignmentCounts);
        for (TopicPartition partition : allPartitionsSorted(partitionsPerTopic, subscriptions, consumersPerTopic)) {
            // Find the most appropriate consumer for the partition.
            String assignedConsumer = null;
            for (String consumer : consumersPerTopic.get(partition.topic())) {
                if (assignedConsumer == null || consumerComparator.compare(consumer, assignedConsumer) < 0) {
                    assignedConsumer = consumer;
                }
            }

            consumerAssignmentCounts.put(assignedConsumer, consumerAssignmentCounts.get(assignedConsumer) + 1);
            assignment.get(assignedConsumer).add(partition);
        }

        return assignment;
    }

    private static List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
                                                            Map<String, List<String>> topicsPerConsumer,
                                                            Map<String, List<String>> consumersPerTopic) {
        // Collect all topics
        Set<String> topics = new HashSet<>();
        for (List<String> consumerTopics : topicsPerConsumer.values()) {
            topics.addAll(consumerTopics);
        }

        // Sort topics for optimal fairness, the general idea is to keep the most flexible assignment choices available
        // as long as possible by starting with the most constrained assignments.
        List<String> sortedTopics = new ArrayList<>(topics);
        Collections.sort(sortedTopics, new TopicOrder(partitionsPerTopic, consumersPerTopic));

        List<TopicPartition> allPartitions = new ArrayList<>();
        for (String topic : sortedTopics) {
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic != null)
                allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
        }
        return allPartitions;
    }

    @Override
    public String name() {
        return "fair";
    }

    private static class TopicOrder implements Comparator<String> {

        private final Map<String, Integer> topicConsumerCounts;
        private final Map<String, Integer> partitionsPerTopic;

        TopicOrder(Map<String, Integer> partitionsPerTopic, Map<String, List<String>> consumersPerTopic) {
            this.partitionsPerTopic = partitionsPerTopic;
            this.topicConsumerCounts = new HashMap<>();
            for (Map.Entry<String, List<String>> consumersPerTopicEntry : consumersPerTopic.entrySet()) {
                topicConsumerCounts.put(consumersPerTopicEntry.getKey(), consumersPerTopicEntry.getValue().size());
            }
        }

        @Override
        public int compare(String t1, String t2) {
            // Assign topics with fewer consumers first, tiebreakers are who has more partitions then topic name
            int comparison = Integer.compare(topicConsumerCounts.get(t1), topicConsumerCounts.get(t2));
            if (comparison == 0) {
                comparison = -Integer.compare(partitionsPerTopic.get(t1), partitionsPerTopic.get(t2));
                if (comparison == 0) {
                    comparison = t1.compareTo(t2);
                }
            }

            return comparison;
        }
    }

    private static class ConsumerFairness implements Comparator<String> {

        private final Map<String, Integer> consumerAssignmentCounts;

        ConsumerFairness(Map<String, Integer> consumerAssignmentCounts) {
            this.consumerAssignmentCounts = consumerAssignmentCounts;
        }

        @Override
        public int compare(String c1, String c2) {
            // Prefer consumer with fewer assignments, tiebreaker is consumer id
            int comparison = Integer.compare(consumerAssignmentCounts.get(c1), consumerAssignmentCounts.get(c2));
            if (comparison == 0) {
                comparison = c1.compareTo(c2);
            }
            return comparison;
        }
    }
}
