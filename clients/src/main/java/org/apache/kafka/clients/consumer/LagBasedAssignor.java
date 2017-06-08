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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The LagBasedAssignor operates on a per-topic basis, and attempts to assign partitions such that lag is
 * distributed evenly across a consumer group.
 *
 * For each topic, we first obtain the lag on all partitions.  Lag on a given partition is the difference between the
 * end offset and the last offset committed by the consumer group.  If no offsets have been committed for a partition we
 * determine the lag based on the {@code auto.offset.reset} property.  If {@code auto.offset.reset=latest}, we assign a
 * lag of 0.  If {@code auto.offset.reset=earliest} (or any other value) we assume assign lag equal to the total number
 * of message currently available in that partition.
 *
 * We then create a map storing the current total lag of all partitions assigned to each member of the consumer group.
 * Partitions are assigned in decreasing order of lag, with each partition assigned to the consumer with least total
 * number of assigned partitions, breaking ties by assigning to the consumer with the least total assigned lag.
 *
 * Distributing partitions as evenly across consumers (by count) ensures that the partition assignment is balanced when
 * all partitions have a current lag of 0 or if the distribution of lags is heavily skewed.  It also gives the consumer
 * group the best possible chance of remaining balanced if the assignment is retained for a long period.
 *
 * For example, suppose there are two consumers C0 and C1, both subscribed to a topic t0 having 3 partitions with the
 * following lags:
 * t0p0: 100,000
 * t0p1:  50,000
 * t0p2:  60,000
 *
 * The assignment will be:
 * C0: [t0p0]
 * C1: [t0p1, t0p2]
 *
 * The total lag or partitions assigned to each consumer will be:
 * C0: 100,000
 * C1: 110,000
 *
 * Compare this to the assignments made by the {@link org.apache.kafka.clients.consumer.RangeAssignor}:
 * C0: [t0p0, t0p1]
 * C1: [t0p2]
 *
 * The RangeAssignor results in a less balanced total lag for each consumer of:
 * C0: 160,000
 * C1:  50,000
 *
 * @see PartitionAssignor
 * @see org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor
 * @see org.apache.kafka.clients.consumer.RangeAssignor
 *
 * @author Grant Neale
 */
public class LagBasedAssignor implements PartitionAssignor, Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LagBasedAssignor.class);

    private Properties consumerGroupProps;
    private Properties metadataConsumerProps;
    private KafkaConsumer<byte[], byte[]> metadataConsumer;

    /**
     * This class is instantiated by reflection, at which point Kafka passes in the consumer config properties for
     * the current instance via this method.
     *
     * @param configs Kafka consumer configuration properties for the current instance
     */
    @Override
    public void configure(Map<String, ?> configs) {

        // Construct Properties from config map
        consumerGroupProps = new Properties();
        for (final Map.Entry<String, ?> prop : configs.entrySet()) {
            consumerGroupProps.put(prop.getKey(), prop.getValue());
        }

        // group.id must be defined
        final String groupId = consumerGroupProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        if (groupId == null) {
            throw new IllegalArgumentException(
                ConsumerConfig.GROUP_ID_CONFIG + " cannot be null when using "
                    + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + "="
                    + this.getClass().getName());
        }

        // Create a new consumer that can be used to get lag metadata for the consumer group
        metadataConsumerProps = new Properties();
        metadataConsumerProps.putAll(consumerGroupProps);
        metadataConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        final String clientId = groupId + ".assignor";
        metadataConsumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        LOGGER.debug("Configured for group.id = {}",  groupId);

    }

    @Override
    public String name() {
        return "lag";
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        return new Subscription(new ArrayList<>(topics));
    }

    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {

        final Set<String> allSubscribedTopics = new HashSet<>();
        final Map<String, List<String>> topicSubscriptions = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            List<String> topics = subscriptionEntry.getValue().topics();
            allSubscribedTopics.addAll(topics);
            topicSubscriptions.put(subscriptionEntry.getKey(), topics);
        }

        final Map<String, List<TopicPartitionLag>> topicLags = readTopicPartitionLags(metadata, allSubscribedTopics);
        Map<String, List<TopicPartition>> rawAssignments = assign(topicLags, topicSubscriptions);

        // this class has maintains no user data, so just wrap the results
        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        return assignments;
    }

    @Override
    public void onAssignment(Assignment assignment) {
        // nothing to do
    }

    /**
     * Perform the group assignment based on the lag on each partition for the current consumer group.
     *
     * @param partitionLagPerTopic Map from topic to a list of the lag on each partition for the current consumer group
     * @param subscriptions      Map from the memberId to their respective topic subscription
     * @return Map from each member to the list of partitions assigned to them.
     */
    static Map<String, List<TopicPartition>> assign(
        Map<String, List<TopicPartitionLag>> partitionLagPerTopic,
        Map<String, List<String>> subscriptions
    ) {

        final Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<TopicPartition>());
        }

        final Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            List<TopicPartitionLag> partitionLag = partitionLagPerTopic.get(topicEntry.getKey());
            if (partitionLag == null) {
                partitionLag = Collections.emptyList();
            }
            assignTopic(
                assignment,
                topicEntry.getKey(),
                topicEntry.getValue(),
                partitionLag
            );
        }

        return assignment;

    }

    /**
     * Assign partitions for the current topic, taking into account the current lag for each partition.
     * <p>
     * To balance load across the cluster according to lag, partitions are sorted in order of descending lag, such that
     * those with the largest lag ar assigned first.
     * <p>
     * We also keep track of the total lag of all partitions assigned to each consumer.  Each partition is assigned
     * to the consumer that currently has the lowest total lag across all assigned partitions.
     *
     * @param assignment    map to which assignment will be appended by this method
     * @param topic         topic to perform assignment for
     * @param consumers     consumers subscribed to the topic
     * @param partitionLags current consumer group lag for each partition in the topic
     */
    private static void assignTopic(
        final Map<String, List<TopicPartition>> assignment,
        final String topic,
        final List<String> consumers,
        final List<TopicPartitionLag> partitionLags
    ) {

        if (consumers.isEmpty()) {
            return;
        }

        // Track total lag assigned to each consumer (for the current topic)
        final Map<String, Long> consumerTotalLags = new HashMap<>(consumers.size());
        for (String memberId : consumers) {
            consumerTotalLags.put(memberId, 0L);
        }

        // Track total number of partitions assigned to each consumer (for the current topic)
        final Map<String, Integer> consumerTotalPartitions = new HashMap<>(consumers.size());
        for (String memberId : consumers) {
            consumerTotalPartitions.put(memberId, 0);
        }

        // Assign partitions in descending order of lag, then ascending by partition
        Collections.sort(partitionLags, new Comparator<TopicPartitionLag>() {
            @Override
            public int compare(TopicPartitionLag p1, TopicPartitionLag p2) {
                // If lag is equal, lowest partition id first
                if (p1.getLag() == p2.getLag()) {
                    return Integer.compare(p1.getPartition(), p2.getPartition());
                }
                // Highest lag first
                return Long.compare(p2.getLag(), p1.getLag());
            }
        });

        for (TopicPartitionLag partition : partitionLags) {

            // Assign to the consumer with least number of partitions, then smallest total lag, then smallest id
            final String memberId = Collections
                .min(
                    consumerTotalLags.entrySet(),
                    new Comparator<Entry<String, Long>>() {
                        @Override
                        public int compare(Entry<String, Long> c1, Entry<String, Long> c2) {
                            // Lowest partition count first
                            final int comparePartitionCount = Integer.compare(consumerTotalPartitions.get(c1.getKey()),
                                                                              consumerTotalPartitions.get(c2.getKey()));
                            if (comparePartitionCount != 0) {
                                return comparePartitionCount;
                            }

                            // If partition count is equal, lowest total lag first
                            final int compareTotalLags = Long.compare(c1.getValue(), c2.getValue());
                            if (compareTotalLags != 0) {
                                return compareTotalLags;
                            }

                            // If total lag is equal, lowest consumer id first
                            return c1.getKey().compareTo(c2.getKey());
                        }
                    }
                )
                .getKey();
            Long memberTotalLag = consumerTotalLags.get(memberId);
            if (memberTotalLag == null) {
                memberTotalLag = 0L;
            }
            Integer memberTotalPartitions = consumerTotalPartitions.get(memberId);
            if (memberTotalPartitions == null) {
                memberTotalPartitions = 0;
            }
            assignment.get(memberId).add(new TopicPartition(partition.getTopic(), partition.getPartition()));
            consumerTotalLags.put(memberId, memberTotalLag + partition.getLag());
            consumerTotalPartitions.put(memberId, memberTotalPartitions + 1);

            LOGGER.trace(
                "Assigned partition {}-{} to consumer {}.  partition_lag={}, consumer_current_total_lag={}",
                partition.getTopic(),
                partition.getPartition(),
                memberId,
                partition.getLag(),
                consumerTotalLags.get(memberId)
            );

        }

        // Log assignment and total consumer lags for current topic
        if (LOGGER.isDebugEnabled()) {

            final StringBuilder topicSummary = new StringBuilder();
            for (Map.Entry<String, Long> entry : consumerTotalLags.entrySet()) {

                final String memberId = entry.getKey();
                topicSummary.append(
                    String.format(
                        "\t%s (total_lag=%d)%n",
                        memberId,
                        consumerTotalLags.get(memberId)
                    )
                );

                if (assignment.get(memberId) != null) {
                    for (TopicPartition tp : assignment.get(memberId)) {
                        topicSummary.append(String.format("\t\t%s%n", tp));
                    }
                }

            }

            LOGGER.debug(
                "Assignment for {}:\n{}",
                topic,
                topicSummary
            );

        }

    }

    /**
     * Lookup the current consumer group lag for each partition
     *
     * @param metadata            cluster metadata
     * @param allSubscribedTopics a list of all topics subscribed to by at least one member of the consumer group
     * @return map from topic to the lag for each partition. Topics not in metadata will be excluded from this map.
     */
    private Map<String, List<TopicPartitionLag>> readTopicPartitionLags(
        final Cluster metadata,
        final Set<String> allSubscribedTopics
    ) {

        if (metadataConsumer == null) {
            metadataConsumer = new KafkaConsumer<>(metadataConsumerProps);
        }

        final Map<String, List<TopicPartitionLag>> topicPartitionLags = new HashMap<>();
        for (String topic : allSubscribedTopics) {

            final List<PartitionInfo> topicPartitionInfo = metadata.partitionsForTopic(topic);
            if (topicPartitionInfo != null && !topicPartitionInfo.isEmpty()) {

                final List<TopicPartition> topicPartitions = new ArrayList<>(topicPartitionInfo.size());
                for (PartitionInfo p : topicPartitionInfo) {
                    topicPartitions.add(new TopicPartition(p.topic(), p.partition()));
                }

                topicPartitionLags.put(topic, new ArrayList<TopicPartitionLag>());

                // Get begin/end offset in each partition
                final Map<TopicPartition, Long> topicBeginOffsets = metadataConsumer.beginningOffsets(topicPartitions);
                final Map<TopicPartition, Long> topicEndOffsets = metadataConsumer.endOffsets(topicPartitions);

                // Determine lag for each partition
                for (TopicPartition partition : topicPartitions) {

                    final OffsetAndMetadata partitionMetadata = metadataConsumer.committed(partition);
                    final String autoOffsetResetMode = consumerGroupProps
                        .getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                    final Long topicBeginOffset;
                    if (topicBeginOffsets.get(partition) != null) {
                        topicBeginOffset = topicBeginOffsets.get(partition);
                    } else {
                        topicBeginOffset = 0L;
                    }
                    final Long topicEndOffset;
                    if (topicEndOffsets.get(partition) != null) {
                        topicEndOffset = topicEndOffsets.get(partition);
                    } else {
                        topicEndOffset = 0L;
                    }

                    final long lag = computePartitionLag(
                        partitionMetadata,
                        topicBeginOffset,
                        topicEndOffset,
                        autoOffsetResetMode
                    );
                    topicPartitionLags.get(topic).add(new TopicPartitionLag(topic, partition.partition(), lag));

                }

            } else {
                LOGGER.warn("Skipping assignment for topic {} as no metadata is available", topic);
            }
        }

        return topicPartitionLags;

    }

    /**
     * Compute the current lag for a partition
     *
     * @param partitionMetadata   last committed offset for the partition
     * @param beginOffset         earliest available offset in the partition
     * @param endOffset           offset of next message to be appended to the partition
     * @param autoOffsetResetMode offset reset mode (earliest, latest, none)
     * @return the current lag
     */
    static long computePartitionLag(
        final OffsetAndMetadata partitionMetadata,
        final long beginOffset,
        final long endOffset,
        final String autoOffsetResetMode
    ) {

        final long nextOffset;
        if (partitionMetadata != null) {

            nextOffset = partitionMetadata.offset();

        } else {

            // No committed offset for this partition, set based on auto.offset.reset
            if (autoOffsetResetMode.equalsIgnoreCase("latest")) {
                nextOffset = endOffset;
            } else {
                // assume earliest
                nextOffset = beginOffset;
            }

        }

        // The max() protects against the unlikely case when reading the partition end offset fails
        // but reading the last committed offsets succeeds
        return Math.max(endOffset - nextOffset, 0L);

    }

    /**
     * @param subscriptions map from the memberId to their respective topic subscription
     * @return list of consumers subscribed to each topic
     */
    private static Map<String, List<String>> consumersPerTopic(Map<String, List<String>> subscriptions) {

        final Map<String, List<String>> consumersPerTopic = new HashMap<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : subscriptions.entrySet()) {

            final String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue()) {
                final List<String> topicConsumers;
                if (consumersPerTopic.get(topic) != null) {
                    topicConsumers = consumersPerTopic.get(topic);
                } else {
                    topicConsumers = new ArrayList<>();
                    consumersPerTopic.put(topic, topicConsumers);
                }
                topicConsumers.add(consumerId);
            }
        }

        return consumersPerTopic;

    }

    /**
     * Extends {@link TopicPartition} to include the lag for current consumer group on each partition
     */
    static class TopicPartitionLag {

        private final String topic;
        private final int partition;
        private final long lag;

        TopicPartitionLag(String topic, int partition, long lag) {
            this.topic = topic;
            this.partition = partition;
            this.lag = lag;
        }

        String getTopic() {
            return topic;
        }

        int getPartition() {
            return partition;
        }

        long getLag() {
            return lag;
        }

    }

}
