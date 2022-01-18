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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.assignment.CopartitionedTopicsEnforcer;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RepartitionTopics {

    private final InternalTopicManager internalTopicManager;
    private final TopologyMetadata topologyMetadata;
    private final Cluster clusterMetadata;
    private final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;
    private final Logger log;
    private final Map<TopicPartition, PartitionInfo> topicPartitionInfos = new HashMap<>();
    private final Map<String, Set<String>> missingUserInputTopicsPerTopology = new HashMap<>();

    public RepartitionTopics(final TopologyMetadata topologyMetadata,
                             final InternalTopicManager internalTopicManager,
                             final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer,
                             final Cluster clusterMetadata,
                             final String logPrefix) {
        this.topologyMetadata = topologyMetadata;
        this.internalTopicManager = internalTopicManager;
        this.clusterMetadata = clusterMetadata;
        this.copartitionedTopicsEnforcer = copartitionedTopicsEnforcer;
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());
    }

    /**
     * @return   true iff setup was completed successfully and all user input topics were verified to exist
     */
    public boolean setup() {
        final Map<String, Collection<TopicsInfo>> topicGroups = topologyMetadata.topicGroupsByTopology();
        final Map<String, InternalTopicConfig> repartitionTopicMetadata
            = computeRepartitionTopicConfig(topicGroups, clusterMetadata);

        if (repartitionTopicMetadata.isEmpty()) {
            if (missingUserInputTopicsPerTopology.isEmpty()) {
                log.info("Skipping the repartition topic validation since there are no repartition topics.");
            } else {
                log.info("Skipping the repartition topic validation since all topologies containing repartition"
                             + "topics are missing external user source topics and cannot be processed.");
            }
        } else {
            // ensure the co-partitioning topics within the group have the same number of partitions,
            // and enforce the number of partitions for those repartition topics to be the same if they
            // are co-partitioned as well.
            ensureCopartitioning(topologyMetadata.copartitionGroups(), repartitionTopicMetadata, clusterMetadata);

            // make sure the repartition source topics exist with the right number of partitions,
            // create these topics if necessary
            internalTopicManager.makeReady(repartitionTopicMetadata);

            // augment the metadata with the newly computed number of partitions for all the
            // repartition source topics
            for (final Map.Entry<String, InternalTopicConfig> entry : repartitionTopicMetadata.entrySet()) {
                final String topic = entry.getKey();
                final int numPartitions = entry.getValue().numberOfPartitions().orElse(-1);

                for (int partition = 0; partition < numPartitions; partition++) {
                    topicPartitionInfos.put(
                        new TopicPartition(topic, partition),
                        new PartitionInfo(topic, partition, null, new Node[0], new Node[0])
                    );
                }
            }
        }

        return missingUserInputTopicsPerTopology.isEmpty();
    }

    public Map<String, Set<String>> missingUserInputTopicsPerTopology() {
        return Collections.unmodifiableMap(missingUserInputTopicsPerTopology);
    }

    public Map<TopicPartition, PartitionInfo> topicPartitionsInfo() {
        return Collections.unmodifiableMap(topicPartitionInfos);
    }

    /**
     * @param topicGroups                            information about the topic groups (subtopologies) in this application
     * @param clusterMetadata                        cluster metadata, eg which topics exist on the brokers
     */
    private Map<String, InternalTopicConfig> computeRepartitionTopicConfig(final Map<String, Collection<TopicsInfo>> topicGroups,
                                                                           final Cluster clusterMetadata) {
        final Set<TopicsInfo> allTopicsInfo = new HashSet<>();
        final Map<String, InternalTopicConfig> allRepartitionTopicConfigs = new HashMap<>();
        for (final Map.Entry<String, Collection<TopicsInfo>> topology : topicGroups.entrySet()) {
            final String topologyName = topology.getKey();
            final Set<String> missingSourceTopics = new HashSet<>();
            final Map<String, InternalTopicConfig> repartitionTopicConfigsPerTopology = new HashMap<>();
            for (final TopicsInfo topicsInfo : topology.getValue()) {
                missingSourceTopics.addAll(computeMissingExternalSourceTopics(topicsInfo, clusterMetadata));
                repartitionTopicConfigsPerTopology.putAll(
                    topicsInfo.repartitionSourceTopics
                        .values()
                        .stream()
                        .collect(Collectors.toMap(InternalTopicConfig::name, topicConfig -> topicConfig)));
            }
            if (missingSourceTopics.isEmpty()) {
                allRepartitionTopicConfigs.putAll(repartitionTopicConfigsPerTopology);
                allTopicsInfo.addAll(topology.getValue());
            } else {
                missingUserInputTopicsPerTopology.put(topologyName, missingSourceTopics);
                log.error("Topology {} was missing source topics {} and will be excluded from the current assignment, "
                              + "this can be due to the consumer client's metadata being stale or because they have "
                              + "not been created yet. Please verify that you have created all input topics; if they "
                              + "do exist, you just need to wait for the metadata to be updated, at which time a new "
                              + "rebalance will be kicked off automatically and the topology will be retried at that time."
                              + topologyName, missingSourceTopics);
            }
        }
        setRepartitionSourceTopicPartitionCount(allRepartitionTopicConfigs, allTopicsInfo, clusterMetadata);

        return allRepartitionTopicConfigs;
    }

    private void ensureCopartitioning(final Collection<Set<String>> copartitionGroups,
                                      final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                      final Cluster clusterMetadata) {
        for (final Set<String> copartitionGroup : copartitionGroups) {
            copartitionedTopicsEnforcer.enforce(copartitionGroup, repartitionTopicMetadata, clusterMetadata);
        }
    }

    private Set<String> computeMissingExternalSourceTopics(final TopicsInfo topicsInfo,
                                                           final Cluster clusterMetadata) {
        final Set<String> missingExternalSourceTopics = new HashSet<>(topicsInfo.sourceTopics);
        missingExternalSourceTopics.removeAll(topicsInfo.repartitionSourceTopics.keySet());
        missingExternalSourceTopics.removeAll(clusterMetadata.topics());
        return missingExternalSourceTopics;
    }

    /**
     * Computes the number of partitions and sets it for each repartition topic in repartitionTopicMetadata
     */
    private void setRepartitionSourceTopicPartitionCount(final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                                         final Collection<TopicsInfo> topicGroups,
                                                         final Cluster clusterMetadata) {
        boolean partitionCountNeeded;
        do {
            partitionCountNeeded = false;
            boolean progressMadeThisIteration = false;  // avoid infinitely looping without making any progress on unknown repartitions

            for (final TopicsInfo topicsInfo : topicGroups) {
                for (final String repartitionSourceTopic : topicsInfo.repartitionSourceTopics.keySet()) {
                    final Optional<Integer> repartitionSourceTopicPartitionCount =
                        repartitionTopicMetadata.get(repartitionSourceTopic).numberOfPartitions();

                    if (!repartitionSourceTopicPartitionCount.isPresent()) {
                        final Integer numPartitions = computePartitionCount(
                            repartitionTopicMetadata,
                            topicGroups,
                            clusterMetadata,
                            repartitionSourceTopic
                        );

                        if (numPartitions == null) {
                            partitionCountNeeded = true;
                            log.trace("Unable to determine number of partitions for {}, another iteration is needed",
                                repartitionSourceTopic);
                        } else {
                            log.trace("Determined number of partitions for {} to be {}", repartitionSourceTopic, numPartitions);
                            repartitionTopicMetadata.get(repartitionSourceTopic).setNumberOfPartitions(numPartitions);
                            progressMadeThisIteration = true;
                        }
                    }
                }
            }
            if (!progressMadeThisIteration && partitionCountNeeded) {
                log.error("Unable to determine the number of partitions of all repartition topics, most likely a source topic is missing or pattern doesn't match any topics\n" +
                    "topic groups: {}\n" +
                    "cluster topics: {}.", topicGroups, clusterMetadata.topics());
                throw new TaskAssignmentException("Failed to compute number of partitions for all repartition topics, " +
                    "make sure all user input topics are created and all Pattern subscriptions match at least one topic in the cluster");
            }
        } while (partitionCountNeeded);
    }

    private Integer computePartitionCount(final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                          final Collection<TopicsInfo> topicGroups,
                                          final Cluster clusterMetadata,
                                          final String repartitionSourceTopic) {
        Integer partitionCount = null;
        // try set the number of partitions for this repartition topic if it is not set yet
        for (final TopicsInfo topicsInfo : topicGroups) {
            final Set<String> sinkTopics = topicsInfo.sinkTopics;

            if (sinkTopics.contains(repartitionSourceTopic)) {
                // if this topic is one of the sink topics of this topology,
                // use the maximum of all its source topic partitions as the number of partitions
                for (final String upstreamSourceTopic : topicsInfo.sourceTopics) {
                    Integer numPartitionsCandidate = null;
                    // It is possible the sourceTopic is another internal topic, i.e,
                    // map().join().join(map())
                    if (repartitionTopicMetadata.containsKey(upstreamSourceTopic)) {
                        if (repartitionTopicMetadata.get(upstreamSourceTopic).numberOfPartitions().isPresent()) {
                            numPartitionsCandidate =
                                repartitionTopicMetadata.get(upstreamSourceTopic).numberOfPartitions().get();
                        }
                    } else {
                        final Integer count = clusterMetadata.partitionCountForTopic(upstreamSourceTopic);
                        if (count == null) {
                            throw new TaskAssignmentException(
                                "No partition count found for source topic "
                                    + upstreamSourceTopic
                                    + ", but it should have been."
                            );
                        }
                        numPartitionsCandidate = count;
                    }

                    if (numPartitionsCandidate != null) {
                        if (partitionCount == null || numPartitionsCandidate > partitionCount) {
                            partitionCount = numPartitionsCandidate;
                        }
                    }
                }
            }
        }
        return partitionCount;
    }
}
