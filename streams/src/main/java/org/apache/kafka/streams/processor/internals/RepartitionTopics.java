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
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
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
    private final InternalTopologyBuilder internalTopologyBuilder;
    private final Cluster clusterMetadata;
    private final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;
    private final Logger log;
    private final Map<TopicPartition, PartitionInfo> topicPartitionInfos = new HashMap<>();

    public RepartitionTopics(final InternalTopologyBuilder internalTopologyBuilder,
                             final InternalTopicManager internalTopicManager,
                             final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer,
                             final Cluster clusterMetadata,
                             final String logPrefix) {
        this.internalTopologyBuilder = internalTopologyBuilder;
        this.internalTopicManager = internalTopicManager;
        this.clusterMetadata = clusterMetadata;
        this.copartitionedTopicsEnforcer = copartitionedTopicsEnforcer;
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());
    }

    public void setup() {
        final Map<Subtopology, TopicsInfo> topicGroups = internalTopologyBuilder.topicGroups();
        final Map<String, InternalTopicConfig> repartitionTopicMetadata = computeRepartitionTopicConfig(topicGroups, clusterMetadata);

        // ensure the co-partitioning topics within the group have the same number of partitions,
        // and enforce the number of partitions for those repartition topics to be the same if they
        // are co-partitioned as well.
        ensureCopartitioning(internalTopologyBuilder.copartitionGroups(), repartitionTopicMetadata, clusterMetadata);

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

    public Map<TopicPartition, PartitionInfo> topicPartitionsInfo() {
        return Collections.unmodifiableMap(topicPartitionInfos);
    }

    private Map<String, InternalTopicConfig> computeRepartitionTopicConfig(final Map<Subtopology, TopicsInfo> topicGroups,
                                                                           final Cluster clusterMetadata) {

        final Map<String, InternalTopicConfig> repartitionTopicConfigs = new HashMap<>();
        for (final TopicsInfo topicsInfo : topicGroups.values()) {
            checkIfExternalSourceTopicsExist(topicsInfo, clusterMetadata);
            repartitionTopicConfigs.putAll(topicsInfo.repartitionSourceTopics.values().stream()
                .collect(Collectors.toMap(InternalTopicConfig::name, topicConfig -> topicConfig)));
        }
        setRepartitionSourceTopicPartitionCount(repartitionTopicConfigs, topicGroups, clusterMetadata);

        return repartitionTopicConfigs;
    }

    private void ensureCopartitioning(final Collection<Set<String>> copartitionGroups,
                                      final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                      final Cluster clusterMetadata) {
        for (final Set<String> copartitionGroup : copartitionGroups) {
            copartitionedTopicsEnforcer.enforce(copartitionGroup, repartitionTopicMetadata, clusterMetadata);
        }
    }

    private void checkIfExternalSourceTopicsExist(final TopicsInfo topicsInfo,
                                                  final Cluster clusterMetadata) {
        final Set<String> missingExternalSourceTopics = new HashSet<>(topicsInfo.sourceTopics);
        missingExternalSourceTopics.removeAll(topicsInfo.repartitionSourceTopics.keySet());
        missingExternalSourceTopics.removeAll(clusterMetadata.topics());
        if (!missingExternalSourceTopics.isEmpty()) {
            log.error("The following source topics are missing/unknown: {}. Please make sure all source topics " +
                    "have been pre-created before starting the Streams application. ",
                missingExternalSourceTopics);
            throw new MissingSourceTopicException("Missing source topics.");
        }
    }

    /**
     * Computes the number of partitions and sets it for each repartition topic in repartitionTopicMetadata
     */
    private void setRepartitionSourceTopicPartitionCount(final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                                         final Map<Subtopology, TopicsInfo> topicGroups,
                                                         final Cluster clusterMetadata) {
        boolean partitionCountNeeded;
        do {
            partitionCountNeeded = false;
            boolean progressMadeThisIteration = false;  // avoid infinitely looping without making any progress on unknown repartitions

            for (final TopicsInfo topicsInfo : topicGroups.values()) {
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
                            repartitionTopicMetadata.get(repartitionSourceTopic).setNumberOfPartitions(numPartitions);
                            progressMadeThisIteration = true;
                        }
                    }
                }
            }
            if (!progressMadeThisIteration && partitionCountNeeded) {
                throw new TaskAssignmentException("Failed to compute number of partitions for all repartition topics");
            }
        } while (partitionCountNeeded);
    }

    private Integer computePartitionCount(final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                          final Map<Subtopology, TopicsInfo> topicGroups,
                                          final Cluster clusterMetadata,
                                          final String repartitionSourceTopic) {
        Integer partitionCount = null;
        // try set the number of partitions for this repartition topic if it is not set yet
        for (final TopicsInfo topicsInfo : topicGroups.values()) {
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
