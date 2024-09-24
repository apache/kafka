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
package org.apache.kafka.coordinator.group.streams.topics;

import java.util.List;
import java.util.function.Function;
import org.apache.kafka.common.errors.StreamsInvalidTopologyException;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class RepartitionTopics {

    private final Map<String, TopicsInfo> subtopologyToTopicsInfo;
    private final Collection<Set<String>> copartitionGroups;
    private final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;
    private final Function<String, Integer> topicPartitionCountProvider;
    private final Logger log;

    private final Map<String, Set<String>> missingInputTopicsBySubtopology = new HashMap<>();

    public RepartitionTopics(final LogContext logContext,
                             final Map<String, TopicsInfo> subtopologyToTopicsInfo,
                             final Collection<Set<String>> copartitionGroups,
                             final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer,
                             final Function<String, Integer> topicPartitionCountProvider) {
        this.subtopologyToTopicsInfo = subtopologyToTopicsInfo;
        this.copartitionGroups = copartitionGroups;
        this.copartitionedTopicsEnforcer = copartitionedTopicsEnforcer;
        this.topicPartitionCountProvider = topicPartitionCountProvider;
        this.log = logContext.logger(getClass());
    }

    public Map<String, InternalTopicConfig> setup() {
        final Map<String, InternalTopicConfig> repartitionTopicMetadata = computeRepartitionTopicConfig();

        if (repartitionTopicMetadata.isEmpty()) {
            if (missingInputTopicsBySubtopology.isEmpty()) {
                log.info("Skipping the repartition topic validation since there are no repartition topics.");
            } else {
                log.info("Skipping the repartition topic validation since all topologies containing repartition"
                             + "topics are missing external user source topics and cannot be processed.");
            }
        } else {
            // ensure the co-partitioning topics within the group have the same number of partitions,
            // and enforce the number of partitions for those repartition topics to be the same if they
            // are co-partitioned as well.
            ensureCopartitioning(copartitionGroups, repartitionTopicMetadata);

        }
        return repartitionTopicMetadata;
    }

    public Queue<StreamsInvalidTopologyException> missingSourceTopicExceptions() {
        return missingInputTopicsBySubtopology.entrySet().stream().map(entry -> {
            final Set<String> missingSourceTopics = entry.getValue();
            final String subtopologyId = entry.getKey();

            return new StreamsInvalidTopologyException(String.format(
                    "Missing source topics %s for subtopology %s",
                    missingSourceTopics, subtopologyId));
        }).collect(Collectors.toCollection(LinkedList::new));
    }

    private Map<String, InternalTopicConfig> computeRepartitionTopicConfig() {
        final Set<TopicsInfo> allTopicInfos = new HashSet<>();
        final Map<String, InternalTopicConfig> allRepartitionTopicConfigs = new HashMap<>();

        final Set<TopicsInfo> topicsInfoForTopology = new HashSet<>();
        final Set<String> missingSourceTopicsForTopology = new HashSet<>();
        final Map<String, InternalTopicConfig> repartitionTopicConfigsForTopology = new HashMap<>();

        for (final Map.Entry<String, TopicsInfo> subtopologyEntry : subtopologyToTopicsInfo.entrySet()) {
            final TopicsInfo topicsInfo = subtopologyEntry.getValue();

            topicsInfoForTopology.add(topicsInfo);
            repartitionTopicConfigsForTopology.putAll(
                topicsInfo.repartitionSourceTopics()
                    .values()
                    .stream()
                    .collect(Collectors.toMap(InternalTopicConfig::name, topicConfig -> topicConfig)));

            final Set<String> missingSourceTopicsForSubtopology = computeMissingExternalSourceTopics(
                topicsInfo);
            missingSourceTopicsForTopology.addAll(missingSourceTopicsForSubtopology);
            if (!missingSourceTopicsForSubtopology.isEmpty()) {
                final String subtopology = subtopologyEntry.getKey();
                missingInputTopicsBySubtopology.put(subtopology, missingSourceTopicsForSubtopology);
                log.error("Subtopology {} has missing source topics {} and will be excluded from the current assignment, "
                    + "this can be due to the consumer client's metadata being stale or because they have "
                    + "not been created yet. Please verify that you have created all input topics; if they "
                    + "do exist, you just need to wait for the metadata to be updated, at which time a new "
                    + "rebalance will be kicked off automatically and the topology will be retried at that time.",
                    subtopology, missingSourceTopicsForSubtopology);
            }
        }

        if (missingSourceTopicsForTopology.isEmpty()) {
            allRepartitionTopicConfigs.putAll(repartitionTopicConfigsForTopology);
            allTopicInfos.addAll(topicsInfoForTopology);
        } else {
            log.debug("Skipping repartition topic validation for entire topology due to missing source topics {}", missingSourceTopicsForTopology);
        }

        setRepartitionSourceTopicPartitionCount(allRepartitionTopicConfigs, allTopicInfos);

        return allRepartitionTopicConfigs;
    }

    private void ensureCopartitioning(final Collection<Set<String>> copartitionGroups,
                                      final Map<String, InternalTopicConfig> repartitionTopicMetadata) {
        for (final Set<String> copartitionedTopics : copartitionGroups) {
            copartitionedTopicsEnforcer.enforce(copartitionedTopics, repartitionTopicMetadata);
        }
    }

    private Set<String> computeMissingExternalSourceTopics(final TopicsInfo topicsInfo) {
        final Set<String> missingExternalSourceTopics = new HashSet<>(topicsInfo.sourceTopics());
        missingExternalSourceTopics.removeAll(topicsInfo.repartitionSourceTopics().keySet());
        missingExternalSourceTopics.removeIf(x -> topicPartitionCountProvider.apply(x) != null);
        return missingExternalSourceTopics;
    }

    /**
     * Computes the number of partitions and sets it for each repartition topic in repartitionTopicMetadata
     */
    private void setRepartitionSourceTopicPartitionCount(final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                                         final Collection<TopicsInfo> topicGroups) {
        boolean partitionCountNeeded;
        do {
            partitionCountNeeded = false;
            boolean progressMadeThisIteration = false;  // avoid infinitely looping without making any progress on unknown repartitions

            for (final TopicsInfo topicsInfo : topicGroups) {
                for (final String repartitionSourceTopic : topicsInfo.repartitionSourceTopics()
                    .keySet()) {
                    final Optional<Integer> repartitionSourceTopicPartitionCount =
                        repartitionTopicMetadata.get(repartitionSourceTopic).numberOfPartitions();

                    if (!repartitionSourceTopicPartitionCount.isPresent()) {
                        final Integer numPartitions = computePartitionCount(
                            repartitionTopicMetadata,
                            topicGroups,
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
                throw new StreamsInvalidTopologyException("Failed to compute number of partitions for all repartition topics, " +
                    "make sure all user input topics are created and all Pattern subscriptions match at least one topic in the cluster");
            }
        } while (partitionCountNeeded);
    }

    private Integer computePartitionCount(final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                          final Collection<TopicsInfo> topicGroups,
                                          final String repartitionSourceTopic) {
        Integer partitionCount = null;
        // try set the number of partitions for this repartition topic if it is not set yet
        for (final TopicsInfo topicsInfo : topicGroups) {
            final List<String> repartitionSinkTopics = topicsInfo.repartitionSinkTopics();

            // TODO: Linear search, can be optimized
            if (repartitionSinkTopics.contains(repartitionSourceTopic)) {
                // if this topic is one of the sink topics of this topology,
                // use the maximum of all its source topic partitions as the number of partitions
                for (final String upstreamSourceTopic : topicsInfo.sourceTopics()) {
                    Integer numPartitionsCandidate = null;
                    // It is possible the sourceTopic is another internal topic, i.e,
                    // map().join().join(map())
                    if (repartitionTopicMetadata.containsKey(upstreamSourceTopic)) {
                        if (repartitionTopicMetadata.get(upstreamSourceTopic).numberOfPartitions().isPresent()) {
                            numPartitionsCandidate =
                                repartitionTopicMetadata.get(upstreamSourceTopic).numberOfPartitions().get();
                        }
                    } else {
                        final Integer count = topicPartitionCountProvider.apply(upstreamSourceTopic);
                        if (count == null) {
                            throw new StreamsInvalidTopologyException(
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
