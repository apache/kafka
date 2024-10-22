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

import org.apache.kafka.common.errors.StreamsMissingSourceTopicsException;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class is responsible for configuring the number of partitions in repartitioning topics.
 */
public class RepartitionTopics {

    private final Logger log;
    private final Map<String, ConfiguredSubtopology> subtopologyToConfiguredSubtopology;
    private final Function<String, Integer> topicPartitionCountProvider;

    private final Map<String, Set<String>> missingInputTopicsBySubtopology = new HashMap<>();

    public RepartitionTopics(final LogContext logContext,
                             final Map<String, ConfiguredSubtopology> subtopologyToConfiguredSubtopology,
                             final Function<String, Integer> topicPartitionCountProvider) {
        this.log = logContext.logger(getClass());
        this.subtopologyToConfiguredSubtopology = subtopologyToConfiguredSubtopology;
        this.topicPartitionCountProvider = topicPartitionCountProvider;
    }

    /**
     * Modifies the provided ConfiguredSubtopology to set the number of partitions for each repartition topic.
     *
     * @return the map of repartition topics for the requested topology that are internal and may need to be created.
     */
    public Map<String, ConfiguredInternalTopic> setup() {
        final Set<String> missingSourceTopicsForTopology = new HashSet<>();
        final Map<String, ConfiguredInternalTopic> configuredRepartitionTopics = new HashMap<>();

        for (final Map.Entry<String, ConfiguredSubtopology> subtopologyEntry : subtopologyToConfiguredSubtopology.entrySet()) {
            final ConfiguredSubtopology configuredSubtopology = subtopologyEntry.getValue();

            configuredRepartitionTopics.putAll(
                configuredSubtopology.repartitionSourceTopics()
                    .values()
                    .stream()
                    .collect(Collectors.toMap(ConfiguredInternalTopic::name, topicConfig -> topicConfig)));

            final Set<String> missingSourceTopicsForSubtopology = computeMissingExternalSourceTopics(configuredSubtopology);
            missingSourceTopicsForTopology.addAll(missingSourceTopicsForSubtopology);
            if (!missingSourceTopicsForSubtopology.isEmpty()) {
                final String subtopologyId = subtopologyEntry.getKey();
                missingInputTopicsBySubtopology.put(subtopologyId, missingSourceTopicsForSubtopology);
                log.error("Subtopology {} has missing source topics {} and will be excluded from the current assignment, "
                        + "this can be due to the consumer client's metadata being stale or because they have "
                        + "not been created yet. Please verify that you have created all input topics; if they "
                        + "do exist, you just need to wait for the metadata to be updated, at which time a new "
                        + "rebalance will be kicked off automatically and the topology will be retried at that time.",
                    subtopologyId, missingSourceTopicsForSubtopology);
            }
        }

        if (missingSourceTopicsForTopology.isEmpty()) {
            setRepartitionSourceTopicPartitionCount(configuredRepartitionTopics);
            return configuredRepartitionTopics;
        } else {
            Set<String> missingSourceTopics = missingInputTopicsBySubtopology.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
            throw new StreamsMissingSourceTopicsException(String.format("Missing source topics: %s",
                String.join(", ", missingSourceTopics)));
        }
    }

    private Set<String> computeMissingExternalSourceTopics(final ConfiguredSubtopology configuredSubtopology) {
        final Set<String> missingExternalSourceTopics = new HashSet<>(configuredSubtopology.sourceTopics());
        missingExternalSourceTopics.removeAll(configuredSubtopology.repartitionSourceTopics().keySet());
        missingExternalSourceTopics.removeIf(x -> topicPartitionCountProvider.apply(x) != null);
        return missingExternalSourceTopics;
    }

    /**
     * Computes the number of partitions and sets it for each repartition topic in repartitionTopicMetadata
     */
    private void setRepartitionSourceTopicPartitionCount(final Map<String, ConfiguredInternalTopic> repartitionTopicMetadata) {
        boolean partitionCountNeeded;
        do {
            partitionCountNeeded = false;
            // avoid infinitely looping without making any progress on unknown repartitions
            boolean progressMadeThisIteration = false;

            for (final ConfiguredSubtopology configuredSubtopology : subtopologyToConfiguredSubtopology.values()) {
                for (final String repartitionSourceTopic : configuredSubtopology.repartitionSourceTopics()
                    .keySet()) {
                    final Optional<Integer> repartitionSourceTopicPartitionCount =
                        repartitionTopicMetadata.get(repartitionSourceTopic).numberOfPartitions();

                    if (!repartitionSourceTopicPartitionCount.isPresent()) {
                        final Integer numPartitions = computePartitionCount(
                            repartitionTopicMetadata,
                            repartitionSourceTopic
                        );

                        if (numPartitions == null) {
                            partitionCountNeeded = true;
                            log.trace("Unable to determine number of partitions for {}, another iteration is needed",
                                repartitionSourceTopic);
                        } else {
                            log.trace("Determined number of partitions for {} to be {}",
                                repartitionSourceTopic,
                                numPartitions);
                            repartitionTopicMetadata.get(repartitionSourceTopic).setNumberOfPartitions(numPartitions);
                            progressMadeThisIteration = true;
                        }
                    }
                }
            }
            if (!progressMadeThisIteration && partitionCountNeeded) {
                throw new StreamsMissingSourceTopicsException("Failed to compute number of partitions for all " +
                    "repartition topics, make sure all user input topics are created and all pattern subscriptions " +
                    "match at least one topic in the cluster");
            }
        } while (partitionCountNeeded);
    }

    private Integer computePartitionCount(final Map<String, ConfiguredInternalTopic> repartitionTopicMetadata,
                                          final String repartitionSourceTopic) {
        Integer partitionCount = null;
        // try set the number of partitions for this repartition topic if it is not set yet
        for (final ConfiguredSubtopology configuredSubtopology : subtopologyToConfiguredSubtopology.values()) {
            final Set<String> repartitionSinkTopics = configuredSubtopology.repartitionSinkTopics();

            if (repartitionSinkTopics.contains(repartitionSourceTopic)) {
                // if this topic is one of the sink topics of this topology,
                // use the maximum of all its source topic partitions as the number of partitions
                for (final String upstreamSourceTopic : configuredSubtopology.sourceTopics()) {
                    Integer numPartitionsCandidate = null;
                    // It is possible the sourceTopic is another internal topic, i.e,
                    // map().join().join(map())
                    if (repartitionTopicMetadata.containsKey(upstreamSourceTopic)) {
                        if (repartitionTopicMetadata.get(upstreamSourceTopic).numberOfPartitions().isPresent()) {
                            numPartitionsCandidate =
                                repartitionTopicMetadata.get(upstreamSourceTopic).numberOfPartitions().get();
                        }
                    } else {
                        numPartitionsCandidate = topicPartitionCountProvider.apply(upstreamSourceTopic);
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
