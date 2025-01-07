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

import org.apache.kafka.common.errors.StreamsInvalidTopologyException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;

/**
 * Responsible for configuring the number of partitions in repartitioning topics. It computes a fix-point iteration, deriving the number of
 * partitions for each repartition topic based on the number of partitions of the source topics of the topology, if the number of
 * partitions is not explicitly set in the topology.
 */
public class RepartitionTopics {

    private final Logger log;
    private final Collection<Subtopology> subtopologies;
    private final Function<String, OptionalInt> topicPartitionCountProvider;

    /**
     * The constructor for the class.
     *
     * @param logContext                   The context for emitting log messages.
     * @param subtopologies                The subtopologies for the requested topology.
     * @param topicPartitionCountProvider  Returns the number of partitions for a given topic, representing the current state of the
     *                                     broker. This class requires the number of partition for all source topics to be defined.
     */
    public RepartitionTopics(final LogContext logContext,
                             final Collection<Subtopology> subtopologies,
                             final Function<String, OptionalInt> topicPartitionCountProvider) {
        this.log = logContext.logger(getClass());
        this.subtopologies = subtopologies;
        this.topicPartitionCountProvider = topicPartitionCountProvider;
    }

    /**
     * Returns the set of the number of partitions for each repartition topic.
     *
     * @return the map of repartition topics for the requested topology to their required number of partitions.
     *
     * @throws IllegalStateException if the number of partitions for a source topic is not defined by topicPartitionCountProvider.
     * @throws StreamsInvalidTopologyException if the number of partitions for all repartition topics cannot be determined, e.g.
     *         because of loops, or if a repartition source topic is not a sink topic of any subtopology.
     */
    public Map<String, Integer> setup() {
        final Set<String> missingSourceTopicsForTopology = new HashSet<>();

        for (final Subtopology subtopology : subtopologies) {
            final Set<String> missingSourceTopicsForSubtopology = computeMissingExternalSourceTopics(subtopology);
            missingSourceTopicsForTopology.addAll(missingSourceTopicsForSubtopology);
        }

        if (!missingSourceTopicsForTopology.isEmpty()) {
            throw new IllegalStateException(String.format("Missing source topics: %s",
                String.join(", ", missingSourceTopicsForTopology)));
        }

        final Map<String, Integer> repartitionTopicPartitionCount = computeRepartitionTopicPartitionCount();

        for (final Subtopology subtopology : subtopologies) {
            if (subtopology.repartitionSourceTopics().stream().anyMatch(repartitionTopic -> !repartitionTopicPartitionCount.containsKey(repartitionTopic.name()))) {
                throw new StreamsInvalidTopologyException("Failed to compute number of partitions for all repartition topics, because "
                    + "a repartition source topic is never used as a sink topic.");
            }
        }

        return repartitionTopicPartitionCount;
    }

    private Set<String> computeMissingExternalSourceTopics(final Subtopology subtopology) {
        final Set<String> missingExternalSourceTopics = new HashSet<>(subtopology.sourceTopics());
        for (final TopicInfo topicInfo : subtopology.repartitionSourceTopics()) {
            missingExternalSourceTopics.remove(topicInfo.name());
        }
        missingExternalSourceTopics.removeIf(x -> topicPartitionCountProvider.apply(x).isPresent());
        return missingExternalSourceTopics;
    }

    /**
     * Computes the number of partitions and returns it for each repartition topic.
     */
    private Map<String, Integer> computeRepartitionTopicPartitionCount() {
        boolean partitionCountNeeded;
        Map<String, Integer> repartitionTopicPartitionCounts = new HashMap<>();

        for (final Subtopology subtopology : subtopologies) {
            for (final TopicInfo repartitionSourceTopic : subtopology.repartitionSourceTopics()) {
                if (repartitionSourceTopic.partitions() != 0) {
                    repartitionTopicPartitionCounts.put(repartitionSourceTopic.name(), repartitionSourceTopic.partitions());
                }
            }
        }

        do {
            partitionCountNeeded = false;
            // avoid infinitely looping without making any progress on unknown repartitions
            boolean progressMadeThisIteration = false;

            for (final Subtopology subtopology : subtopologies) {
                for (final String repartitionSinkTopic : subtopology.repartitionSinkTopics()) {
                    if (!repartitionTopicPartitionCounts.containsKey(repartitionSinkTopic)) {
                        final Integer numPartitions = computePartitionCount(
                            repartitionTopicPartitionCounts,
                            subtopology
                        );

                        if (numPartitions == null) {
                            partitionCountNeeded = true;
                            log.trace("Unable to determine number of partitions for {}, another iteration is needed",
                                repartitionSinkTopic);
                        } else {
                            log.trace("Determined number of partitions for {} to be {}",
                                repartitionSinkTopic,
                                numPartitions);
                            repartitionTopicPartitionCounts.put(repartitionSinkTopic, numPartitions);
                            progressMadeThisIteration = true;
                        }
                    }
                }
            }
            if (!progressMadeThisIteration && partitionCountNeeded) {
                throw new StreamsInvalidTopologyException("Failed to compute number of partitions for all " +
                    "repartition topics. There may be loops in the topology that cannot be resolved.");
            }
        } while (partitionCountNeeded);

        return repartitionTopicPartitionCounts;
    }

    private Integer computePartitionCount(final Map<String, Integer> repartitionTopicPartitionCounts,
                                          final Subtopology subtopology) {
        Integer partitionCount = null;
        // try set the number of partitions for this repartition topic if it is not set yet
        // use the maximum of all its source topic partitions as the number of partitions

        // It is possible that there is another internal topic, i.e,
        // map().join().join(map())
        for (final TopicInfo repartitionSourceTopic : subtopology.repartitionSourceTopics()) {
            Integer numPartitionsCandidate = repartitionTopicPartitionCounts.get(repartitionSourceTopic.name());
            if (numPartitionsCandidate != null && (partitionCount == null || numPartitionsCandidate > partitionCount)) {
                partitionCount = numPartitionsCandidate;
            }
        }
        for (final String externalSourceTopic : subtopology.sourceTopics()) {
            final OptionalInt actualPartitionCount = topicPartitionCountProvider.apply(externalSourceTopic);
            if (actualPartitionCount.isPresent() && (partitionCount == null || actualPartitionCount.getAsInt() > partitionCount)) {
                partitionCount = actualPartitionCount.getAsInt();
            }
        }
        return partitionCount;
    }
}
