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

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class is responsible for enforcing the number of partitions in copartitioned topics. For each copartition group, it checks whether
 * the number of partitions for all repartition topics is the same, and enforces copartitioning for repartition topics whose number of
 * partitions is not enforced by the topology.
 */
public class CopartitionedTopicsEnforcer {

    private final Logger log;
    private final Function<String, OptionalInt> topicPartitionCountProvider;

    /**
     * The constructor for the class.
     *
     * @param logContext                  The context for emitting log messages.
     * @param topicPartitionCountProvider Returns the number of partitions for a given topic, representing the current state of the broker
     *                                    as well as any partition number decisions that have already been made. In particular, we expect
     *                                    the number of partitions for all topics in all co-partitions groups to be defined.
     */
    public CopartitionedTopicsEnforcer(final LogContext logContext,
                                       final Function<String, OptionalInt> topicPartitionCountProvider) {
        this.log = logContext.logger(getClass());
        this.topicPartitionCountProvider = topicPartitionCountProvider;
    }

    /**
     * Enforces the number of partitions for copartitioned topics.
     *
     * @param copartitionedTopics          The set of copartitioned topics (external source topics and repartition topics).
     * @param fixedRepartitionTopics       The set of repartition topics whose partition count is fixed by the topology sent by the
     *                                     client (in particular, when the user uses `repartition` in the DSL).
     * @param flexibleRepartitionTopics    The set of repartition topics whose partition count is flexible, and can be changed.
     *
     * @throws IllegalStateException If the partition count for any topic in copartitionedTopics is not defined by
     *                               topicPartitionCountProvider.
     *
     * @return A map from all repartition topics in copartitionedTopics to their updated partition counts.
     */
    public Map<String, Integer> enforce(final Set<String> copartitionedTopics,
                                        final Set<String> fixedRepartitionTopics,
                                        final Set<String> flexibleRepartitionTopics) throws StreamsInvalidTopologyException {
        if (copartitionedTopics.isEmpty()) {
            log.debug("Ignoring unexpected empty copartitioned topics set.");
            return Collections.emptyMap();
        }
        final Map<String, Integer> returnedPartitionCounts = new HashMap<>();

        final Map<String, Integer> repartitionTopicPartitionCounts =
            copartitionedTopics.stream()
                .filter(x -> fixedRepartitionTopics.contains(x) || flexibleRepartitionTopics.contains(x))
                .collect(Collectors.toMap(topic -> topic, this::getPartitionCount));

        final Map<String, Integer> nonRepartitionTopicPartitions =
            copartitionedTopics.stream().filter(topic -> !repartitionTopicPartitionCounts.containsKey(topic))
                .collect(Collectors.toMap(topic -> topic, this::getPartitionCount));

        final int numPartitionsToUseForRepartitionTopics;

        if (copartitionedTopics.equals(repartitionTopicPartitionCounts.keySet())) {

            // if there's at least one repartition topic with fixed number of partitions
            // validate that they all have same number of partitions
            if (!fixedRepartitionTopics.isEmpty()) {
                numPartitionsToUseForRepartitionTopics = validateAndGetNumOfPartitions(
                    repartitionTopicPartitionCounts,
                    fixedRepartitionTopics
                );
            } else {
                // If all topics for this co-partition group are repartition topics,
                // then set the number of partitions to be the maximum of the number of partitions.
                numPartitionsToUseForRepartitionTopics = getMaxPartitions(repartitionTopicPartitionCounts);
            }
        } else {
            // Otherwise, use the number of partitions from external topics (which must all be the same)
            numPartitionsToUseForRepartitionTopics = getSamePartitions(nonRepartitionTopicPartitions);
        }

        // coerce all the repartition topics to use the decided number of partitions.
        for (final Entry<String, Integer> repartitionTopic : repartitionTopicPartitionCounts.entrySet()) {
            returnedPartitionCounts.put(repartitionTopic.getKey(), numPartitionsToUseForRepartitionTopics);
            if (fixedRepartitionTopics.contains(repartitionTopic.getKey())
                && repartitionTopic.getValue() != numPartitionsToUseForRepartitionTopics) {
                final String msg = String.format("Number of partitions [%d] of repartition topic [%s] " +
                        "doesn't match number of partitions [%d] of the source topic.",
                    repartitionTopic.getValue(),
                    repartitionTopic.getKey(),
                    numPartitionsToUseForRepartitionTopics);
                throw TopicConfigurationException.incorrectlyPartitionedTopics(msg);
            }
        }

        return returnedPartitionCounts;
    }

    private int getPartitionCount(final String topicName) {
        OptionalInt partitions = topicPartitionCountProvider.apply(topicName);
        if (partitions.isPresent()) {
            return partitions.getAsInt();
        } else {
            throw new IllegalStateException("Number of partitions is not set for topic: " + topicName);
        }
    }

    private int validateAndGetNumOfPartitions(final Map<String, Integer> repartitionTopics,
                                              final Collection<String> fixedRepartitionTopics) {
        final String firstTopicName = fixedRepartitionTopics.iterator().next();

        final int firstNumberOfPartitionsOfInternalTopic = getPartitionCount(firstTopicName);

        for (final String topicName : fixedRepartitionTopics) {
            final int numberOfPartitions = getPartitionCount(topicName);

            if (numberOfPartitions != firstNumberOfPartitionsOfInternalTopic) {
                final String msg = String.format("Following topics do not have the same number of partitions: [%s]",
                    new TreeMap<>(repartitionTopics));
                throw TopicConfigurationException.incorrectlyPartitionedTopics(msg);
            }
        }

        return firstNumberOfPartitionsOfInternalTopic;
    }

    private int getSamePartitions(final Map<String, Integer> nonRepartitionTopicsInCopartitionGroup) {
        final int partitions = nonRepartitionTopicsInCopartitionGroup.values().iterator().next();
        for (final Entry<String, Integer> entry : nonRepartitionTopicsInCopartitionGroup.entrySet()) {
            if (entry.getValue() != partitions) {
                final TreeMap<String, Integer> sorted = new TreeMap<>(nonRepartitionTopicsInCopartitionGroup);
                throw TopicConfigurationException.incorrectlyPartitionedTopics(
                    String.format("Following topics do not have the same number of partitions: [%s]", sorted));
            }
        }
        return partitions;
    }

    private int getMaxPartitions(final Map<String, Integer> repartitionTopicsInCopartitionGroup) {
        int maxPartitions = 0;

        for (final Integer numPartitions : repartitionTopicsInCopartitionGroup.values()) {
            maxPartitions = Integer.max(maxPartitions, numPartitions);
        }
        if (maxPartitions == 0) {
            throw new StreamsInvalidTopologyException("All topics in the copartition group had undefined partition number: " +
                repartitionTopicsInCopartitionGroup.keySet());
        }
        return maxPartitions;
    }

}
