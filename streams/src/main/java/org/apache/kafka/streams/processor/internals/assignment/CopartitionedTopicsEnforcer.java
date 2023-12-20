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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CopartitionedTopicsEnforcer {
    private final String logPrefix;
    private final Logger log;

    public CopartitionedTopicsEnforcer(final String logPrefix) {
        this.logPrefix = logPrefix;
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());
    }

    public void enforce(final Set<String> copartitionGroup,
                        final Map<String, InternalTopicConfig> allRepartitionTopicsNumPartitions,
                        final Cluster metadata) {
        if (copartitionGroup.isEmpty()) {
            return;
        }

        final Map<Object, InternalTopicConfig> repartitionTopicConfigs =
            copartitionGroup.stream()
                            .filter(allRepartitionTopicsNumPartitions::containsKey)
                            .collect(Collectors.toMap(topic -> topic, allRepartitionTopicsNumPartitions::get));

        final Map<String, Integer> nonRepartitionTopicPartitions =
            copartitionGroup.stream().filter(topic -> !allRepartitionTopicsNumPartitions.containsKey(topic))
                            .collect(Collectors.toMap(topic -> topic, topic -> {
                                final Integer partitions = metadata.partitionCountForTopic(topic);
                                if (partitions == null) {
                                    final String str = String.format("%sTopic not found: %s", logPrefix, topic);
                                    log.error(str);
                                    throw new IllegalStateException(str);
                                } else {
                                    return partitions;
                                }
                            }));

        final int numPartitionsToUseForRepartitionTopics;
        final Collection<InternalTopicConfig> internalTopicConfigs = repartitionTopicConfigs.values();

        if (copartitionGroup.equals(repartitionTopicConfigs.keySet())) {
            final Collection<InternalTopicConfig> internalTopicConfigsWithEnforcedNumberOfPartitions = internalTopicConfigs
                .stream()
                .filter(InternalTopicConfig::hasEnforcedNumberOfPartitions)
                .collect(Collectors.toList());

            // if there's at least one repartition topic with enforced number of partitions
            // validate that they all have same number of partitions
            if (!internalTopicConfigsWithEnforcedNumberOfPartitions.isEmpty()) {
                numPartitionsToUseForRepartitionTopics = validateAndGetNumOfPartitions(
                    repartitionTopicConfigs,
                    internalTopicConfigsWithEnforcedNumberOfPartitions
                );
            } else {
                // If all topics for this co-partition group are repartition topics,
                // then set the number of partitions to be the maximum of the number of partitions.
                numPartitionsToUseForRepartitionTopics = getMaxPartitions(repartitionTopicConfigs);
            }
        } else {
            // Otherwise, use the number of partitions from external topics (which must all be the same)
            numPartitionsToUseForRepartitionTopics = getSamePartitions(nonRepartitionTopicPartitions);
        }

        // coerce all the repartition topics to use the decided number of partitions.
        for (final InternalTopicConfig config : internalTopicConfigs) {
            maybeSetNumberOfPartitionsForInternalTopic(numPartitionsToUseForRepartitionTopics, config);

            final int numberOfPartitionsOfInternalTopic = config
                .numberOfPartitions()
                .orElseThrow(emptyNumberOfPartitionsExceptionSupplier(config.name()));

            if (numberOfPartitionsOfInternalTopic != numPartitionsToUseForRepartitionTopics) {
                final String msg = String.format("%sNumber of partitions [%d] of repartition topic [%s] " +
                                                 "doesn't match number of partitions [%d] of the source topic.",
                                                 logPrefix,
                                                 numberOfPartitionsOfInternalTopic,
                                                 config.name(),
                                                 numPartitionsToUseForRepartitionTopics);
                throw new TopologyException(msg);
            }
        }
    }

    private static void maybeSetNumberOfPartitionsForInternalTopic(final int numPartitionsToUseForRepartitionTopics,
                                                                   final InternalTopicConfig config) {
        if (!config.hasEnforcedNumberOfPartitions()) {
            config.setNumberOfPartitions(numPartitionsToUseForRepartitionTopics);
        }
    }

    private int validateAndGetNumOfPartitions(final Map<Object, InternalTopicConfig> repartitionTopicConfigs,
                                              final Collection<InternalTopicConfig> internalTopicConfigs) {
        final InternalTopicConfig firstInternalTopicConfig = internalTopicConfigs.iterator().next();

        final int firstNumberOfPartitionsOfInternalTopic = firstInternalTopicConfig
            .numberOfPartitions()
            .orElseThrow(emptyNumberOfPartitionsExceptionSupplier(firstInternalTopicConfig.name()));

        for (final InternalTopicConfig internalTopicConfig : internalTopicConfigs) {
            final Integer numberOfPartitions = internalTopicConfig
                .numberOfPartitions()
                .orElseThrow(emptyNumberOfPartitionsExceptionSupplier(internalTopicConfig.name()));

            if (numberOfPartitions != firstNumberOfPartitionsOfInternalTopic) {
                final Map<Object, Integer> repartitionTopics = repartitionTopicConfigs
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().numberOfPartitions().get()));

                final String msg = String.format("%sFollowing topics do not have the same number of partitions: [%s]",
                                                 logPrefix,
                                                 new TreeMap<>(repartitionTopics));
                throw new TopologyException(msg);
            }
        }

        return firstNumberOfPartitionsOfInternalTopic;
    }

    private static Supplier<TopologyException> emptyNumberOfPartitionsExceptionSupplier(final String topic) {
        return () -> new TopologyException("Number of partitions is not set for topic: " + topic);
    }

    private int getSamePartitions(final Map<String, Integer> nonRepartitionTopicsInCopartitionGroup) {
        final int partitions = nonRepartitionTopicsInCopartitionGroup.values().iterator().next();
        for (final Entry<String, Integer> entry : nonRepartitionTopicsInCopartitionGroup.entrySet()) {
            if (entry.getValue() != partitions) {
                final TreeMap<String, Integer> sorted = new TreeMap<>(nonRepartitionTopicsInCopartitionGroup);
                throw new TopologyException(
                    String.format("%sTopics not co-partitioned: [%s]",
                                  logPrefix, sorted)
                );
            }
        }
        return partitions;
    }

    private int getMaxPartitions(final Map<Object, InternalTopicConfig> repartitionTopicsInCopartitionGroup) {
        int maxPartitions = 0;

        for (final InternalTopicConfig config : repartitionTopicsInCopartitionGroup.values()) {
            final Optional<Integer> partitions = config.numberOfPartitions();
            maxPartitions = Integer.max(maxPartitions, partitions.orElse(maxPartitions));
        }
        if (maxPartitions <= 0) {
            throw new IllegalStateException(logPrefix + "Could not validate the copartitioning of topics: " + repartitionTopicsInCopartitionGroup.keySet());
        }
        return maxPartitions;
    }

}
