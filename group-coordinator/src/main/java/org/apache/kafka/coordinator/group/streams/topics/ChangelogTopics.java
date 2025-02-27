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
import java.util.stream.Collectors;

/**
 * This class is responsible for setting up the changelog topics for a topology. For a given topology, which does not have the number
 * of partitions specified for changelog partitions, this class will determine the number of partitions for each non-source changelog topic.
 */
public class ChangelogTopics {

    private final Logger log;
    private final Collection<Subtopology> subtopologies;
    private final Function<String, OptionalInt> topicPartitionCountProvider;

    /**
     * Constructor for ChangelogTopics.
     *
     * @param logContext                    The context for emitting log messages.
     * @param subtopologies                 The subtopologies for the requested topology.
     * @param topicPartitionCountProvider   Returns the number of partitions for a given topic, representing the current state of the broker
     *                                      as well as any partition number decisions that have already been made. In particular, we expect
     *                                      the number of partitions for all repartition topics defined, even if they do not exist in the
     *                                      broker yet.
     */
    public ChangelogTopics(
        final LogContext logContext,
        final Collection<Subtopology> subtopologies,
        final Function<String, OptionalInt> topicPartitionCountProvider
    ) {
        this.log = logContext.logger(getClass());
        this.subtopologies = subtopologies;
        this.topicPartitionCountProvider = topicPartitionCountProvider;
    }

    /**
     * Determines the number of partitions for each non-source changelog topic in the requested topology.
     *
     * @throws IllegalStateException If a source topic does not have a partition count defined through topicPartitionCountProvider.
     *
     * @return the map of all non-source changelog topics for the requested topology to their required number of partitions.
     */
    public Map<String, Integer> setup() {
        final Map<String, Integer> changelogTopicPartitions = new HashMap<>();
        for (Subtopology subtopology : subtopologies) {
            final Set<String> sourceTopics = new HashSet<>(subtopology.sourceTopics());

            final OptionalInt maxNumPartitions =
                subtopology.sourceTopics().stream().mapToInt(this::getPartitionCountOrFail).max();

            if (maxNumPartitions.isEmpty()) {
                throw new StreamsInvalidTopologyException("No source topics found for subtopology " + subtopology.subtopologyId());
            }
            for (final TopicInfo topicInfo : subtopology.stateChangelogTopics()) {
                if (!sourceTopics.contains(topicInfo.name())) {
                    changelogTopicPartitions.put(topicInfo.name(), maxNumPartitions.getAsInt());
                }
            }
        }

        log.debug("Expecting state changelog topic partitions {} for the requested topology.",
            changelogTopicPartitions.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")));

        return changelogTopicPartitions;
    }

    private int getPartitionCountOrFail(String topic) {
        final OptionalInt topicPartitionCount = topicPartitionCountProvider.apply(topic);
        if (topicPartitionCount.isEmpty()) {
            throw new IllegalStateException("No partition count for source topic " + topic);
        }
        return topicPartitionCount.getAsInt();
    }
}