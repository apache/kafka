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
import org.apache.kafka.common.errors.StreamsMissingSourceTopicsException;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;

/**
 * This class is responsible for setting up the changelog topics for a topology.
 */
public class ChangelogTopics {

    private final Map<String, ConfiguredSubtopology> subtopologies;
    private final Function<String, Integer> topicPartitionCountProvider;
    private final Logger log;

    public ChangelogTopics(
        final LogContext logContext,
        final Map<String, ConfiguredSubtopology> subtopologies,
        final Function<String, Integer> topicPartitionCountProvider
    ) {
        this.log = logContext.logger(getClass());
        this.subtopologies = subtopologies;
        this.topicPartitionCountProvider = topicPartitionCountProvider;
    }

    /**
     * Modifies the provided ConfiguredSubtopology to set the number of partitions for each changelog topic.
     *
     * @return the map of changelog topics for the requested topology that are internal and may need to be created.
     */
    public Map<String, ConfiguredInternalTopic> setup() {
        final Map<String, ConfiguredInternalTopic> changelogTopicMetadata = new HashMap<>();
        for (final Map.Entry<String, ConfiguredSubtopology> entry : subtopologies.entrySet()) {
            final ConfiguredSubtopology configuredSubtopology = entry.getValue();

            final OptionalInt maxNumPartitions =
                configuredSubtopology.sourceTopics().stream().mapToInt(this::getPartitionCountOrFail).max();

            if (!maxNumPartitions.isPresent()) {
                throw new StreamsInvalidTopologyException("No source topics found for subtopology " + entry.getKey());
            }
            for (final ConfiguredInternalTopic topicConfig : configuredSubtopology.nonSourceChangelogTopics()) {
                changelogTopicMetadata.put(topicConfig.name(), topicConfig);
                topicConfig.setNumberOfPartitions(maxNumPartitions.getAsInt());
            }
        }

        log.debug("Expecting state changelog topics {} for the requested topology.", changelogTopicMetadata.values());
        return changelogTopicMetadata;
    }

    private int getPartitionCountOrFail(String topic) {
        final Integer topicPartitionCount = topicPartitionCountProvider.apply(topic);
        if (topicPartitionCount == null) {
            throw new StreamsMissingSourceTopicsException("No partition count for source topic " + topic);
        }
        return topicPartitionCount;
    }
}