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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.coordinator.group.streams.assignor.TopologyDescriber;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The topology metadata class is used by the {@link org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor} to obtain topic and
 * partition metadata for the topology that the streams group using.
 *
 * @param topicMetadata The topic Ids mapped to their corresponding {@link TopicMetadata} object, which contains topic and partition
 *                      metadata.
 */
public record TopologyMetadata(Map<String, TopicMetadata> topicMetadata, ConfiguredTopology topology) implements TopologyDescriber {

    public TopologyMetadata {
        Objects.requireNonNull(topicMetadata);
        Objects.requireNonNull(topology);
    }

    /**
     * Map of topic names to topic metadata.
     *
     * @return The map of topic Ids to topic metadata.
     */
    @Override
    public Map<String, TopicMetadata> topicMetadata() {
        return this.topicMetadata;
    }

    @Override
    public boolean isStateful(String subtopologyId) {
        final ConfiguredSubtopology subtopology = getSubtopologyOrFail(subtopologyId);
        return !subtopology.stateChangelogTopics().isEmpty();
    }

    @Override
    public List<String> subtopologies() {
        return getSubtopologiesOrFail().keySet().stream().toList();
    }

    /**
     * The number of partitions for the given subtopology ID.
     *
     * @param subtopologyId ID of the corresponding subtopology
     * @return The number of partitions corresponding to the given subtopology ID, or -1 if the subtopology ID does not exist.
     */
    @Override
    public int numTasks(String subtopologyId) {
        final ConfiguredSubtopology subtopology = getSubtopologyOrFail(subtopologyId);
        return Stream.concat(
            subtopology.sourceTopics().stream(),
            subtopology.repartitionSourceTopics().keySet().stream()
        ).map(topic -> this.topicMetadata.get(topic).numPartitions()).max(Integer::compareTo).orElse(-1);
    }

    private ConfiguredSubtopology getSubtopologyOrFail(String subtopologyId) {
        final Map<String, ConfiguredSubtopology> subtopologies = getSubtopologiesOrFail();
        if (!subtopologies.containsKey(subtopologyId)) {
            throw new IllegalStateException(String.format("Topology does not contain subtopology %s", subtopologyId));
        }
        return subtopologies.get(subtopologyId);
    }

    private Map<String, ConfiguredSubtopology> getSubtopologiesOrFail() {
        final Optional<Map<String, ConfiguredSubtopology>> subtopologies = topology.subtopologies();
        if (subtopologies.isEmpty()) {
            throw new IllegalStateException("Topology is not configured");
        }
        return subtopologies.get();
    }
}
