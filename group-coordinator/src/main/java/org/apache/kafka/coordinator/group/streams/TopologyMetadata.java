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

import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.streams.assignor.TopologyDescriber;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;

/**
 * The topology metadata class is used by the {@link org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor} to get topic and
 * partition metadata for the topology that the streams group using.
 *
 * @param metadataImage  The metadata image
 * @param subtopologyMap The configured subtopologies
 */
public record TopologyMetadata(CoordinatorMetadataImage metadataImage, SortedMap<String, ConfiguredSubtopology> subtopologyMap) implements TopologyDescriber {

    public TopologyMetadata {
        Objects.requireNonNull(metadataImage);
        subtopologyMap = Collections.unmodifiableSortedMap(Objects.requireNonNull(subtopologyMap));
    }

    /**
     * Checks whether the given subtopology is associated with a changelog topic.
     *
     * @param subtopologyId String identifying the subtopology.
     * @throws NoSuchElementException if the subtopology ID does not exist.
     * @return true if the subtopology is associated with a changelog topic, false otherwise.
     */
    @Override
    public boolean isStateful(String subtopologyId) {
        final ConfiguredSubtopology subtopology = getSubtopologyOrFail(subtopologyId);
        return !subtopology.stateChangelogTopics().isEmpty();
    }

    /**
     * The list of subtopologies in the topology.
     *
     * @return a list of subtopology IDs.
     */
    @Override
    public List<String> subtopologies() {
        return subtopologyMap.keySet().stream().toList();
    }

    /**
     * The maximal number of input partitions among all source topics for the given subtopology.
     *
     * @param subtopologyId String identifying the subtopology.
     *
     * @throws NoSuchElementException if the subtopology ID does not exist.
     * @throws IllegalStateException if the subtopology contains no source topics.
     * @return The maximal number of input partitions among all source topics for the given subtopology.
     */
    @Override
    public int maxNumInputPartitions(String subtopologyId) {
        final ConfiguredSubtopology subtopology = getSubtopologyOrFail(subtopologyId);
        return subtopology.numberOfTasks();
    }

    private ConfiguredSubtopology getSubtopologyOrFail(String subtopologyId) {
        if (!subtopologyMap.containsKey(subtopologyId)) {
            throw new NoSuchElementException(String.format("Topology does not contain subtopology %s", subtopologyId));
        }
        return subtopologyMap.get(subtopologyId);
    }

}
