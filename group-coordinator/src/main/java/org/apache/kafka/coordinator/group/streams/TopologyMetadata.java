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

import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.streams.assignor.TopologyDescriber;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * The topology metadata class is used by the {@link org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor} to get topic and
 * partition metadata for the topology that the streams group is using.
 * <p>
 * This class uses pre-computed partition counts (computed by InternalTopicManager),
 * providing O(1) lookups for maxNumInputPartitions.
 */
public class TopologyMetadata implements TopologyDescriber {

    private final StreamsTopology topology;
    private final Map<String, Integer> numTasksBySubtopology;

    /**
     * Constructs TopologyMetadata with pre-computed max partition counts for each subtopology.
     *
     * @param topology                    The streams topology.
     * @param numTasksBySubtopology Pre-computed max partition counts per subtopology.
     */
    public TopologyMetadata(StreamsTopology topology, Map<String, Integer> numTasksBySubtopology) {
        this.topology = Objects.requireNonNull(topology, "topology can't be null");
        Objects.requireNonNull(numTasksBySubtopology, "numTasksBySubtopology can't be null");
        this.numTasksBySubtopology = Collections.unmodifiableMap(numTasksBySubtopology);
    }

    /**
     * Returns the underlying StreamsTopology.
     *
     * @return The streams topology.
     */
    public StreamsTopology topology() {
        return topology;
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
        StreamsGroupTopologyValue.Subtopology subtopology = getSubtopologyOrFail(subtopologyId);
        return !subtopology.stateChangelogTopics().isEmpty();
    }

    /**
     * The list of subtopologies in the topology.
     *
     * @return a list of subtopology IDs.
     */
    @Override
    public List<String> subtopologies() {
        return topology.subtopologies().keySet().stream().toList();
    }

    /**
     * The maximal number of input partitions among all source topics for the given subtopology.
     *
     * @param subtopologyId String identifying the subtopology.
     *
     * @throws NoSuchElementException if the subtopology ID does not exist.
     * @return The maximal number of input partitions among all source topics for the given subtopology.
     */
    @Override
    public int maxNumInputPartitions(String subtopologyId) {
        Integer cached = numTasksBySubtopology.get(subtopologyId);
        if (cached == null) {
            throw new NoSuchElementException(String.format("Topology does not contain subtopology %s", subtopologyId));
        }
        return cached;
    }

    private StreamsGroupTopologyValue.Subtopology getSubtopologyOrFail(String subtopologyId) {
        StreamsGroupTopologyValue.Subtopology subtopology = topology.subtopologies().get(subtopologyId);
        if (subtopology == null) {
            throw new NoSuchElementException(String.format("Topology does not contain subtopology %s", subtopologyId));
        }
        return subtopology;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopologyMetadata that = (TopologyMetadata) o;
        return Objects.equals(topology, that.topology) &&
               Objects.equals(numTasksBySubtopology, that.numTasksBySubtopology);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topology, numTasksBySubtopology);
    }

    @Override
    public String toString() {
        return "TopologyMetadata{" +
            "topology=" + topology +
            ", numTasksBySubtopology=" + numTasksBySubtopology +
            '}';
    }
}
