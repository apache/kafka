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

import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.taskassignor.TopologyDescriber;

import java.util.Map;
import java.util.Objects;

/**
 * The topology metadata class is used by the {@link org.apache.kafka.coordinator.group.taskassignor.TaskAssignor} to obtain topic and
 * partition metadata for the topology that the streams group using.
 */
public class TopologyMetadata implements TopologyDescriber {

    /**
     * The topic Ids mapped to their corresponding {@link org.apache.kafka.coordinator.group.streams.TopicMetadata} object, which contains
     * topic and partition metadata.
     */
    private final Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> topicMetadata;

    private final StreamsTopology topology;

    public TopologyMetadata(
        Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> topicMetadata,
        StreamsTopology topology
    ) {
        this.topicMetadata = Objects.requireNonNull(topicMetadata);
        this.topology = Objects.requireNonNull(topology);
    }

    /**
     * Map of topic names to topic metadata.
     *
     * @return The map of topic Ids to topic metadata.
     */
    public Map<String, org.apache.kafka.coordinator.group.streams.TopicMetadata> topicMetadata() {
        return this.topicMetadata;
    }

    public StreamsTopology topology() {
        return topology;
    }

    @Override
    public boolean isStateful(String subtopologyId) {
        //TODO
        return false;
    }

    /**
     * The number of partitions for the given subtopology ID.
     *
     * @param subtopologyId ID of the corresponding subtopology
     * @return The number of partitions corresponding to the given subtopology ID, or -1 if the subtopology ID does not exist.
     */
    @Override
    public int numPartitions(String subtopologyId) {
        final Subtopology subtopology = topology.subtopologies().get(subtopologyId);
        if (subtopology == null) {
            return -1;
        }
        // TODO: We need to validate the validity of the subtopology here or somewhere else
        final String firstSourceTopic = subtopology.sourceTopics().get(0);
        if (firstSourceTopic == null) {
            return -1;
        }
        org.apache.kafka.coordinator.group.streams.TopicMetadata topic = this.topicMetadata.get(firstSourceTopic);
        return topic == null ? -1 : topic.numPartitions();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TopologyMetadata that = (TopologyMetadata) o;
        return Objects.equals(topicMetadata, that.topicMetadata) && Objects.equals(topology, that.topology);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicMetadata, topology);
    }

    @Override
    public String toString() {
        return "TopologyMetadata{" +
            "topicMetadata=" + topicMetadata +
            ", topology=" + topology +
            '}';
    }
}
