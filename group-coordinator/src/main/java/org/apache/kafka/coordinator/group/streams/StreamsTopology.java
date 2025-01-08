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
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contains all information related to a topology of a Streams group.
 * <p>
 * This class is immutable and is fully backed by records stored in the __consumer_offsets topic.
 *
 * @param topologyEpoch The epoch of the topology.
 * @param subtopologies The subtopologies of the topology containing information about source topics,
 *                      repartition topics, changelog topics, co-partition groups etc.
 */
 public record StreamsTopology (int topologyEpoch,
                               Map<String, Subtopology> subtopologies) {

    /**
     * Returns the set of topics required by the topology.
     *
     * @return set of topics required by the topology
     */
     public Set<String> requiredTopics() {
        return subtopologies.values().stream()
            .flatMap(x ->
                Stream.concat(
                    Stream.concat(
                        x.sourceTopics().stream(),
                        x.repartitionSourceTopics().stream().map(TopicInfo::name)
                    ),
                    x.stateChangelogTopics().stream().map(TopicInfo::name)
                )
            ).collect(Collectors.toSet());
    }

    /**
     * Creates a instance of StreamsTopology from a StreamsGroupTopologyValue record.
     *
     * @param record StreamsGroupTopologyValue record
     * @return instance of StreamsTopology
     */
    public static StreamsTopology fromRecord(StreamsGroupTopologyValue record) {
        return new StreamsTopology(
            record.epoch(),
            record.subtopologies().stream().collect(Collectors.toMap(Subtopology::subtopologyId, x -> x))
        );
    }
}
