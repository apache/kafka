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

import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.stream.Collectors;

/**
 * This class captures the result of taking a topology definition sent by the client and using the current state of the topics inside the
 * broker to configure the internal topics required for the topology.
 *
 * @param topologyEpoch               The epoch of the topology. Same as the topology epoch in the heartbeat request that last initialized
 *                                    the topology.
 * @param subtopologies               Contains the subtopologies that have been configured. This can be used by the task assignors, since it
 *                                    specifies the number of tasks available for every subtopology. Undefined if topology configuration
 *                                    failed.
 * @param internalTopicsToBeCreated   Contains a list of internal topics that need to be created. This is used to create the topics in the
 *                                    broker.
 * @param topicConfigurationException If the topic configuration process failed, e.g. because expected topics are missing or have an
 *                                    incorrect number of partitions, this field will store the error that occurred, so that is can be
 *                                    reported back to the client.
 */
public record ConfiguredTopology(int topologyEpoch,
                                 Optional<SortedMap<String, ConfiguredSubtopology>> subtopologies,
                                 Map<String, CreatableTopic> internalTopicsToBeCreated,
                                 Optional<TopicConfigurationException> topicConfigurationException) {

    public ConfiguredTopology {
        if (topologyEpoch < 0) {
            throw new IllegalArgumentException("Topology epoch must be non-negative.");
        }
        if (topicConfigurationException.isEmpty() && subtopologies.isEmpty()) {
            throw new IllegalArgumentException("Subtopologies must be present if topicConfigurationException is empty.");
        }
        Objects.requireNonNull(subtopologies, "subtopologies can't be null");
        Objects.requireNonNull(internalTopicsToBeCreated, "internalTopicsToBeCreated can't be null");
        Objects.requireNonNull(topicConfigurationException, "topicConfigurationException can't be null");
    }

    public boolean isReady() {
        return topicConfigurationException.isEmpty();
    }

    public StreamsGroupDescribeResponseData.Topology asStreamsGroupDescribeTopology() {
        return new StreamsGroupDescribeResponseData.Topology()
            .setEpoch(topologyEpoch)
            .setSubtopologies(
                subtopologies.map(stringConfiguredSubtopologyMap -> stringConfiguredSubtopologyMap.entrySet().stream().map(
                    entry -> entry.getValue().asStreamsGroupDescribeSubtopology(entry.getKey())
                ).collect(Collectors.toList())).orElse(Collections.emptyList())
            );
    }

}
