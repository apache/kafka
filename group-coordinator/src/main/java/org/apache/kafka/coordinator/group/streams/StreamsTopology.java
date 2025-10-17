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

import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contains the topology sent by a Streams client in the Streams heartbeat during initialization.
 * <p>
 * This topology is used together with the partition metadata on the broker to create a
 * {@link org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology configured topology}.
 * This class allows to look-up subtopologies by subtopology ID in constant time by getting the subtopologies map.
 * The information in this class is fully backed by records stored in the __consumer_offsets topic.
 *
 * @param topologyEpoch The epoch of the topology (must be non-negative).
 * @param subtopologies The subtopologies of the topology containing information about source topics,
 *                      repartition topics, changelog topics, co-partition groups etc. (must be non-null)
 * @param sourceTopicMap A precomputed map of source topics to their corresponding subtopology (must be non-null)
 */
public record StreamsTopology(int topologyEpoch,
                              Map<String, Subtopology> subtopologies,
                              Map<String, Subtopology> sourceTopicMap) {

    /**
     * Constructor that automatically computes the sourceTopicMap from subtopologies.
     *
     * @param topologyEpoch The epoch of the topology (must be non-negative).
     * @param subtopologies The subtopologies of the topology.
     */
    public StreamsTopology(int topologyEpoch, Map<String, Subtopology> subtopologies) {
        this(topologyEpoch, subtopologies, computeSourceTopicMap(subtopologies));
    }

    public StreamsTopology {
        if (topologyEpoch < 0) {
            throw new IllegalArgumentException("Topology epoch must be non-negative.");
        }
        subtopologies = Collections.unmodifiableMap(Objects.requireNonNull(subtopologies, "Subtopologies cannot be null."));
        sourceTopicMap = Collections.unmodifiableMap(Objects.requireNonNull(sourceTopicMap, "Source topic map cannot be null."));
    }

    private static Map<String, Subtopology> computeSourceTopicMap(Map<String, Subtopology> subtopologies) {
        Objects.requireNonNull(subtopologies, "Subtopologies cannot be null.");
        Map<String, Subtopology> computedMap = new HashMap<>();
        for (Subtopology subtopology : subtopologies.values()) {
            // Add regular source topics
            for (String sourceTopic : subtopology.sourceTopics()) {
                computedMap.put(sourceTopic, subtopology);
            }
            // Add repartition source topics
            for (TopicInfo repartitionSourceTopic : subtopology.repartitionSourceTopics()) {
                computedMap.put(repartitionSourceTopic.name(), subtopology);
            }
        }
        return computedMap;
    }

    /**
     * Returns the set of topics that are required by the topology.
     * <p>
     * The required topics are used to determine the partition metadata on the brokers needed to configure the topology.
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
     * Creates an instance of StreamsTopology from a StreamsGroupTopologyValue record.
     *
     * @param record The StreamsGroupTopologyValue record.
     * @return The instance of StreamsTopology created from the record.
     */
    public static StreamsTopology fromRecord(StreamsGroupTopologyValue record) {
        return new StreamsTopology(
            record.epoch(),
            record.subtopologies().stream().collect(Collectors.toMap(Subtopology::subtopologyId, x -> x))
        );
    }

    /**
     * Creates an instance of StreamsTopology from a StreamsGroupHeartbeatRequestData request.
     *
     * @param topology The topology supplied in the request.
     * @return The instance of StreamsTopology created from the request.
     */
    public static StreamsTopology fromHeartbeatRequest(StreamsGroupHeartbeatRequestData.Topology topology) {
        StreamsGroupTopologyValue recordValue = StreamsCoordinatorRecordHelpers.convertToStreamsGroupTopologyRecord(topology);
        final Map<String, StreamsGroupTopologyValue.Subtopology> subtopologyMap = recordValue.subtopologies().stream()
            .collect(Collectors.toMap(StreamsGroupTopologyValue.Subtopology::subtopologyId, x -> x));
        return new StreamsTopology(topology.epoch(), subtopologyMap);
    }

    public StreamsGroupDescribeResponseData.Topology asStreamsGroupDescribeTopology() {
        return new StreamsGroupDescribeResponseData.Topology()
            .setEpoch(topologyEpoch)
            .setSubtopologies(
                subtopologies.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> asStreamsGroupDescribeSubtopology(entry.getKey(), entry.getValue()))
                    .toList()
            );
    }

    private StreamsGroupDescribeResponseData.Subtopology asStreamsGroupDescribeSubtopology(String subtopologyId, StreamsGroupTopologyValue.Subtopology subtopology) {
        return new StreamsGroupDescribeResponseData.Subtopology()
            .setSubtopologyId(subtopologyId)
            .setSourceTopics(subtopology.sourceTopics().stream().sorted().toList())
            .setRepartitionSinkTopics(subtopology.repartitionSinkTopics().stream().sorted().toList())
            .setRepartitionSourceTopics(subtopology.repartitionSourceTopics().stream()
                .map(this::asStreamsGroupDescribeTopicInfo)
                .sorted(Comparator.comparing(StreamsGroupDescribeResponseData.TopicInfo::name)).toList())
            .setStateChangelogTopics(subtopology.stateChangelogTopics().stream()
                .map(this::asStreamsGroupDescribeTopicInfo)
                .sorted(Comparator.comparing(StreamsGroupDescribeResponseData.TopicInfo::name)).toList());
    }

    private StreamsGroupDescribeResponseData.TopicInfo asStreamsGroupDescribeTopicInfo(StreamsGroupTopologyValue.TopicInfo topicInfo) {
        return new StreamsGroupDescribeResponseData.TopicInfo()
            .setName(topicInfo.name())
            .setPartitions(topicInfo.partitions())
            .setReplicationFactor(topicInfo.replicationFactor())
            .setTopicConfigs(
                topicInfo.topicConfigs().stream().map(
                    topicConfig -> new StreamsGroupDescribeResponseData.KeyValue()
                        .setKey(topicConfig.key())
                        .setValue(topicConfig.value())
                ).toList()
            );
    }

}
