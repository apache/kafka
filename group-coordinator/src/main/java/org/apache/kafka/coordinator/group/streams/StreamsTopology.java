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
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.CopartitionGroup;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
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
            record.subtopologies().stream()
                .collect(Collectors.toMap(Subtopology::subtopologyId, x -> x))
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

    /**
     * Checks whether the request describes this topology without converting or modifying it.
     * Subtopologies are matched by ID. Copartition groups and their topic indexes are compared
     * without regard to order, but duplicate occurrences must still match. All other lists
     * retain their order-sensitive comparison, including the topic lists referenced by indexes.
     *
     * @param topology The topology supplied in the heartbeat request.
     * @return Whether the request matches this topology.
     */
    public boolean matches(StreamsGroupHeartbeatRequestData.Topology topology) {
        if (topologyEpoch != topology.epoch() || subtopologies.size() != topology.subtopologies().size()) {
            return false;
        }
        for (int i = 0; i < topology.subtopologies().size(); i++) {
            StreamsGroupHeartbeatRequestData.Subtopology requested = topology.subtopologies().get(i);
            Subtopology current = subtopologies.get(requested.subtopologyId());
            if (current == null || !matchesSubtopology(current, requested)) {
                return false;
            }

            // Do not let repeated IDs match one stored subtopology while omitting another.
            for (int j = 0; j < i; j++) {
                if (requested.subtopologyId().equals(topology.subtopologies().get(j).subtopologyId())) {
                    return false;
                }
            }
        }
        return true;
    }

    // Keep these comparisons in sync with the generated fields guarded by StreamsTopologyTest.
    private static boolean matchesSubtopology(Subtopology current, StreamsGroupHeartbeatRequestData.Subtopology requested) {
        if (!current.subtopologyId().equals(requested.subtopologyId())) {
            return false;
        }

        return current.sourceTopics().equals(requested.sourceTopics())
            && current.sourceTopicRegex().equals(requested.sourceTopicRegex())
            && current.repartitionSinkTopics().equals(requested.repartitionSinkTopics())
            && matchesTopicInfos(current.repartitionSourceTopics(), requested.repartitionSourceTopics())
            && matchesTopicInfos(current.stateChangelogTopics(), requested.stateChangelogTopics())
            && matchesCopartitionGroups(current.copartitionGroups(), requested.copartitionGroups());
    }

    private static boolean matchesTopicInfos(List<TopicInfo> current, List<StreamsGroupHeartbeatRequestData.TopicInfo> requested) {
        if (current.size() != requested.size()) {
            return false;
        }
        for (int i = 0; i < current.size(); i++) {
            TopicInfo fromCurrent = current.get(i);
            StreamsGroupHeartbeatRequestData.TopicInfo fromRequested = requested.get(i);
            if (!fromCurrent.name().equals(fromRequested.name())
                || fromCurrent.partitions() != fromRequested.partitions()
                || fromCurrent.replicationFactor() != fromRequested.replicationFactor()
                || !matchesTopicConfigs(fromCurrent.topicConfigs(), fromRequested.topicConfigs())) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesTopicConfigs(
        List<StreamsGroupTopologyValue.TopicConfig> current,
        List<StreamsGroupHeartbeatRequestData.KeyValue> requested
    ) {
        // Preserve the null handling of convertToTopicInfo for directly constructed requests.
        if (current == null || requested == null) {
            return current == null && requested == null;
        }
        if (current.size() != requested.size()) {
            return false;
        }
        for (int i = 0; i < current.size(); i++) {
            if (!Objects.equals(current.get(i).key(), requested.get(i).key())
                || !Objects.equals(current.get(i).value(), requested.get(i).value())) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesCopartitionGroups(
        List<CopartitionGroup> current,
        List<StreamsGroupHeartbeatRequestData.CopartitionGroup> requested
    ) {
        if (current.size() != requested.size()) {
            return false;
        }

        boolean sameOrder = true;
        for (int i = 0; i < current.size(); i++) {
            if (!matchesCopartitionGroup(current.get(i), requested.get(i))) {
                sameOrder = false;
                break;
            }
        }
        if (sameOrder) {
            return true;
        }

        // Count equivalent occurrences rather than allocating sorted copies
        // or sets, which would lose duplicate groups.
        for (int i = 0; i < current.size(); i++) {
            CopartitionGroup group = current.get(i);
            StreamsGroupHeartbeatRequestData.CopartitionGroup representative = null;
            int requestedCount = 0;
            for (int j = 0; j < requested.size(); j++) {
                if (matchesCopartitionGroup(group, requested.get(j))) {
                    representative = requested.get(j);
                    requestedCount++;
                }
            }
            if (representative == null) {
                return false;
            }
            int currentCount = 0;
            for (int j = 0; j < current.size(); j++) {
                if (matchesCopartitionGroup(current.get(j), representative)) {
                    currentCount++;
                }
            }
            if (currentCount != requestedCount) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesCopartitionGroup(
        CopartitionGroup current,
        StreamsGroupHeartbeatRequestData.CopartitionGroup requested
    ) {
        return sameTopicIndexes(current.sourceTopics(), requested.sourceTopics())
            && sameTopicIndexes(current.sourceTopicRegex(), requested.sourceTopicRegex())
            && sameTopicIndexes(current.repartitionSourceTopics(), requested.repartitionSourceTopics());
    }

    private static boolean sameTopicIndexes(List<Short> current, List<Short> requested) {
        // [0] != [0, 1, 2]
        if (current.size() != requested.size()) {
            return false;
        }

        // [0, 1, 2] == [0, 1, 2]
        if (current.equals(requested)) {
            return true;
        }

        int size = current.size();

        // Ignore ordering while preserving duplicate counts.
        // For example, [0, 0, 1] matches [1, 0, 0], but not [0, 1, 1].
        for (int i = 0; i < size; i++) {
            short index = current.get(i);
            int currentCount = 0;
            int requestedCount = 0;

            for (int j = 0; j < size; j++) {
                if (current.get(j) == index) {
                    currentCount++;
                }
                if (requested.get(j) == index) {
                    requestedCount++;
                }
            }
            if (currentCount != requestedCount) {
                return false;
            }
        }
        return true;
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
