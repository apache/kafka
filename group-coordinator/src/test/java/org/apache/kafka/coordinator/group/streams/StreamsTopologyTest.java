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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamsTopologyTest {

    private static final String SUBTOPOLOGY_ID_1 = "subtopology-1";
    private static final String SUBTOPOLOGY_ID_2 = "subtopology-2";
    private static final String SOURCE_TOPIC_1 = "source-topic-1";
    private static final String SOURCE_TOPIC_2 = "source-topic-2";
    private static final String SOURCE_TOPIC_3 = "source-topic-3";
    private static final String REPARTITION_TOPIC_1 = "repartition-topic-1";
    private static final String REPARTITION_TOPIC_2 = "repartition-topic-2";
    private static final String REPARTITION_TOPIC_3 = "repartition-topic-3";
    private static final String CHANGELOG_TOPIC_1 = "changelog-1";
    private static final String CHANGELOG_TOPIC_2 = "changelog-2";
    private static final String CHANGELOG_TOPIC_3 = "changelog-3";

    @Test
    public void subtopologiesMapShouldNotBeNull() {
        final Exception exception = assertThrows(NullPointerException.class, () -> new StreamsTopology(1, null));
        assertEquals("Subtopologies cannot be null.", exception.getMessage());
    }

    @Test
    public void topologyEpochShouldNotBeNegative() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry(SUBTOPOLOGY_ID_1, mkSubtopology1())
        );
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> new StreamsTopology(-1, subtopologies));
        assertEquals("Topology epoch must be non-negative.", exception.getMessage());
    }

    @Test
    public void subtopologiesMapShouldBeImmutable() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry(SUBTOPOLOGY_ID_1, mkSubtopology1())
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> new StreamsTopology(1, subtopologies).subtopologies().put("subtopology-2", mkSubtopology2())
        );
    }

    @Test
    public void requiredTopicsShouldBeCorrect() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry(SUBTOPOLOGY_ID_1, mkSubtopology1()),
            mkEntry(SUBTOPOLOGY_ID_2, mkSubtopology2())
        );
        StreamsTopology topology = new StreamsTopology(1, subtopologies);
        Set<String> expectedTopics = Set.of(
            SOURCE_TOPIC_1, SOURCE_TOPIC_2, SOURCE_TOPIC_3,
            REPARTITION_TOPIC_1, REPARTITION_TOPIC_2, REPARTITION_TOPIC_3,
            CHANGELOG_TOPIC_1, CHANGELOG_TOPIC_2, CHANGELOG_TOPIC_3
        );

        assertEquals(expectedTopics, topology.requiredTopics());
    }

    @Test
    public void fromRecordShouldCreateCorrectTopology() {
        StreamsGroupTopologyValue record = new StreamsGroupTopologyValue()
            .setEpoch(1)
            .setSubtopologies(Arrays.asList(mkSubtopology1(), mkSubtopology2()));
        StreamsTopology topology = StreamsTopology.fromRecord(record);
        assertEquals(1, topology.topologyEpoch());
        assertEquals(2, topology.subtopologies().size());
        assertTrue(topology.subtopologies().containsKey(SUBTOPOLOGY_ID_1));
        assertEquals(mkSubtopology1(), topology.subtopologies().get(SUBTOPOLOGY_ID_1));
        assertTrue(topology.subtopologies().containsKey(SUBTOPOLOGY_ID_2));
        assertEquals(mkSubtopology2(), topology.subtopologies().get(SUBTOPOLOGY_ID_2));
    }

    @Test
    public void fromHeartbeatRequestShouldCreateCorrectTopology() {
        StreamsGroupHeartbeatRequestData.Topology requestTopology = new StreamsGroupHeartbeatRequestData.Topology()
            .setEpoch(1)
            .setSubtopologies(List.of(mkRequestSubtopology1(), mkRequestSubtopology2()));

        StreamsTopology topology = StreamsTopology.fromHeartbeatRequest(requestTopology);

        assertEquals(1, topology.topologyEpoch());
        assertEquals(2, topology.subtopologies().size());
        assertTrue(topology.subtopologies().containsKey(SUBTOPOLOGY_ID_1));
        assertEquals(mkSubtopology1(), topology.subtopologies().get(SUBTOPOLOGY_ID_1));
        assertTrue(topology.subtopologies().containsKey(SUBTOPOLOGY_ID_2));
        assertEquals(mkSubtopology2(), topology.subtopologies().get(SUBTOPOLOGY_ID_2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"source", "regex", "repartition"})
    public void copartitionOrderShouldNotAffectTopologyEquality(String topicType) {
        StreamsGroupHeartbeatRequestData.Topology request = topologyWithCopartitionGroups(topicType);
        StreamsGroupHeartbeatRequestData.Topology originalRequest = request.duplicate();
        StreamsGroupTopologyValue record = StreamsCoordinatorRecordHelpers.convertToStreamsGroupTopologyRecord(request).duplicate();
        List<StreamsGroupTopologyValue.CopartitionGroup> groups = record.subtopologies().get(0).copartitionGroups();
        Collections.reverse(groups);
        for (StreamsGroupTopologyValue.CopartitionGroup group : groups) {
            Collections.reverse(group.sourceTopics());
            Collections.reverse(group.sourceTopicRegex());
            Collections.reverse(group.repartitionSourceTopics());
        }
        StreamsGroupTopologyValue originalRecord = record.duplicate();

        StreamsTopology replayed = StreamsTopology.fromRecord(record);
        StreamsTopology received = StreamsTopology.fromHeartbeatRequest(request);

        assertEquals(received, replayed);
        assertEquals(received.hashCode(), replayed.hashCode());
        assertEquals(originalRecord, record);
        assertEquals(originalRequest, request);
        // The lookup map must also refer to the normalized subtopology.
        assertSame(replayed.subtopologies().get(SUBTOPOLOGY_ID_1), replayed.sourceTopicMap().get("z"));
        assertEquals(List.of("z", "y", "x", "w"), replayed.subtopologies().get(SUBTOPOLOGY_ID_1).sourceTopics());
        assertEquals(originalRecord.subtopologies().get(0).sourceTopicRegex(),
            replayed.subtopologies().get(SUBTOPOLOGY_ID_1).sourceTopicRegex());
        assertEquals(originalRecord.subtopologies().get(0).repartitionSourceTopics(),
            replayed.subtopologies().get(SUBTOPOLOGY_ID_1).repartitionSourceTopics());

        // Exercise normalization of an unsorted heartbeat as well as an unsorted stored record.
        StreamsGroupHeartbeatRequestData.Topology reorderedRequest = request.duplicate();
        Collections.reverse(reorderedRequest.subtopologies().get(0).copartitionGroups());
        reorderedRequest.subtopologies().get(0).copartitionGroups().forEach(group -> {
            Collections.reverse(group.sourceTopics());
            Collections.reverse(group.sourceTopicRegex());
            Collections.reverse(group.repartitionSourceTopics());
        });
        StreamsGroupHeartbeatRequestData.Topology originalReorderedRequest = reorderedRequest.duplicate();
        assertEquals(received, StreamsTopology.fromHeartbeatRequest(reorderedRequest));
        assertEquals(originalReorderedRequest, reorderedRequest);
    }

    @Test
    public void mixedCopartitionGroupsShouldUseRepartitionIndexesToBreakSourceTies() {
        StreamsGroupHeartbeatRequestData.Topology request = topologyWithCopartitionGroups("source");
        request.subtopologies().get(0).setCopartitionGroups(List.of(
            new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                .setSourceTopics(List.of((short) 1))
                .setRepartitionSourceTopics(List.of((short) 3, (short) 2)),
            new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                .setSourceTopics(List.of((short) 1))
                .setRepartitionSourceTopics(List.of((short) 0))
        ));
        StreamsGroupHeartbeatRequestData.Topology originalRequest = request.duplicate();
        StreamsGroupTopologyValue record = StreamsCoordinatorRecordHelpers.convertToStreamsGroupTopologyRecord(request).duplicate();
        StreamsGroupTopologyValue originalRecord = record.duplicate();

        // Source indexes tie, so repartition indexes must determine the group order.
        List<StreamsGroupTopologyValue.CopartitionGroup> expectedGroups = List.of(
            new StreamsGroupTopologyValue.CopartitionGroup()
                .setSourceTopics(List.of((short) 1))
                .setRepartitionSourceTopics(List.of((short) 0)),
            new StreamsGroupTopologyValue.CopartitionGroup()
                .setSourceTopics(List.of((short) 1))
                .setRepartitionSourceTopics(List.of((short) 2, (short) 3))
        );

        StreamsTopology received = StreamsTopology.fromHeartbeatRequest(request);
        StreamsTopology replayed = StreamsTopology.fromRecord(record);

        assertEquals(expectedGroups, received.subtopologies().get(SUBTOPOLOGY_ID_1).copartitionGroups());
        assertEquals(expectedGroups, replayed.subtopologies().get(SUBTOPOLOGY_ID_1).copartitionGroups());
        assertEquals(received, replayed);
        assertEquals(originalRequest, request);
        assertEquals(originalRecord, record);
    }

    @ParameterizedTest
    @ValueSource(strings = {"source", "regex", "repartition"})
    public void differentCopartitionConstraintsShouldNotBeEqual(String topicType) {
        StreamsGroupHeartbeatRequestData.Topology request = topologyWithCopartitionGroups(topicType);
        StreamsTopology original = StreamsTopology.fromHeartbeatRequest(request);
        StreamsGroupHeartbeatRequestData.Topology changed = request.duplicate();
        changed.subtopologies().get(0).copartitionGroups().set(2,
            copartitionGroup(topicType, List.of((short) 1, (short) 3)));

        assertNotEquals(original, StreamsTopology.fromHeartbeatRequest(changed));
        assertNotEquals(original, StreamsTopology.fromHeartbeatRequest(request.duplicate().setEpoch(2)));
    }

    private StreamsGroupHeartbeatRequestData.Topology topologyWithCopartitionGroups(String topicType) {
        return new StreamsGroupHeartbeatRequestData.Topology()
            .setEpoch(1)
            .setSubtopologies(List.of(new StreamsGroupHeartbeatRequestData.Subtopology()
                .setSubtopologyId(SUBTOPOLOGY_ID_1)
                // Keep these unsorted to verify that index targets are preserved.
                .setSourceTopics(List.of("z", "y", "x", "w"))
                .setSourceTopicRegex(List.of("z.*", "y.*", "x.*", "w.*"))
                .setRepartitionSourceTopics(List.of(
                    new StreamsGroupHeartbeatRequestData.TopicInfo().setName("repartition-z"),
                    new StreamsGroupHeartbeatRequestData.TopicInfo().setName("repartition-y"),
                    new StreamsGroupHeartbeatRequestData.TopicInfo().setName("repartition-x"),
                    new StreamsGroupHeartbeatRequestData.TopicInfo().setName("repartition-w")
                ))
                .setCopartitionGroups(List.of(
                    copartitionGroup(topicType, List.of((short) 0)),
                    copartitionGroup(topicType, List.of((short) 0, (short) 1)),
                    copartitionGroup(topicType, List.of((short) 2, (short) 3))
                ))));
    }

    private StreamsGroupHeartbeatRequestData.CopartitionGroup copartitionGroup(String topicType, List<Short> indexes) {
        StreamsGroupHeartbeatRequestData.CopartitionGroup group = new StreamsGroupHeartbeatRequestData.CopartitionGroup();
        return switch (topicType) {
            case "source" -> group.setSourceTopics(indexes);
            case "regex" -> group.setSourceTopicRegex(indexes);
            case "repartition" -> group.setRepartitionSourceTopics(indexes);
            default -> throw new IllegalArgumentException("Unknown topic type: " + topicType);
        };
    }

    @Test
    public void asStreamsGroupDescribeTopologyShouldReturnCorrectStructure() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry(SUBTOPOLOGY_ID_1, mkSubtopology1()),
            mkEntry(SUBTOPOLOGY_ID_2, mkSubtopology2())
        );
        StreamsTopology topology = new StreamsTopology(1, subtopologies);

        StreamsGroupDescribeResponseData.Topology describeTopology = topology.asStreamsGroupDescribeTopology();

        assertEquals(1, describeTopology.epoch());
        assertEquals(2, describeTopology.subtopologies().size());

        // Verify subtopologies are correctly converted and sorted
        List<StreamsGroupDescribeResponseData.Subtopology> sortedSubtopologies = 
            describeTopology.subtopologies().stream()
                .sorted(Comparator.comparing(StreamsGroupDescribeResponseData.Subtopology::subtopologyId))
                .toList();

        // Verify first subtopology
        StreamsGroupDescribeResponseData.Subtopology sub1 = sortedSubtopologies.get(0);
        assertEquals(SUBTOPOLOGY_ID_1, sub1.subtopologyId());
        // Source topics are sorted alphabetically
        assertEquals(List.of(REPARTITION_TOPIC_1, REPARTITION_TOPIC_2, SOURCE_TOPIC_1, SOURCE_TOPIC_2), 
            sub1.sourceTopics());
        assertEquals(List.of(REPARTITION_TOPIC_3), sub1.repartitionSinkTopics());
        assertEquals(2, sub1.repartitionSourceTopics().size());
        assertEquals(2, sub1.stateChangelogTopics().size());

        // Verify second subtopology
        StreamsGroupDescribeResponseData.Subtopology sub2 = sortedSubtopologies.get(1);
        assertEquals(SUBTOPOLOGY_ID_2, sub2.subtopologyId());
        // Source topics are sorted alphabetically
        assertEquals(List.of(REPARTITION_TOPIC_3, SOURCE_TOPIC_3), sub2.sourceTopics());
        assertEquals(List.of(), sub2.repartitionSinkTopics());
        assertEquals(1, sub2.repartitionSourceTopics().size());
        assertEquals(1, sub2.stateChangelogTopics().size());
    }

    @Test
    public void asStreamsGroupDescribeTopicInfoShouldConvertCorrectly() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry(SUBTOPOLOGY_ID_1, mkSubtopology1())
        );
        StreamsTopology topology = new StreamsTopology(1, subtopologies);

        StreamsGroupDescribeResponseData.Topology describeTopology = topology.asStreamsGroupDescribeTopology();
        StreamsGroupDescribeResponseData.Subtopology describedSub = describeTopology.subtopologies().get(0);

        // Verify repartition source topics are correctly converted
        List<StreamsGroupDescribeResponseData.TopicInfo> repartitionTopics = describedSub.repartitionSourceTopics();
        assertEquals(2, repartitionTopics.size());
        
        // Find the first repartition topic (they should be sorted by name)
        StreamsGroupDescribeResponseData.TopicInfo firstTopic = repartitionTopics.stream()
            .filter(topic -> topic.name().equals(REPARTITION_TOPIC_1))
            .findFirst()
            .orElseThrow();
        assertEquals(REPARTITION_TOPIC_1, firstTopic.name());

        // Verify changelog topics are correctly converted
        List<StreamsGroupDescribeResponseData.TopicInfo> changelogTopics = describedSub.stateChangelogTopics();
        assertEquals(2, changelogTopics.size());
        
        // Find the first changelog topic (they should be sorted by name)
        StreamsGroupDescribeResponseData.TopicInfo firstChangelog = changelogTopics.stream()
            .filter(topic -> topic.name().equals(CHANGELOG_TOPIC_1))
            .findFirst()
            .orElseThrow();
        assertEquals(CHANGELOG_TOPIC_1, firstChangelog.name());
    }

    @Test
    public void asStreamsGroupDescribeTopologyWithEmptySubtopologies() {
        StreamsTopology topology = new StreamsTopology(0, Map.of());

        StreamsGroupDescribeResponseData.Topology describeTopology = topology.asStreamsGroupDescribeTopology();

        assertEquals(0, describeTopology.epoch());
        assertEquals(0, describeTopology.subtopologies().size());
    }

    @Test
    public void sourceTopicMapShouldBeComputedCorrectly() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry(SUBTOPOLOGY_ID_1, mkSubtopology1()),
            mkEntry(SUBTOPOLOGY_ID_2, mkSubtopology2())
        );
        StreamsTopology topology = new StreamsTopology(1, subtopologies);
        
        // Verify sourceTopicMap contains all source topics from both subtopologies
        Map<String, Subtopology> sourceTopicMap = topology.sourceTopicMap();
        
        // From subtopology 1: SOURCE_TOPIC_1, SOURCE_TOPIC_2, REPARTITION_TOPIC_1, REPARTITION_TOPIC_2
        // From subtopology 2: SOURCE_TOPIC_3, REPARTITION_TOPIC_3
        assertEquals(6, sourceTopicMap.size());
        
        // Verify regular source topics
        assertTrue(sourceTopicMap.containsKey(SOURCE_TOPIC_1));
        assertEquals(mkSubtopology1(), sourceTopicMap.get(SOURCE_TOPIC_1));
        assertTrue(sourceTopicMap.containsKey(SOURCE_TOPIC_2));
        assertEquals(mkSubtopology1(), sourceTopicMap.get(SOURCE_TOPIC_2));
        assertTrue(sourceTopicMap.containsKey(SOURCE_TOPIC_3));
        assertEquals(mkSubtopology2(), sourceTopicMap.get(SOURCE_TOPIC_3));
        
        // Verify repartition source topics
        assertTrue(sourceTopicMap.containsKey(REPARTITION_TOPIC_1));
        assertEquals(mkSubtopology1(), sourceTopicMap.get(REPARTITION_TOPIC_1));
        assertTrue(sourceTopicMap.containsKey(REPARTITION_TOPIC_2));
        assertEquals(mkSubtopology1(), sourceTopicMap.get(REPARTITION_TOPIC_2));
        assertTrue(sourceTopicMap.containsKey(REPARTITION_TOPIC_3));
        assertEquals(mkSubtopology2(), sourceTopicMap.get(REPARTITION_TOPIC_3));
    }

    @Test
    public void sourceTopicMapShouldBeImmutable() {
        Map<String, Subtopology> subtopologies = mkMap(
            mkEntry(SUBTOPOLOGY_ID_1, mkSubtopology1())
        );
        StreamsTopology topology = new StreamsTopology(1, subtopologies);
        
        assertThrows(
            UnsupportedOperationException.class,
            () -> topology.sourceTopicMap().put("test-topic", mkSubtopology1())
        );
    }

    private Subtopology mkSubtopology1() {
        return new Subtopology()
            .setSubtopologyId(SUBTOPOLOGY_ID_1)
            .setSourceTopics(List.of(
                SOURCE_TOPIC_1,
                SOURCE_TOPIC_2,
                REPARTITION_TOPIC_1,
                REPARTITION_TOPIC_2
            ))
            .setRepartitionSourceTopics(List.of(
                new TopicInfo().setName(REPARTITION_TOPIC_1),
                new TopicInfo().setName(REPARTITION_TOPIC_2)
            ))
            .setRepartitionSinkTopics(List.of(
                REPARTITION_TOPIC_3
            ))
            .setStateChangelogTopics(List.of(
                new TopicInfo().setName(CHANGELOG_TOPIC_1),
                new TopicInfo().setName(CHANGELOG_TOPIC_2)
            ))
            .setCopartitionGroups(List.of(
                new StreamsGroupTopologyValue.CopartitionGroup()
                    .setRepartitionSourceTopics(List.of((short) 0))
                    .setSourceTopics(List.of((short) 0)),
                new StreamsGroupTopologyValue.CopartitionGroup()
                    .setRepartitionSourceTopics(List.of((short) 1))
                    .setSourceTopics(List.of((short) 1))
            ));
    }

    private Subtopology mkSubtopology2() {
        return new Subtopology()
            .setSubtopologyId(SUBTOPOLOGY_ID_2)
            .setSourceTopics(List.of(
                SOURCE_TOPIC_3,
                REPARTITION_TOPIC_3
            ))
            .setRepartitionSourceTopics(List.of(
                new TopicInfo().setName(REPARTITION_TOPIC_3)
            ))
            .setStateChangelogTopics(List.of(
                new TopicInfo().setName(CHANGELOG_TOPIC_3)
            ));
    }

    private StreamsGroupHeartbeatRequestData.Subtopology mkRequestSubtopology1() {
        return new StreamsGroupHeartbeatRequestData.Subtopology()
            .setSubtopologyId(SUBTOPOLOGY_ID_1)
            .setSourceTopics(List.of(
                SOURCE_TOPIC_1,
                SOURCE_TOPIC_2,
                REPARTITION_TOPIC_1,
                REPARTITION_TOPIC_2
            ))
            .setRepartitionSourceTopics(List.of(
                new StreamsGroupHeartbeatRequestData.TopicInfo().setName(REPARTITION_TOPIC_1),
                new StreamsGroupHeartbeatRequestData.TopicInfo().setName(REPARTITION_TOPIC_2)
            ))
            .setRepartitionSinkTopics(List.of(
                REPARTITION_TOPIC_3
            ))
            .setStateChangelogTopics(List.of(
                new StreamsGroupHeartbeatRequestData.TopicInfo().setName(CHANGELOG_TOPIC_1),
                new StreamsGroupHeartbeatRequestData.TopicInfo().setName(CHANGELOG_TOPIC_2)
            ))
            .setCopartitionGroups(List.of(
                new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                    .setRepartitionSourceTopics(List.of((short) 0))
                    .setSourceTopics(List.of((short) 0)),
                new StreamsGroupHeartbeatRequestData.CopartitionGroup()
                    .setRepartitionSourceTopics(List.of((short) 1))
                    .setSourceTopics(List.of((short) 1))
            ));
    }

    private StreamsGroupHeartbeatRequestData.Subtopology mkRequestSubtopology2() {
        return new StreamsGroupHeartbeatRequestData.Subtopology()
            .setSubtopologyId(SUBTOPOLOGY_ID_2)
            .setSourceTopics(List.of(
                SOURCE_TOPIC_3,
                REPARTITION_TOPIC_3
            ))
            .setRepartitionSourceTopics(List.of(
                new StreamsGroupHeartbeatRequestData.TopicInfo().setName(REPARTITION_TOPIC_3)
            ))
            .setStateChangelogTopics(List.of(
                new StreamsGroupHeartbeatRequestData.TopicInfo().setName(CHANGELOG_TOPIC_3)
            ));
    }
}
