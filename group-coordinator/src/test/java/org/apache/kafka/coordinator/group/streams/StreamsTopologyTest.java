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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
