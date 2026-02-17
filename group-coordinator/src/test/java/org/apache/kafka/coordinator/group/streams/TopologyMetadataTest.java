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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class TopologyMetadataTest {

    private StreamsTopology topology;
    private TopologyMetadata topologyMetadata;
    private Map<String, Integer> numTasksBySubtopology;

    @BeforeEach
    void setUp() {
        StreamsGroupTopologyValue.Subtopology subtopology1 = new StreamsGroupTopologyValue.Subtopology()
            .setSubtopologyId("subtopology1")
            .setSourceTopics(List.of("source_topic"))
            .setStateChangelogTopics(List.of(
                new StreamsGroupTopologyValue.TopicInfo().setName("changelog_topic")
            ));

        StreamsGroupTopologyValue.Subtopology subtopology2 = new StreamsGroupTopologyValue.Subtopology()
            .setSubtopologyId("subtopology2")
            .setSourceTopics(List.of())
            .setRepartitionSourceTopics(List.of(
                new StreamsGroupTopologyValue.TopicInfo().setName("repartition_source_topic")
            ));

        topology = new StreamsTopology(1, Map.of(
            "subtopology1", subtopology1,
            "subtopology2", subtopology2
        ));

        // Pre-computed max partitions (simulating what InternalTopicManager computes)
        numTasksBySubtopology = Map.of(
            "subtopology1", 3,
            "subtopology2", 4
        );

        topologyMetadata = new TopologyMetadata(topology, numTasksBySubtopology);
    }

    @Test
    void testTopology() {
        assertEquals(topology, topologyMetadata.topology());
    }

    @Test
    void testIsStateful() {
        assertTrue(topologyMetadata.isStateful("subtopology1"));
        assertFalse(topologyMetadata.isStateful("subtopology2"));
    }

    @Test
    void testMaxNumInputPartitions() {
        assertEquals(3, topologyMetadata.maxNumInputPartitions("subtopology1"));
        assertEquals(4, topologyMetadata.maxNumInputPartitions("subtopology2"));
    }

    @Test
    void testSubtopologies() {
        List<String> subtopologies = topologyMetadata.subtopologies();
        assertEquals(2, subtopologies.size());
        assertTrue(subtopologies.contains("subtopology1"));
        assertTrue(subtopologies.contains("subtopology2"));
    }

    @Test
    void testIsStatefulThrowsExceptionWhenSubtopologyIdDoesNotExist() {
        assertThrows(NoSuchElementException.class, () -> topologyMetadata.isStateful("non_existent_subtopology"));
    }

    @Test
    void testMaxNumInputPartitionsThrowsExceptionWhenSubtopologyIdDoesNotExist() {
        assertThrows(NoSuchElementException.class, () -> topologyMetadata.maxNumInputPartitions("non_existent_subtopology"));
    }

    @Test
    void testConstructorWithNullTopology() {
        assertThrows(NullPointerException.class, () -> new TopologyMetadata(null, numTasksBySubtopology));
    }

    @Test
    void testConstructorWithNullMaxPartitionsPerSubtopology() {
        assertThrows(NullPointerException.class, () -> new TopologyMetadata(topology, null));
    }

    @Test
    void testMaxNumInputPartitionsWithMultipleSourceTopics() {
        StreamsGroupTopologyValue.Subtopology subtopology = new StreamsGroupTopologyValue.Subtopology()
            .setSubtopologyId("multi_source")
            .setSourceTopics(List.of("topic_a", "topic_b"));

        StreamsTopology multiTopology = new StreamsTopology(1, Map.of("multi_source", subtopology));

        // Pre-computed value simulating max(5, 10) = 10
        Map<String, Integer> multiMaxPartitions = Map.of("multi_source", 10);

        TopologyMetadata multiTopologyMetadata = new TopologyMetadata(multiTopology, multiMaxPartitions);

        // Should return max(5, 10) = 10
        assertEquals(10, multiTopologyMetadata.maxNumInputPartitions("multi_source"));
    }
}
