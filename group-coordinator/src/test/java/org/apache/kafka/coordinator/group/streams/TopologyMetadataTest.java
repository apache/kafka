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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredInternalTopic;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class TopologyMetadataTest {

    private CoordinatorMetadataImage metadataImage;
    private SortedMap<String, ConfiguredSubtopology> subtopologyMap;
    private TopologyMetadata topologyMetadata;

    @BeforeEach
    void setUp() {
        metadataImage = new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "source_topic", 3)
            .addTopic(Uuid.randomUuid(), "repartition_source_topic", 4)
            .build());
        subtopologyMap = new TreeMap<>();
        topologyMetadata = new TopologyMetadata(metadataImage, subtopologyMap);
    }

    @Test
    void testMetadataImage() {
        assertEquals(metadataImage, topologyMetadata.metadataImage());
    }

    @Test
    void testTopology() {
        assertEquals(subtopologyMap, topologyMetadata.subtopologyMap());
    }

    @Test
    void testIsStateful() {
        ConfiguredInternalTopic internalTopic = mock(ConfiguredInternalTopic.class);
        ConfiguredSubtopology subtopology1 = mock(ConfiguredSubtopology.class);
        ConfiguredSubtopology subtopology2 = mock(ConfiguredSubtopology.class);
        subtopologyMap.put("subtopology1", subtopology1);
        subtopologyMap.put("subtopology2", subtopology2);
        when(subtopology1.stateChangelogTopics()).thenReturn(Map.of("state_changelog_topic", internalTopic));
        when(subtopology2.stateChangelogTopics()).thenReturn(Map.of());

        assertTrue(topologyMetadata.isStateful("subtopology1"));
        assertFalse(topologyMetadata.isStateful("subtopology2"));
    }

    @Test
    void testMaxNumInputPartitions() {
        ConfiguredSubtopology subtopology = mock(ConfiguredSubtopology.class);
        subtopologyMap.put("subtopology1", subtopology);
        when(subtopology.numberOfTasks()).thenReturn(4);

        assertEquals(4, topologyMetadata.maxNumInputPartitions("subtopology1"));
    }

    @Test
    void testSubtopologies() {
        ConfiguredSubtopology subtopology1 = mock(ConfiguredSubtopology.class);
        ConfiguredSubtopology subtopology2 = mock(ConfiguredSubtopology.class);
        subtopologyMap.put("subtopology1", subtopology1);
        subtopologyMap.put("subtopology2", subtopology2);

        List<String> expectedSubtopologies = List.of("subtopology1", "subtopology2");
        assertEquals(expectedSubtopologies, topologyMetadata.subtopologies());
    }

    @Test
    void testIsStatefulThrowsExceptionWhenSubtopologyIdDoesNotExist() {
        assertThrows(NoSuchElementException.class, () -> topologyMetadata.isStateful("non_existent_subtopology"));
    }

    @Test
    void testMaxNumInputPartitionsThrowsExceptionWhenSubtopologyIdDoesNotExist() {
        assertThrows(NoSuchElementException.class, () -> topologyMetadata.maxNumInputPartitions("non_existent_subtopology"));
    }
}
