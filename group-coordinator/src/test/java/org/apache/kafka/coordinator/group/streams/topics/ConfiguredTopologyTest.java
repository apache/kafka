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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfiguredTopologyTest {

    @Test
    public void testConstructorWithNullInternalTopicsToBeCreated() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredTopology(
                0,
                0L,
                null,
                Optional.empty(),
                Optional.of(Map.of("subtopology1", 10)),
                Map.of()
            )
        );
    }

    @Test
    public void testConstructorWithNullTopicConfigurationException() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredTopology(
                0,
                0L,
                Map.of(),
                null,
                Optional.of(Map.of("subtopology1", 10)),
                Map.of()
            )
        );
    }

    @Test
    public void testConstructorWithNullNumTasksBySubtopology() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredTopology(
                0,
                0L,
                Map.of(),
                Optional.empty(),
                null,
                Map.of()
            )
        );
    }

    @Test
    public void testConstructorWithNullResolvedPartitionCounts() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredTopology(
                0,
                0L,
                Map.of(),
                Optional.empty(),
                Optional.of(Map.of("subtopology1", 10)),
                null
            )
        );
    }

    @Test
    public void testConstructorWithInvalidTopologyEpoch() {
        assertThrows(IllegalArgumentException.class,
            () -> new ConfiguredTopology(
                -1,
                0L,
                Map.of(),
                Optional.empty(),
                Optional.of(Map.of("subtopology1", 10)),
                Map.of()
            )
        );
    }

    @Test
    public void testNoExceptionButNoNumTasksBySubtopology() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> new ConfiguredTopology(
                1,
                0L,
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                Map.of()
            )
        );
        assertEquals("numTasksBySubtopology must be present if topicConfigurationException is empty.", ex.getMessage());
    }

    @Test
    public void testIsReady() {
        Map<String, Integer> numTasksBySubtopology = Map.of("subtopology1", 10);
        ConfiguredTopology readyResult = new ConfiguredTopology(
            1, 0L, new HashMap<>(), Optional.empty(), Optional.of(numTasksBySubtopology), Map.of());
        assertTrue(readyResult.isReady());

        ConfiguredTopology notReadyResult = new ConfiguredTopology(
            1, 0L, new HashMap<>(), Optional.of(TopicConfigurationException.missingSourceTopics("missing")), Optional.empty(), Map.of());
        assertFalse(notReadyResult.isReady());
    }

    @Test
    public void testAccessors() {
        Map<String, Integer> numTasksBySubtopology = Map.of("subtopology1", 10, "subtopology2", 5);
        Map<String, CreatableTopic> internalTopics = new HashMap<>();

        ConfiguredTopology result = new ConfiguredTopology(
            1,
            42L,
            internalTopics,
            Optional.empty(),
            Optional.of(numTasksBySubtopology),
            Map.of()
        );

        assertEquals(1, result.topologyEpoch());
        assertEquals(42L, result.metadataHash());
        assertEquals(internalTopics, result.internalTopicsToBeCreated());
        assertTrue(result.topicConfigurationException().isEmpty());
        assertTrue(result.numTasksBySubtopology().isPresent());
        assertEquals(numTasksBySubtopology, result.numTasksBySubtopology().get());
    }
}
