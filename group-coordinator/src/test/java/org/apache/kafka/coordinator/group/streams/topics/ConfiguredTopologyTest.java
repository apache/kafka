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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfiguredTopologyTest {

    @Test
    public void testConstructorWithNullSubtopologies() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredTopology(
                0,
                null,
                Collections.emptyMap(),
                Optional.empty()
            )
        );
    }

    @Test
    public void testConstructorWithNullInternalTopicsToBeCreated() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredTopology(
                0,
                Optional.of(new TreeMap<>()),
                null,
                Optional.empty()
            )
        );
    }

    @Test
    public void testConstructorWithNullTopicConfigurationException() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredTopology(
                0,
                Optional.empty(),
                Collections.emptyMap(),
                null
            )
        );
    }

    @Test
    public void testConstructorWithInvalidTopologyEpoch() {
        assertThrows(IllegalArgumentException.class,
            () -> new ConfiguredTopology(
                -1,
                Optional.of(new TreeMap<>()),
                Collections.emptyMap(),
                Optional.empty()
            )
        );
    }

    @Test
    public void testNoExceptionButNoSubtopologies() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> new ConfiguredTopology(
                1,
                Optional.empty(),
                Collections.emptyMap(),
                Optional.empty()
            )
        );
        assertEquals(ex.getMessage(), "Subtopologies must be present if topicConfigurationException is empty.");
    }

    @Test
    public void testIsReady() {
        ConfiguredTopology readyTopology = new ConfiguredTopology(
            1, Optional.of(new TreeMap<>()), new HashMap<>(), Optional.empty());
        assertTrue(readyTopology.isReady());

        ConfiguredTopology notReadyTopology = new ConfiguredTopology(
            1, Optional.empty(), new HashMap<>(), Optional.of(TopicConfigurationException.missingSourceTopics("missing")));
        assertFalse(notReadyTopology.isReady());
    }

    @Test
    public void testAsStreamsGroupDescribeTopology() {
        int topologyEpoch = 1;
        ConfiguredSubtopology subtopologyMock = mock(ConfiguredSubtopology.class);
        StreamsGroupDescribeResponseData.Subtopology subtopologyResponse = new StreamsGroupDescribeResponseData.Subtopology();
        when(subtopologyMock.asStreamsGroupDescribeSubtopology(Mockito.anyString())).thenReturn(subtopologyResponse);
        SortedMap<String, ConfiguredSubtopology> subtopologies = new TreeMap<>();
        subtopologies.put("subtopology1", subtopologyMock);
        Map<String, CreatableTopic> internalTopicsToBeCreated = new HashMap<>();
        Optional<TopicConfigurationException> topicConfigurationException = Optional.empty();
        ConfiguredTopology configuredTopology = new ConfiguredTopology(
            topologyEpoch, Optional.of(subtopologies), internalTopicsToBeCreated, topicConfigurationException);

        StreamsGroupDescribeResponseData.Topology topology = configuredTopology.asStreamsGroupDescribeTopology();

        assertEquals(topologyEpoch, topology.epoch());
        assertEquals(1, topology.subtopologies().size());
        assertEquals(subtopologyResponse, topology.subtopologies().get(0));
    }
}