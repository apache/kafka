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

import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfiguredSubtopologyTest {

    @Test
    public void testConstructorWithNullSourceTopics() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredSubtopology(
                2,
                null,
                Map.of(),
                Set.of(),
                Map.of()
            )
        );
    }

    @Test
    public void testConstructorWithNullRepartitionSourceTopics() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredSubtopology(
                2,
                Set.of(),
                null,
                Set.of(),
                Map.of()
            )
        );
    }

    @Test
    public void testConstructorWithNullRepartitionSinkTopics() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredSubtopology(
                2,
                Set.of(),
                Map.of(),
                null,
                Map.of()
            )
        );
    }

    @Test
    public void testConstructorWithNullStateChangelogTopics() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredSubtopology(
                2,
                Set.of(),
                Map.of(),
                Set.of(),
                null
            )
        );
    }

    @Test
    public void testConstructorWithNegativeTaskCount() {
        assertThrows(IllegalArgumentException.class,
            () -> new ConfiguredSubtopology(
                -1,
                Set.of(),
                Map.of(),
                Set.of(),
                Map.of()
            )
        );
    }

    @Test
    public void testAsStreamsGroupDescribeSubtopology() {
        String subtopologyId = "subtopology1";
        Set<String> sourceTopics = Set.of("sourceTopic1", "sourceTopic2");
        Set<String> repartitionSinkTopics = Set.of("repartitionSinkTopic1", "repartitionSinkTopic2");
        ConfiguredInternalTopic internalTopicMock = mock(ConfiguredInternalTopic.class);
        StreamsGroupDescribeResponseData.TopicInfo topicInfo = new StreamsGroupDescribeResponseData.TopicInfo();
        when(internalTopicMock.asStreamsGroupDescribeTopicInfo()).thenReturn(topicInfo);
        Map<String, ConfiguredInternalTopic> repartitionSourceTopics = Map.of("repartitionSourceTopic1", internalTopicMock);
        Map<String, ConfiguredInternalTopic> stateChangelogTopics = Map.of("stateChangelogTopic1", internalTopicMock);
        ConfiguredSubtopology configuredSubtopology = new ConfiguredSubtopology(
            1, sourceTopics, repartitionSourceTopics, repartitionSinkTopics, stateChangelogTopics);

        StreamsGroupDescribeResponseData.Subtopology subtopology = configuredSubtopology.asStreamsGroupDescribeSubtopology(subtopologyId);

        assertEquals(subtopologyId, subtopology.subtopologyId());
        assertEquals(sourceTopics.stream().sorted().toList(), subtopology.sourceTopics());
        assertEquals(repartitionSinkTopics.stream().sorted().toList(), subtopology.repartitionSinkTopics());
        assertEquals(List.of(topicInfo), subtopology.repartitionSourceTopics());
        assertEquals(List.of(topicInfo), subtopology.stateChangelogTopics());
    }

}