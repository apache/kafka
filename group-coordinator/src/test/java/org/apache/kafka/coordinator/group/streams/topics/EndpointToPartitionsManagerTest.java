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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs;
import org.apache.kafka.image.MetadataImage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopology;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopologyWithCommonEpoch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EndpointToPartitionsManagerTest {

    private StreamsGroup streamsGroup;
    private StreamsGroupMember streamsGroupMember;
    private ConfiguredTopology configuredTopology;
    private ConfiguredSubtopology configuredSubtopologyOne;
    private ConfiguredSubtopology configuredSubtopologyTwo;
    private final StreamsGroupHeartbeatResponseData.Endpoint responseEndpoint = new StreamsGroupHeartbeatResponseData.Endpoint();

    @BeforeEach
    public void setUp() {
        streamsGroup = mock(StreamsGroup.class);
        streamsGroupMember = mock(StreamsGroupMember.class);
        configuredTopology = mock(ConfiguredTopology.class);
        configuredSubtopologyOne = new ConfiguredSubtopology(1, Set.of("Topic-A"), new HashMap<>(), new HashSet<>(), new HashMap<>());
        Map<String, ConfiguredInternalTopic> repartitionSourceTopics = Map.of("Topic-B",  new ConfiguredInternalTopic("Topic-B", 1, Optional.of((short) 1), Collections.emptyMap()));
        configuredSubtopologyTwo = new ConfiguredSubtopology(1, new HashSet<>(), repartitionSourceTopics, new HashSet<>(), new HashMap<>());
        SortedMap<String, ConfiguredSubtopology> configuredSubtopologyOneMap = new TreeMap<>();
        configuredSubtopologyOneMap.put("0", configuredSubtopologyOne);
        SortedMap<String, ConfiguredSubtopology> configuredSubtopologyTwoMap = new TreeMap<>();
        configuredSubtopologyOneMap.put("1", configuredSubtopologyTwo);
        when(configuredTopology.subtopologies()).thenReturn(Optional.of(configuredSubtopologyOneMap));
        when(configuredTopology.subtopologies()).thenReturn(Optional.of(configuredSubtopologyTwoMap));
        responseEndpoint.setHost("localhost");
        responseEndpoint.setPort(9092);
    }

    @Test
    void testEndpointToPartitionsWithStandbyTaskAssignments() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "Topic-A", 3)
            .addTopic(Uuid.randomUuid(), "Topic-B", 3)
            .build();

        when(streamsGroupMember.assignedTasks()).thenReturn(
            new TasksTupleWithEpochs(
                mkTasksPerSubtopologyWithCommonEpoch(0, mkEntry("0", Set.of(0, 1, 2))),
                mkTasksPerSubtopology(mkEntry("1", Set.of(0, 1, 2))),
                Map.of()
            )
        );
        when(streamsGroup.configuredTopology()).thenReturn(Optional.of(configuredTopology));
        SortedMap<String, ConfiguredSubtopology> configuredSubtopologyMap = new TreeMap<>();
        configuredSubtopologyMap.put("0", configuredSubtopologyOne);
        configuredSubtopologyMap.put("1", configuredSubtopologyTwo);
        when(configuredTopology.subtopologies()).thenReturn(Optional.of(configuredSubtopologyMap));

        StreamsGroupHeartbeatResponseData.EndpointToPartitions result =
                EndpointToPartitionsManager.endpointToPartitions(streamsGroupMember, responseEndpoint, streamsGroup, new KRaftCoordinatorMetadataImage(metadataImage));

        assertEquals(responseEndpoint, result.userEndpoint());
        assertEquals(1, result.activePartitions().size());
        assertEquals(1, result.standbyPartitions().size());
        List<StreamsGroupHeartbeatResponseData.TopicPartition> activePartitions = result.activePartitions();
        List<StreamsGroupHeartbeatResponseData.TopicPartition> standbyPartitions = result.standbyPartitions();
        activePartitions.sort(Comparator.comparing(StreamsGroupHeartbeatResponseData.TopicPartition::topic));
        standbyPartitions.sort(Comparator.comparing(StreamsGroupHeartbeatResponseData.TopicPartition::topic));
        assertTopicPartitionsAssigned(activePartitions, "Topic-A");
        assertTopicPartitionsAssigned(standbyPartitions, "Topic-B");
    }

    private static void assertTopicPartitionsAssigned(List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitions, String topicName) {
        StreamsGroupHeartbeatResponseData.TopicPartition topicPartition = topicPartitions.stream().filter(tp -> tp.topic().equals(topicName)).findFirst().get();
        assertEquals(topicName, topicPartition.topic());
        assertEquals(List.of(0, 1, 2), topicPartition.partitions().stream().sorted().toList());
    }

    @ParameterizedTest(name = "{4}")
    @MethodSource("argsProvider")
    void testEndpointToPartitionsWithTwoTopicsAndDifferentPartitions(int topicAPartitions,
                                                                     int topicBPartitions,
                                                                     List<Integer> topicAExpectedPartitions,
                                                                     List<Integer> topicBExpectedPartitions,
                                                                     String testName
                                                                     ) {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "Topic-A", topicAPartitions)
            .addTopic(Uuid.randomUuid(), "Topic-B", topicBPartitions)
            .build();
        configuredSubtopologyOne = new ConfiguredSubtopology(Math.max(topicAPartitions, topicBPartitions), Set.of("Topic-A", "Topic-B"), new HashMap<>(), new HashSet<>(), new HashMap<>());

        when(streamsGroupMember.assignedTasks()).thenReturn(
            new TasksTupleWithEpochs(
                mkTasksPerSubtopologyWithCommonEpoch(0, mkEntry("0", Set.of(0, 1, 2, 3, 4))),
                Map.of(),
                Map.of()
            )
        );
        when(streamsGroup.configuredTopology()).thenReturn(Optional.of(configuredTopology));
        SortedMap<String, ConfiguredSubtopology> configuredSubtopologyOneMap = new TreeMap<>();
        configuredSubtopologyOneMap.put("0", configuredSubtopologyOne);
        when(configuredTopology.subtopologies()).thenReturn(Optional.of(configuredSubtopologyOneMap));

        StreamsGroupHeartbeatResponseData.EndpointToPartitions result = EndpointToPartitionsManager.endpointToPartitions(streamsGroupMember, responseEndpoint, streamsGroup, new KRaftCoordinatorMetadataImage(metadataImage));

        assertEquals(responseEndpoint, result.userEndpoint());
        assertEquals(2, result.activePartitions().size());

        List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitions = result.activePartitions();
        topicPartitions.sort(Comparator.comparing(StreamsGroupHeartbeatResponseData.TopicPartition::topic));

        StreamsGroupHeartbeatResponseData.TopicPartition topicAPartition = result.activePartitions().get(0);
        assertEquals("Topic-A", topicAPartition.topic());
        assertEquals(topicAExpectedPartitions, topicAPartition.partitions().stream().sorted().toList());
        
        StreamsGroupHeartbeatResponseData.TopicPartition topicBPartition = result.activePartitions().get(1);
        assertEquals("Topic-B", topicBPartition.topic());
        assertEquals(topicBExpectedPartitions, topicBPartition.partitions().stream().sorted().toList());
    }

    static Stream<Arguments> argsProvider() {
        return Stream.of(
                arguments(2, 5, List.of(0, 1), List.of(0, 1, 2, 3, 4), "Should assign correct partitions when partitions differ between topics"),
                arguments(3, 3, List.of(0, 1, 2), List.of(0, 1, 2), "Should assign correct partitions when partitions same between topics")
        );
    }
}
