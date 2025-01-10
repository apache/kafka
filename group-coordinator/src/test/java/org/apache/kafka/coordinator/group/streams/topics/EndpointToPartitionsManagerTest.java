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
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TopicMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EndpointToPartitionsManagerTest {

    private StreamsGroup streamsGroup;
    private StreamsGroupMember streamsGroupMember;
    private ConfiguredTopology configuredTopology;
    private ConfiguredSubtopology configuredSubtopology;

    private EndpointToPartitionsManager endpointToPartitionsManager;
    private final Map<String, Set<Integer>> activeTasks = new HashMap<>();
    private final Map<String, Set<Integer>> standbyTasks = new HashMap<>();
    private final StreamsGroupHeartbeatResponseData.Endpoint responseEndpoint = new StreamsGroupHeartbeatResponseData.Endpoint();

    @BeforeEach
    public void setUp() {
        streamsGroup = mock(StreamsGroup.class);
        streamsGroupMember = mock(StreamsGroupMember.class);
        configuredTopology = mock(ConfiguredTopology.class);
        endpointToPartitionsManager = new EndpointToPartitionsManager();

        configuredSubtopology = new ConfiguredSubtopology();
        configuredSubtopology.setSourceTopics( Set.of( "Topic-A", "Topic-B"));
        ConfiguredInternalTopic stateChangelogTopic = new ConfiguredInternalTopic("StateChangeLog-A");
        ConfiguredInternalTopic stateChangelogTopic2 = new ConfiguredInternalTopic("StateChangeLog-B");
        configuredSubtopology.setStateChangelogTopics( Map.of( "StateChangeLog-A", stateChangelogTopic, "StateChangeLog-B", stateChangelogTopic2 ) );
        configuredTopology.subtopologies().put("0", configuredSubtopology);

        responseEndpoint.setHost("localhost");
        responseEndpoint.setPort(9092);
    }

    @Test
    void testEndpointToPartitionsWithStandbyTaskAssignments() {

        Map<String, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put("Topic-A", new TopicMetadata(Uuid.randomUuid(), "Topic-A", 3, Collections.emptyMap()));
        topicMetadata.put("Topic-B", new TopicMetadata(Uuid.randomUuid(), "Topic-B", 3, Collections.emptyMap()));
        topicMetadata.put("StateChangeLog-A", new TopicMetadata(Uuid.randomUuid(), "StateChangeLog-A", 3, Collections.emptyMap()));
        topicMetadata.put("StateChangeLog-B", new TopicMetadata(Uuid.randomUuid(), "StateChangeLog-B", 3, Collections.emptyMap()));

         activeTasks.put("0", Set.of(0, 1, 2));
         standbyTasks.put("0", Set.of(0, 1, 2));
         when(streamsGroupMember.assignedActiveTasks()).thenReturn(activeTasks);
         when(streamsGroupMember.assignedStandbyTasks()).thenReturn(standbyTasks);
         when((streamsGroup.partitionMetadata())).thenReturn(topicMetadata);
         when(streamsGroup.configuredTopology()).thenReturn(configuredTopology);
         when(configuredTopology.subtopologies()).thenReturn(Map.of( "0", configuredSubtopology ) );

        // Invoke the method under test
        StreamsGroupHeartbeatResponseData.EndpointToPartitions result =
                endpointToPartitionsManager.endpointToPartitions(streamsGroupMember, responseEndpoint, streamsGroup);

        // Validate the results
        assertEquals(responseEndpoint, result.userEndpoint());
        assertEquals(4, result.partitions().size());

        List< StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitions = result.partitions();

        topicPartitions.sort(Comparator.comparing(StreamsGroupHeartbeatResponseData.TopicPartition::topic));
        
        StreamsGroupHeartbeatResponseData.TopicPartition activePartition = topicPartitions.get(0);
        assertEquals("StateChangeLog-A", activePartition.topic());
        assertEquals(List.of(0, 1, 2), activePartition.partitions().stream().sorted().toList());

        StreamsGroupHeartbeatResponseData.TopicPartition standbyPartition = result.partitions().get(1);
        assertEquals("StateChangeLog-B", standbyPartition.topic());
        assertEquals(List.of(0, 1, 2), standbyPartition.partitions().stream().sorted().toList());

        StreamsGroupHeartbeatResponseData.TopicPartition topicAPartition = result.partitions().get(2);
        assertEquals("Topic-A", topicAPartition.topic());
        assertEquals(List.of(0, 1, 2), topicAPartition.partitions().stream().sorted().toList());

        StreamsGroupHeartbeatResponseData.TopicPartition topicBPartition = result.partitions().get(3);
        assertEquals("Topic-B", topicBPartition.topic());
        assertEquals(List.of(0, 1, 2), topicBPartition.partitions().stream().sorted().toList());
    }

    @ParameterizedTest(name = "{4}")
    @MethodSource("argsProvider")
    void testEndpointToPartitionsWithTwoTopicsAndDifferentPartitions(int topicAPartitions,
                                                                     int topicBPartitions,
                                                                     List<Integer> topicAExpectedPartitions,
                                                                     List<Integer> topicBExpectedPartitions,
                                                                     String testName
                                                                     ) {

        Map<Integer, Set<String>> emptyRackMap = Collections.emptyMap();

        Map<String, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put("Topic-A", new TopicMetadata(Uuid.randomUuid(), "Topic-A", topicAPartitions, emptyRackMap));
        topicMetadata.put("Topic-B", new TopicMetadata(Uuid.randomUuid(), "Topic-B", topicBPartitions, emptyRackMap));

        activeTasks.put("0", Set.of(0, 1, 2, 3, 4));
        when(streamsGroupMember.assignedStandbyTasks()).thenReturn(Collections.emptyMap());
        when(streamsGroupMember.assignedActiveTasks()).thenReturn(activeTasks);
        when(streamsGroup.partitionMetadata()).thenReturn(topicMetadata);
        when(streamsGroup.configuredTopology()).thenReturn(configuredTopology);
        when(configuredTopology.subtopologies()).thenReturn(Map.of( "0", configuredSubtopology ) );

        EndpointToPartitionsManager endpointToPartitionsManager = new EndpointToPartitionsManager();

        StreamsGroupHeartbeatResponseData.EndpointToPartitions result = endpointToPartitionsManager.endpointToPartitions(streamsGroupMember, responseEndpoint, streamsGroup);

        assertEquals(responseEndpoint, result.userEndpoint());
        assertEquals(2, result.partitions().size());

        List<StreamsGroupHeartbeatResponseData.TopicPartition> topicPartitions = result.partitions();
        topicPartitions.sort(Comparator.comparing(StreamsGroupHeartbeatResponseData.TopicPartition::topic));

        StreamsGroupHeartbeatResponseData.TopicPartition topicAPartition = result.partitions().get(0);
        assertEquals("Topic-A", topicAPartition.topic());
        assertEquals(topicAExpectedPartitions, topicAPartition.partitions().stream().sorted().toList());
        
        StreamsGroupHeartbeatResponseData.TopicPartition topicBPartition = result.partitions().get(1);
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