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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TopicMetadata;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;

class EndpointToPartitionsManagerTest {

    private static final LogContext LOG_CONTEXT = new LogContext();

    @ParameterizedTest(name = "{5}")
    @MethodSource("argsProvider")
    void testEndpointToPartitionsWithTwoTopicsAndDifferentPartitions(int topicAPartitions,
                                                                     int topicBPartitions,
                                                                     List<Integer> topicAExpectedPartitions,
                                                                     List<Integer> topicBExpectedPartitions,
                                                                     String testName
                                                                     ) {
        Uuid topicAId = Uuid.randomUuid();
        Uuid topicBId = Uuid.randomUuid();

        Map<Integer, Set<String>> emptyRackMap = Collections.emptyMap();
        StreamsGroupHeartbeatResponseData.Endpoint endpoint = new StreamsGroupHeartbeatResponseData.Endpoint();
        endpoint.setPort(8080);
        endpoint.setHost("localhost");

        Map<String, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put("Topic-A", new TopicMetadata(topicAId, "Topic-A", topicAPartitions, emptyRackMap));
        topicMetadata.put("Topic-B", new TopicMetadata(topicBId, "Topic-B", topicBPartitions, emptyRackMap));

        StreamsGroup streamsGroup = createStreamsGroup("streamsGroup");
        streamsGroup.setPartitionMetadata(topicMetadata);
        streamsGroup.setGroupEpoch(1);
        streamsGroup.setTopology(topology());

        EndpointToPartitionsManager endpointToPartitionsManager = new EndpointToPartitionsManager();

        StreamsGroupHeartbeatResponseData.EndpointToPartitions result = endpointToPartitionsManager.endpointToPartitions(null, endpoint, streamsGroup);

        assertEquals(endpoint, result.userEndpoint());
        assertEquals(2, result.partitions().size());

        StreamsGroupHeartbeatResponseData.TopicPartition topicAPartition = result.partitions().get(0);
        assertEquals("Topic-A", topicAPartition.topic());
        assertEquals(topicAExpectedPartitions, topicAPartition.partitions());
        
        StreamsGroupHeartbeatResponseData.TopicPartition topicBPartition = result.partitions().get(1);
        assertEquals("Topic-B", topicBPartition.topic());
        assertEquals(topicBExpectedPartitions, topicBPartition.partitions());
    }

    static Stream<Arguments> argsProvider() {
        return Stream.of(
                arguments(2, 5, new TreeSet<>(List.of(0, 1, 2, 3, 4)), List.of(0, 1), List.of(0, 1, 2, 3, 4), "Should assign correct partitions when partitions differ between topics"),
                arguments(3, 3, new TreeSet<>(List.of(0, 1, 2)), List.of(0, 1, 2), List.of(0, 1, 2), "Should assign correct partitions when partitions same between topics")
        );
    }

    private StreamsTopology topology() {
        StreamsGroupTopologyValue.Subtopology subtopology = new StreamsGroupTopologyValue.Subtopology();
        subtopology.setSubtopologyId("subtopology-1");
        subtopology.setSourceTopics(List.of("Topic-A", "Topic-B"));
        return new StreamsTopology(1, Map.of("subtopology-1", subtopology));
    }

    private StreamsGroup createStreamsGroup(String groupId) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        return new StreamsGroup(
                LOG_CONTEXT,
                snapshotRegistry,
                groupId,
                mock(GroupCoordinatorMetricsShard.class)
        );
    }

}