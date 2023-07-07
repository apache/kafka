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
package org.apache.kafka.streams.processor.internals.assignment;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.RackAwareTaskAssignor;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.junit.Test;

public class RackAwareTaskAssignorTest {
    private final static String TOPIC0 = "topic0";
    private final static String TOPIC1 = "topic1";
    private static final String USER_END_POINT = "localhost:8080";
    private static final String APPLICATION_ID = "stream-partition-assignor-test";

    private final Node node0 = new Node(0, "node0", 1, "rack1");
    private final Node node1 = new Node(1, "node1", 1, "rack2");
    private final Node node2 = new Node(2, "node2", 1, "rack3");
    private final Node noRackNode = new Node(3, "node3", 1);
    private final Node[] replicas1 = new Node[] {node0, node1};
    private final Node[] replicas2 = new Node[] {node1, node2};

    private final PartitionInfo partitionInfo00 = new PartitionInfo(TOPIC0, 0, node0, replicas1, replicas1);
    private final PartitionInfo partitionInfo01 = new PartitionInfo(TOPIC0, 1, node0, replicas2, replicas2);

    private final TopicPartition partitionWithoutInfo00 = new TopicPartition(TOPIC0, 0);
    private final TopicPartition partitionWithoutInfo01 = new TopicPartition(TOPIC0, 1);
    private final TopicPartition partitionWithoutInfo10 = new TopicPartition(TOPIC1, 0);

    private final UUID process0UUID  = UUID.randomUUID();
    private final UUID process1UUID = UUID.randomUUID();

    private final Subtopology subtopology1 = new Subtopology(1, "topology1");

    private final MockTime time = new MockTime();
    private final StreamsConfig streamsConfig = new StreamsConfig(configProps());
    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();

    private final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            time,
            streamsConfig,
            mockClientSupplier.restoreConsumer,
            false
    );

    private Map<String, Object> configProps() {
        final Map<String, Object> configurationMap = new HashMap<>();
        configurationMap.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        configurationMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, USER_END_POINT);

        final ReferenceContainer referenceContainer = new ReferenceContainer();
        /*
        referenceContainer.mainConsumer = consumer;
        referenceContainer.adminClient = adminClient;
        referenceContainer.taskManager = taskManager;
        referenceContainer.streamsMetadataState = streamsMetadataState;
        referenceContainer.time = time;
        */
        configurationMap.put(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR, referenceContainer);
        return configurationMap;
    }

    @Test
    public void disableActiveSinceMissingClusterInfo() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask1(true),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        // False since partitionWithoutInfo10 is missing in cluster metadata
        assertFalse(assignor.canEnableRackAwareAssignorForActiveTasks());
        assertFalse(assignor.populateTopicsToDiscribe(new HashSet<>()));
        assertTrue(assignor.validateClientRack());
    }

    @Test
    public void disableActiveSinceRackMissingInNode() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterWithPartitionMissingRack(),
            getTaskTopicPartitionMapForTask1(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertTrue(assignor.validateClientRack());
        assertFalse(assignor.populateTopicsToDiscribe(new HashSet<>()));
        // False since nodeMissingRack has one node which doesn't have rack
        assertFalse(assignor.canEnableRackAwareAssignorForActiveTasks());
    }

    @Test
    public void disableActiveSinceRackMissingInClient() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask1(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(true),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        // False since process1 doesn't have rackId
        assertFalse(assignor.validateClientRack());
        assertFalse(assignor.canEnableRackAwareAssignorForActiveTasks());
    }

    @Test
    public void disableActiveSinceRackDiffersInSameProcess() {
        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();

        // Different consumers in same process have different rack ID. This shouldn't happen.
        // If happens, there's a bug somewhere
        processRacks.computeIfAbsent(process0UUID, k -> new HashMap<>()).put("consumer1", Optional.of("rack1"));
        processRacks.computeIfAbsent(process0UUID, k -> new HashMap<>()).put("consumer2", Optional.of("rack2"));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask1(),
            getTopologyGroupTaskMap(),
            processRacks,
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertFalse(assignor.validateClientRack());
        assertFalse(assignor.canEnableRackAwareAssignorForActiveTasks());
    }

    @Test
    public void enableRackAwareAssignorForActiveWithoutDescribingTopics() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask1(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        // partitionWithoutInfo00 has rackInfo in cluster metadata
        assertTrue(assignor.canEnableRackAwareAssignorForActiveTasks());
    }

    @Test
    public void enableRackAwareAssignorForActiveWithDescribingTopics() {
        final MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doReturn(
            Collections.singletonMap(
                TOPIC0,
                Collections.singletonList(
                    new TopicPartitionInfo(0, node0, Arrays.asList(replicas1), Collections.emptyList())
                )
            )
        ).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(TOPIC0));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterWithNoNode(),
            getTaskTopicPartitionMapForTask1(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            spyTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertTrue(assignor.canEnableRackAwareAssignorForActiveTasks());
    }

    @Test
    public void disableRackAwareAssignorForActiveWithDescribingTopicsFailure() {
        final MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doThrow(new TimeoutException("Timeout describing topic")).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(TOPIC0));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterWithNoNode(),
            getTaskTopicPartitionMapForTask1(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            spyTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertFalse(assignor.canEnableRackAwareAssignorForActiveTasks());
        assertTrue(assignor.populateTopicsToDiscribe(new HashSet<>()));
    }

    private Cluster getClusterForTopic0() {
        return new Cluster(
            "cluster",
            new HashSet<>(Arrays.asList(node0, node1, node2)),
            new HashSet<>(Arrays.asList(partitionInfo00, partitionInfo01)),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Cluster getClusterWithPartitionMissingRack() {
        final Node[] nodeMissingRack = new Node[]{node0, noRackNode};
        final PartitionInfo partitionInfoMissingNode = new PartitionInfo(TOPIC0, 0, node0, nodeMissingRack, nodeMissingRack);
        return new Cluster(
            "cluster",
            new HashSet<>(Arrays.asList(node0, node1, node2)),
            new HashSet<>(Arrays.asList(partitionInfoMissingNode, partitionInfo01)),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Cluster getClusterWithNoNode() {
        final PartitionInfo noNodeInfo = new PartitionInfo(TOPIC0, 0, null, new Node[0], new Node[0]);

        return new Cluster(
            "cluster",
            new HashSet<>(Arrays.asList(node0, node1, node2, Node.noNode())), // mockClientSupplier.setCluster requires noNode
            Collections.singleton(noNodeInfo),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Map<UUID, Map<String, Optional<String>>> getProcessRacksForProcess0() {
        return getProcessRacksForProcess0(false);
    }

    private Map<UUID, Map<String, Optional<String>>> getProcessRacksForProcess0(final boolean missingRack) {
        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();
        final Optional<String> rack = missingRack ? Optional.empty() : Optional.of("rack1");
        processRacks.put(process0UUID, Collections.singletonMap("consumer1", rack));
        return processRacks;
    }

    private Map<TaskId, Set<TopicPartition>> getTaskTopicPartitionMapForTask1() {
        return getTaskTopicPartitionMapForTask1(false);
    }

    private Map<TaskId, Set<TopicPartition>> getTaskTopicPartitionMapForTask1(final boolean extraTopic) {
        final Set<TopicPartition> topicPartitions = new HashSet<>();
        topicPartitions.add(partitionWithoutInfo00);
        if (extraTopic) {
            topicPartitions.add(partitionWithoutInfo10);
        }
        return Collections.singletonMap(new TaskId(1, 1), topicPartitions);
    }

    private Map<Subtopology, Set<TaskId>> getTopologyGroupTaskMap() {
        return Collections.singletonMap(subtopology1, Collections.singleton(new TaskId(1, 1)));
    }
}
