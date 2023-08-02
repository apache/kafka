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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_CLIENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NO_RACK_NODE;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PI_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PI_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PI_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PI_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PI_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.REPLICA_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.SUBTOPOLOGY_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TP_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TP_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TP_0_NAME;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TP_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TP_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TP_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuidForInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

@RunWith(Parameterized.class)
public class RackAwareTaskAssignorTest {
    private static final String USER_END_POINT = "localhost:8080";
    private static final String APPLICATION_ID = "stream-partition-assignor-test";

    private final MockTime time = new MockTime();
    private final StreamsConfig streamsConfig = new StreamsConfig(configProps());
    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();

    private int trafficCost;
    private int nonOverlapCost;

    @Parameter
    public boolean stateful;

    @Parameterized.Parameters(name = "stateful={0}")
    public static Collection<Object[]> getParamStoreType() {
        return asList(new Object[][] {
            {true},
            {false}
        });
    }

    private final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            time,
            streamsConfig,
            mockClientSupplier.restoreConsumer,
            false
    );

    @Before
    public void setUp() {
        if (stateful) {
            trafficCost = 10;
            nonOverlapCost = 1;
        } else {
            trafficCost = 1;
            nonOverlapCost = 0;
        }
    }

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
    public void shouldDisableActiveWhenMissingClusterInfo() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(true),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        // False since partitionWithoutInfo10 is missing in cluster metadata
        assertFalse(assignor.canEnableRackAwareAssignor());
        assertFalse(assignor.populateTopicsToDescribe(new HashSet<>()));
    }

    @Test
    public void shouldDisableActiveWhenRackMissingInNode() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterWithPartitionMissingRack(),
            getTaskTopicPartitionMapForTask0(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertFalse(assignor.populateTopicsToDescribe(new HashSet<>()));
        // False since nodeMissingRack has one node which doesn't have rack
        assertFalse(assignor.canEnableRackAwareAssignor());
    }

    @Test
    public void shouldReturnInvalidClientRackWhenRackMissingInClientConsumer() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(true),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );
        // False since process1 doesn't have rackId
        assertFalse(assignor.validClientRack());
    }

    @Test
    public void shouldReturnFalseWhenRackMissingInProcess() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            getTopologyGroupTaskMap(),
            getProcessWithNoConsumerRacks(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );
        // False since process1 doesn't have rackId
        assertFalse(assignor.validClientRack());
    }

    @Test
    public void shouldPopulateRacksForProcess() {
        // Throws since process1 doesn't have rackId
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );
        final Map<UUID, String> racksForProcess = assignor.racksForProcess();
        assertEquals(mkMap(mkEntry(UUID_1, RACK_1)), racksForProcess);
    }

    @Test
    public void shouldReturnInvalidClientRackWhenRackDiffersInSameProcess() {
        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();

        // Different consumers in same process have different rack ID. This shouldn't happen.
        // If happens, there's a bug somewhere
        processRacks.computeIfAbsent(UUID_1, k -> new HashMap<>()).put("consumer1", Optional.of("rack1"));
        processRacks.computeIfAbsent(UUID_1, k -> new HashMap<>()).put("consumer2", Optional.of("rack2"));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            getTopologyGroupTaskMap(),
            processRacks,
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertFalse(assignor.validClientRack());
    }

    @Test
    public void shouldEnableRackAwareAssignorWithoutDescribingTopics() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        // partitionWithoutInfo00 has rackInfo in cluster metadata
        assertTrue(assignor.canEnableRackAwareAssignor());
    }

    @Test
    public void shouldEnableRackAwareAssignorWithDescribingTopics() {
        final MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doReturn(
            Collections.singletonMap(
                TP_0_NAME,
                Collections.singletonList(
                    new TopicPartitionInfo(0, NODE_0, Arrays.asList(REPLICA_1), Collections.emptyList())
                )
            )
        ).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(TP_0_NAME));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterWithNoNode(),
            getTaskTopicPartitionMapForTask0(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            spyTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertTrue(assignor.canEnableRackAwareAssignor());
    }

    @Test
    public void shouldDisableRackAwareAssignorWithDescribingTopicsFailure() {
        final MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doThrow(new TimeoutException("Timeout describing topic")).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(
            TP_0_NAME));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterWithNoNode(),
            getTaskTopicPartitionMapForTask0(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            spyTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        assertFalse(assignor.canEnableRackAwareAssignor());
        assertTrue(assignor.populateTopicsToDescribe(new HashSet<>()));
    }

    @Test
    public void shouldOptimizeEmptyActiveTasks() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0And1(),
            getTaskTopicPartitionMapForAllTasks(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        final ClientState clientState0 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState0.assignActiveTasks(mkSet(TASK_0_1, TASK_1_1));

        final SortedMap<UUID, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(UUID_1, clientState0)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet();

        assertTrue(assignor.canEnableRackAwareAssignor());
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(0, originalCost);

        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(0, cost);

        assertEquals(mkSet(TASK_0_1, TASK_1_1), clientState0.activeTasks());
    }

    @Test
    public void shouldOptimizeActiveTasks() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0And1(),
            getTaskTopicPartitionMapForAllTasks(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        final ClientState clientState0 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState0.assignActiveTasks(mkSet(TASK_0_1, TASK_1_1));
        clientState1.assignActive(TASK_1_0);
        clientState2.assignActive(TASK_0_0);

        // task_0_0 has same rack as UUID_1
        // task_0_1 has same rack as UUID_2 and UUID_3
        // task_1_0 has same rack as UUID_1 and UUID_3
        // task_1_1 has same rack as UUID_2
        // Optimal assignment is UUID_1: {0_0, 1_0}, UUID_2: {1_1}, UUID_3: {0_1} which result in no cross rack traffic
        final SortedMap<UUID, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(UUID_1, clientState0),
            mkEntry(UUID_2, clientState1),
            mkEntry(UUID_3, clientState2)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);

        assertTrue(assignor.canEnableRackAwareAssignor());
        int expected = stateful ? 40 : 4;
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expected, originalCost);

        expected = stateful ? 4 : 0;
        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expected, cost);

        assertEquals(mkSet(TASK_0_0, TASK_1_0), clientState0.activeTasks());
        assertEquals(mkSet(TASK_1_1), clientState1.activeTasks());
        assertEquals(mkSet(TASK_0_1), clientState2.activeTasks());
    }

    @Test
    public void shouldOptimizeRandom() {
        final int nodeSize = 30;
        final int tpSize = 40;
        final int clientSize = 30;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(tpSize);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getRandomCluster(nodeSize, tpSize),
            taskTopicPartitionMap,
            getTopologyGroupTaskMap(),
            getRandomProcessRacks(clientSize, nodeSize),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        final SortedMap<UUID, ClientState> clientStateMap = getRandomClientState(clientSize, tpSize);
        final SortedSet<TaskId> taskIds = (SortedSet<TaskId>) taskTopicPartitionMap.keySet();

        final Map<UUID, Integer> clientTaskCount = clientStateMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().activeTasks().size()));

        assertTrue(assignor.canEnableRackAwareAssignor());
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertThat(cost, lessThanOrEqualTo(originalCost));

        for (final Entry<UUID, ClientState> entry : clientStateMap.entrySet()) {
            assertEquals((int) clientTaskCount.get(entry.getKey()), entry.getValue().activeTasks().size());
        }
    }

    @Test
    public void shouldMaintainOriginalAssignment() {
        final int nodeSize = 20;
        final int tpSize = 40;
        final int clientSize = 30;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(tpSize);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getRandomCluster(nodeSize, tpSize),
            taskTopicPartitionMap,
            getTopologyGroupTaskMap(),
            getRandomProcessRacks(clientSize, nodeSize),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        final SortedMap<UUID, ClientState> clientStateMap = getRandomClientState(clientSize, tpSize);
        final SortedSet<TaskId> taskIds = (SortedSet<TaskId>) taskTopicPartitionMap.keySet();

        final Map<TaskId, UUID> taskClientMap = new HashMap<>();
        for (final Entry<UUID, ClientState> entry : clientStateMap.entrySet()) {
            entry.getValue().activeTasks().forEach(t -> taskClientMap.put(t, entry.getKey()));
        }
        assertEquals(taskIds.size(), taskClientMap.size());

        // Because trafficCost is 0, original assignment should be maintained
        assertTrue(assignor.canEnableRackAwareAssignor());
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, 0, 1);
        assertEquals(0, originalCost);

        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, 0, 1);
        assertEquals(0, cost);

        // Make sure assignment doesn't change
        for (final Entry<TaskId, UUID> entry : taskClientMap.entrySet()) {
            final ClientState clientState = clientStateMap.get(entry.getValue());
            assertTrue(clientState.hasAssignedTask(entry.getKey()));
        }
    }

    @Test
    public void shouldOptimizeActiveTasksWithMoreClients() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0And1(),
            getTaskTopicPartitionMapForAllTasks(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        final ClientState clientState0 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState1.assignActive(TASK_1_0);
        clientState2.assignActive(TASK_0_0);

        // task_0_0 has same rack as UUID_1 and UUID_2
        // task_1_0 has same rack as UUID_1 and UUID_3
        // Optimal assignment is UUID_1: {}, UUID_2: {0_0}, UUID_3: {1_0} which result in no cross rack traffic
        // and keeps UUID_1 empty since it was originally empty
        final SortedMap<UUID, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(UUID_1, clientState0),
            mkEntry(UUID_2, clientState1),
            mkEntry(UUID_3, clientState2)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_1_0);

        assertTrue(assignor.canEnableRackAwareAssignor());
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        int expected = stateful ? 20 : 2;
        assertEquals(expected, originalCost);

        expected = stateful ? 2 : 0;
        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expected, cost);

        // UUID_1 remains empty
        assertEquals(mkSet(), clientState0.activeTasks());
        assertEquals(mkSet(TASK_0_0), clientState1.activeTasks());
        assertEquals(mkSet(TASK_1_0), clientState2.activeTasks());
    }

    @Test
    public void shouldOptimizeActiveTasksWithMoreClientsWithMoreThanOneTask() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0And1(),
            getTaskTopicPartitionMapForAllTasks(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        final ClientState clientState0 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState1.assignActiveTasks(mkSet(TASK_0_1, TASK_1_0));
        clientState2.assignActive(TASK_0_0);

        // task_0_0 has same rack as UUID_1 and UUID_2
        // task_0_1 has same rack as UUID_2 and UUID_3
        // task_1_0 has same rack as UUID_1 and UUID_3
        // Optimal assignment is UUID_1: {}, UUID_2: {0_0, 0_1}, UUID_3: {1_0} which result in no cross rack traffic
        final SortedMap<UUID, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(UUID_1, clientState0),
            mkEntry(UUID_2, clientState1),
            mkEntry(UUID_3, clientState2)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_0);

        assertTrue(assignor.canEnableRackAwareAssignor());
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        int expected = stateful ? 20 : 2;
        assertEquals(expected, originalCost);

        expected = stateful ? 2 : 0;
        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expected, cost);

        // Because original assignment is not balanced (3 tasks but client 0 has no task), we maintain it
        assertEquals(mkSet(), clientState0.activeTasks());
        assertEquals(mkSet(TASK_0_0, TASK_0_1), clientState1.activeTasks());
        assertEquals(mkSet(TASK_1_0), clientState2.activeTasks());
    }

    @Test
    public void shouldBalanceAssignmentWithMoreCost() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0And1(),
            getTaskTopicPartitionMapForAllTasks(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        final ClientState clientState0 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState0.assignActiveTasks(mkSet(TASK_0_0, TASK_1_1));
        clientState1.assignActive(TASK_0_1);

        // task_0_0 has same rack as UUID_2
        // task_0_1 has same rack as UUID_2
        // task_1_1 has same rack as UUID_2
        // UUID_5 is not in same rack as any task
        final SortedMap<UUID, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(UUID_2, clientState0),
            mkEntry(UUID_5, clientState1)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_1);

        assertTrue(assignor.canEnableRackAwareAssignor());
        final int expectedCost = stateful ? 10 : 1;
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expectedCost, originalCost);

        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expectedCost, cost);

        // Even though assigning all tasks to UUID_2 will result in min cost, but it's not balanced
        // assignment. That's why TASK_0_1 is still assigned to UUID_5
        assertEquals(mkSet(TASK_0_0, TASK_1_1), clientState0.activeTasks());
        assertEquals(mkSet(TASK_0_1), clientState1.activeTasks());
    }

    @Test
    public void shouldThrowIfMissingCallcanEnableRackAwareAssignor() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0And1(),
            getTaskTopicPartitionMapForAllTasks(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        final ClientState clientState0 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState0.assignActiveTasks(mkSet(TASK_0_0, TASK_1_1));
        clientState1.assignActive(TASK_0_1);

        final SortedMap<UUID, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(UUID_2, clientState0),
            mkEntry(UUID_5, clientState1)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_1);
        final Exception exception = assertThrows(IllegalStateException.class,
            () -> assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost));
        Assertions.assertEquals("TopicPartition topic0-0 has no rack information. "
            + "Maybe forgot to call canEnableRackAwareAssignor first", exception.getMessage());
    }

    @Test
    public void shouldThrowIfTaskInMultipleClients() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0And1(),
            getTaskTopicPartitionMapForAllTasks(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        final ClientState clientState0 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState0.assignActiveTasks(mkSet(TASK_0_0, TASK_1_1));
        clientState1.assignActiveTasks(mkSet(TASK_0_1, TASK_1_1));

        final SortedMap<UUID, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(UUID_2, clientState0),
            mkEntry(UUID_5, clientState1)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_1);
        assertTrue(assignor.canEnableRackAwareAssignor());
        final Exception exception = assertThrows(IllegalArgumentException.class,
            () -> assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost));
        Assertions.assertEquals(
            "Task 1_1 assigned to multiple clients 00000000-0000-0000-0000-000000000005, "
                + "00000000-0000-0000-0000-000000000002", exception.getMessage());
    }

    @Test
    public void shouldThrowIfTaskMissingInClients() {
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0And1(),
            getTaskTopicPartitionMapForAllTasks(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            new AssignorConfiguration(streamsConfig.originals()).assignmentConfigs()
        );

        final ClientState clientState0 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState0.assignActiveTasks(mkSet(TASK_0_0, TASK_1_1));
        clientState1.assignActive(TASK_0_1);

        final SortedMap<UUID, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(UUID_2, clientState0),
            mkEntry(UUID_5, clientState1)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);
        assertTrue(assignor.canEnableRackAwareAssignor());
        final Exception exception = assertThrows(IllegalArgumentException.class,
            () -> assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost));
        Assertions.assertEquals(
            "Task 1_0 not assigned to any client", exception.getMessage());
    }

    private Cluster getRandomCluster(final int nodeSize, final int tpSize) {
        final List<Node> nodeList = new ArrayList<>(nodeSize);
        for (int i = 0; i < nodeSize; i++) {
            nodeList.add(new Node(i, "node" + i, 1, "rack" + i));
        }
        Collections.shuffle(nodeList);
        final Set<PartitionInfo> partitionInfoSet = new HashSet<>();
        for (int i = 0; i < tpSize; i++) {
            final Node firstNode = nodeList.get(i % nodeSize);
            final Node secondNode = nodeList.get((i + 1) % nodeSize);
            final Node[] replica = new Node[] {firstNode, secondNode};
            partitionInfoSet.add(new PartitionInfo("topic" + i, 0, firstNode, replica, replica));
        }

        return new Cluster(
            "cluster",
            new HashSet<>(nodeList),
            partitionInfoSet,
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Map<UUID, Map<String, Optional<String>>> getRandomProcessRacks(final int clientSize, final int nodeSize) {
        final List<String> racks = new ArrayList<>(nodeSize);
        for (int i = 0; i < nodeSize; i++) {
            racks.add("rack" + i);
        }
        Collections.shuffle(racks);
        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();
        for (int i = 0; i < clientSize; i++) {
            final String rack = racks.get(i % nodeSize);
            processRacks.put(uuidForInt(i), mkMap(mkEntry("1", Optional.of(rack))));
        }
        return processRacks;
    }

    private SortedMap<TaskId, Set<TopicPartition>> getTaskTopicPartitionMap(final int tpSize) {
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = new TreeMap<>();
        for (int i = 0; i < tpSize; i++) {
            taskTopicPartitionMap.put(new TaskId(i, 0), mkSet(new TopicPartition("topic" + i, 0)));
        }
        return taskTopicPartitionMap;
    }

    private SortedMap<UUID, ClientState> getRandomClientState(final int clientSize, final int tpSize) {
        final SortedMap<UUID, ClientState> clientStates = new TreeMap<>();
        final List<TaskId> taskIds = new ArrayList<>(tpSize);
        for (int i = 0; i < tpSize; i++) {
            taskIds.add(new TaskId(i, 0));
        }
        Collections.shuffle(taskIds);
        for (int i = 0; i < clientSize; i++) {
            final ClientState clientState = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
            clientStates.put(uuidForInt(i), clientState);
        }
        Iterator<Entry<UUID, ClientState>> iterator = clientStates.entrySet().iterator();
        for (final TaskId taskId : taskIds) {
            if (iterator.hasNext()) {
                iterator.next().getValue().assignActive(taskId);
            } else {
                iterator = clientStates.entrySet().iterator();
                iterator.next().getValue().assignActive(taskId);
            }
        }
        return clientStates;
    }

    private Cluster getClusterForTopic0And1() {
        return new Cluster(
            "cluster",
            mkSet(NODE_0, NODE_1, NODE_2, NODE_3),
            mkSet(PI_0_0, PI_0_1, PI_1_0, PI_1_1, PI_1_2),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Cluster getClusterForTopic0() {
        return new Cluster(
            "cluster",
            mkSet(NODE_0, NODE_1, NODE_2),
            mkSet(PI_0_0, PI_0_1),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Cluster getClusterWithPartitionMissingRack() {
        final Node[] nodeMissingRack = new Node[]{NODE_0, NO_RACK_NODE};
        final PartitionInfo partitionInfoMissingNode = new PartitionInfo(TP_0_NAME, 0, NODE_0, nodeMissingRack, nodeMissingRack);
        return new Cluster(
            "cluster",
            mkSet(NODE_0, NODE_1, NODE_2),
            mkSet(partitionInfoMissingNode, PI_0_1),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Cluster getClusterWithNoNode() {
        final PartitionInfo noNodeInfo = new PartitionInfo(TP_0_NAME, 0, null, new Node[0], new Node[0]);

        return new Cluster(
            "cluster",
            mkSet(NODE_0, NODE_1, NODE_2, Node.noNode()), // mockClientSupplier.setCluster requires noNode
            Collections.singleton(noNodeInfo),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Map<UUID, Map<String, Optional<String>>> getProcessRacksForAllProcess() {
        return mkMap(
            mkEntry(UUID_1, mkMap(mkEntry("1", Optional.of(RACK_0)))),
            mkEntry(UUID_2, mkMap(mkEntry("1", Optional.of(RACK_1)))),
            mkEntry(UUID_3, mkMap(mkEntry("1", Optional.of(RACK_2)))),
            mkEntry(UUID_4, mkMap(mkEntry("1", Optional.of(RACK_3)))),
            mkEntry(UUID_5, mkMap(mkEntry("1", Optional.of(RACK_4))))
        );
    }

    private Map<UUID, Map<String, Optional<String>>> getProcessRacksForProcess0() {
        return getProcessRacksForProcess0(false);
    }

    private Map<UUID, Map<String, Optional<String>>> getProcessRacksForProcess0(final boolean missingRack) {
        final Map<UUID, Map<String, Optional<String>>> processRacks = new HashMap<>();
        final Optional<String> rack = missingRack ? Optional.empty() : Optional.of("rack1");
        processRacks.put(UUID_1, Collections.singletonMap("consumer1", rack));
        return processRacks;
    }

    private Map<UUID, Map<String, Optional<String>>> getProcessWithNoConsumerRacks() {
        return mkMap(
            mkEntry(UUID_1, mkMap())
        );
    }

    private Map<TaskId, Set<TopicPartition>> getTaskTopicPartitionMapForTask0() {
        return getTaskTopicPartitionMapForTask0(false);
    }

    private Map<TaskId, Set<TopicPartition>> getTaskTopicPartitionMapForTask0(final boolean extraTopic) {
        final Set<TopicPartition> topicPartitions = new HashSet<>();
        topicPartitions.add(TP_0_0);
        if (extraTopic) {
            topicPartitions.add(TP_1_0);
        }
        return Collections.singletonMap(TASK_0_0, topicPartitions);
    }

    private Map<TaskId, Set<TopicPartition>> getTaskTopicPartitionMapForAllTasks() {
        return mkMap(
            mkEntry(TASK_0_0, mkSet(TP_0_0)),
            mkEntry(TASK_0_1, mkSet(TP_0_1)),
            mkEntry(TASK_1_0, mkSet(TP_1_0)),
            mkEntry(TASK_1_1, mkSet(TP_1_1)),
            mkEntry(TASK_1_2, mkSet(TP_1_2))
        );
    }

    private Map<Subtopology, Set<TaskId>> getTopologyGroupTaskMap() {
        return Collections.singletonMap(SUBTOPOLOGY_0, Collections.singleton(new TaskId(1, 1)));
    }
}
