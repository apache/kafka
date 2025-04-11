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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockInternalTopicManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.CHANGELOG_TP_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.CHANGELOG_TP_0_NAME;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_CLIENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NODE_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NO_RACK_NODE;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_6;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_7;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PI_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PI_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.REPLICA_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TP_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TP_0_NAME;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TP_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertBalancedTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.clientTaskCount;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.configProps;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getClusterForAllTopics;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getProcessRacksForAllProcess;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomClientState;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomCluster;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomProcessRacks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTaskChangelogMapForAllTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTaskTopicPartitionMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTaskTopicPartitionMapForAllTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTasksForTopicGroup;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTopologyGroupTaskMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.mockInternalTopicManagerForChangelog;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.mockInternalTopicManagerForRandomChangelog;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.verifyStandbySatisfyRackReplica;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RackAwareTaskAssignorTest {

    private final MockTime time = new MockTime();
    private final StreamsConfig streamsConfig = new StreamsConfig(configProps(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC));
    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();

    private int trafficCost;
    private int nonOverlapCost;

    private final MockInternalTopicManager mockInternalTopicManager = new MockInternalTopicManager(
            time,
            streamsConfig,
            mockClientSupplier.restoreConsumer,
            false
    );

    static Stream<Arguments> paramStoreType() {
        return Stream.of(
            Arguments.of(true, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY),
            Arguments.of(false, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC),
            Arguments.of(true, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC),
            Arguments.of(false, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)
        );
    }

    public void setUp(final boolean stateful) {
        if (stateful) {
            trafficCost = 10;
            nonOverlapCost = 1;
        } else {
            trafficCost = 1;
            nonOverlapCost = 0;
        }
    }

    private AssignmentConfigs getRackAwareEnabledConfig(final String assignmentStrategy) {
        return new AssignorConfiguration(
            new StreamsConfig(configProps(assignmentStrategy)).originals()).assignmentConfigs();
    }

    private AssignmentConfigs getRackAwareEnabledConfigWithStandby(final int replicaNum, final String assignmentStrategy) {
        return new AssignorConfiguration(
            new StreamsConfig(configProps(assignmentStrategy, replicaNum)).originals()).assignmentConfigs();
    }

    private AssignmentConfigs getRackAwareDisabledConfig() {
        return new AssignorConfiguration(
            new StreamsConfig(configProps(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE)).originals()).assignmentConfigs();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldDisableAssignorFromConfig(final boolean stateful) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = spy(new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(true),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            getRackAwareDisabledConfig(),
            time
        ));

        // False since partitionWithoutInfo10 is missing in cluster metadata
        assertFalse(assignor.canEnableRackAwareAssignor());
        verify(assignor, never()).populateTopicsToDescribe(anySet(), eq(false));
        verify(assignor, never()).populateTopicsToDescribe(anySet(), eq(true));
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldDisableActiveWhenMissingClusterInfo(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = spy(new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(true),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        ));

        // False since partitionWithoutInfo10 is missing in cluster metadata
        assertFalse(assignor.canEnableRackAwareAssignor());
        verify(assignor, times(1)).populateTopicsToDescribe(anySet(), eq(false));
        verify(assignor, never()).populateTopicsToDescribe(anySet(), eq(true));
        assertFalse(assignor.populateTopicsToDescribe(new HashSet<>(), false));
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldDisableActiveWhenRackMissingInNode(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = spy(new RackAwareTaskAssignor(
            getClusterWithPartitionMissingRack(),
            getTaskTopicPartitionMapForTask0(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        ));

        // False since nodeMissingRack has one node which doesn't have rack
        assertFalse(assignor.canEnableRackAwareAssignor());
        verify(assignor, times(1)).populateTopicsToDescribe(anySet(), eq(false));
        verify(assignor, never()).populateTopicsToDescribe(anySet(), eq(true));
        assertFalse(assignor.populateTopicsToDescribe(new HashSet<>(), false));
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldReturnInvalidClientRackWhenRackMissingInClientConsumer(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(true),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );
        // False since process1 doesn't have rackId
        assertFalse(assignor.validClientRack());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldReturnFalseWhenRackMissingInProcess(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessWithNoConsumerRacks(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );
        // False since process1 doesn't have rackId
        assertFalse(assignor.validClientRack());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldPopulateRacksForProcess(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        // Throws since process1 doesn't have rackId
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );
        final Map<ProcessId, String> racksForProcess = assignor.racksForProcess();
        assertEquals(mkMap(mkEntry(PID_1, RACK_1)), racksForProcess);
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldReturnInvalidClientRackWhenRackDiffersInSameProcess(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final Map<ProcessId, Map<String, Optional<String>>> processRacks = new HashMap<>();

        // Different consumers in same process have different rack ID. This shouldn't happen.
        // If happens, there's a bug somewhere
        processRacks.computeIfAbsent(PID_1, k -> new HashMap<>()).put("consumer1", Optional.of("rack1"));
        processRacks.computeIfAbsent(PID_1, k -> new HashMap<>()).put("consumer2", Optional.of("rack2"));

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            mkMap(),
            getTopologyGroupTaskMap(),
            processRacks,
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        assertFalse(assignor.validClientRack());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldEnableRackAwareAssignorWithoutDescribingTopics(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        // partitionWithoutInfo00 has rackInfo in cluster metadata
        assertTrue(assignor.canEnableRackAwareAssignor());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldEnableRackAwareAssignorWithCacheResult(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = spy(new RackAwareTaskAssignor(
            getClusterForTopic0(),
            getTaskTopicPartitionMapForTask0(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        ));

        // partitionWithoutInfo00 has rackInfo in cluster metadata
        assertTrue(assignor.canEnableRackAwareAssignor());
        verify(assignor, times(1)).populateTopicsToDescribe(anySet(), eq(false));

        // Should use cache result
        Mockito.reset(assignor);
        assertTrue(assignor.canEnableRackAwareAssignor());
        verify(assignor, never()).populateTopicsToDescribe(anySet(), eq(false));
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldEnableRackAwareAssignorWithDescribingTopics(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
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
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            spyTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        assertTrue(assignor.canEnableRackAwareAssignor());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldEnableRackAwareAssignorWithStandbyDescribingTopics(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doReturn(
            Collections.singletonMap(
                TP_0_NAME,
                Collections.singletonList(
                    new TopicPartitionInfo(0, NODE_0, Arrays.asList(REPLICA_1), Collections.emptyList())
                )
            )
        ).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(TP_0_NAME));

        doReturn(
            Collections.singletonMap(
                CHANGELOG_TP_0_NAME,
                Collections.singletonList(
                    new TopicPartitionInfo(0, NODE_0, Arrays.asList(REPLICA_1), Collections.emptyList())
                )
            )
        ).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(CHANGELOG_TP_0_NAME));

        final RackAwareTaskAssignor assignor = spy(new RackAwareTaskAssignor(
            getClusterWithNoNode(),
            getTaskTopicPartitionMapForTask0(),
            getTaskChangeLogTopicPartitionMapForTask0(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            spyTopicManager,
            getRackAwareEnabledConfigWithStandby(1, assignmentStrategy),
            time
        ));

        assertTrue(assignor.canEnableRackAwareAssignor());
        verify(assignor, times(1)).populateTopicsToDescribe(anySet(), eq(false));
        verify(assignor, times(1)).populateTopicsToDescribe(anySet(), eq(true));

        final Map<TopicPartition, Set<String>> racksForPartition = assignor.racksForPartition();
        final Map<TopicPartition, Set<String>> expected = mkMap(
            mkEntry(TP_0_0, Set.of(RACK_1, RACK_2)),
            mkEntry(CHANGELOG_TP_0_0, Set.of(RACK_1, RACK_2))
        );
        assertEquals(expected, racksForPartition);
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldDisableRackAwareAssignorWithStandbyDescribingTopicsFailure(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doReturn(
            Collections.singletonMap(
                TP_0_NAME,
                Collections.singletonList(
                    new TopicPartitionInfo(0, NODE_0, Arrays.asList(REPLICA_1), Collections.emptyList())
                )
            )
        ).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(TP_0_NAME));

        doThrow(new TimeoutException("Timeout describing topic")).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(
            CHANGELOG_TP_0_NAME));

        final RackAwareTaskAssignor assignor = spy(new RackAwareTaskAssignor(
            getClusterWithNoNode(),
            getTaskTopicPartitionMapForTask0(),
            getTaskChangeLogTopicPartitionMapForTask0(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            spyTopicManager,
            getRackAwareEnabledConfigWithStandby(1, assignmentStrategy),
            time
        ));

        assertFalse(assignor.canEnableRackAwareAssignor());
        verify(assignor, times(1)).populateTopicsToDescribe(anySet(), eq(false));
        verify(assignor, times(1)).populateTopicsToDescribe(anySet(), eq(true));
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldDisableRackAwareAssignorWithDescribingTopicsFailure(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final MockInternalTopicManager spyTopicManager = spy(mockInternalTopicManager);
        doThrow(new TimeoutException("Timeout describing topic")).when(spyTopicManager).getTopicPartitionInfo(Collections.singleton(
            TP_0_NAME));

        final RackAwareTaskAssignor assignor = spy(new RackAwareTaskAssignor(
            getClusterWithNoNode(),
            getTaskTopicPartitionMapForTask0(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForProcess0(),
            spyTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        ));

        assertFalse(assignor.canEnableRackAwareAssignor());
        verify(assignor, times(1)).populateTopicsToDescribe(anySet(), eq(false));
        verify(assignor, never()).populateTopicsToDescribe(anySet(), eq(true));
        assertTrue(assignor.populateTopicsToDescribe(new HashSet<>(), false));
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldOptimizeEmptyActiveTasks(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState1.assignActiveTasks(Set.of(TASK_0_1, TASK_1_1));

        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_1, clientState1)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet();

        assertTrue(assignor.canEnableRackAwareAssignor());
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(0, originalCost);

        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(0, cost);

        assertEquals(Set.of(TASK_0_1, TASK_1_1), clientState1.activeTasks());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldOptimizeActiveTasks(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1))
        );
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            mkMap(),
            tasksForTopicGroup,
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState1.assignActiveTasks(Set.of(TASK_0_1, TASK_1_1));
        clientState2.assignActive(TASK_1_0);
        clientState3.assignActive(TASK_0_0);

        // task_0_0 has same rack as ProcessId_1
        // task_0_1 has same rack as ProcessId_2 and ProcessId_3
        // task_1_0 has same rack as ProcessId_1 and ProcessId_3
        // task_1_1 has same rack as ProcessId_2
        // Optimal assignment is ProcessId_1: {0_0, 1_0}, ProcessId_2: {1_1}, ProcessId_3: {0_1} which result in no cross rack traffic
        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_1, clientState1),
            mkEntry(PID_2, clientState2),
            mkEntry(PID_3, clientState3)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);

        assertTrue(assignor.canEnableRackAwareAssignor());
        int expected = stateful ? 40 : 4;
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expected, originalCost);

        expected = stateful ? 4 : 0;
        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expected, cost);

        assertEquals(Set.of(TASK_0_0, TASK_1_0), clientState1.activeTasks());
        assertEquals(Set.of(TASK_1_1), clientState2.activeTasks());
        assertEquals(Set.of(TASK_0_1), clientState3.activeTasks());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldOptimizeRandomActive(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final int nodeSize = 30;
        final int tpSize = 40;
        final int partitionSize = 3;
        final int clientSize = 30;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(tpSize, partitionSize, false);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getRandomCluster(nodeSize, tpSize, partitionSize),
            taskTopicPartitionMap,
            mkMap(),
            getTasksForTopicGroup(tpSize, partitionSize),
            getRandomProcessRacks(clientSize, nodeSize),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        final SortedSet<TaskId> taskIds = (SortedSet<TaskId>) taskTopicPartitionMap.keySet();
        final SortedMap<ProcessId, ClientState> clientStateMap = getRandomClientState(clientSize, tpSize, partitionSize, 1, taskIds);

        final Map<ProcessId, Integer> clientTaskCount = clientTaskCount(clientStateMap, ClientState::activeTaskCount);

        assertTrue(assignor.canEnableRackAwareAssignor());
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertThat(cost, lessThanOrEqualTo(originalCost));

        for (final Entry<ProcessId, ClientState> entry : clientStateMap.entrySet()) {
            assertEquals((int) clientTaskCount.get(entry.getKey()), entry.getValue().activeTasks().size());
        }

        if (assignmentStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            assertBalancedTasks(clientStateMap);
        }
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldMaintainOriginalAssignmentForMinCost(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final int nodeSize = 20;
        final int tpSize = 40;
        final int partitionSize = 3;
        final int clientSize = 30;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(tpSize, partitionSize, false);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getRandomCluster(nodeSize, tpSize, partitionSize),
            taskTopicPartitionMap,
            mkMap(),
            getTasksForTopicGroup(tpSize, partitionSize),
            getRandomProcessRacks(clientSize, nodeSize),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        final SortedSet<TaskId> taskIds = (SortedSet<TaskId>) taskTopicPartitionMap.keySet();
        final SortedMap<ProcessId, ClientState> clientStateMap = getRandomClientState(clientSize, tpSize, partitionSize, 1, taskIds);

        final Map<TaskId, ProcessId> taskClientMap = new HashMap<>();
        for (final Entry<ProcessId, ClientState> entry : clientStateMap.entrySet()) {
            entry.getValue().activeTasks().forEach(t -> taskClientMap.put(t, entry.getKey()));
        }
        assertEquals(taskIds.size(), taskClientMap.size());

        // Because trafficCost is 0, original assignment should be maintained
        assertTrue(assignor.canEnableRackAwareAssignor());
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, 0, 1);
        assertEquals(0, originalCost);

        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, 0, 1);

        if (assignmentStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
            assertEquals(0, cost);

            // Make sure assignment doesn't change
            for (final Entry<TaskId, ProcessId> entry : taskClientMap.entrySet()) {
                final ClientState clientState = clientStateMap.get(entry.getValue());
                assertTrue(clientState.hasAssignedTask(entry.getKey()));
            }
        } else {
            // Balanced assignment recalculate the assignment using max flow first, so original assignment may not be maintained
            assertValidAssignment(0, taskIds, emptySet(), clientStateMap, new StringBuilder());
        }
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldOptimizeActiveTasksWithMoreClients(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0))
        );
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            mkMap(),
            tasksForTopicGroup,
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState2.assignActive(TASK_1_0);
        clientState3.assignActive(TASK_0_0);

        // task_0_0 has same rack as ProcessId_1 and ProcessId_2
        // task_1_0 has same rack as ProcessId_1 and ProcessId_3
        // Optimal assignment is ProcessId_1: {}, ProcessId_2: {0_0}, ProcessId_3: {1_0} which result in no cross rack traffic
        // and keeps ProcessId_1 empty since it was originally empty
        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_1, clientState1),
            mkEntry(PID_2, clientState2),
            mkEntry(PID_3, clientState3)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_1_0);

        assertTrue(assignor.canEnableRackAwareAssignor());
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        int expected = stateful ? 20 : 2;
        assertEquals(expected, originalCost);

        expected = stateful ? 2 : 0;
        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expected, cost);

        // ProcessId_1 remains empty
        assertEquals(Set.of(), clientState1.activeTasks());
        assertEquals(Set.of(TASK_0_0), clientState2.activeTasks());
        assertEquals(Set.of(TASK_1_0), clientState3.activeTasks());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldOptimizeActiveTasksWithMoreClientsWithMoreThanOneTask(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0))
        );
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            mkMap(),
            tasksForTopicGroup,
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState2.assignActiveTasks(Set.of(TASK_0_1, TASK_1_0));
        clientState3.assignActive(TASK_0_0);

        // task_0_0 has same rack as ProcessId_1 and ProcessId_2
        // task_0_1 has same rack as ProcessId_2 and ProcessId_3
        // task_1_0 has same rack as ProcessId_1 and ProcessId_3
        // Optimal assignment is ProcessId_1: {}, ProcessId_2: {0_0, 0_1}, ProcessId_3: {1_0} which result in no cross rack traffic
        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_1, clientState1),
            mkEntry(PID_2, clientState2),
            mkEntry(PID_3, clientState3)
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
        assertEquals(Set.of(), clientState1.activeTasks());
        assertEquals(Set.of(TASK_0_0, TASK_0_1), clientState2.activeTasks());
        assertEquals(Set.of(TASK_1_0), clientState3.activeTasks());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldBalanceAssignmentWithMoreCost(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_1))
        );
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            mkMap(),
            tasksForTopicGroup,
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState1.assignActiveTasks(Set.of(TASK_0_0, TASK_1_1));
        clientState2.assignActive(TASK_0_1);

        // task_0_0 has same rack as ProcessId_2
        // task_0_1 has same rack as ProcessId_2
        // task_1_1 has same rack as ProcessId_2
        // ProcessId_5 is not in same rack as any task
        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_2, clientState1),
            mkEntry(PID_5, clientState2)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_1);

        assertTrue(assignor.canEnableRackAwareAssignor());
        final int expectedCost = stateful ? 10 : 1;
        final long originalCost = assignor.activeTasksCost(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expectedCost, originalCost);

        final long cost = assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost);
        assertEquals(expectedCost, cost);

        if (stateful || assignmentStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
            // Even though assigning all tasks to ProcessId_2 will result in min cost, but it's not balanced
            // assignment. That's why TASK_0_1 is still assigned to ProcessId_5
            assertEquals(Set.of(TASK_0_0, TASK_1_1), clientState1.activeTasks());
            assertEquals(Set.of(TASK_0_1), clientState2.activeTasks());
        } else {
            assertEquals(Set.of(TASK_0_0, TASK_0_1), clientState1.activeTasks());
            assertEquals(Set.of(TASK_1_1), clientState2.activeTasks());
        }
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldThrowIfMissingCallcanEnableRackAwareAssignor(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState1.assignActiveTasks(Set.of(TASK_0_0, TASK_1_1));
        clientState2.assignActive(TASK_0_1);

        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_2, clientState1),
            mkEntry(PID_5, clientState2)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_1);
        final Exception exception = assertThrows(IllegalStateException.class,
            () -> assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost));
        Assertions.assertEquals("TopicPartition topic0-0 has no rack information. "
            + "Maybe forgot to call canEnableRackAwareAssignor first", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldThrowIfTaskInMultipleClients(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState1.assignActiveTasks(Set.of(TASK_0_0, TASK_1_1));
        clientState2.assignActiveTasks(Set.of(TASK_0_1, TASK_1_1));

        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_2, clientState1),
            mkEntry(PID_5, clientState2)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_1);
        assertTrue(assignor.canEnableRackAwareAssignor());
        final Exception exception = assertThrows(IllegalArgumentException.class,
            () -> assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost));
        Assertions.assertEquals(
            "Task 1_1 assigned to multiple clients 00000000-0000-0000-0000-000000000005, "
                + "00000000-0000-0000-0000-000000000002", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldThrowIfTaskMissingInClients(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManager,
            getRackAwareEnabledConfig(assignmentStrategy),
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1);

        clientState1.assignActiveTasks(Set.of(TASK_0_0, TASK_1_1));
        clientState2.assignActive(TASK_0_1);

        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_2, clientState1),
            mkEntry(PID_5, clientState2)
        ));
        final SortedSet<TaskId> taskIds = mkSortedSet(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);
        assertTrue(assignor.canEnableRackAwareAssignor());
        final Exception exception = assertThrows(IllegalArgumentException.class,
            () -> assignor.optimizeActiveTasks(taskIds, clientStateMap, trafficCost, nonOverlapCost));
        Assertions.assertEquals(
            "Task 1_0 not assigned to any client", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldNotCrashForEmptyStandby(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            mkMap(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManagerForChangelog(),
            getRackAwareEnabledConfigWithStandby(1, assignmentStrategy),
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_3
        );

        clientState1.assignActiveTasks(Set.of(TASK_0_1, TASK_1_1));
        clientState2.assignActive(TASK_1_0);
        clientState3.assignActive(TASK_0_0);

        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_1, clientState1),
            mkEntry(PID_2, clientState2),
            mkEntry(PID_3, clientState3)
        ));

        final long originalCost = assignor.standByTasksCost(new TreeSet<>(), clientStateMap, 10, 1);
        assertEquals(0, originalCost);

        final long cost = assignor.optimizeStandbyTasks(clientStateMap, 10, 1,
            (source, destination, task, clientStates) -> true);
        assertEquals(0, cost);
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldOptimizeStandbyTasksWhenTasksAllMovable(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final int replicaCount = 2;
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            getTaskChangelogMapForAllTasks(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManagerForChangelog(),
            getRackAwareEnabledConfigWithStandby(replicaCount, assignmentStrategy),
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_3
        );
        final ClientState clientState4 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_4
        );
        final ClientState clientState5 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_6
        );
        final ClientState clientState6 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_7
        );

        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_1, clientState1),
            mkEntry(PID_2, clientState2),
            mkEntry(PID_3, clientState3),
            mkEntry(PID_4, clientState4),
            mkEntry(PID_6, clientState5),
            mkEntry(PID_7, clientState6)
        ));

        clientState1.assignActive(TASK_0_0);
        clientState2.assignActive(TASK_0_1);
        clientState3.assignActive(TASK_1_0);
        clientState4.assignActive(TASK_1_1);
        clientState5.assignActive(TASK_0_2);
        clientState6.assignActive(TASK_1_2);

        clientState1.assignStandbyTasks(Set.of(TASK_0_1, TASK_1_1)); // Cost 10
        clientState2.assignStandbyTasks(Set.of(TASK_0_0, TASK_1_0)); // Cost 10
        clientState3.assignStandbyTasks(Set.of(TASK_0_0, TASK_0_2)); // Cost 20
        clientState4.assignStandbyTasks(Set.of(TASK_0_1, TASK_1_2)); // Cost 10
        clientState5.assignStandbyTasks(Set.of(TASK_1_0, TASK_1_2)); // Cost 10
        clientState6.assignStandbyTasks(Set.of(TASK_0_2, TASK_1_1)); // Cost 10

        final SortedSet<TaskId> taskIds = new TreeSet<>(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2));
        final Map<ProcessId, Integer> standbyTaskCount = clientTaskCount(clientStateMap, ClientState::standbyTaskCount);

        assertTrue(assignor.canEnableRackAwareAssignor());
        verifyStandbySatisfyRackReplica(taskIds, assignor.racksForProcess(), clientStateMap, replicaCount, false, null);

        final long originalCost = assignor.standByTasksCost(taskIds, clientStateMap, 10, 1);
        assertEquals(60, originalCost);

        // Task can be moved anywhere so cost can be reduced to 30 compared to in shouldOptimizeStandbyTasksWithMovingConstraint it
        // can only be reduced to 50 since there are moving constraints
        final long cost = assignor.optimizeStandbyTasks(clientStateMap, 10, 1,
            (source, destination, task, clients) -> true);
        assertEquals(20, cost);
        // Don't validate tasks in different racks after moving
        verifyStandbySatisfyRackReplica(taskIds, assignor.racksForProcess(), clientStateMap, replicaCount, true, standbyTaskCount);
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldOptimizeStandbyTasksWithMovingConstraint(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final int replicaCount = 2;
        final AssignmentConfigs assignorConfiguration = getRackAwareEnabledConfigWithStandby(replicaCount, assignmentStrategy);
        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getClusterForAllTopics(),
            getTaskTopicPartitionMapForAllTasks(),
            getTaskChangelogMapForAllTasks(),
            getTopologyGroupTaskMap(),
            getProcessRacksForAllProcess(),
            mockInternalTopicManagerForChangelog(),
            assignorConfiguration,
            time
        );

        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_3
        );
        final ClientState clientState4 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_4
        );
        final ClientState clientState5 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_6
        );
        final ClientState clientState6 = new ClientState(emptySet(), emptySet(), emptyMap(), EMPTY_CLIENT_TAGS, 1,
            PID_7
        );

        final SortedMap<ProcessId, ClientState> clientStateMap = new TreeMap<>(mkMap(
            mkEntry(PID_1, clientState1),
            mkEntry(PID_2, clientState2),
            mkEntry(PID_3, clientState3),
            mkEntry(PID_4, clientState4),
            mkEntry(PID_6, clientState5),
            mkEntry(PID_7, clientState6)
        ));

        clientState1.assignActive(TASK_0_0);
        clientState2.assignActive(TASK_0_1);
        clientState3.assignActive(TASK_1_0);
        clientState4.assignActive(TASK_1_1);
        clientState5.assignActive(TASK_0_2);
        clientState6.assignActive(TASK_1_2);

        clientState1.assignStandbyTasks(Set.of(TASK_0_1, TASK_1_1)); // Cost 10
        clientState2.assignStandbyTasks(Set.of(TASK_0_0, TASK_1_0)); // Cost 10
        clientState3.assignStandbyTasks(Set.of(TASK_0_0, TASK_0_2)); // Cost 20
        clientState4.assignStandbyTasks(Set.of(TASK_0_1, TASK_1_2)); // Cost 10
        clientState5.assignStandbyTasks(Set.of(TASK_1_0, TASK_1_2)); // Cost 10
        clientState6.assignStandbyTasks(Set.of(TASK_0_2, TASK_1_1)); // Cost 10

        final SortedSet<TaskId> taskIds = new TreeSet<>(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2));
        final Map<ProcessId, Integer> standbyTaskCount = clientTaskCount(clientStateMap, ClientState::standbyTaskCount);

        assertTrue(assignor.canEnableRackAwareAssignor());
        verifyStandbySatisfyRackReplica(taskIds, assignor.racksForProcess(), clientStateMap, replicaCount, false, null);

        final long originalCost = assignor.standByTasksCost(taskIds, clientStateMap, 10, 1);
        assertEquals(60, originalCost);

        final StandbyTaskAssignor standbyTaskAssignor = StandbyTaskAssignorFactory.create(assignorConfiguration, assignor);
        assertInstanceOf(ClientTagAwareStandbyTaskAssignor.class, standbyTaskAssignor);
        final long cost = assignor.optimizeStandbyTasks(clientStateMap, 10, 1,
            standbyTaskAssignor::isAllowedTaskMovement);
        assertEquals(50, cost);
        // Validate tasks in different racks after moving
        verifyStandbySatisfyRackReplica(taskIds, assignor.racksForProcess(), clientStateMap, replicaCount, false, standbyTaskCount);
    }

    @ParameterizedTest
    @MethodSource("paramStoreType")
    public void shouldOptimizeRandomStandby(final boolean stateful, final String assignmentStrategy) {
        setUp(stateful);
        final int nodeSize = 50;
        final int tpSize = 60;
        final int partionSize = 3;
        final int clientSize = 50;
        final int replicaCount = 3;
        final int maxCapacity = 3;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(
            tpSize, partionSize, false);
        final AssignmentConfigs assignorConfiguration = getRackAwareEnabledConfigWithStandby(replicaCount, assignmentStrategy);

        final RackAwareTaskAssignor assignor = new RackAwareTaskAssignor(
            getRandomCluster(nodeSize, tpSize, partionSize),
            taskTopicPartitionMap,
            getTaskTopicPartitionMap(tpSize, partionSize, true),
            getTopologyGroupTaskMap(),
            getRandomProcessRacks(clientSize, nodeSize),
            mockInternalTopicManagerForRandomChangelog(nodeSize, tpSize, partionSize),
            assignorConfiguration,
            time
        );

        final SortedSet<TaskId> taskIds = (SortedSet<TaskId>) taskTopicPartitionMap.keySet();
        final SortedMap<ProcessId, ClientState> clientStateMap = getRandomClientState(clientSize,
            tpSize, partionSize, maxCapacity, taskIds);

        final StandbyTaskAssignor standbyTaskAssignor = StandbyTaskAssignorFactory.create(
            assignorConfiguration, assignor);
        assertInstanceOf(ClientTagAwareStandbyTaskAssignor.class, standbyTaskAssignor);
        // Get a standby assignment
        standbyTaskAssignor.assign(clientStateMap, taskIds, taskIds, assignorConfiguration);
        final Map<ProcessId, Integer> standbyTaskCount = clientTaskCount(clientStateMap,
            ClientState::standbyTaskCount);

        assertTrue(assignor.canEnableRackAwareAssignor());
        verifyStandbySatisfyRackReplica(taskIds, assignor.racksForProcess(), clientStateMap,
            replicaCount, false, null);

        final long originalCost = assignor.standByTasksCost(taskIds, clientStateMap, 10, 1);
        assertThat(originalCost, greaterThanOrEqualTo(0L));

        final long cost = assignor.optimizeStandbyTasks(clientStateMap, 10, 1,
            standbyTaskAssignor::isAllowedTaskMovement);
        assertThat(cost, lessThanOrEqualTo(originalCost));
        // Validate tasks in different racks after moving
        verifyStandbySatisfyRackReplica(taskIds, assignor.racksForProcess(), clientStateMap,
            replicaCount, false, standbyTaskCount);
    }

    private Cluster getClusterForTopic0() {
        return new Cluster(
            "cluster",
            Set.of(NODE_0, NODE_1, NODE_2),
            Set.of(PI_0_0, PI_0_1),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Cluster getClusterWithPartitionMissingRack() {
        final Node[] nodeMissingRack = new Node[]{NODE_0, NO_RACK_NODE};
        final PartitionInfo partitionInfoMissingNode = new PartitionInfo(TP_0_NAME, 0, NODE_0, nodeMissingRack, nodeMissingRack);
        return new Cluster(
            "cluster",
            Set.of(NODE_0, NODE_1, NODE_2),
            Set.of(partitionInfoMissingNode, PI_0_1),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Cluster getClusterWithNoNode() {
        final PartitionInfo noNodeInfo = new PartitionInfo(TP_0_NAME, 0, null, new Node[0], new Node[0]);

        return new Cluster(
            "cluster",
            Set.of(NODE_0, NODE_1, NODE_2, Node.noNode()), // mockClientSupplier.setCluster requires noNode
            Collections.singleton(noNodeInfo),
            Collections.emptySet(),
            Collections.emptySet()
        );
    }

    private Map<ProcessId, Map<String, Optional<String>>> getProcessRacksForProcess0() {
        return getProcessRacksForProcess0(false);
    }

    private Map<ProcessId, Map<String, Optional<String>>> getProcessRacksForProcess0(final boolean missingRack) {
        final Map<ProcessId, Map<String, Optional<String>>> processRacks = new HashMap<>();
        final Optional<String> rack = missingRack ? Optional.empty() : Optional.of("rack1");
        processRacks.put(PID_1, Collections.singletonMap("consumer1", rack));
        return processRacks;
    }

    private Map<ProcessId, Map<String, Optional<String>>> getProcessWithNoConsumerRacks() {
        return mkMap(
            mkEntry(PID_1, mkMap())
        );
    }

    private Map<TaskId, Set<TopicPartition>> getTaskTopicPartitionMapForTask0() {
        return getTaskTopicPartitionMapForTask0(false);
    }

    private Map<TaskId, Set<TopicPartition>> getTaskChangeLogTopicPartitionMapForTask0() {
        return mkMap(
            mkEntry(TASK_0_0, Set.of(CHANGELOG_TP_0_0))
        );
    }

    private Map<TaskId, Set<TopicPartition>> getTaskTopicPartitionMapForTask0(final boolean extraTopic) {
        final Set<TopicPartition> topicPartitions = new HashSet<>();
        topicPartitions.add(TP_0_0);
        if (extraTopic) {
            topicPartitions.add(TP_1_0);
        }
        return Collections.singletonMap(TASK_0_0, topicPartitions);
    }
}
