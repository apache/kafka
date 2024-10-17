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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;

import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_CLIENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_RACK_AWARE_ASSIGNMENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASKS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.analyzeTaskAssignmentBalance;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertBalancedActiveAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertBalancedStatefulAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertBalancedTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.copyClientStateMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getClientStatesMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRackAwareTaskAssignor;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomClientState;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomCluster;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomProcessRacks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomSubset;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTaskTopicPartitionMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTasksForTopicGroup;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.hasActiveTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.hasAssignedTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.hasStandbyTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.mockInternalTopicManagerForRandomChangelog;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.verifyTaskPlacementWithRackAwareAssignor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;

public class HighAvailabilityTaskAssignorTest {
    private AssignmentConfigs getConfigWithoutStandbys(final String rackAwareStrategy) {
        return new AssignmentConfigs(
            /*acceptableRecoveryLag*/ 100L,
            /*maxWarmupReplicas*/ 2,
            /*numStandbyReplicas*/ 0,
            /*probingRebalanceIntervalMs*/ 60 * 1000L,
            /*rackAwareAssignmentTags*/ EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
    }

    private AssignmentConfigs getConfigWithStandbys(final String rackAwareStrategy) {
        return getConfigWithStandbys(1, rackAwareStrategy);
    }

    private AssignmentConfigs getConfigWithStandbys(final int replicaNum, final String rackAwareStrategy) {
        return new AssignmentConfigs(
            /*acceptableRecoveryLag*/ 100L,
            /*maxWarmupReplicas*/ 2,
            /*numStandbyReplicas*/ replicaNum,
            /*probingRebalanceIntervalMs*/ 60 * 1000L,
            /*rackAwareAssignmentTags*/ EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
    }

    private final Time time = new MockTime();

    static Stream<Arguments> parameter() {
        return Stream.of(
            Arguments.of(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE, false, 1),
            Arguments.of(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC, true, 1),
            Arguments.of(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY, true, 4)
        );
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldBeStickyForActiveAndStandbyTasksWhileWarmingUp(final String rackAwareStrategy,
                                                                     final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final ClientState clientState1 = new ClientState(allTaskIds, emptySet(), allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 0L)), EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), allTaskIds, allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L)), EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE)), EMPTY_CLIENT_TAGS, 1,
            PID_3
        );

        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, clientState1),
            mkEntry(PID_2, clientState2),
            mkEntry(PID_3, clientState3)
        );

        final AssignmentConfigs configs = new AssignmentConfigs(
            11L,
            2,
            1,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), Set.of(TASK_2_0, TASK_2_1, TASK_2_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(clientState1, hasAssignedTasks(allTaskIds.size()));

        assertThat(clientState2, hasAssignedTasks(allTaskIds.size()));

        assertThat(clientState3, hasAssignedTasks(2));

        assertThat(unstable, is(true));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldSkipWarmupsWhenAcceptableLagIsMax(final String rackAwareStrategy,
                                                        final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final ClientState clientState1 = new ClientState(allTaskIds, emptySet(), allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 0L)), EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE)), EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE)), EMPTY_CLIENT_TAGS, 1,
            PID_3
        );

        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, clientState1),
            mkEntry(PID_2, clientState2),
            mkEntry(PID_3, clientState3)
        );

        final AssignmentConfigs configs = new AssignmentConfigs(
            Long.MAX_VALUE,
            1,
            1,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), Set.of(TASK_2_0, TASK_2_1, TASK_2_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(clientState1, hasAssignedTasks(6));
        assertThat(clientState2, hasAssignedTasks(6));
        assertThat(clientState3, hasAssignedTasks(6));
        assertThat(unstable, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignActiveStatefulTasksEvenlyOverClientsWhereNumberOfClientsIntegralDivisorOfNumberOfTasks(final String rackAwareStrategy,
                                                                                                                   final boolean enableRackAwareTaskAssignor,
                                                                                                                   final int maxSkew) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1,
            PID_3
        );
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(clientState1, clientState2, clientState3);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), Set.of(TASK_2_0, TASK_2_1, TASK_2_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            configs
        );
        assertThat(unstable, is(false));
        assertValidAssignment(0, allTaskIds, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTaskIds, clientStates, new StringBuilder());

        if (!rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
            // Subtopology is not balanced with min_traffic rack aware assignment
            assertBalancedTasks(clientStates, maxSkew);
        }

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignActiveStatefulTasksEvenlyOverClientsWhereNumberOfThreadsIntegralDivisorOfNumberOfTasks(final String rackAwareStrategy,
                                                                                                                   final boolean enableRackAwareTaskAssignor,
                                                                                                                   final int maxSkew) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 3,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 3,
            PID_2
        );
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 3,
            PID_3
        );
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(clientState1, clientState2, clientState3);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), Set.of(TASK_2_0, TASK_2_1, TASK_2_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(unstable, is(false));
        assertValidAssignment(0, allTaskIds, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTaskIds, clientStates, new StringBuilder());

        if (!rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
            // Subtopology is not balanced with min_traffic rack aware assignment
            assertBalancedTasks(clientStates, maxSkew);
        }

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignActiveStatefulTasksEvenlyOverClientsWhereNumberOfClientsNotIntegralDivisorOfNumberOfTasks(final String rackAwareStrategy,
                                                                                                                      final boolean enableRackAwareTaskAssignor,
                                                                                                                      final int maxSkew) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(clientState1, clientState2);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), Set.of(TASK_2_0, TASK_2_1, TASK_2_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(unstable, is(false));
        assertValidAssignment(0, allTaskIds, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTaskIds, clientStates, new StringBuilder());
        assertBalancedTasks(clientStates, maxSkew);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignActiveStatefulTasksEvenlyOverUnevenlyDistributedStreamThreads(final String rackAwareStrategy,
                                                                                          final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 2,
            PID_2
        );
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 3,
            PID_3
        );
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(clientState1, clientState2, clientState3);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(unstable, is(false));
        assertValidAssignment(0, allTaskIds, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTaskIds, clientStates, new StringBuilder());

        assertThat(clientState1, hasActiveTasks(1));
        assertThat(clientState2, hasActiveTasks(2));
        assertThat(clientState3, hasActiveTasks(3));
        final AssignmentTestUtils.TaskSkewReport taskSkewReport = analyzeTaskAssignmentBalance(clientStates, 1);
        if (taskSkewReport.totalSkewedTasks() == 0) {
            fail("Expected a skewed task assignment, but was: " + taskSkewReport);
        }

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignActiveStatefulTasksEvenlyOverClientsWithMoreClientsThanTasks(final String rackAwareStrategy,
                                                                                         final boolean enableRackAwareTaskAssignor,
                                                                                         final int maxSkew) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1,
            PID_3
        );
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(clientState1, clientState2, clientState3);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(unstable, is(false));
        assertValidAssignment(0, allTaskIds, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTaskIds, clientStates, new StringBuilder());
        assertBalancedTasks(clientStates, maxSkew);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignActiveStatefulTasksEvenlyOverClientsAndStreamThreadsWithEqualStreamThreadsPerClientAsTasks(final String rackAwareStrategy,
                                                                                                                       final boolean enableRackAwareTaskAssignor,
                                                                                                                       final int maxSkew) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 9,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 9,
            PID_2
        );
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 9,
            PID_3
        );
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(clientState1, clientState2, clientState3);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), Set.of(TASK_2_0, TASK_2_1, TASK_2_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(unstable, is(false));
        assertValidAssignment(0, allTaskIds, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTaskIds, clientStates, new StringBuilder());

        if (!rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
            // Subtopology is not balanced with min_traffic rack aware assignment
            assertBalancedTasks(clientStates, maxSkew);
        }

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignWarmUpTasksIfStatefulActiveTasksBalancedOverStreamThreadsButNotOverClients(final String rackAwareStrategy,
                                                                                                       final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);
        final Map<TaskId, Long> lagsForCaughtUpClient = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 0L));
        final Map<TaskId, Long> lagsForNotCaughtUpClient =
            allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE));
        final ClientState caughtUpClientState = new ClientState(allTaskIds, emptySet(), lagsForCaughtUpClient, EMPTY_CLIENT_TAGS, 5,
            PID_1
        );
        final ClientState notCaughtUpClientState1 = new ClientState(emptySet(), emptySet(), lagsForNotCaughtUpClient, EMPTY_CLIENT_TAGS, 5,
            PID_2
        );
        final ClientState notCaughtUpClientState2 = new ClientState(emptySet(), emptySet(), lagsForNotCaughtUpClient, EMPTY_CLIENT_TAGS, 5,
            PID_3
        );
        final Map<ProcessId, ClientState> clientStates =
            getClientStatesMap(caughtUpClientState, notCaughtUpClientState1, notCaughtUpClientState2);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            allTaskIds.size() / 3 + 1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(unstable, is(true));
        assertThat(notCaughtUpClientState1.standbyTaskCount(), greaterThanOrEqualTo(allTaskIds.size() / 3));
        assertThat(notCaughtUpClientState2.standbyTaskCount(), greaterThanOrEqualTo(allTaskIds.size() / 3));
        assertValidAssignment(0, allTaskIds.size() / 3 + 1, allTaskIds, emptySet(), clientStates, new StringBuilder());

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldEvenlyAssignActiveStatefulTasksIfClientsAreWarmedUpToBalanceTaskOverClients(final String rackAwareStrategy,
                                                                                                  final boolean enableRackAwareTaskAssignor,
                                                                                                  final int maxSkew) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);

        // If RackAwareTaskAssignor is enabled, TASK_1_1 is assigned ProcessId_2
        final TaskId warmupTaskId1 = enableRackAwareTaskAssignor ? TASK_1_1 : TASK_0_1;
        // If RackAwareTaskAssignor is enabled, TASK_0_1 is assigned ProcessId_3
        final TaskId warmupTaskId2 = enableRackAwareTaskAssignor ? TASK_0_1 : TASK_1_0;
        final Set<TaskId> warmedUpTaskIds1 = Set.of(warmupTaskId1);
        final Set<TaskId> warmedUpTaskIds2 = Set.of(warmupTaskId2);
        final Map<TaskId, Long> lagsForCaughtUpClient = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 0L));
        final Map<TaskId, Long> lagsForWarmedUpClient1 =
            allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE));
        lagsForWarmedUpClient1.put(warmupTaskId1, 0L);
        final Map<TaskId, Long> lagsForWarmedUpClient2 =
            allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE));
        lagsForWarmedUpClient2.put(warmupTaskId2, 0L);

        final ClientState caughtUpClientState = new ClientState(allTaskIds, emptySet(), lagsForCaughtUpClient, EMPTY_CLIENT_TAGS, 5,
            PID_1
        );
        final ClientState warmedUpClientState1 = new ClientState(emptySet(), warmedUpTaskIds1, lagsForWarmedUpClient1, EMPTY_CLIENT_TAGS, 5,
            PID_2
        );
        final ClientState warmedUpClientState2 = new ClientState(emptySet(), warmedUpTaskIds2, lagsForWarmedUpClient2, EMPTY_CLIENT_TAGS, 5,
            PID_3
        );
        final Map<ProcessId, ClientState> clientStates =
            getClientStatesMap(caughtUpClientState, warmedUpClientState1, warmedUpClientState2);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            allTaskIds.size() / 3 + 1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            new AssignmentConfigs(0L, allTaskIds.size() / 3 + 1, 0, 60_000L, EMPTY_RACK_AWARE_ASSIGNMENT_TAGS)
        );

        assertThat(unstable, is(false));
        assertValidAssignment(0, allTaskIds, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTaskIds, clientStates, new StringBuilder());
        assertBalancedTasks(clientStates, maxSkew);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignActiveStatefulTasksEvenlyOverStreamThreadsButBestEffortOverClients(final String rackAwareStrategy,
                                                                                               final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTaskIds = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 6,
            PID_1
        );
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 3,
            PID_2
        );
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(clientState1, clientState2);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), Set.of(TASK_2_0, TASK_2_1, TASK_2_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean unstable = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTaskIds,
            allTaskIds,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(unstable, is(false));
        assertValidAssignment(0, allTaskIds, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTaskIds, clientStates, new StringBuilder());
        assertThat(clientState1, hasActiveTasks(6));
        assertThat(clientState2, hasActiveTasks(3));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTaskIds, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldComputeNewAssignmentIfThereAreUnassignedActiveTasks(final String rackAwareStrategy,
                                                                          final boolean enableRackAwareTaskAssignor,
                                                                          final int maxSkew) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1);
        final ClientState client1 = new ClientState(singleton(TASK_0_0), emptySet(), singletonMap(TASK_0_0, 0L), EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final Map<ProcessId, ClientState> clientStates = singletonMap(PID_1, client1);

        final AssignmentConfigs configs = getConfigWithoutStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(clientStates,
                                                                                         allTasks,
                                                                                         singleton(TASK_0_0),
                                                                                         rackAwareTaskAssignor,
                                                                                         configs);

        assertThat(probingRebalanceNeeded, is(false));
        assertThat(client1, hasActiveTasks(2));
        assertThat(client1, hasStandbyTasks(0));

        assertValidAssignment(0, allTasks, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTasks, clientStates, new StringBuilder());
        assertBalancedTasks(clientStates, maxSkew);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldComputeNewAssignmentIfThereAreUnassignedStandbyTasks(final String rackAwareStrategy, final boolean enableRackAwareTaskAssignor, final int maxSkew) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0);
        final ClientState client1 = new ClientState(singleton(TASK_0_0), emptySet(), singletonMap(TASK_0_0, 0L), EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState client2 = new ClientState(emptySet(), emptySet(), singletonMap(TASK_0_0, 0L), EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final Map<ProcessId, ClientState> clientStates = mkMap(mkEntry(PID_1, client1), mkEntry(PID_2, client2));

        final AssignmentConfigs configs = getConfigWithStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(clientStates,
                                                                                         allTasks,
                                                                                         statefulTasks,
                                                                                         rackAwareTaskAssignor,
                                                                                         configs);

        assertThat(clientStates.get(PID_2).standbyTasks(), not(empty()));
        assertThat(probingRebalanceNeeded, is(false));
        assertValidAssignment(1, allTasks, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTasks, clientStates, new StringBuilder());
        assertBalancedTasks(clientStates, maxSkew);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldComputeNewAssignmentIfActiveTasksWasNotOnCaughtUpClient(final String rackAwareStrategy, final boolean enableRackAwareTaskAssignor, final int maxSkew) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0);
        final ClientState client1 = new ClientState(singleton(TASK_0_0), emptySet(), singletonMap(TASK_0_0, 500L), EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState client2 = new ClientState(singleton(TASK_0_1), emptySet(), singletonMap(TASK_0_0, 0L), EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, client1),
            mkEntry(PID_2, client2)
        );

        final AssignmentConfigs configs = getConfigWithoutStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);

        assertThat(clientStates.get(PID_1).activeTasks(), is(singleton(TASK_0_1)));
        assertThat(clientStates.get(PID_2).activeTasks(), is(singleton(TASK_0_0)));
        // we'll warm up task 0_0 on client1 because it's first in sorted order,
        // although this isn't an optimal convergence
        assertThat(probingRebalanceNeeded, is(true));
        assertValidAssignment(0, 1, allTasks, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTasks, clientStates, new StringBuilder());
        assertBalancedTasks(clientStates, maxSkew);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignToMostCaughtUpIfActiveTasksWasNotOnCaughtUpClient(final String rackAwareStrategy, final boolean enableRackAwareTaskAssignor, final int maxSkew) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0);
        final ClientState client1 = new ClientState(emptySet(), emptySet(), singletonMap(TASK_0_0, Long.MAX_VALUE), EMPTY_CLIENT_TAGS, 1,
            PID_1
        );
        final ClientState client2 = new ClientState(emptySet(), emptySet(), singletonMap(TASK_0_0, 1000L), EMPTY_CLIENT_TAGS, 1,
            PID_2
        );
        final ClientState client3 = new ClientState(emptySet(), emptySet(), singletonMap(TASK_0_0, 500L), EMPTY_CLIENT_TAGS, 1,
            PID_3
        );
        final Map<ProcessId, ClientState> clientStates = mkMap(
                mkEntry(PID_1, client1),
                mkEntry(PID_2, client2),
                mkEntry(PID_3, client3)
        );

        final AssignmentConfigs configs = getConfigWithStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
                new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);

        assertThat(clientStates.get(PID_1).activeTasks(), is(emptySet()));
        assertThat(clientStates.get(PID_2).activeTasks(), is(emptySet()));
        assertThat(clientStates.get(PID_3).activeTasks(), is(singleton(TASK_0_0)));

        assertThat(clientStates.get(PID_1).standbyTasks(), is(singleton(TASK_0_0))); // warm up
        assertThat(clientStates.get(PID_2).standbyTasks(), is(singleton(TASK_0_0))); // standby
        assertThat(clientStates.get(PID_3).standbyTasks(), is(emptySet()));

        assertThat(probingRebalanceNeeded, is(true));
        assertValidAssignment(1, 1, allTasks, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTasks, clientStates, new StringBuilder());
        assertBalancedTasks(clientStates, maxSkew);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignStandbysForStatefulTasks(final String rackAwareStrategy, final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0, TASK_0_1);

        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(Set.of(TASK_0_0), statefulTasks,
            PID_1
        );
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(Set.of(TASK_0_1), statefulTasks,
            PID_2
        );

        final AssignmentConfigs configs = getConfigWithStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);


        assertThat(client1.activeTasks(), equalTo(Set.of(TASK_0_0)));
        assertThat(client2.activeTasks(), equalTo(Set.of(TASK_0_1)));
        assertThat(client1.standbyTasks(), equalTo(Set.of(TASK_0_1)));
        assertThat(client2.standbyTasks(), equalTo(Set.of(TASK_0_0)));
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldNotAssignStandbysForStatelessTasks(final String rackAwareStrategy, final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;

        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_1
        );
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_2
        );

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = getConfigWithStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);


        assertThat(client1.activeTaskCount(), equalTo(1));
        assertThat(client2.activeTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1, client2);
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignWarmupReplicasEvenIfNoStandbyReplicasConfigured(final String rackAwareStrategy,
                                                                            final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(Set.of(TASK_0_0, TASK_0_1), statefulTasks,
            PID_1
        );
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_2
        );

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = getConfigWithoutStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);


        assertThat(client1.activeTasks(), equalTo(Set.of(TASK_0_0, TASK_0_1)));
        assertThat(client2.standbyTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
        assertThat(probingRebalanceNeeded, is(true));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }


    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldNotAssignMoreThanMaxWarmupReplicas(final String rackAwareStrategy,
                                                         final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), statefulTasks,
            PID_1
        );
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_2
        );

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = new AssignmentConfigs(
            /*acceptableRecoveryLag*/ 100L,
            /*maxWarmupReplicas*/ 1,
            /*numStandbyReplicas*/ 0,
            /*probingRebalanceIntervalMs*/ 60 * 1000L,
            /*rackAwareAssignmentTags*/ EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTasks,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );


        assertThat(client1.activeTasks(), equalTo(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertThat(client2.standbyTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
        assertThat(probingRebalanceNeeded, is(true));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldNotAssignWarmupAndStandbyToTheSameClient(final String rackAwareStrategy,
                                                               final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), statefulTasks,
            PID_1
        );
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_2
        );

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = new AssignmentConfigs(
            /*acceptableRecoveryLag*/ 100L,
            /*maxWarmupReplicas*/ 1,
            /*numStandbyReplicas*/ 1,
            /*probingRebalanceIntervalMs*/ 60 * 1000L,
            /*rackAwareAssignmentTags*/ EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTasks,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(client1.activeTasks(), equalTo(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertThat(client2.standbyTasks(), equalTo(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
        assertThat(probingRebalanceNeeded, is(true));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldNotAssignAnyStandbysWithInsufficientCapacity(final String rackAwareStrategy,
                                                                   final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(Set.of(TASK_0_0, TASK_0_1), statefulTasks,
            PID_1
        );

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1);

        final AssignmentConfigs configs = getConfigWithStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);

        assertThat(client1.activeTasks(), equalTo(Set.of(TASK_0_0, TASK_0_1)));
        assertHasNoStandbyTasks(client1);
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignActiveTasksToNotCaughtUpClientIfNoneExist(final String rackAwareStrategy,
                                                                      final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_1
        );

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1);

        final AssignmentConfigs configs = getConfigWithStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);
        assertThat(client1.activeTasks(), equalTo(Set.of(TASK_0_0, TASK_0_1)));
        assertHasNoStandbyTasks(client1);
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldNotAssignMoreThanMaxWarmupReplicasWithStandbys(final String rackAwareStrategy,
                                                                     final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(statefulTasks, statefulTasks,
            PID_1
        );
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_2
        );
        final ClientState client3 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_3
        );

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final AssignmentConfigs configs = getConfigWithStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);

        assertValidAssignment(
            1,
            2,
            statefulTasks,
            emptySet(),
            clientStates,
            new StringBuilder()
        );
        assertThat(probingRebalanceNeeded, is(true));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDistributeStatelessTasksToBalanceTotalTaskLoad(final String rackAwareStrategy,
                                                                     final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statelessTasks = Set.of(TASK_1_0, TASK_1_1, TASK_1_2);

        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(statefulTasks, statefulTasks,
            PID_1
        );
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_2
        );

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = getConfigWithStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);
        assertValidAssignment(
            1,
            2,
            statefulTasks,
            statelessTasks,
            clientStates,
            new StringBuilder()
        );
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(statefulTasks, clientStates, new StringBuilder());

        // since only client1 is caught up on the stateful tasks, we expect it to get _all_ the active tasks,
        // which means that client2 should have gotten all of the stateless tasks, so the tasks should be skewed
        final AssignmentTestUtils.TaskSkewReport taskSkewReport = analyzeTaskAssignmentBalance(clientStates, 1);
        assertThat(taskSkewReport.toString(), taskSkewReport.skewedSubtopologies(), not(empty()));

        assertThat(probingRebalanceNeeded, is(true));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDistributeStatefulActiveTasksToAllClients(final String rackAwareStrategy,
                                                                final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks =
            Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3, TASK_2_0); // 9 total
        final Map<TaskId, Long> allTaskLags = allTasks.stream().collect(Collectors.toMap(t -> t, t -> 0L));
        final Set<TaskId> statefulTasks = new HashSet<>(allTasks);
        final ClientState client1 = new ClientState(emptySet(), emptySet(), allTaskLags, EMPTY_CLIENT_TAGS, 100);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), allTaskLags, EMPTY_CLIENT_TAGS, 50);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), allTaskLags, EMPTY_CLIENT_TAGS, 1);

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final AssignmentConfigs configs = getConfigWithoutStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3)),
            mkEntry(new Subtopology(2, null), Set.of(TASK_2_0))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);

        assertThat(client1.activeTasks(), not(empty()));
        assertThat(client2.activeTasks(), not(empty()));
        assertThat(client3.activeTasks(), not(empty()));
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldReturnFalseIfPreviousAssignmentIsReused(final String rackAwareStrategy,
                                                              final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = new HashSet<>(allTasks);
        final Set<TaskId> caughtUpTasks1 = enableRackAwareTaskAssignor ? Set.of(TASK_0_0, TASK_0_3) : Set.of(TASK_0_0, TASK_0_2);
        final Set<TaskId> caughtUpTasks2 = enableRackAwareTaskAssignor ? Set.of(TASK_0_1, TASK_0_2) : Set.of(TASK_0_1, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(caughtUpTasks1, statefulTasks,
            PID_1
        );
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(caughtUpTasks2, statefulTasks,
            PID_2
        );

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = getConfigWithoutStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);

        assertThat(probingRebalanceNeeded, is(false));
        assertThat(client1.activeTasks(), equalTo(client1.prevActiveTasks()));
        assertThat(client2.activeTasks(), equalTo(client2.prevActiveTasks()));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldReturnFalseIfNoWarmupTasksAreAssigned(final String rackAwareStrategy,
                                                            final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_1
        );
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_2
        );

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = getConfigWithoutStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);
        assertThat(probingRebalanceNeeded, is(false));
        assertHasNoStandbyTasks(client1, client2);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldReturnTrueIfWarmupTasksAreAssigned(final String rackAwareStrategy, final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = Set.of(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(allTasks, statefulTasks,
            PID_1
        );
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks,
            PID_2
        );

        final AssignmentConfigs configs = getConfigWithoutStandbys(rackAwareStrategy);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);
        assertThat(probingRebalanceNeeded, is(true));
        assertThat(client2.standbyTaskCount(), equalTo(1));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDistributeStatelessTasksEvenlyOverClientsWithEqualStreamThreadsPerClientAsTasksAndNoStatefulTasks(final String rackAwareStrategy,
                                                                                                                        final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final Set<TaskId> statelessTasks = new HashSet<>(allTasks);

        final Map<TaskId, Long> taskLags = new HashMap<>();
        final ClientState client1 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 7);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 7);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 7);

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTasks,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        assertValidAssignment(
            0,
            EMPTY_TASKS,
            statelessTasks,
            clientStates,
            new StringBuilder()
        );
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDistributeStatelessTasksEvenlyOverClientsWithLessStreamThreadsPerClientAsTasksAndNoStatefulTasks(final String rackAwareStrategy,
                                                                                                                       final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final Set<TaskId> statelessTasks = new HashSet<>(allTasks);

        final Map<TaskId, Long> taskLags = new HashMap<>();
        final ClientState client1 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 2);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 2);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 2);

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTasks,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        assertValidAssignment(
            0,
            EMPTY_TASKS,
            statelessTasks,
            clientStates,
            new StringBuilder()
        );
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDistributeStatelessTasksEvenlyOverClientsWithUnevenlyDistributedStreamThreadsAndNoStatefulTasks(final String rackAwareStrategy,
                                                                                                                      final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final Set<TaskId> statelessTasks = new HashSet<>(allTasks);

        final Map<TaskId, Long> taskLags = new HashMap<>();
        final ClientState client1 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 1);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 2);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 3);

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTasks,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        assertValidAssignment(
            0,
            EMPTY_TASKS,
            statelessTasks,
            clientStates,
            new StringBuilder()
        );
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldDistributeStatelessTasksEvenlyWithPreviousAssignmentAndNoStatefulTasks(final String rackAwareStrategy,
                                                                                             final boolean enableRackAwareTaskAssignor) {
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final Set<TaskId> statelessTasks = new HashSet<>(allTasks);

        final Map<TaskId, Long> taskLags = new HashMap<>();
        final ClientState client1 = new ClientState(statelessTasks, emptySet(), taskLags, EMPTY_CLIENT_TAGS, 3);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 3);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 3);

        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), Set.of(TASK_1_0, TASK_1_1, TASK_1_2))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTasks,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        assertValidAssignment(
            0,
            EMPTY_TASKS,
            statelessTasks,
            clientStates,
            new StringBuilder()
        );
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldAssignRandomInput(final String rackAwareStrategy, final boolean enableRackAwareTaskAssignor, final int maxSkew) {
        final int nodeSize = 50;
        final int tpSize = 60;
        final int partitionSize = 3;
        final int clientSize = 50;
        final int replicaCount = 3;
        final int maxCapacity = 3;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(
            tpSize, partitionSize, false);
        final AssignmentConfigs assignorConfiguration = getConfigWithStandbys(replicaCount, rackAwareStrategy);

        final RackAwareTaskAssignor rackAwareTaskAssignor = spy(new RackAwareTaskAssignor(
            getRandomCluster(nodeSize, tpSize, partitionSize),
            taskTopicPartitionMap,
            getTaskTopicPartitionMap(tpSize, partitionSize, true),
            getTasksForTopicGroup(tpSize, partitionSize),
            getRandomProcessRacks(clientSize, nodeSize),
            mockInternalTopicManagerForRandomChangelog(nodeSize, tpSize, partitionSize),
            assignorConfiguration,
            time
        ));

        final SortedSet<TaskId> taskIds = (SortedSet<TaskId>) taskTopicPartitionMap.keySet();
        final List<Set<TaskId>> statefulAndStatelessTasks = getRandomSubset(taskIds, 2);
        final Set<TaskId> statefulTasks = statefulAndStatelessTasks.get(0);
        final Set<TaskId> statelessTasks = statefulAndStatelessTasks.get(1);
        final SortedMap<ProcessId, ClientState> clientStateMap = getRandomClientState(clientSize,
            tpSize, partitionSize, maxCapacity, false, statefulTasks);


        new HighAvailabilityTaskAssignor().assign(
            clientStateMap,
            taskIds,
            statefulTasks,
            rackAwareTaskAssignor,
            assignorConfiguration
        );

        assertValidAssignment(
            replicaCount,
            statefulTasks,
            statelessTasks,
            clientStateMap,
            new StringBuilder()
        );
        assertBalancedActiveAssignment(clientStateMap, new StringBuilder());
        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, taskIds, clientStateMap, true, enableRackAwareTaskAssignor);

        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            assertBalancedTasks(clientStateMap, maxSkew);
        }
    }

    @ParameterizedTest
    @MethodSource("parameter")
    public void shouldRemainOriginalAssignmentWithoutTrafficCostForMinCostStrategy(final String rackAwareStrategy,
                                                                                   final boolean enableRackAwareTaskAssignor,
                                                                                   final int maxSkew) {
        // This test tests that if the traffic cost is 0, we should have same assignment with or without
        // rack aware assignor enabled
        final int nodeSize = 50;
        final int tpSize = 60;
        final int partitionSize = 3;
        final int clientSize = 50;
        final int replicaCount = 1;
        final int maxCapacity = 3;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(
            tpSize, partitionSize, false);
        final Cluster cluster = getRandomCluster(nodeSize, tpSize, partitionSize);
        final Map<TaskId, Set<TopicPartition>> taskChangelogTopicPartitionMap = getTaskTopicPartitionMap(tpSize, partitionSize, true);
        final Map<Subtopology, Set<TaskId>> subtopologySetMap = getTasksForTopicGroup(tpSize, partitionSize);
        final Map<ProcessId, Map<String, Optional<String>>> processRackMap = getRandomProcessRacks(clientSize, nodeSize);
        final InternalTopicManager mockInternalTopicManager = mockInternalTopicManagerForRandomChangelog(nodeSize, tpSize, partitionSize);

        AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            replicaCount,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            0,
            10,
            rackAwareStrategy
        );

        RackAwareTaskAssignor rackAwareTaskAssignor = spy(new RackAwareTaskAssignor(
            cluster,
            taskTopicPartitionMap,
            taskChangelogTopicPartitionMap,
            subtopologySetMap,
            processRackMap,
            mockInternalTopicManager,
            configs,
            time
        ));

        final SortedSet<TaskId> taskIds = (SortedSet<TaskId>) taskTopicPartitionMap.keySet();
        final List<Set<TaskId>> statefulAndStatelessTasks = getRandomSubset(taskIds, 2);
        final Set<TaskId> statefulTasks = statefulAndStatelessTasks.get(0);
        final Set<TaskId> statelessTasks = statefulAndStatelessTasks.get(1);
        final SortedMap<ProcessId, ClientState> clientStateMap = getRandomClientState(clientSize,
            tpSize, partitionSize, maxCapacity, false, statefulTasks);

        new HighAvailabilityTaskAssignor().assign(
            clientStateMap,
            taskIds,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        assertValidAssignment(1, statefulTasks, statelessTasks, clientStateMap, new StringBuilder());
        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE)) {
            return;
        }
        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            // Original assignment won't be maintained because we calculate the assignment using max flow first
            // in balance subtopology strategy
            assertBalancedTasks(clientStateMap, maxSkew);
            return;
        }

        final SortedMap<ProcessId, ClientState> clientStateMapCopy = copyClientStateMap(clientStateMap);
        configs = new AssignmentConfigs(
            0L,
            1,
            replicaCount,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            0,
            10,
            StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE
        );

        rackAwareTaskAssignor = spy(new RackAwareTaskAssignor(
            cluster,
            taskTopicPartitionMap,
            taskChangelogTopicPartitionMap,
            subtopologySetMap,
            processRackMap,
            mockInternalTopicManager,
            configs,
            time
        ));

        new HighAvailabilityTaskAssignor().assign(
            clientStateMapCopy,
            taskIds,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        for (final Map.Entry<ProcessId, ClientState> entry : clientStateMap.entrySet()) {
            assertThat(entry.getValue().statefulActiveTasks(), Matchers.equalTo(clientStateMapCopy.get(entry.getKey()).statefulActiveTasks()));
            assertThat(entry.getValue().standbyTasks(), Matchers.equalTo(clientStateMapCopy.get(entry.getKey()).standbyTasks()));
        }
    }

    private static void assertHasNoActiveTasks(final ClientState... clients) {
        for (final ClientState client : clients) {
            assertThat(client.activeTasks(), is(empty()));
        }
    }

    private static void assertHasNoStandbyTasks(final ClientState... clients) {
        for (final ClientState client : clients) {
            assertThat(client, hasStandbyTasks(0));
        }
    }

    private static ClientState getMockClientWithPreviousCaughtUpTasks(final Set<TaskId> statefulActiveTasks,
                                                                      final Set<TaskId> statefulTasks,
                                                                      final ProcessId processId) {
        if (!statefulTasks.containsAll(statefulActiveTasks)) {
            throw new IllegalArgumentException("Need to initialize stateful tasks set before creating mock clients");
        }
        final Map<TaskId, Long> taskLags = new HashMap<>();
        for (final TaskId task : statefulTasks) {
            if (statefulActiveTasks.contains(task)) {
                taskLags.put(task, 0L);
            } else {
                taskLags.put(task, Long.MAX_VALUE);
            }
        }
        return new ClientState(statefulActiveTasks, emptySet(), taskLags, EMPTY_CLIENT_TAGS, 1, processId);
    }


}
