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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.SortedSet;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_CLIENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_RACK_AWARE_ASSIGNMENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASKS;
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
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;

@RunWith(Parameterized.class)
public class HighAvailabilityTaskAssignorTest {
    private AssignmentConfigs getConfigWithoutStandbys() {
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

    private AssignmentConfigs getConfigWithStandbys() {
        return getConfigWithStandbys(1);
    }

    private AssignmentConfigs getConfigWithStandbys(final int replicaNum) {
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

    private boolean enableRackAwareTaskAssignor;
    private int maxSkew = 1;

    @Parameter
    public String rackAwareStrategy;

    @Before
    public void setUp() {
        enableRackAwareTaskAssignor = !rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE);
        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            maxSkew = 4;
        }
    }

    @Parameterized.Parameters(name = "rackAwareStrategy={0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][] {
            {StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE},
            {StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC},
            {StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY},
        });
    }

    @Test
    public void shouldBeStickyForActiveAndStandbyTasksWhileWarmingUp() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final ClientState clientState1 = new ClientState(allTaskIds, emptySet(), allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 0L)), EMPTY_CLIENT_TAGS, 1, UUID_1);
        final ClientState clientState2 = new ClientState(emptySet(), allTaskIds, allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L)), EMPTY_CLIENT_TAGS, 1, UUID_2);
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE)), EMPTY_CLIENT_TAGS, 1, UUID_3);

        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, clientState1),
            mkEntry(UUID_2, clientState2),
            mkEntry(UUID_3, clientState3)
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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), mkSet(TASK_2_0, TASK_2_1, TASK_2_2))
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

    @Test
    public void shouldSkipWarmupsWhenAcceptableLagIsMax() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final ClientState clientState1 = new ClientState(allTaskIds, emptySet(), allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 0L)), EMPTY_CLIENT_TAGS, 1, UUID_1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE)), EMPTY_CLIENT_TAGS, 1, UUID_2);
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE)), EMPTY_CLIENT_TAGS, 1, UUID_3);

        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, clientState1),
            mkEntry(UUID_2, clientState2),
            mkEntry(UUID_3, clientState3)
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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), mkSet(TASK_2_0, TASK_2_1, TASK_2_2))
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

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverClientsWhereNumberOfClientsIntegralDivisorOfNumberOfTasks() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1, UUID_1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1, UUID_2);
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1, UUID_3);
        final Map<UUID, ClientState> clientStates = getClientStatesMap(clientState1, clientState2, clientState3);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), mkSet(TASK_2_0, TASK_2_1, TASK_2_2))
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

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverClientsWhereNumberOfThreadsIntegralDivisorOfNumberOfTasks() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 3, UUID_1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 3, UUID_2);
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 3, UUID_3);
        final Map<UUID, ClientState> clientStates = getClientStatesMap(clientState1, clientState2, clientState3);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), mkSet(TASK_2_0, TASK_2_1, TASK_2_2))
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

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverClientsWhereNumberOfClientsNotIntegralDivisorOfNumberOfTasks() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1, UUID_1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1, UUID_2);
        final Map<UUID, ClientState> clientStates = getClientStatesMap(clientState1, clientState2);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), mkSet(TASK_2_0, TASK_2_1, TASK_2_2))
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

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverUnevenlyDistributedStreamThreads() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1, UUID_1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 2, UUID_2);
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 3, UUID_3);
        final Map<UUID, ClientState> clientStates = getClientStatesMap(clientState1, clientState2, clientState3);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2))
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

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverClientsWithMoreClientsThanTasks() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1, UUID_1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1, UUID_2);
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 1, UUID_3);
        final Map<UUID, ClientState> clientStates = getClientStatesMap(clientState1, clientState2, clientState3);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1))
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

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverClientsAndStreamThreadsWithEqualStreamThreadsPerClientAsTasks() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 9, UUID_1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 9, UUID_2);
        final ClientState clientState3 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 9, UUID_3);
        final Map<UUID, ClientState> clientStates = getClientStatesMap(clientState1, clientState2, clientState3);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), mkSet(TASK_2_0, TASK_2_1, TASK_2_2))
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

    @Test
    public void shouldAssignWarmUpTasksIfStatefulActiveTasksBalancedOverStreamThreadsButNotOverClients() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);
        final Map<TaskId, Long> lagsForCaughtUpClient = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 0L));
        final Map<TaskId, Long> lagsForNotCaughtUpClient =
            allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE));
        final ClientState caughtUpClientState = new ClientState(allTaskIds, emptySet(), lagsForCaughtUpClient, EMPTY_CLIENT_TAGS, 5, UUID_1);
        final ClientState notCaughtUpClientState1 = new ClientState(emptySet(), emptySet(), lagsForNotCaughtUpClient, EMPTY_CLIENT_TAGS, 5, UUID_2);
        final ClientState notCaughtUpClientState2 = new ClientState(emptySet(), emptySet(), lagsForNotCaughtUpClient, EMPTY_CLIENT_TAGS, 5, UUID_3);
        final Map<UUID, ClientState> clientStates =
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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1))
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

    @Test
    public void shouldEvenlyAssignActiveStatefulTasksIfClientsAreWarmedUpToBalanceTaskOverClients() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);

        // If RackAwareTaskAssignor is enabled, TASK_1_1 is assigned UUID_2
        final TaskId warmupTaskId1 = enableRackAwareTaskAssignor ? TASK_1_1 : TASK_0_1;
        // If RackAwareTaskAssignor is enabled, TASK_0_1 is assigned UUID_3
        final TaskId warmupTaskId2 = enableRackAwareTaskAssignor ? TASK_0_1 : TASK_1_0;
        final Set<TaskId> warmedUpTaskIds1 = mkSet(warmupTaskId1);
        final Set<TaskId> warmedUpTaskIds2 = mkSet(warmupTaskId2);
        final Map<TaskId, Long> lagsForCaughtUpClient = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 0L));
        final Map<TaskId, Long> lagsForWarmedUpClient1 =
            allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE));
        lagsForWarmedUpClient1.put(warmupTaskId1, 0L);
        final Map<TaskId, Long> lagsForWarmedUpClient2 =
            allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> Long.MAX_VALUE));
        lagsForWarmedUpClient2.put(warmupTaskId2, 0L);

        final ClientState caughtUpClientState = new ClientState(allTaskIds, emptySet(), lagsForCaughtUpClient, EMPTY_CLIENT_TAGS, 5, UUID_1);
        final ClientState warmedUpClientState1 = new ClientState(emptySet(), warmedUpTaskIds1, lagsForWarmedUpClient1, EMPTY_CLIENT_TAGS, 5, UUID_2);
        final ClientState warmedUpClientState2 = new ClientState(emptySet(), warmedUpTaskIds2, lagsForWarmedUpClient2, EMPTY_CLIENT_TAGS, 5, UUID_3);
        final Map<UUID, ClientState> clientStates =
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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1))
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

    @Test
    public void shouldAssignActiveStatefulTasksEvenlyOverStreamThreadsButBestEffortOverClients() {
        final Set<TaskId> allTaskIds = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, Long> lags = allTaskIds.stream().collect(Collectors.toMap(k -> k, k -> 10L));
        final ClientState clientState1 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 6, UUID_1);
        final ClientState clientState2 = new ClientState(emptySet(), emptySet(), lags, EMPTY_CLIENT_TAGS, 3, UUID_2);
        final Map<UUID, ClientState> clientStates = getClientStatesMap(clientState1, clientState2);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2)),
            mkEntry(new Subtopology(2, null), mkSet(TASK_2_0, TASK_2_1, TASK_2_2))
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

    @Test
    public void shouldComputeNewAssignmentIfThereAreUnassignedActiveTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final ClientState client1 = new ClientState(singleton(TASK_0_0), emptySet(), singletonMap(TASK_0_0, 0L), EMPTY_CLIENT_TAGS, 1, UUID_1);
        final Map<UUID, ClientState> clientStates = singletonMap(UUID_1, client1);

        final AssignmentConfigs configs = getConfigWithoutStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1))
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

    @Test
    public void shouldComputeNewAssignmentIfThereAreUnassignedStandbyTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0);
        final ClientState client1 = new ClientState(singleton(TASK_0_0), emptySet(), singletonMap(TASK_0_0, 0L), EMPTY_CLIENT_TAGS, 1, UUID_1);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), singletonMap(TASK_0_0, 0L), EMPTY_CLIENT_TAGS, 1, UUID_2);
        final Map<UUID, ClientState> clientStates = mkMap(mkEntry(UUID_1, client1), mkEntry(UUID_2, client2));

        final AssignmentConfigs configs = getConfigWithStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(clientStates,
                                                                                         allTasks,
                                                                                         statefulTasks,
                                                                                         rackAwareTaskAssignor,
                                                                                         configs);

        assertThat(clientStates.get(UUID_2).standbyTasks(), not(empty()));
        assertThat(probingRebalanceNeeded, is(false));
        assertValidAssignment(1, allTasks, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTasks, clientStates, new StringBuilder());
        assertBalancedTasks(clientStates, maxSkew);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldComputeNewAssignmentIfActiveTasksWasNotOnCaughtUpClient() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0);
        final ClientState client1 = new ClientState(singleton(TASK_0_0), emptySet(), singletonMap(TASK_0_0, 500L), EMPTY_CLIENT_TAGS, 1, UUID_1);
        final ClientState client2 = new ClientState(singleton(TASK_0_1), emptySet(), singletonMap(TASK_0_0, 0L), EMPTY_CLIENT_TAGS, 1, UUID_2);
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2)
        );

        final AssignmentConfigs configs = getConfigWithoutStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);

        assertThat(clientStates.get(UUID_1).activeTasks(), is(singleton(TASK_0_1)));
        assertThat(clientStates.get(UUID_2).activeTasks(), is(singleton(TASK_0_0)));
        // we'll warm up task 0_0 on client1 because it's first in sorted order,
        // although this isn't an optimal convergence
        assertThat(probingRebalanceNeeded, is(true));
        assertValidAssignment(0, 1, allTasks, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTasks, clientStates, new StringBuilder());
        assertBalancedTasks(clientStates, maxSkew);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldAssignToMostCaughtUpIfActiveTasksWasNotOnCaughtUpClient() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0);
        final ClientState client1 = new ClientState(emptySet(), emptySet(), singletonMap(TASK_0_0, Long.MAX_VALUE), EMPTY_CLIENT_TAGS, 1, UUID_1);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), singletonMap(TASK_0_0, 1000L), EMPTY_CLIENT_TAGS, 1, UUID_2);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), singletonMap(TASK_0_0, 500L), EMPTY_CLIENT_TAGS, 1, UUID_3);
        final Map<UUID, ClientState> clientStates = mkMap(
                mkEntry(UUID_1, client1),
                mkEntry(UUID_2, client2),
                mkEntry(UUID_3, client3)
        );

        final AssignmentConfigs configs = getConfigWithStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
                new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);

        assertThat(clientStates.get(UUID_1).activeTasks(), is(emptySet()));
        assertThat(clientStates.get(UUID_2).activeTasks(), is(emptySet()));
        assertThat(clientStates.get(UUID_3).activeTasks(), is(singleton(TASK_0_0)));

        assertThat(clientStates.get(UUID_1).standbyTasks(), is(singleton(TASK_0_0))); // warm up
        assertThat(clientStates.get(UUID_2).standbyTasks(), is(singleton(TASK_0_0))); // standby
        assertThat(clientStates.get(UUID_3).standbyTasks(), is(emptySet()));

        assertThat(probingRebalanceNeeded, is(true));
        assertValidAssignment(1, 1, allTasks, emptySet(), clientStates, new StringBuilder());
        assertBalancedActiveAssignment(clientStates, new StringBuilder());
        assertBalancedStatefulAssignment(allTasks, clientStates, new StringBuilder());
        assertBalancedTasks(clientStates, maxSkew);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldAssignStandbysForStatefulTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1);

        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0), statefulTasks, UUID_1);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_1), statefulTasks, UUID_2);

        final AssignmentConfigs configs = getConfigWithStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);


        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0)));
        assertThat(client2.activeTasks(), equalTo(mkSet(TASK_0_1)));
        assertThat(client1.standbyTasks(), equalTo(mkSet(TASK_0_1)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(TASK_0_0)));
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldNotAssignStandbysForStatelessTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;

        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_1);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_2);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = getConfigWithStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1))
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

    @Test
    public void shouldAssignWarmupReplicasEvenIfNoStandbyReplicasConfigured() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1), statefulTasks, UUID_1);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_2);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = getConfigWithoutStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);


        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
        assertThat(client2.standbyTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
        assertThat(probingRebalanceNeeded, is(true));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }


    @Test
    public void shouldNotAssignMoreThanMaxWarmupReplicas() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), statefulTasks, UUID_1);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_2);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTasks,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );


        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertThat(client2.standbyTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
        assertThat(probingRebalanceNeeded, is(true));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldNotAssignWarmupAndStandbyToTheSameClient() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), statefulTasks, UUID_1);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_2);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTasks,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
        assertThat(probingRebalanceNeeded, is(true));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldNotAssignAnyStandbysWithInsufficientCapacity() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1), statefulTasks, UUID_1);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1);

        final AssignmentConfigs configs = getConfigWithStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);

        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
        assertHasNoStandbyTasks(client1);
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldAssignActiveTasksToNotCaughtUpClientIfNoneExist() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_1);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1);

        final AssignmentConfigs configs = getConfigWithStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);
        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
        assertHasNoStandbyTasks(client1);
        assertThat(probingRebalanceNeeded, is(false));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, true, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldNotAssignMoreThanMaxWarmupReplicasWithStandbys() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(statefulTasks, statefulTasks, UUID_1);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_2);
        final ClientState client3 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_3);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final AssignmentConfigs configs = getConfigWithStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3))
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

    @Test
    public void shouldDistributeStatelessTasksToBalanceTotalTaskLoad() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statelessTasks = mkSet(TASK_1_0, TASK_1_1, TASK_1_2);

        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(statefulTasks, statefulTasks, UUID_1);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_2);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = getConfigWithStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2))
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

    @Test
    public void shouldDistributeStatefulActiveTasksToAllClients() {
        final Set<TaskId> allTasks =
            mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3, TASK_2_0); // 9 total
        final Map<TaskId, Long> allTaskLags = allTasks.stream().collect(Collectors.toMap(t -> t, t -> 0L));
        final Set<TaskId> statefulTasks = new HashSet<>(allTasks);
        final ClientState client1 = new ClientState(emptySet(), emptySet(), allTaskLags, EMPTY_CLIENT_TAGS, 100);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), allTaskLags, EMPTY_CLIENT_TAGS, 50);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), allTaskLags, EMPTY_CLIENT_TAGS, 1);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final AssignmentConfigs configs = getConfigWithoutStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3)),
            mkEntry(new Subtopology(2, null), mkSet(TASK_2_0))
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

    @Test
    public void shouldReturnFalseIfPreviousAssignmentIsReused() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = new HashSet<>(allTasks);
        final Set<TaskId> caughtUpTasks1 = enableRackAwareTaskAssignor ? mkSet(TASK_0_0, TASK_0_3) : mkSet(TASK_0_0, TASK_0_2);
        final Set<TaskId> caughtUpTasks2 = enableRackAwareTaskAssignor ? mkSet(TASK_0_1, TASK_0_2) : mkSet(TASK_0_1, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(caughtUpTasks1, statefulTasks, UUID_1);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(caughtUpTasks2, statefulTasks, UUID_2);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = getConfigWithoutStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);

        assertThat(probingRebalanceNeeded, is(false));
        assertThat(client1.activeTasks(), equalTo(client1.prevActiveTasks()));
        assertThat(client2.activeTasks(), equalTo(client2.prevActiveTasks()));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldReturnFalseIfNoWarmupTasksAreAssigned() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_1);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_2);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);

        final AssignmentConfigs configs = getConfigWithoutStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);
        assertThat(probingRebalanceNeeded, is(false));
        assertHasNoStandbyTasks(client1, client2);

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldReturnTrueIfWarmupTasksAreAssigned() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(allTasks, statefulTasks, UUID_1);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks, UUID_2);

        final AssignmentConfigs configs = getConfigWithoutStandbys();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = mkMap(
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1))
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = getRackAwareTaskAssignor(configs, tasksForTopicGroup);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, rackAwareTaskAssignor, configs);
        assertThat(probingRebalanceNeeded, is(true));
        assertThat(client2.standbyTaskCount(), equalTo(1));

        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, allTasks, clientStates, false, enableRackAwareTaskAssignor);
    }

    @Test
    public void shouldDistributeStatelessTasksEvenlyOverClientsWithEqualStreamThreadsPerClientAsTasksAndNoStatefulTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final Set<TaskId> statelessTasks = new HashSet<>(allTasks);

        final Map<TaskId, Long> taskLags = new HashMap<>();
        final ClientState client1 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 7);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 7);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 7);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2))
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

    @Test
    public void shouldDistributeStatelessTasksEvenlyOverClientsWithLessStreamThreadsPerClientAsTasksAndNoStatefulTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final Set<TaskId> statelessTasks = new HashSet<>(allTasks);

        final Map<TaskId, Long> taskLags = new HashMap<>();
        final ClientState client1 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 2);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 2);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 2);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2))
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

    @Test
    public void shouldDistributeStatelessTasksEvenlyOverClientsWithUnevenlyDistributedStreamThreadsAndNoStatefulTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final Set<TaskId> statelessTasks = new HashSet<>(allTasks);

        final Map<TaskId, Long> taskLags = new HashMap<>();
        final ClientState client1 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 1);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 2);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 3);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2))
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

    @Test
    public void shouldDistributeStatelessTasksEvenlyWithPreviousAssignmentAndNoStatefulTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final Set<TaskId> statelessTasks = new HashSet<>(allTasks);

        final Map<TaskId, Long> taskLags = new HashMap<>();
        final ClientState client1 = new ClientState(statelessTasks, emptySet(), taskLags, EMPTY_CLIENT_TAGS, 3);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 3);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), taskLags, EMPTY_CLIENT_TAGS, 3);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

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
            mkEntry(new Subtopology(0, null), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)),
            mkEntry(new Subtopology(1, null), mkSet(TASK_1_0, TASK_1_1, TASK_1_2))
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

    @Test
    public void shouldAssignRandomInput() {
        final int nodeSize = 50;
        final int tpSize = 60;
        final int partitionSize = 3;
        final int clientSize = 50;
        final int replicaCount = 3;
        final int maxCapacity = 3;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(
            tpSize, partitionSize, false);
        final AssignmentConfigs assignorConfiguration = getConfigWithStandbys(replicaCount);

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
        final SortedMap<UUID, ClientState> clientStateMap = getRandomClientState(clientSize,
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

    @Test
    public void shouldRemainOriginalAssignmentWithoutTrafficCostForMinCostStrategy() {
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
        final Map<UUID, Map<String, Optional<String>>> processRackMap = getRandomProcessRacks(clientSize, nodeSize);
        final InternalTopicManager mockInternalTopicManager = mockInternalTopicManagerForRandomChangelog(nodeSize, tpSize, partitionSize);

        AssignmentConfigs configs = new AssignorConfiguration.AssignmentConfigs(
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
        final SortedMap<UUID, ClientState> clientStateMap = getRandomClientState(clientSize,
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

        final SortedMap<UUID, ClientState> clientStateMapCopy = copyClientStateMap(clientStateMap);
        configs = new AssignorConfiguration.AssignmentConfigs(
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

        for (final Map.Entry<UUID, ClientState> entry : clientStateMap.entrySet()) {
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
                                                                      final UUID processId) {
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
