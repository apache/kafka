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

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.internals.assignment.ClientTagAwareStandbyTaskAssignor.TagEntry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_6;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_7;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_8;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_9;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.computeTasksToRemainingStandbys;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.createLeastLoadedPrioritySetConstrainedByAssignedTask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClientTagAwareStandbyTaskAssignorTest {
    private static final String ZONE_TAG = "zone";
    private static final String CLUSTER_TAG = "cluster";

    private static final String ZONE_1 = "zone1";
    private static final String ZONE_2 = "zone2";
    private static final String ZONE_3 = "zone3";

    private static final String CLUSTER_1 = "cluster1";
    private static final String CLUSTER_2 = "cluster2";
    private static final String CLUSTER_3 = "cluster3";

    private StandbyTaskAssignor standbyTaskAssignor;

    @BeforeEach
    public void setup() {
        standbyTaskAssignor = new ClientTagAwareStandbyTaskAssignor();
    }

    @Test
    public void shouldNotAssignStatelessTasksToAnyClients() {
        final Set<TaskId> statefulTasks = Set.of(
            TASK_1_0,
            TASK_1_1,
            TASK_1_2
        );

        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0, TASK_1_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)))),
            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)))),

            mkEntry(PID_4, createClientStateWithCapacity(PID_4, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)), TASK_0_1, TASK_1_1)),
            mkEntry(PID_5, createClientStateWithCapacity(PID_5, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
            mkEntry(PID_6, createClientStateWithCapacity(PID_6, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)))),

            mkEntry(PID_7, createClientStateWithCapacity(PID_7, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)), TASK_0_2, TASK_1_2)),
            mkEntry(PID_8, createClientStateWithCapacity(PID_8, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_3)))),
            mkEntry(PID_9, createClientStateWithCapacity(PID_9, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_3))))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);

        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(2, ZONE_TAG, CLUSTER_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, statefulTasks, assignmentConfigs);

        final Set<TaskId> statelessTasks = allActiveTasks.stream().filter(taskId -> !statefulTasks.contains(taskId)).collect(Collectors.toSet());
        assertTrue(
            clientStates.values().stream().allMatch(clientState -> statelessTasks.stream().noneMatch(clientState::hasStandbyTask))
        );
    }

    @Test
    public void shouldRemoveClientToRemainingStandbysAndNotPopulatePendingStandbyTasksToClientIdWhenAllStandbyTasksWereAssigned() {
        final int numStandbyReplicas = 2;
        final Set<String> rackAwareAssignmentTags = Set.of(ZONE_TAG, CLUSTER_TAG);
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)), TASK_0_1)),
            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)), TASK_0_2))
        );

        final ConstrainedPrioritySet constrainedPrioritySet = createLeastLoadedPrioritySetConstrainedByAssignedTask(clientStates);
        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final Map<TaskId, ProcessId> taskToClientId = mkMap(mkEntry(TASK_0_0, PID_1),
                                                       mkEntry(TASK_0_1, PID_2),
                                                       mkEntry(TASK_0_2, PID_3));

        final Map<String, Set<String>> tagKeyToValues = new HashMap<>();
        final Map<TagEntry, Set<ProcessId>> tagEntryToClients = new HashMap<>();

        new ClientTagAwareStandbyTaskAssignor().fillClientsTagStatistics(clientStates, tagEntryToClients, tagKeyToValues);

        final Map<TaskId, ProcessId> pendingStandbyTasksToClientId = new HashMap<>();
        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(numStandbyReplicas, allActiveTasks);

        for (final TaskId activeTaskId : allActiveTasks) {
            new ClientTagAwareStandbyTaskAssignor().assignStandbyTasksToClientsWithDifferentTags(
                numStandbyReplicas,
                constrainedPrioritySet,
                activeTaskId,
                taskToClientId.get(activeTaskId),
                rackAwareAssignmentTags,
                clientStates,
                tasksToRemainingStandbys,
                tagKeyToValues,
                tagEntryToClients,
                pendingStandbyTasksToClientId
            );
        }

        assertTrue(tasksToRemainingStandbys.isEmpty());
        assertTrue(pendingStandbyTasksToClientId.isEmpty());
    }

    @Test
    public void shouldUpdateClientToRemainingStandbysAndPendingStandbyTasksToClientIdWhenNotAllStandbyTasksWereAssigned() {
        final Set<String> rackAwareAssignmentTags = Set.of(ZONE_TAG, CLUSTER_TAG);
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)), TASK_0_1)),
            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)), TASK_0_2))
        );

        final ConstrainedPrioritySet constrainedPrioritySet = createLeastLoadedPrioritySetConstrainedByAssignedTask(clientStates);
        final int numStandbyReplicas = 3;
        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final Map<TaskId, ProcessId> taskToClientId = mkMap(mkEntry(TASK_0_0, PID_1),
                                                       mkEntry(TASK_0_1, PID_2),
                                                       mkEntry(TASK_0_2, PID_3));

        final Map<String, Set<String>> tagKeyToValues = new HashMap<>();
        final Map<TagEntry, Set<ProcessId>> tagEntryToClients = new HashMap<>();

        new ClientTagAwareStandbyTaskAssignor().fillClientsTagStatistics(clientStates, tagEntryToClients, tagKeyToValues);

        final Map<TaskId, ProcessId> pendingStandbyTasksToClientId = new HashMap<>();
        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(numStandbyReplicas, allActiveTasks);

        for (final TaskId activeTaskId : allActiveTasks) {
            new ClientTagAwareStandbyTaskAssignor().assignStandbyTasksToClientsWithDifferentTags(
                numStandbyReplicas,
                constrainedPrioritySet,
                activeTaskId,
                taskToClientId.get(activeTaskId),
                rackAwareAssignmentTags,
                clientStates,
                tasksToRemainingStandbys,
                tagKeyToValues,
                tagEntryToClients,
                pendingStandbyTasksToClientId
            );
        }

        allActiveTasks.forEach(
            activeTaskId -> assertEquals(
                1,
                tasksToRemainingStandbys.get(activeTaskId).longValue(),
                String.format("Active task with id [%s] didn't match expected number of remaining standbys value.", activeTaskId)
            )
        );

        allActiveTasks.forEach(
            activeTaskId -> assertEquals(
                taskToClientId.get(activeTaskId),
                pendingStandbyTasksToClientId.get(activeTaskId),
                String.format("Active task with id [%s] didn't match expected client ID value.", activeTaskId)
            )
        );
    }

    @Test
    public void shouldPermitTaskMovementWhenClientTagsMatch() {
        final ClientState source = createClientStateWithCapacity(PID_1, 1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)));
        final ClientState destination = createClientStateWithCapacity(PID_2, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)));

        assertTrue(standbyTaskAssignor.isAllowedTaskMovement(source, destination));
    }

    @Test
    public void shouldDeclineTaskMovementWhenClientTagsDoNotMatch() {
        final ClientState source = createClientStateWithCapacity(PID_1, 1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)));
        final ClientState destination = createClientStateWithCapacity(PID_2, 1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)));

        assertFalse(standbyTaskAssignor.isAllowedTaskMovement(source, destination));
    }

    @Test
    public void shouldPermitSingleTaskMoveWhenClientTagMatch() {
        final ClientState source = createClientStateWithCapacity(PID_1, 1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)));
        final ClientState destination = createClientStateWithCapacity(PID_2, 1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)));
        final ClientState clientState = createClientStateWithCapacity(PID_3, 1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)));
        final Map<ProcessId, ClientState> clientStateMap = mkMap(
            mkEntry(PID_1, source),
            mkEntry(PID_2, destination),
            mkEntry(PID_3, clientState)
        );
        final TaskId taskId = new TaskId(0, 0);
        clientState.assignActive(taskId);
        source.assignStandby(taskId);

        assertTrue(standbyTaskAssignor.isAllowedTaskMovement(source, destination, taskId, clientStateMap));
    }

    @Test
    public void shouldPermitSingleTaskMoveWhenDifferentClientTagCountNotChange() {
        final ClientState source = createClientStateWithCapacity(PID_1, 1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)));
        final ClientState destination = createClientStateWithCapacity(PID_2, 1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)));
        final ClientState clientState = createClientStateWithCapacity(PID_3, 1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)));
        final Map<ProcessId, ClientState> clientStateMap = mkMap(
            mkEntry(PID_1, source),
            mkEntry(PID_2, destination),
            mkEntry(PID_3, clientState)
        );
        final TaskId taskId = new TaskId(0, 0);
        clientState.assignActive(taskId);
        source.assignStandby(taskId);

        assertTrue(standbyTaskAssignor.isAllowedTaskMovement(source, destination, taskId, clientStateMap));
    }

    @Test
    public void shouldDeclineSingleTaskMoveWhenReduceClientTagCount() {
        final ClientState source = createClientStateWithCapacity(PID_1, 1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)));
        final ClientState destination = createClientStateWithCapacity(PID_2, 1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)));
        final ClientState clientState = createClientStateWithCapacity(PID_3, 1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)));
        final Map<ProcessId, ClientState> clientStateMap = mkMap(
            mkEntry(PID_1, source),
            mkEntry(PID_2, destination),
            mkEntry(PID_3, clientState)
        );
        final TaskId taskId = new TaskId(0, 0);
        clientState.assignActive(taskId);
        source.assignStandby(taskId);

        // Because destination has ZONE_3 which is the same as active's zone
        assertFalse(standbyTaskAssignor.isAllowedTaskMovement(source, destination, taskId, clientStateMap));
    }

    @Test
    public void shouldDistributeStandbyTasksWhenActiveTasksAreLocatedOnSameZone() {
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0, TASK_1_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)))),
            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)))),

            mkEntry(PID_4, createClientStateWithCapacity(PID_4, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)), TASK_0_1, TASK_1_1)),
            mkEntry(PID_5, createClientStateWithCapacity(PID_5, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
            mkEntry(PID_6, createClientStateWithCapacity(PID_6, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)))),

            mkEntry(PID_7, createClientStateWithCapacity(PID_7, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)), TASK_0_2, TASK_1_2)),
            mkEntry(PID_8, createClientStateWithCapacity(PID_8, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_3)))),
            mkEntry(PID_9, createClientStateWithCapacity(PID_9, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_3))))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(2, ZONE_TAG, CLUSTER_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTrue(clientStates.values().stream().allMatch(ClientState::reachedCapacity));

        Stream.of(PID_1, PID_4, PID_7).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 0));
        Stream.of(PID_2, PID_3, PID_5, PID_6, PID_8, PID_9).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 2));
        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 12);

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_0,
                clientStates,
                asList(
                    Set.of(PID_9, PID_5), Set.of(PID_6, PID_8)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_0,
                clientStates,
                asList(
                    Set.of(PID_9, PID_5), Set.of(PID_6, PID_8)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_1,
                clientStates,
                asList(
                    Set.of(PID_2, PID_9), Set.of(PID_3, PID_8)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_1,
                clientStates,
                asList(
                    Set.of(PID_2, PID_9), Set.of(PID_3, PID_8)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_2,
                clientStates,
                asList(
                    Set.of(PID_5, PID_3), Set.of(PID_2, PID_6)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_2,
                clientStates,
                asList(
                    Set.of(PID_5, PID_3), Set.of(PID_2, PID_6)
                )
            )
        );
    }

    @Test
    public void shouldDistributeStandbyTasksUsingFunctionAndSupplierTags() {
        final Map<ProcessId, String> racksForProcess = mkMap(
            mkEntry(PID_1, "rack1"),
            mkEntry(PID_2, "rack2"),
            mkEntry(PID_3, "rack3"),
            mkEntry(PID_4, "rack1"),
            mkEntry(PID_5, "rack2"),
            mkEntry(PID_6, "rack3"),
            mkEntry(PID_7, "rack1"),
            mkEntry(PID_8, "rack2"),
            mkEntry(PID_9, "rack3")
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = mock(RackAwareTaskAssignor.class);
        when(rackAwareTaskAssignor.validClientRack()).thenReturn(true);
        when(rackAwareTaskAssignor.racksForProcess()).thenReturn(racksForProcess);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(2);
        standbyTaskAssignor = StandbyTaskAssignorFactory.create(assignmentConfigs, rackAwareTaskAssignor);
        verify(rackAwareTaskAssignor, times(1)).racksForProcess();

        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 2, mkMap(), TASK_0_0, TASK_1_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 2, mkMap(), TASK_0_1, TASK_1_1)),
            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 2, mkMap(), TASK_0_2, TASK_1_2)),

            mkEntry(PID_4, createClientStateWithCapacity(PID_4, 2, mkMap())),
            mkEntry(PID_5, createClientStateWithCapacity(PID_5, 2, mkMap())),
            mkEntry(PID_6, createClientStateWithCapacity(PID_6, 2, mkMap())),

            mkEntry(PID_7, createClientStateWithCapacity(PID_7, 2, mkMap())),
            mkEntry(PID_8, createClientStateWithCapacity(PID_8, 2, mkMap())),
            mkEntry(PID_9, createClientStateWithCapacity(PID_9, 2, mkMap()))
        );

        final Map<ProcessId, ClientState> clientStatesWithTags = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1)), TASK_0_0, TASK_1_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2)), TASK_0_1, TASK_1_1)),
            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3)), TASK_0_2, TASK_1_2)),

            mkEntry(PID_4, createClientStateWithCapacity(PID_4, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1)))),
            mkEntry(PID_5, createClientStateWithCapacity(PID_5, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2)))),
            mkEntry(PID_6, createClientStateWithCapacity(PID_6, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3)))),

            mkEntry(PID_7, createClientStateWithCapacity(PID_7, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1)))),
            mkEntry(PID_8, createClientStateWithCapacity(PID_8, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2)))),
            mkEntry(PID_9, createClientStateWithCapacity(PID_9, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3))))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        final AssignmentConfigs assignmentConfigsWithTags = newAssignmentConfigs(2, ZONE_TAG);
        standbyTaskAssignor = new ClientTagAwareStandbyTaskAssignor();
        standbyTaskAssignor.assign(clientStatesWithTags, allActiveTasks, allActiveTasks, assignmentConfigsWithTags);

        Stream.of(clientStates, clientStatesWithTags).forEach(
            cs -> {
                assertTrue(cs.values().stream().allMatch(ClientState::reachedCapacity));
                Stream.of(PID_1, PID_2, PID_3)
                    .forEach(client -> assertStandbyTaskCountForClientEqualsTo(cs, client, 0));
                Stream.of(PID_4, PID_5, PID_6, PID_7, PID_8, PID_9)
                    .forEach(client -> assertStandbyTaskCountForClientEqualsTo(cs, client, 2));
                assertTotalNumberOfStandbyTasksEqualsTo(cs, 12);

                assertTrue(
                    containsStandbyTasks(
                        TASK_0_0,
                        cs,
                        Set.of(PID_2, PID_3, PID_5, PID_6, PID_8, PID_9)
                    )
                );
                assertTrue(
                    containsStandbyTasks(
                        TASK_1_0,
                        cs,
                        Set.of(PID_2, PID_3, PID_5, PID_6, PID_8, PID_9)
                    )
                );

                assertTrue(
                    containsStandbyTasks(
                        TASK_0_1,
                        cs,
                        Set.of(PID_1, PID_3, PID_4, PID_6, PID_7, PID_9)
                    )
                );
                assertTrue(
                    containsStandbyTasks(
                        TASK_1_1,
                        cs,
                        Set.of(PID_1, PID_3, PID_4, PID_6, PID_7, PID_9)
                    )
                );

                assertTrue(
                    containsStandbyTasks(
                        TASK_0_2,
                        cs,
                        Set.of(PID_1, PID_2, PID_4, PID_5, PID_7, PID_8)
                    )
                );
                assertTrue(
                    containsStandbyTasks(
                        TASK_1_2,
                        cs,
                        Set.of(PID_1, PID_2, PID_4, PID_5, PID_7, PID_8)
                    )
                );
            }
        );
    }

    @Test
    public void shouldDistributeStandbyTasksWhenActiveTasksAreLocatedOnSameCluster() {
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0, TASK_1_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_1, TASK_1_1)),
            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_2, TASK_1_2)),

            mkEntry(PID_4, createClientStateWithCapacity(PID_4, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
            mkEntry(PID_5, createClientStateWithCapacity(PID_5, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
            mkEntry(PID_6, createClientStateWithCapacity(PID_6, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)))),

            mkEntry(PID_7, createClientStateWithCapacity(PID_7, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)))),
            mkEntry(PID_8, createClientStateWithCapacity(PID_8, 2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_3)))),
            mkEntry(PID_9, createClientStateWithCapacity(PID_9, 2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_3))))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(2, ZONE_TAG, CLUSTER_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTrue(clientStates.values().stream().allMatch(ClientState::reachedCapacity));

        Stream.of(PID_1, PID_2, PID_3).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 0));
        Stream.of(PID_4, PID_5, PID_6, PID_7, PID_8, PID_9).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 2));
        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 12);

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_0,
                clientStates,
                asList(
                    Set.of(PID_9, PID_5), Set.of(PID_6, PID_8)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_0,
                clientStates,
                asList(
                    Set.of(PID_9, PID_5), Set.of(PID_6, PID_8)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_1,
                clientStates,
                asList(
                    Set.of(PID_4, PID_9), Set.of(PID_6, PID_7)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_1,
                clientStates,
                asList(
                    Set.of(PID_4, PID_9), Set.of(PID_6, PID_7)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_2,
                clientStates,
                asList(
                    Set.of(PID_5, PID_7), Set.of(PID_4, PID_8)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_2,
                clientStates,
                asList(
                    Set.of(PID_5, PID_7), Set.of(PID_4, PID_8)
                )
            )
        );
    }

    @Test
    public void shouldDoThePartialRackAwareness() {
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_2)))),
            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_3)))),

            mkEntry(PID_4, createClientStateWithCapacity(PID_4, 1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_2), mkEntry(ZONE_TAG, ZONE_1)))),
            mkEntry(PID_5, createClientStateWithCapacity(PID_5, 1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_2), mkEntry(ZONE_TAG, ZONE_2)))),
            mkEntry(PID_6, createClientStateWithCapacity(PID_6, 1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_2), mkEntry(ZONE_TAG, ZONE_3)), TASK_1_0))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(2, CLUSTER_TAG, ZONE_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        // We need to distribute 2 standby tasks (+1 active task).
        // Since we have only two unique `cluster` tag values,
        // we can only achieve "ideal" distribution on the 1st standby task assignment.
        // We can't consider the `cluster` tag for the 2nd standby task assignment because the 1st standby
        // task would already be assigned on different clusters compared to the active one, which means
        // we have already used all the available cluster tag values. Taking the `cluster` tag into consideration
        // for the  2nd standby task assignment would affectively mean excluding all the clients.
        // Instead, for the 2nd standby task, we can only achieve partial rack awareness based on the `zone` tag.
        // As we don't consider the `cluster` tag for the 2nd standby task assignment, partial rack awareness
        // can be satisfied by placing the 2nd standby client on a different `zone` tag compared to active and corresponding standby tasks.
        // The `zone` on either `cluster` tags are valid candidates for the partial rack awareness, as our goal is to distribute clients on the different `zone` tags.

        Stream.of(PID_2, PID_5).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 1));
        // There's no strong guarantee where 2nd standby task will end up.
        Stream.of(PID_1, PID_3, PID_4, PID_6).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 0, 1));
        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 4);

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_0,
                clientStates,
                asList(
                    // Since it's located on a different `cluster` and `zone` tag dimensions,
                    // `PID_5` is the "ideal" distribution for the 1st standby task assignment.
                    // For the 2nd standby, either `PID_3` or `PID_6` are valid destinations as
                    // we need to distribute the clients on different `zone`
                    // tags without considering the `cluster` tag value.
                    Set.of(PID_5, PID_3),
                    Set.of(PID_5, PID_6)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_0,
                clientStates,
                asList(
                    // The same comment as above applies here too.
                    // `PID_2` is the ideal distribution on different `cluster`
                    // and `zone` tag dimensions. In contrast, `PID_4` and `PID_1`
                    // satisfy only the partial rack awareness as they are located on a different `zone` tag dimension.
                    Set.of(PID_2, PID_4),
                    Set.of(PID_2, PID_1)
                )
            )
        );
    }

    @Test
    public void shouldDistributeClientsOnDifferentZoneTagsEvenWhenClientsReachedCapacity() {
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_1)),
            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_2)),
            mkEntry(PID_4, createClientStateWithCapacity(PID_4, 1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_1_0)),
            mkEntry(PID_5, createClientStateWithCapacity(PID_5, 1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_1_1)),
            mkEntry(PID_6, createClientStateWithCapacity(PID_6, 1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_1_2))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(1, ZONE_TAG, CLUSTER_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        clientStates.keySet().forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 1));
        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 6);

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_0,
                clientStates,
                asList(
                    Set.of(PID_2), Set.of(PID_5), Set.of(PID_3), Set.of(PID_6)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_0,
                clientStates,
                asList(
                    Set.of(PID_2), Set.of(PID_5), Set.of(PID_3), Set.of(PID_6)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_1,
                clientStates,
                asList(
                    Set.of(PID_1), Set.of(PID_4), Set.of(PID_3), Set.of(PID_6)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_1,
                clientStates,
                asList(
                    Set.of(PID_1), Set.of(PID_4), Set.of(PID_3), Set.of(PID_6)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_2,
                clientStates,
                asList(
                    Set.of(PID_1), Set.of(PID_4), Set.of(PID_2), Set.of(PID_5)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_2,
                clientStates,
                asList(
                    Set.of(PID_1), Set.of(PID_4), Set.of(PID_2), Set.of(PID_5)
                )
            )
        );
    }

    @Test
    public void shouldIgnoreTagsThatAreNotPresentInRackAwareness() {
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 2, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_2)))),

            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_2), mkEntry(ZONE_TAG, ZONE_1))))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(1, CLUSTER_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 1);
        assertEquals(1, clientStates.get(PID_3).standbyTaskCount());
    }

    @Test
    public void shouldHandleOverlappingTagValuesBetweenDifferentTagKeys() {
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 2, mkMap(mkEntry(ZONE_TAG, CLUSTER_1), mkEntry(CLUSTER_TAG, CLUSTER_3))))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(1, ZONE_TAG, CLUSTER_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 1);
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_0,
                clientStates,
                singletonList(
                    Set.of(PID_2)
                )
            )
        );
    }

    @Test
    public void shouldDistributeStandbyTasksOnLeastLoadedClientsWhenClientsAreNotOnDifferentTagDimensions() {
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 3, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_0)),
            mkEntry(PID_2, createClientStateWithCapacity(PID_2, 3, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_1)),
            mkEntry(PID_3, createClientStateWithCapacity(PID_3, 3, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_2)),
            mkEntry(PID_4, createClientStateWithCapacity(PID_4, 3, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_1_0))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(1, CLUSTER_TAG, ZONE_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 4);
        assertEquals(1, clientStates.get(PID_1).standbyTaskCount());
        assertEquals(1, clientStates.get(PID_2).standbyTaskCount());
        assertEquals(1, clientStates.get(PID_3).standbyTaskCount());
        assertEquals(1, clientStates.get(PID_4).standbyTaskCount());
    }

    @Test
    public void shouldNotAssignStandbyTasksIfThereAreNoEnoughClients() {
        final Map<ProcessId, ClientState> clientStates = mkMap(
            mkEntry(PID_1, createClientStateWithCapacity(PID_1, 3, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_0))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(1, CLUSTER_TAG, ZONE_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 0);
        assertEquals(0, clientStates.get(PID_1).standbyTaskCount());
    }

    private static void assertTotalNumberOfStandbyTasksEqualsTo(final Map<ProcessId, ClientState> clientStates, final int expectedTotalNumberOfStandbyTasks) {
        final int actualTotalNumberOfStandbyTasks = clientStates.values().stream().map(ClientState::standbyTaskCount).reduce(0, Integer::sum);
        assertEquals(expectedTotalNumberOfStandbyTasks, actualTotalNumberOfStandbyTasks);
    }

    private static void assertStandbyTaskCountForClientEqualsTo(final Map<ProcessId, ClientState> clientStates,
                                                                final ProcessId client,
                                                                final int... expectedStandbyTaskCounts) {
        final int standbyTaskCount = clientStates.get(client).standbyTaskCount();
        final String msg = String.format("Client [%s] doesn't have expected number of standby tasks. " +
                                         "Expected any of %s, actual [%s]",
                                         client, Arrays.toString(expectedStandbyTaskCounts), standbyTaskCount);

        assertTrue(Arrays.stream(expectedStandbyTaskCounts).anyMatch(expectedStandbyTaskCount -> expectedStandbyTaskCount == standbyTaskCount), msg);
    }

    private static boolean standbyClientsHonorRackAwareness(final TaskId activeTaskId,
                                                            final Map<ProcessId, ClientState> clientStates,
                                                            final List<Set<ProcessId>> validClientIdsBasedOnRackAwareAssignmentTags) {
        final Set<ProcessId> standbyTaskClientIds = findAllStandbyTaskClients(clientStates, activeTaskId);

        return validClientIdsBasedOnRackAwareAssignmentTags.stream()
                                                           .filter(it -> it.equals(standbyTaskClientIds))
                                                           .count() == 1;
    }

    private static boolean containsStandbyTasks(final TaskId activeTaskId,
                                                final Map<ProcessId, ClientState> clientStates,
                                                final Set<ProcessId> validClientIdsBasedOnRackAwareAssignmentTags) {
        final Set<ProcessId> standbyTaskClientIds = findAllStandbyTaskClients(clientStates, activeTaskId);
        return validClientIdsBasedOnRackAwareAssignmentTags.containsAll(standbyTaskClientIds);
    }

    private static Set<ProcessId> findAllStandbyTaskClients(final Map<ProcessId, ClientState> clientStates, final TaskId task) {
        return clientStates.keySet()
                           .stream()
                           .filter(clientId -> clientStates.get(clientId).standbyTasks().contains(task))
                           .collect(Collectors.toSet());
    }

    private static AssignmentConfigs newAssignmentConfigs(final int numStandbyReplicas,
                                                          final String... rackAwareAssignmentTags) {
        return new AssignmentConfigs(0L,
                                     1,
                                     numStandbyReplicas,
                                     60000L,
                                     asList(rackAwareAssignmentTags));
    }

    private static ClientState createClientStateWithCapacity(final ProcessId processId,
                                                             final int capacity,
                                                             final Map<String, String> clientTags,
                                                             final TaskId... tasks) {
        final ClientState clientState = new ClientState(processId, capacity, clientTags);

        Optional.ofNullable(tasks).ifPresent(t -> clientState.assignActiveTasks(asList(t)));

        return clientState;
    }

    private static Set<TaskId> findAllActiveTasks(final Map<ProcessId, ClientState> clientStates) {
        return clientStates.entrySet()
                           .stream()
                           .flatMap(clientStateEntry -> clientStateEntry.getValue().activeTasks().stream())
                           .collect(Collectors.toSet());
    }
}
