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
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.apache.kafka.streams.processor.internals.assignment.ClientTagAwareStandbyTaskAssignor.TagEntry;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuidForInt;
import static org.apache.kafka.streams.processor.internals.assignment.ClientTagAwareStandbyTaskAssignor.assignStandbyTasksToClientsWithDifferentTags;
import static org.apache.kafka.streams.processor.internals.assignment.ClientTagAwareStandbyTaskAssignor.fillClientsTagStatistics;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.computeTasksToRemainingStandbys;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.createLeastLoadedPrioritySetConstrainedByAssignedTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClientTagAwareStandbyTaskAssignorTest {
    private static final String ZONE_TAG = "zone";
    private static final String CLUSTER_TAG = "cluster";

    private static final String ZONE_1 = "zone1";
    private static final String ZONE_2 = "zone2";
    private static final String ZONE_3 = "zone3";

    private static final String CLUSTER_1 = "cluster1";
    private static final String CLUSTER_2 = "cluster2";
    private static final String CLUSTER_3 = "cluster3";

    private static final UUID UUID_1 = uuidForInt(1);
    private static final UUID UUID_2 = uuidForInt(2);
    private static final UUID UUID_3 = uuidForInt(3);
    private static final UUID UUID_4 = uuidForInt(4);
    private static final UUID UUID_5 = uuidForInt(5);
    private static final UUID UUID_6 = uuidForInt(6);
    private static final UUID UUID_7 = uuidForInt(7);
    private static final UUID UUID_8 = uuidForInt(8);
    private static final UUID UUID_9 = uuidForInt(9);

    private StandbyTaskAssignor standbyTaskAssignor;

    @Before
    public void setup() {
        standbyTaskAssignor = new ClientTagAwareStandbyTaskAssignor();
    }

    @Test
    public void shouldNotAssignStatelessTasksToAnyClients() {
        final Set<TaskId> statefulTasks = mkSet(
            TASK_1_0,
            TASK_1_1,
            TASK_1_2
        );

        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)))),
            mkEntry(UUID_3, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)))),

            mkEntry(UUID_4, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)), TASK_0_1, TASK_1_1)),
            mkEntry(UUID_5, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
            mkEntry(UUID_6, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)))),

            mkEntry(UUID_7, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)), TASK_0_2, TASK_1_2)),
            mkEntry(UUID_8, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_3)))),
            mkEntry(UUID_9, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_3))))
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
        final Set<String> rackAwareAssignmentTags = mkSet(ZONE_TAG, CLUSTER_TAG);
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0)),
            mkEntry(UUID_2, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)), TASK_0_1)),
            mkEntry(UUID_3, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)), TASK_0_2))
        );

        final ConstrainedPrioritySet constrainedPrioritySet = createLeastLoadedPrioritySetConstrainedByAssignedTask(clientStates);
        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final Map<TaskId, UUID> taskToClientId = mkMap(mkEntry(TASK_0_0, UUID_1),
                                                       mkEntry(TASK_0_1, UUID_2),
                                                       mkEntry(TASK_0_2, UUID_3));

        final Map<String, Set<String>> tagKeyToValues = new HashMap<>();
        final Map<TagEntry, Set<UUID>> tagEntryToClients = new HashMap<>();

        fillClientsTagStatistics(clientStates, tagEntryToClients, tagKeyToValues);

        final Map<TaskId, UUID> pendingStandbyTasksToClientId = new HashMap<>();
        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(numStandbyReplicas, allActiveTasks);

        for (final TaskId activeTaskId : allActiveTasks) {
            assignStandbyTasksToClientsWithDifferentTags(
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
        final Set<String> rackAwareAssignmentTags = mkSet(ZONE_TAG, CLUSTER_TAG);
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0)),
            mkEntry(UUID_2, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)), TASK_0_1)),
            mkEntry(UUID_3, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)), TASK_0_2))
        );

        final ConstrainedPrioritySet constrainedPrioritySet = createLeastLoadedPrioritySetConstrainedByAssignedTask(clientStates);
        final int numStandbyReplicas = 3;
        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final Map<TaskId, UUID> taskToClientId = mkMap(mkEntry(TASK_0_0, UUID_1),
                                                       mkEntry(TASK_0_1, UUID_2),
                                                       mkEntry(TASK_0_2, UUID_3));

        final Map<String, Set<String>> tagKeyToValues = new HashMap<>();
        final Map<TagEntry, Set<UUID>> tagEntryToClients = new HashMap<>();

        fillClientsTagStatistics(clientStates, tagEntryToClients, tagKeyToValues);

        final Map<TaskId, UUID> pendingStandbyTasksToClientId = new HashMap<>();
        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(numStandbyReplicas, allActiveTasks);

        for (final TaskId activeTaskId : allActiveTasks) {
            assignStandbyTasksToClientsWithDifferentTags(
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
            activeTaskId -> assertEquals(String.format("Active task with id [%s] didn't match expected number " +
                                                       "of remaining standbys value.", activeTaskId),
                                         1,
                                         tasksToRemainingStandbys.get(activeTaskId).longValue())
        );

        allActiveTasks.forEach(
            activeTaskId -> assertEquals(String.format("Active task with id [%s] didn't match expected " +
                                                       "client ID value.", activeTaskId),
                                         taskToClientId.get(activeTaskId),
                                         pendingStandbyTasksToClientId.get(activeTaskId))
        );
    }

    @Test
    public void shouldPermitTaskMovementWhenClientTagsMatch() {
        final ClientState source = createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)));
        final ClientState destination = createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)));

        assertTrue(standbyTaskAssignor.isAllowedTaskMovement(source, destination));
    }

    @Test
    public void shouldDeclineTaskMovementWhenClientTagsDoNotMatch() {
        final ClientState source = createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)));
        final ClientState destination = createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)));

        assertFalse(standbyTaskAssignor.isAllowedTaskMovement(source, destination));
    }

    @Test
    public void shouldDistributeStandbyTasksWhenActiveTasksAreLocatedOnSameZone() {
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)))),
            mkEntry(UUID_3, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)))),

            mkEntry(UUID_4, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)), TASK_0_1, TASK_1_1)),
            mkEntry(UUID_5, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
            mkEntry(UUID_6, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)))),

            mkEntry(UUID_7, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)), TASK_0_2, TASK_1_2)),
            mkEntry(UUID_8, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_3)))),
            mkEntry(UUID_9, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_3))))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(2, ZONE_TAG, CLUSTER_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTrue(clientStates.values().stream().allMatch(ClientState::reachedCapacity));

        Stream.of(UUID_1, UUID_4, UUID_7).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 0));
        Stream.of(UUID_2, UUID_3, UUID_5, UUID_6, UUID_8, UUID_9).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 2));
        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 12);

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_0,
                clientStates,
                asList(
                    mkSet(UUID_9, UUID_5), mkSet(UUID_6, UUID_8)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_0,
                clientStates,
                asList(
                    mkSet(UUID_9, UUID_5), mkSet(UUID_6, UUID_8)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_1,
                clientStates,
                asList(
                    mkSet(UUID_2, UUID_9), mkSet(UUID_3, UUID_8)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_1,
                clientStates,
                asList(
                    mkSet(UUID_2, UUID_9), mkSet(UUID_3, UUID_8)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_2,
                clientStates,
                asList(
                    mkSet(UUID_5, UUID_3), mkSet(UUID_2, UUID_6)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_2,
                clientStates,
                asList(
                    mkSet(UUID_5, UUID_3), mkSet(UUID_2, UUID_6)
                )
            )
        );
    }

    @Test
    public void shouldDistributeStandbyTasksWhenActiveTasksAreLocatedOnSameCluster() {
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_2, TASK_1_2)),

            mkEntry(UUID_4, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
            mkEntry(UUID_5, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
            mkEntry(UUID_6, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)))),

            mkEntry(UUID_7, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)))),
            mkEntry(UUID_8, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_3)))),
            mkEntry(UUID_9, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_3))))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(2, ZONE_TAG, CLUSTER_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTrue(clientStates.values().stream().allMatch(ClientState::reachedCapacity));

        Stream.of(UUID_1, UUID_2, UUID_3).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 0));
        Stream.of(UUID_4, UUID_5, UUID_6, UUID_7, UUID_8, UUID_9).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 2));
        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 12);

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_0,
                clientStates,
                asList(
                    mkSet(UUID_9, UUID_5), mkSet(UUID_6, UUID_8)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_0,
                clientStates,
                asList(
                    mkSet(UUID_9, UUID_5), mkSet(UUID_6, UUID_8)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_1,
                clientStates,
                asList(
                    mkSet(UUID_4, UUID_9), mkSet(UUID_6, UUID_7)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_1,
                clientStates,
                asList(
                    mkSet(UUID_4, UUID_9), mkSet(UUID_6, UUID_7)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_2,
                clientStates,
                asList(
                    mkSet(UUID_5, UUID_7), mkSet(UUID_4, UUID_8)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_2,
                clientStates,
                asList(
                    mkSet(UUID_5, UUID_7), mkSet(UUID_4, UUID_8)
                )
            )
        );
    }

    @Test
    public void shouldDoThePartialRackAwareness() {
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_0)),
            mkEntry(UUID_2, createClientStateWithCapacity(1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_2)))),
            mkEntry(UUID_3, createClientStateWithCapacity(1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_3)))),

            mkEntry(UUID_4, createClientStateWithCapacity(1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_2), mkEntry(ZONE_TAG, ZONE_1)))),
            mkEntry(UUID_5, createClientStateWithCapacity(1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_2), mkEntry(ZONE_TAG, ZONE_2)))),
            mkEntry(UUID_6, createClientStateWithCapacity(1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_2), mkEntry(ZONE_TAG, ZONE_3)), TASK_1_0))
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

        Stream.of(UUID_2, UUID_5).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 1));
        // There's no strong guarantee where 2nd standby task will end up.
        Stream.of(UUID_1, UUID_3, UUID_4, UUID_6).forEach(client -> assertStandbyTaskCountForClientEqualsTo(clientStates, client, 0, 1));
        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 4);

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_0,
                clientStates,
                asList(
                    // Since it's located on a different `cluster` and `zone` tag dimensions,
                    // `UUID_5` is the "ideal" distribution for the 1st standby task assignment.
                    // For the 2nd standby, either `UUID_3` or `UUID_6` are valid destinations as
                    // we need to distribute the clients on different `zone`
                    // tags without considering the `cluster` tag value.
                    mkSet(UUID_5, UUID_3),
                    mkSet(UUID_5, UUID_6)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_0,
                clientStates,
                asList(
                    // The same comment as above applies here too.
                    // `UUID_2` is the ideal distribution on different `cluster`
                    // and `zone` tag dimensions. In contrast, `UUID_4` and `UUID_1`
                    // satisfy only the partial rack awareness as they are located on a different `zone` tag dimension.
                    mkSet(UUID_2, UUID_4),
                    mkSet(UUID_2, UUID_1)
                )
            )
        );
    }

    @Test
    public void shouldDistributeClientsOnDifferentZoneTagsEvenWhenClientsReachedCapacity() {
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0)),
            mkEntry(UUID_2, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_1)),
            mkEntry(UUID_3, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_2)),
            mkEntry(UUID_4, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_1_0)),
            mkEntry(UUID_5, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_1_1)),
            mkEntry(UUID_6, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_1_2))
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
                    mkSet(UUID_2), mkSet(UUID_5), mkSet(UUID_3), mkSet(UUID_6)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_0,
                clientStates,
                asList(
                    mkSet(UUID_2), mkSet(UUID_5), mkSet(UUID_3), mkSet(UUID_6)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_1,
                clientStates,
                asList(
                    mkSet(UUID_1), mkSet(UUID_4), mkSet(UUID_3), mkSet(UUID_6)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_1,
                clientStates,
                asList(
                    mkSet(UUID_1), mkSet(UUID_4), mkSet(UUID_3), mkSet(UUID_6)
                )
            )
        );

        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_0_2,
                clientStates,
                asList(
                    mkSet(UUID_1), mkSet(UUID_4), mkSet(UUID_2), mkSet(UUID_5)
                )
            )
        );
        assertTrue(
            standbyClientsHonorRackAwareness(
                TASK_1_2,
                clientStates,
                asList(
                    mkSet(UUID_1), mkSet(UUID_4), mkSet(UUID_2), mkSet(UUID_5)
                )
            )
        );
    }

    @Test
    public void shouldIgnoreTagsThatAreNotPresentInRackAwareness() {
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_0)),
            mkEntry(UUID_2, createClientStateWithCapacity(2, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_2)))),

            mkEntry(UUID_3, createClientStateWithCapacity(1, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_2), mkEntry(ZONE_TAG, ZONE_1))))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(1, CLUSTER_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 1);
        assertEquals(1, clientStates.get(UUID_3).standbyTaskCount());
    }

    @Test
    public void shouldHandleOverlappingTagValuesBetweenDifferentTagKeys() {
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)), TASK_0_0)),
            mkEntry(UUID_2, createClientStateWithCapacity(2, mkMap(mkEntry(ZONE_TAG, CLUSTER_1), mkEntry(CLUSTER_TAG, CLUSTER_3))))
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
                    mkSet(UUID_2)
                )
            )
        );
    }

    @Test
    public void shouldDistributeStandbyTasksOnLeastLoadedClientsWhenClientsAreNotOnDifferentTagDimensions() {
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(3, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_0)),
            mkEntry(UUID_2, createClientStateWithCapacity(3, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_1)),
            mkEntry(UUID_3, createClientStateWithCapacity(3, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_2)),
            mkEntry(UUID_4, createClientStateWithCapacity(3, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_1_0))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(1, CLUSTER_TAG, ZONE_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 4);
        assertEquals(1, clientStates.get(UUID_1).standbyTaskCount());
        assertEquals(1, clientStates.get(UUID_2).standbyTaskCount());
        assertEquals(1, clientStates.get(UUID_3).standbyTaskCount());
        assertEquals(1, clientStates.get(UUID_4).standbyTaskCount());
    }

    @Test
    public void shouldNotAssignStandbyTasksIfThereAreNoEnoughClients() {
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(3, mkMap(mkEntry(CLUSTER_TAG, CLUSTER_1), mkEntry(ZONE_TAG, ZONE_1)), TASK_0_0))
        );

        final Set<TaskId> allActiveTasks = findAllActiveTasks(clientStates);
        final AssignmentConfigs assignmentConfigs = newAssignmentConfigs(1, CLUSTER_TAG, ZONE_TAG);

        standbyTaskAssignor.assign(clientStates, allActiveTasks, allActiveTasks, assignmentConfigs);

        assertTotalNumberOfStandbyTasksEqualsTo(clientStates, 0);
        assertEquals(0, clientStates.get(UUID_1).standbyTaskCount());
    }

    private static void assertTotalNumberOfStandbyTasksEqualsTo(final Map<UUID, ClientState> clientStates, final int expectedTotalNumberOfStandbyTasks) {
        final int actualTotalNumberOfStandbyTasks = clientStates.values().stream().map(ClientState::standbyTaskCount).reduce(0, Integer::sum);
        assertEquals(expectedTotalNumberOfStandbyTasks, actualTotalNumberOfStandbyTasks);
    }

    private static void assertStandbyTaskCountForClientEqualsTo(final Map<UUID, ClientState> clientStates,
                                                                final UUID client,
                                                                final int... expectedStandbyTaskCounts) {
        final int standbyTaskCount = clientStates.get(client).standbyTaskCount();
        final String msg = String.format("Client [%s] doesn't have expected number of standby tasks. " +
                                         "Expected any of %s, actual [%s]",
                                         client, Arrays.toString(expectedStandbyTaskCounts), standbyTaskCount);

        assertTrue(msg, Arrays.stream(expectedStandbyTaskCounts).anyMatch(expectedStandbyTaskCount -> expectedStandbyTaskCount == standbyTaskCount));
    }

    private static boolean standbyClientsHonorRackAwareness(final TaskId activeTaskId,
                                                            final Map<UUID, ClientState> clientStates,
                                                            final List<Set<UUID>> validClientIdsBasedOnRackAwareAssignmentTags) {
        final Set<UUID> standbyTaskClientIds = findAllStandbyTaskClients(clientStates, activeTaskId);

        return validClientIdsBasedOnRackAwareAssignmentTags.stream()
                                                           .filter(it -> it.equals(standbyTaskClientIds))
                                                           .count() == 1;
    }

    private static Set<UUID> findAllStandbyTaskClients(final Map<UUID, ClientState> clientStates, final TaskId task) {
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
                                     false,
                                     1L,
                                     60000L,
                                     asList(rackAwareAssignmentTags));
    }

    private static ClientState createClientStateWithCapacity(final int capacity,
                                                             final Map<String, String> clientTags,
                                                             final TaskId... tasks) {
        final ClientState clientState = new ClientState(capacity, clientTags);

        Optional.ofNullable(tasks).ifPresent(t -> clientState.assignActiveTasks(asList(t)));

        return clientState;
    }

    private static Set<TaskId> findAllActiveTasks(final Map<UUID, ClientState> clientStates) {
        return clientStates.entrySet()
                           .stream()
                           .flatMap(clientStateEntry -> clientStateEntry.getValue().activeTasks().stream())
                           .collect(Collectors.toSet());
    }
}
