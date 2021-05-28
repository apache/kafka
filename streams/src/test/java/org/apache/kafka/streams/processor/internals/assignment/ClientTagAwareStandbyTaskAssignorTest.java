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
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
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

        new ClientTagAwareStandbyTaskAssignor(newAssignmentConfigs(2, ZONE_TAG, CLUSTER_TAG))
            .assignStandbyTasks(
                findAllActiveTasks(clientStates),
                new TreeMap<>(clientStates)
            );

        assertTrue(clientStates.values().stream().allMatch(ClientState::reachedCapacity));

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

        new ClientTagAwareStandbyTaskAssignor(newAssignmentConfigs(2, ZONE_TAG, CLUSTER_TAG))
            .assignStandbyTasks(
                findAllActiveTasks(clientStates),
                new TreeMap<>(clientStates)
            );

        assertTrue(clientStates.values().stream().allMatch(ClientState::reachedCapacity));

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

    private static AssignorConfiguration.AssignmentConfigs newAssignmentConfigs(final int numStandbyReplicas,
                                                                                final String... rackAwareAssignmentTags) {
        return new AssignorConfiguration.AssignmentConfigs(0L, 1, numStandbyReplicas, 60000L, Collections.emptyList());
    }

    private static ClientState createClientStateWithCapacity(final int capacity,
                                                             final Map<String, String> clientTags,
                                                             final TaskId... tasks) {
        final ClientState clientState = new ClientState(clientTags);

        IntStream.range(0, capacity).forEach(i -> clientState.incrementCapacity());
        Optional.ofNullable(tasks).ifPresent(t -> clientState.assignActiveTasks(asList(t)));

        return clientState;
    }

    private static Map<TaskId, UUID> findAllActiveTasks(final Map<UUID, ClientState> clientStates) {
        return clientStates.entrySet()
                           .stream()
                           .flatMap(
                               clientStateEntry -> clientStateEntry.getValue()
                                                                   .activeTasks()
                                                                   .stream()
                                                                   .map(taskId -> mkEntry(taskId,
                                                                                          clientStateEntry.getKey()))
                           )
                           .collect(
                               Collectors.toMap(
                                   Map.Entry::getKey,
                                   Map.Entry::getValue
                               )
                           );
    }
}