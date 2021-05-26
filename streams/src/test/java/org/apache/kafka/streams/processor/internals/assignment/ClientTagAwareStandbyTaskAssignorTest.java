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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuidForInt;
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
    private static final String CLUSTER_4 = "cluster4";

    private static final UUID UUID_1 = uuidForInt(1);
    private static final UUID UUID_2 = uuidForInt(2);
    private static final UUID UUID_3 = uuidForInt(3);
    private static final UUID UUID_4 = uuidForInt(4);
    private static final UUID UUID_5 = uuidForInt(5);
    private static final UUID UUID_6 = uuidForInt(6);
    private static final UUID UUID_7 = uuidForInt(7);
    private static final UUID UUID_8 = uuidForInt(8);
    private static final UUID UUID_9 = uuidForInt(9);
    private static final UUID UUID_10 = uuidForInt(10);
    private static final UUID UUID_11 = uuidForInt(11);
    private static final UUID UUID_12 = uuidForInt(12);

    @Test
    public void shouldDistributeStandbyTasksOnAppropriateClientTagDimensions() {
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

        final AssignorConfiguration.AssignmentConfigs configs = newAssignmentConfigs(2, ZONE_TAG, CLUSTER_TAG);

        final Map<TaskId, UUID> allActiveTasks = findAllActiveTasks(clientStates);

        new ClientTagAwareStandbyTaskAssignor(configs).assignStandbyTasks(
            allActiveTasks,
            new TreeMap<>(clientStates)
        );

        assertFalse(isStandbyTaskOnClientTag(clientStates, TASK_0_0, ZONE_TAG, ZONE_1));
        assertFalse(isStandbyTaskOnClientTag(clientStates, TASK_0_0, CLUSTER_TAG, CLUSTER_1));
    }

    private static boolean isStandbyTaskOnClientTag(final Map<UUID, ClientState> clientStates,
                                                    final TaskId taskId,
                                                    final String tagKey,
                                                    final String tagValue) {
        return clientStates.values()
                           .stream()
                           .filter(clientState -> clientState.standbyTasks().contains(taskId))
                           .anyMatch(clientState -> clientState.clientTags().get(tagKey).equals(tagValue));
    }

    private static AssignorConfiguration.AssignmentConfigs newAssignmentConfigs(final int numStandbyReplicas, String... rackAwareAssignmentTags) {
        return new AssignorConfiguration.AssignmentConfigs(0L, 1, numStandbyReplicas, 60000L, Arrays.asList(rackAwareAssignmentTags));
    }

    private static ClientState createClientStateWithCapacity(final int capacity,
                                                             final Map<String, String> clientTags,
                                                             final TaskId... tasks) {
        final ClientState clientState = new ClientState(clientTags);

        IntStream.range(0, capacity).forEach(i -> clientState.incrementCapacity());
        Optional.ofNullable(tasks).ifPresent(t -> clientState.assignActiveTasks(Arrays.asList(t)));

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