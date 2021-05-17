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
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuidForInt;

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

    private Map<UUID, ClientState> clientStates;
    private Map<String, String> tagValueToTagKeyMapping;
    private Function<UUID, ClientState> clientStateFunction;
    private Function<String, String> tagValueToTagKeyFunction;

    @Before
    public void setup() {
        clientStates = mkMap(
            mkEntry(UUID_1, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_1)))), // 0_0 (A)
            mkEntry(UUID_2, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_1)))), // 0_1 (S_1)
            mkEntry(UUID_3, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_1)))), // 0_2 (S_1)

            mkEntry(UUID_4, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)))), // 0_1 (A)
            mkEntry(UUID_5, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_2)))), // 0_0 (S_1)
            mkEntry(UUID_6, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)))), //

            mkEntry(UUID_7, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_3)))), // 0_2 (A)
            mkEntry(UUID_8, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_3)))), // 0_2 (S_2)
            mkEntry(UUID_9, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_3))))  // 0_0 (S_2)
//            mkEntry(UUID_7, new ClientState(mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
//            mkEntry(UUID_8, new ClientState(mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
//            mkEntry(UUID_9, new ClientState(mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_2)))),
//            mkEntry(UUID_10, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_1), mkEntry(CLUSTER_TAG, CLUSTER_4)))), // 0_0 (S_3)
//            mkEntry(UUID_11, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_2), mkEntry(CLUSTER_TAG, CLUSTER_4)))), // 0_1 (S_3)
//            mkEntry(UUID_12, createClientStateWithCapacity(1, mkMap(mkEntry(ZONE_TAG, ZONE_3), mkEntry(CLUSTER_TAG, CLUSTER_4)))) // 0_1 (S_2), 0_2 (S_2)
        );
        tagValueToTagKeyMapping = mkMap(
            mkEntry(ZONE_1, ZONE_TAG),
            mkEntry(ZONE_2, ZONE_TAG),
            mkEntry(ZONE_3, ZONE_TAG),
            mkEntry(CLUSTER_1, CLUSTER_TAG),
            mkEntry(CLUSTER_2, CLUSTER_TAG)
        );

        tagValueToTagKeyFunction = tagValue -> tagValueToTagKeyMapping.get(tagValue);
        clientStateFunction = clientId -> clientStates.get(clientId);
    }

    private ClientState createClientStateWithCapacity(final int capacity, final Map<String, String> clientTags) {
        final ClientState clientState = new ClientState(clientTags);

        for (int i = 0; i < capacity; i++) {
            clientState.incrementCapacity();
        }

        return clientState;
    }

    @Test
    public void baseTest() {
//        mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2, TASK_2_0, TASK_2_1, TASK_2_2);
        final Map<TaskId, UUID> allTaskIds = mkMap(
            mkEntry(TASK_0_0, UUID_1),
            mkEntry(TASK_0_1, UUID_4),
            mkEntry(TASK_0_2, UUID_7)
//            mkEntry(TASK_1_0, UUID_4),
//            mkEntry(TASK_1_1, UUID_5),
//            mkEntry(TASK_1_2, UUID_6),
//            mkEntry(TASK_2_0, UUID_1),
//            mkEntry(TASK_2_1, UUID_2),
//            mkEntry(TASK_2_2, UUID_3)
        );

        clientStates.get(UUID_1).assignActive(TASK_0_0);
        clientStates.get(UUID_4).assignActive(TASK_0_1);
        clientStates.get(UUID_7).assignActive(TASK_0_2);

        new ClientTagAwareStandbyTaskAssignor().assignStandbyTasks(
            allTaskIds,
            new TreeMap<>(clientStates),
            2,
            "zone,cluster"
        );

        final String a = "";
    }

//    private static Map<UUID, ClientState> generateClientStates(final int numberOfZones,
//                                                               final int numberOfClusters,
//                                                               final int numberOfRegions) {
//
//    }
}