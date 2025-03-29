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

package org.apache.kafka.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.metadata.placement.UsableBroker;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MinimalMovementReplicaBalancerTest {

    private static List<UsableBroker> evenRackDistributionUsableBroker = Arrays.asList(
        new UsableBroker(1, Optional.of("rack1"), false),
        new UsableBroker(2, Optional.of("rack2"), false),
        new UsableBroker(3, Optional.of("rack3"), false),
        new UsableBroker(4, Optional.of("rack1"), false),
        new UsableBroker(5, Optional.of("rack2"), false),
        new UsableBroker(6, Optional.of("rack3"), false)
    );

    private static List<UsableBroker> unevenRackDistributionUsableBroker = Arrays.asList(
        new UsableBroker(1, Optional.of("rack1"), false),
        new UsableBroker(2, Optional.of("rack2"), false),
        new UsableBroker(3, Optional.of("rack3"), false),
        new UsableBroker(4, Optional.of("rack1"), false),
        new UsableBroker(5, Optional.of("rack2"), false),
        new UsableBroker(6, Optional.of("rack3"), false),
        new UsableBroker(7, Optional.of("rack1"), false)
    );

    private static List<UsableBroker> highWeightRackDistributionUsableBroker = Arrays.asList(
        new UsableBroker(1, Optional.of("rack1"), false),
        new UsableBroker(2, Optional.of("rack2"), false),
        new UsableBroker(3, Optional.of("rack3"), false),
        new UsableBroker(4, Optional.of("rack1"), false),
        new UsableBroker(5, Optional.of("rack1"), false),
        new UsableBroker(6, Optional.of("rack1"), false),
        new UsableBroker(7, Optional.of("rack4"), false)
    );

    private static List<UsableBroker> fourRackDistributionUsableBroker = Arrays.asList(
        new UsableBroker(1, Optional.of("rack1"), false),
        new UsableBroker(2, Optional.of("rack2"), false),
        new UsableBroker(3, Optional.of("rack3"), false),
        new UsableBroker(4, Optional.of("rack4"), false)

    );

    public static Map<Integer, List<Integer>> evenDistributionAssignment = Map.of(
        0, List.of(1, 2, 3),
        1, List.of(2, 3, 1),
        2, List.of(3, 1, 2),
        3, List.of(1, 2, 3),
        4, List.of(2, 3, 1),
        5, List.of(3, 1, 2)
    );

    private static Map<Integer, List<Integer>> randomDistributionAssignment = Map.of(
        0, List.of(1, 4, 3),
        1, List.of(2, 5, 3),
        2, List.of(3, 2, 1),
        3, List.of(4, 2, 5),
        4, List.of(5, 3, 4),
        5, List.of(5, 1, 4)
    );

    private static Map<Integer, List<Integer>> rackConflictAssignment = Map.of(
        0, List.of(1, 2),
        1, List.of(5, 1),
        2, List.of(5, 1),
        3, List.of(1, 2),
        4, List.of(6, 1),
        5, List.of(1, 6)
    );


    private static Map<Integer, List<Integer>> twoConflictAssignment = Map.of(
        0, List.of(1, 2),
        1, List.of(2, 3),
        2, List.of(1, 2),
        3, List.of(2, 3),
        4, List.of(3, 1),
        5, List.of(3, 1)
    );

    private static Map<Integer, List<Integer>> threeReplicationFactorAssignment = Map.of(
        0, List.of(1, 2, 3),
        1, List.of(1, 2, 3),
        2, List.of(1, 2, 3),
        3, List.of(1, 2, 3),
        4, List.of(1, 2, 3),
        5, List.of(1, 2, 3),
        6, List.of(1, 2, 3),
        7, List.of(1, 2, 3),
        8, List.of(1, 2, 3)
    );

    private static Map<Integer, List<Integer>> oneReplicaAssignment = Map.of(
        0, List.of(1),
        1, List.of(2),
        2, List.of(3),
        3, List.of(4),
        4, List.of(1),
        5, List.of(2),
        6, List.of(3),
        7, List.of(4),
        8, List.of(1),
        9, List.of(1)
    );

    private static Map<Integer, List<Integer>> replicaAssignment = Map.of(
        0, List.of(0, 1, 2),
        1, List.of(1, 2, 3),
        2, List.of(2, 3, 4),
        3, List.of(3, 4, 0),
        4, List.of(4, 0, 1),
        5, List.of(0, 2, 3),
        6, List.of(1, 3, 4),
        7, List.of(2, 4, 0),
        8, List.of(3, 0, 1),
        9, List.of(4, 1, 2)
    );

    @Test
    public void evenRackDistributionBrokerExtendTest() {
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(evenDistributionAssignment, Arrays.asList(1, 2, 3, 4, 5, 6), evenRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = Map.of(
            0, List.of(4, 5, 6),
            1, List.of(5, 4, 6),
            2, List.of(6, 4, 5),
            3, List.of(1, 2, 3),
            4, List.of(2, 1, 3),
            5, List.of(3, 1, 2)
        );
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    @Test
    public void unevenRackDistributionBrokerExtendTest() {
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(evenDistributionAssignment, Arrays.asList(1, 2, 3, 4, 5, 6, 7), unevenRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = Map.of(
            0, List.of(4, 5, 6),
            1, List.of(5, 4, 6),
            2, List.of(6, 7, 5),
            3, List.of(7, 2, 3),
            4, List.of(2, 1, 3),
            5, List.of(3, 1, 2)
        );
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    @Test
    public void highWeightRackDistributionBrokerExtendTest() {
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(evenDistributionAssignment, Arrays.asList(1, 2, 3, 4, 5, 6, 7), highWeightRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = Map.of(
            0, List.of(4, 7, 3),
            1, List.of(7, 4, 3),
            2, List.of(5, 7, 2),
            3, List.of(6, 2, 7),
            4, List.of(2, 3, 1),
            5, List.of(3, 2, 1)
        );
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }


    @Test
    public void brokerScaleInTest() {
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(randomDistributionAssignment, Arrays.asList(1, 2, 3), evenRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = Map.of(
            0, List.of(1, 2, 3),
            1, List.of(2, 1, 3),
            2, List.of(3, 2, 1),
            3, List.of(1, 2, 3),
            4, List.of(2, 3, 1),
            5, List.of(3, 2, 1)
        );
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    @Test
    public void highWeightRackDistributionBrokerExtendRandomTest() {
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(randomDistributionAssignment, Arrays.asList(1, 2, 3, 4, 5, 6), highWeightRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = Map.of(
            0, List.of(4, 2, 3),
            1, List.of(2, 4, 3),
            2, List.of(6, 3, 2),
            3, List.of(5, 3, 2),
            4, List.of(3, 5, 2),
            5, List.of(1, 3, 2)
        );
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    @Test
    public void towReplicationFactorTest() {
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(rackConflictAssignment, Arrays.asList(1, 2, 3, 4, 5, 6), highWeightRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = Map.of(
            0, List.of(1, 2),
            1, List.of(5, 2),
            2, List.of(3, 5),
            3, List.of(2, 1),
            4, List.of(4, 3),
            5, List.of(6, 3)
        );
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    @Test
    public void towReplicationFactorIsrOrderTest() {
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(twoConflictAssignment, Arrays.asList(1, 2, 3), evenRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = Map.of(
            0, List.of(1, 2),
            1, List.of(2, 3),
            2, List.of(2, 1),
            3, List.of(3, 2),
            4, List.of(3, 1),
            5, List.of(1, 3)
        );
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    @Test
    public void threeReplicationFactorIsrOrderTest() {
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(threeReplicationFactorAssignment, Arrays.asList(1, 2, 3), evenRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = Map.of(
            0, List.of(1, 2, 3),
            1, List.of(2, 1, 3),
            2, List.of(3, 1, 2),
            3, List.of(1, 3, 2),
            4, List.of(2, 1, 3),
            5, List.of(3, 1, 2),
            6, List.of(1, 2, 3),
            7, List.of(2, 1, 3),
            8, List.of(3, 2, 1)
        );
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    @Test
    public void fourReplicationFactorBrokerScaleInIsrOrderTest() {
        Map<Integer, List<Integer>> fourConflictAssignment = new HashMap<>();
        fourConflictAssignment.put(0, List.of(1, 2, 6, 4));
        fourConflictAssignment.put(1, List.of(3, 9, 7, 5));
        fourConflictAssignment.put(2, List.of(5, 2, 6, 4));
        fourConflictAssignment.put(3, List.of(1, 5, 3, 4));
        fourConflictAssignment.put(4, List.of(5, 2, 3, 8));
        fourConflictAssignment.put(5, List.of(6, 2, 8, 10));
        fourConflictAssignment.put(6, List.of(7, 2, 9, 4));
        fourConflictAssignment.put(7, List.of(9, 2, 11, 4));
        fourConflictAssignment.put(8, List.of(1, 2, 8, 4));
        fourConflictAssignment.put(9, List.of(6, 2, 3, 5));
        fourConflictAssignment.put(10, List.of(10, 2, 3, 4));
        fourConflictAssignment.put(11, List.of(1, 8, 3, 9));
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(fourConflictAssignment, Arrays.asList(1, 2, 3, 4), fourRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = new HashMap<>();
        expectAssignment.put(0, List.of(1, 2, 3, 4));
        expectAssignment.put(1, List.of(3, 2, 4, 1));
        expectAssignment.put(2, List.of(2, 1, 3, 4));
        expectAssignment.put(3, List.of(4, 1, 2, 3));
        expectAssignment.put(4, List.of(1, 3, 2, 4));
        expectAssignment.put(5, List.of(2, 3, 1, 4));
        expectAssignment.put(6, List.of(3, 1, 2, 4));
        expectAssignment.put(7, List.of(4, 2, 1, 3));
        expectAssignment.put(8, List.of(1, 4, 2, 3));
        expectAssignment.put(9, List.of(4, 3, 2, 1));
        expectAssignment.put(10, List.of(2, 4, 3, 1));
        expectAssignment.put(11, List.of(3, 4, 1, 2));
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    @Test
    public void oneReplicationFactorExtendTest() {
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(oneReplicaAssignment, Arrays.asList(1, 2, 3, 4, 5), evenRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = Map.of(
            0, List.of(5),
            1, List.of(2),
            2, List.of(3),
            3, List.of(4),
            4, List.of(5),
            5, List.of(2),
            6, List.of(3),
            7, List.of(4),
            8, List.of(1),
            9, List.of(1)
        );
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    @Test
    public void replaceBrokerTest() {
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(replicaAssignment, Arrays.asList(1, 2, 3, 4), evenRackDistributionUsableBroker, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = Map.of(
            0, List.of(1, 3, 2),
            1, List.of(2, 1, 3),
            2, List.of(3, 1, 2),
            3, List.of(4, 3, 2),
            4, List.of(4, 2, 3),
            5, List.of(1, 2, 3),
            6, List.of(2, 4, 3),
            7, List.of(3, 4, 2),
            8, List.of(1, 3, 2),
            9, List.of(4, 3, 2)
        );
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    @Test
    public void fiveReplicaAssignmentTest() {
        Map<Integer, List<Integer>> fiveReplicaAssignment = new HashMap<>();
        fiveReplicaAssignment.put(0, List.of(1, 2, 3, 4, 5));
        fiveReplicaAssignment.put(1, List.of(1, 2, 5, 6, 7));
        fiveReplicaAssignment.put(2, List.of(3, 4, 5, 6, 7));
        fiveReplicaAssignment.put(3, List.of(1, 3, 5, 6, 7));
        fiveReplicaAssignment.put(4, List.of(4, 2, 5, 6, 7));
        fiveReplicaAssignment.put(5, List.of(1, 2, 3, 4, 6));
        fiveReplicaAssignment.put(6, List.of(1, 2, 3, 4, 8));
        fiveReplicaAssignment.put(7, List.of(0, 2, 1, 4, 9));
        fiveReplicaAssignment.put(8, List.of(0, 2, 1, 4, 9));
        fiveReplicaAssignment.put(9, List.of(0, 2, 6, 4, 10));
        fiveReplicaAssignment.put(10, List.of(0, 2, 6, 5, 10));
        fiveReplicaAssignment.put(11, List.of(11, 12, 13, 14, 15));
        fiveReplicaAssignment.put(12, List.of(15, 16, 17, 0, 14));
        fiveReplicaAssignment.put(13, List.of(0, 2, 6, 13, 11));
        List<UsableBroker> usableBrokers = new ArrayList<>();
        usableBrokers.add(new UsableBroker(1, Optional.of("rack1"), false));
        usableBrokers.add(new UsableBroker(2, Optional.of("rack2"), false));
        usableBrokers.add(new UsableBroker(3, Optional.of("rack1"), false));
        usableBrokers.add(new UsableBroker(4, Optional.of("rack2"), false));
        usableBrokers.add(new UsableBroker(5, Optional.of("rack3"), false));
        usableBrokers.add(new UsableBroker(6, Optional.of("rack4"), false));
        usableBrokers.add(new UsableBroker(7, Optional.of("rack5"), false));
        MinimalMovementReplicaBalancer minimalMovementReplicaBalancer = new MinimalMovementReplicaBalancer(fiveReplicaAssignment, Arrays.asList(1, 2, 3, 4, 5, 6, 7), usableBrokers, true);
        Map<Integer, List<Integer>> newAssignment = minimalMovementReplicaBalancer.assignReplicasToBrokers();
        Map<Integer, List<Integer>> expectAssignment = new HashMap<>();
        expectAssignment.put(0, List.of(3, 7, 6, 2, 5));
        expectAssignment.put(1, List.of(2, 1, 5, 6, 7));
        expectAssignment.put(2, List.of(4, 3, 5, 6, 7));
        expectAssignment.put(3, List.of(5, 2, 3, 6, 7));
        expectAssignment.put(4, List.of(6, 4, 5, 3, 7));
        expectAssignment.put(5, List.of(7, 5, 4, 3, 6));
        expectAssignment.put(6, List.of(3, 6, 7, 4, 5));
        expectAssignment.put(7, List.of(1, 6, 5, 4, 7));
        expectAssignment.put(8, List.of(4, 6, 1, 7, 5));
        expectAssignment.put(9, List.of(5, 3, 4, 6, 7));
        expectAssignment.put(10, List.of(1, 2, 5, 7, 6));
        expectAssignment.put(11, List.of(7, 1, 2, 6, 5));
        expectAssignment.put(12, List.of(6, 5, 7, 2, 1));
        expectAssignment.put(13, List.of(2, 6, 1, 7, 5));
        assertTrue(areAssignmentsEqual(newAssignment, expectAssignment));
    }

    private boolean areAssignmentsEqual(Map<Integer, List<Integer>> map1, Map<Integer, List<Integer>> map2) {
        if (!map1.keySet().equals(map2.keySet())) {
            return false;
        }
        for (Integer key : map1.keySet()) {
            List<Integer> list1 = map1.get(key);
            List<Integer> list2 = map2.get(key);
            if (!Objects.equals(list1, list2)) {
                return false;
            }
        }
        return true;
    }

}
