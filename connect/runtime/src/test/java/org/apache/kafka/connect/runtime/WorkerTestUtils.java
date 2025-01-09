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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.runtime.distributed.ExtendedAssignment;
import org.apache.kafka.connect.storage.AppliedConnectorConfig;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class WorkerTestUtils {

    public static ClusterConfigState clusterConfigState(long offset,
                                                        int connectorNum,
                                                        int taskNum) {
        Map<String, Map<String, String>> connectorConfigs = connectorConfigs(1, connectorNum);
        Map<String, AppliedConnectorConfig> appliedConnectorConfigs = connectorConfigs.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new AppliedConnectorConfig(e.getValue())
                ));
        return new ClusterConfigState(
                offset,
                null,
                connectorTaskCounts(1, connectorNum, taskNum),
                connectorConfigs,
                connectorTargetStates(1, connectorNum, TargetState.STARTED),
                taskConfigs(0, connectorNum, connectorNum * taskNum),
                Collections.emptyMap(),
                Collections.emptyMap(),
                appliedConnectorConfigs,
                Collections.emptySet(),
                Collections.emptySet());
    }

    public static Map<String, Integer> connectorTaskCounts(int start,
                                                           int connectorNum,
                                                           int taskCounts) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("connector" + i, taskCounts))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    public static Map<String, Map<String, String>> connectorConfigs(int start, int connectorNum) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("connector" + i, new HashMap<String, String>()))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    public static Map<String, TargetState> connectorTargetStates(int start,
                                                                 int connectorNum,
                                                                 TargetState state) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("connector" + i, state))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    public static Map<ConnectorTaskId, Map<String, String>> taskConfigs(int start,
                                                                        int connectorNum,
                                                                        int taskNum) {
        return IntStream.range(start, taskNum + 1)
                .mapToObj(i -> new SimpleEntry<>(
                        new ConnectorTaskId("connector" + i / connectorNum + 1, i),
                        new HashMap<String, String>())
                ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    public static String expectedLeaderUrl(String givenLeader) {
        return "http://" + givenLeader + ":8083";
    }

    public static void assertAssignment(String expectedLeader,
                                        long expectedOffset,
                                        List<String> expectedAssignedConnectors,
                                        int expectedAssignedTaskNum,
                                        List<String> expectedRevokedConnectors,
                                        int expectedRevokedTaskNum,
                                        ExtendedAssignment assignment) {
        assertAssignment(false, expectedLeader, expectedOffset,
                expectedAssignedConnectors, expectedAssignedTaskNum,
                expectedRevokedConnectors, expectedRevokedTaskNum,
                0,
                assignment);
    }

    public static void assertAssignment(String expectedLeader,
                                        long expectedOffset,
                                        List<String> expectedAssignedConnectors,
                                        int expectedAssignedTaskNum,
                                        List<String> expectedRevokedConnectors,
                                        int expectedRevokedTaskNum,
                                        int expectedDelay,
                                        ExtendedAssignment assignment) {
        assertAssignment(false, expectedLeader, expectedOffset,
                expectedAssignedConnectors, expectedAssignedTaskNum,
                expectedRevokedConnectors, expectedRevokedTaskNum,
                expectedDelay,
                assignment);
    }

    public static void assertAssignment(boolean expectFailed,
                                        String expectedLeader,
                                        long expectedOffset,
                                        List<String> expectedAssignedConnectors,
                                        int expectedAssignedTaskNum,
                                        List<String> expectedRevokedConnectors,
                                        int expectedRevokedTaskNum,
                                        int expectedDelay,
                                        ExtendedAssignment assignment) {
        assertNotNull(assignment, "Assignment can't be null");

        assertEquals(expectFailed, assignment.failed(), "Wrong status in " + assignment);

        assertEquals(expectedLeader, assignment.leader(), "Wrong leader in " + assignment);

        assertEquals(expectedLeaderUrl(expectedLeader),
                assignment.leaderUrl(), "Wrong leaderUrl in " + assignment);

        assertEquals(expectedOffset, assignment.offset(), "Wrong offset in " + assignment);

        assertEquals(expectedAssignedConnectors, assignment.connectors(), "Wrong set of assigned connectors in " + assignment);

        assertEquals(expectedAssignedTaskNum, assignment.tasks().size(),
                "Wrong number of assigned tasks in " + assignment);

        assertEquals(expectedRevokedConnectors, assignment.revokedConnectors(), "Wrong set of revoked connectors in " + assignment);

        assertEquals(expectedRevokedTaskNum, assignment.revokedTasks().size(),
                "Wrong number of revoked tasks in " + assignment);

        assertEquals(expectedDelay, assignment.delay(),
                "Wrong rebalance delay in " + assignment);
    }
}
