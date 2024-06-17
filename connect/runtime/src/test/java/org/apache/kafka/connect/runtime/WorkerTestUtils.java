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

import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.runtime.distributed.ExtendedAssignment;
import org.apache.kafka.connect.runtime.distributed.ExtendedWorkerState;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WorkerTestUtils {

    public static WorkerLoad emptyWorkerLoad(String worker) {
        return new WorkerLoad.Builder(worker).build();
    }

    public WorkerLoad workerLoad(String worker, int connectorStart, int connectorNum,
                                  int taskStart, int taskNum) {
        return new WorkerLoad.Builder(worker).with(
                newConnectors(connectorStart, connectorStart + connectorNum),
                newTasks(taskStart, taskStart + taskNum)).build();
    }

    public static List<String> newConnectors(int start, int end) {
        return IntStream.range(start, end)
                .mapToObj(i -> "connector" + i)
                .collect(Collectors.toList());
    }

    public static List<ConnectorTaskId> newTasks(int start, int end) {
        return IntStream.range(start, end)
                .mapToObj(i -> new ConnectorTaskId("task", i))
                .collect(Collectors.toList());
    }

    public static ClusterConfigState clusterConfigState(long offset,
                                                        int connectorNum,
                                                        int taskNum) {
        return new ClusterConfigState(
                offset,
                null,
                connectorTaskCounts(1, connectorNum, taskNum),
                connectorConfigs(1, connectorNum),
                connectorTargetStates(1, connectorNum, TargetState.STARTED),
                taskConfigs(0, connectorNum, connectorNum * taskNum),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet());
    }

    public static Map<String, ExtendedWorkerState> memberConfigs(String givenLeader,
                                                                 long givenOffset,
                                                                 Map<String, ExtendedAssignment> givenAssignments) {
        return givenAssignments.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new ExtendedWorkerState(expectedLeaderUrl(givenLeader), givenOffset, e.getValue())));
    }

    public static Map<String, ExtendedWorkerState> memberConfigs(String givenLeader,
                                                                 long givenOffset,
                                                                 int start,
                                                                 int connectorNum) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("worker" + i, new ExtendedWorkerState(expectedLeaderUrl(givenLeader), givenOffset, null)))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
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
        assertNotNull("Assignment can't be null", assignment);

        assertEquals("Wrong status in " + assignment, expectFailed, assignment.failed());

        assertEquals("Wrong leader in " + assignment, expectedLeader, assignment.leader());

        assertEquals("Wrong leaderUrl in " + assignment, expectedLeaderUrl(expectedLeader),
                assignment.leaderUrl());

        assertEquals("Wrong offset in " + assignment, expectedOffset, assignment.offset());

        assertThat("Wrong set of assigned connectors in " + assignment,
                assignment.connectors(), is(expectedAssignedConnectors));

        assertEquals("Wrong number of assigned tasks in " + assignment,
                expectedAssignedTaskNum, assignment.tasks().size());

        assertThat("Wrong set of revoked connectors in " + assignment,
                assignment.revokedConnectors(), is(expectedRevokedConnectors));

        assertEquals("Wrong number of revoked tasks in " + assignment,
                expectedRevokedTaskNum, assignment.revokedTasks().size());

        assertEquals("Wrong rebalance delay in " + assignment, expectedDelay, assignment.delay());
    }
}
