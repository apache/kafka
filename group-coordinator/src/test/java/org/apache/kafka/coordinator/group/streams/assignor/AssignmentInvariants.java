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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.Scenario;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.Topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.toTaskIds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The properties every valid assignment must satisfy, independent of the assignor: the assignment is non-null and
 * covers exactly the group members, every task id exists in the topology, every active task has exactly one owner,
 * stateless tasks have no standby task, no process holds two replicas of the same task, and every stateful task
 * has {@code min(numStandbyReplicas, processes - 1)} standby tasks.
 */
final class AssignmentInvariants {

    private AssignmentInvariants() {
    }

    static void assertValid(final Scenario scenario, final GroupAssignment result) {
        assertNotNull(result, "assignor returned null");
        assertMembersMatch(scenario, result);

        final Map<TaskId, List<String>> activeOwners = new HashMap<>();
        final Map<TaskId, List<String>> standbyOwners = new HashMap<>();
        collectOwners(scenario, result, activeOwners, standbyOwners);

        assertEachActiveTaskOwnedOnce(scenario.topology, activeOwners);
        assertStandbyCount(scenario, standbyOwners);
        assertTotals(scenario, activeOwners, standbyOwners);
    }

    /** The standby tasks a stateful task has when every process can hold at most one of its replicas. */
    static int expectedStandbys(final Scenario scenario) {
        return Math.min(scenario.numStandbyReplicas, scenario.processes.size() - 1);
    }

    private static void assertMembersMatch(final Scenario scenario, final GroupAssignment result) {
        assertEquals(scenario.memberIds(), result.members().keySet(), "assignment must cover exactly the group members");
    }

    /**
     * Fills the owner maps, asserting on the way that every task is known, that standbys are only assigned for
     * stateful tasks and that no process holds a task twice.
     */
    private static void collectOwners(
        final Scenario scenario,
        final GroupAssignment result,
        final Map<TaskId, List<String>> activeOwners,
        final Map<TaskId, List<String>> standbyOwners
    ) {
        final Topology topology = scenario.topology;
        final Map<String, Set<TaskId>> tasksPerProcess = new HashMap<>();
        for (final Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            final String memberId = entry.getKey();
            final String processId = scenario.processOf(memberId);
            final Set<TaskId> processTasks = tasksPerProcess.computeIfAbsent(processId, id -> new HashSet<>());
            for (final TaskId task : toTaskIds(entry.getValue().activeTasks())) {
                assertTrue(topology.tasks().contains(task), "unknown active task " + task + " on " + memberId);
                activeOwners.computeIfAbsent(task, t -> new ArrayList<>()).add(memberId);
                assertTrue(processTasks.add(task), "process " + processId + " holds task " + task + " twice");
            }
            for (final TaskId task : toTaskIds(entry.getValue().standbyTasks())) {
                assertTrue(topology.statefulTasks().contains(task), "standby for stateless or unknown task " + task + " on " + memberId);
                standbyOwners.computeIfAbsent(task, t -> new ArrayList<>()).add(memberId);
                assertTrue(processTasks.add(task), "process " + processId + " holds task " + task + " twice");
            }
        }
    }

    private static void assertEachActiveTaskOwnedOnce(final Topology topology, final Map<TaskId, List<String>> activeOwners) {
        for (final TaskId task : topology.tasks()) {
            final List<String> owners = activeOwners.getOrDefault(task, List.of());
            assertEquals(1, owners.size(), "active task " + task + " must have exactly one owner but has " + owners);
        }
    }

    private static void assertStandbyCount(final Scenario scenario, final Map<TaskId, List<String>> standbyOwners) {
        final int expected = expectedStandbys(scenario);
        for (final TaskId task : scenario.topology.statefulTasks()) {
            final List<String> owners = standbyOwners.getOrDefault(task, List.of());
            assertEquals(expected, owners.size(), "stateful task " + task + " must have " + expected + " standby tasks but has " + owners);
        }
    }

    private static void assertTotals(
        final Scenario scenario,
        final Map<TaskId, List<String>> activeOwners,
        final Map<TaskId, List<String>> standbyOwners
    ) {
        final int activeTasks = activeOwners.values().stream().mapToInt(List::size).sum();
        final int standbyTasks = standbyOwners.values().stream().mapToInt(List::size).sum();
        assertEquals(scenario.topology.tasks().size(), activeTasks, "active tasks assigned in total");
        assertEquals(scenario.topology.statefulTasks().size() * expectedStandbys(scenario), standbyTasks, "standby tasks assigned in total");
    }
}
