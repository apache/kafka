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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The properties every valid assignment must satisfy, independent of the assignor: the assignment covers exactly
 * the group members, every active task has exactly one owner, no task has more than {@code numStandbyReplicas}
 * standbys, stateless tasks have no standby, and no process holds two copies of the same task.
 */
final class AssignmentInvariants {

    private AssignmentInvariants() {
    }

    static void assertValid(final Scenario scenario, final GroupAssignment result) {
        assertMembersMatch(scenario, result);

        final Map<TaskId, List<String>> activeOwners = new HashMap<>();
        final Map<TaskId, List<String>> standbyOwners = new HashMap<>();
        collectOwners(scenario, result, activeOwners, standbyOwners);

        assertEachActiveTaskOwnedOnce(scenario.topology, activeOwners);
        assertStandbyBound(scenario, standbyOwners);
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

    private static void assertStandbyBound(final Scenario scenario, final Map<TaskId, List<String>> standbyOwners) {
        standbyOwners.forEach((task, owners) ->
            assertTrue(
                owners.size() <= scenario.numStandbyReplicas,
                "task " + task + " has " + owners.size() + " standbys, above the configured " + scenario.numStandbyReplicas + ": " + owners
            )
        );
    }
}
