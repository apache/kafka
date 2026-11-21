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
import java.util.function.Function;

import static org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.toTaskIds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The checks every valid assignment must pass, independent of the assignor: members match, task ids exist, one
 * active per task, no stateless standby, one replica per process, exact standby count, and no failure (a null
 * result here; the testbed fails a scenario whose assignor throws). Each assertion message starts with its check.
 */
final class AssignmentInvariants {

    private AssignmentInvariants() {
    }

    static void assertValid(final Scenario scenario, final GroupAssignment result) {
        assertNotNull(result, "No failure: assignor returned null");
        assertMembersMatch(scenario, result);
        assertTaskIdsExist(scenario.topology, result);
        assertOneActivePerTask(scenario.topology, result);
        assertNoStatelessStandby(scenario.topology, result);
        assertOneReplicaPerProcess(scenario, result);
        assertExactStandbyCount(scenario, result);
    }

    /** The standby tasks a stateful task has when every process can hold at most one of its replicas. */
    private static int expectedStandbys(final Scenario scenario) {
        return Math.min(scenario.numStandbyReplicas, scenario.processes.size() - 1);
    }

    private static void assertMembersMatch(final Scenario scenario, final GroupAssignment result) {
        assertEquals(scenario.memberIds(), result.members().keySet(), "Members match: assignment must cover exactly the group members");
    }

    private static void assertTaskIdsExist(final Topology topology, final GroupAssignment result) {
        for (final Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            for (final TaskId task : replicas(entry.getValue())) {
                assertTrue(topology.tasks().contains(task), "Task ids exist: unknown task " + task + " on " + entry.getKey());
            }
        }
    }

    private static void assertOneActivePerTask(final Topology topology, final GroupAssignment result) {
        final Map<TaskId, List<String>> activeOwners = owners(result, MemberAssignment::activeTasks);
        for (final TaskId task : topology.tasks()) {
            final List<String> owners = activeOwners.getOrDefault(task, List.of());
            assertEquals(1, owners.size(), "One active per task: active task " + task + " must have exactly one owner but has " + owners);
        }
    }

    private static void assertNoStatelessStandby(final Topology topology, final GroupAssignment result) {
        for (final Map.Entry<TaskId, List<String>> entry : owners(result, MemberAssignment::standbyTasks).entrySet()) {
            assertTrue(topology.statefulTasks().contains(entry.getKey()), "No stateless standby: stateless task " + entry.getKey() + " has standby tasks on " + entry.getValue());
        }
    }

    /** A member holding a task both as active and as standby task counts as two replicas. */
    private static void assertOneReplicaPerProcess(final Scenario scenario, final GroupAssignment result) {
        final Map<String, Set<TaskId>> replicasPerProcess = new HashMap<>();
        for (final Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            final String processId = scenario.processOf(entry.getKey());
            final Set<TaskId> processReplicas = replicasPerProcess.computeIfAbsent(processId, id -> new HashSet<>());
            final List<TaskId> memberReplicas = new ArrayList<>(toTaskIds(entry.getValue().activeTasks()));
            memberReplicas.addAll(toTaskIds(entry.getValue().standbyTasks()));
            for (final TaskId task : memberReplicas) {
                assertTrue(processReplicas.add(task), "One replica per process: process " + processId + " holds task " + task + " twice");
            }
        }
    }

    private static void assertExactStandbyCount(final Scenario scenario, final GroupAssignment result) {
        final int expected = expectedStandbys(scenario);
        final Map<TaskId, List<String>> standbyOwners = owners(result, MemberAssignment::standbyTasks);
        for (final TaskId task : scenario.topology.statefulTasks()) {
            final List<String> owners = standbyOwners.getOrDefault(task, List.of());
            assertEquals(expected, owners.size(), "Exact standby count: stateful task " + task + " must have " + expected + " standby tasks but has " + owners);
        }
    }

    /** The members holding each task in the given role. */
    private static Map<TaskId, List<String>> owners(final GroupAssignment result, final Function<MemberAssignment, Map<String, Set<Integer>>> role) {
        final Map<TaskId, List<String>> owners = new HashMap<>();
        for (final Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            for (final TaskId task : toTaskIds(role.apply(entry.getValue()))) {
                owners.computeIfAbsent(task, t -> new ArrayList<>()).add(entry.getKey());
            }
        }
        return owners;
    }

    /** The member's active and standby tasks together. */
    private static Set<TaskId> replicas(final MemberAssignment assignment) {
        final Set<TaskId> tasks = toTaskIds(assignment.activeTasks());
        tasks.addAll(toTaskIds(assignment.standbyTasks()));
        return tasks;
    }
}
