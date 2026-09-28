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
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.Profile;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.Scenario;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;

import static org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.toTaskIds;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs {@link StickyTaskAssignor} through the {@link TaskAssignorTestbed} with the check and metrics specific to it.
 * The check: no member exceeds the stateful or total active task quota. The metrics: of the tasks that could have
 * stayed on their previous owner, the fraction that did, which is what the sticky assignor promises.
 */
public class StickyTaskAssignorFuzzTest {

    private final TaskAssignorTestbed testbed = new TaskAssignorTestbed(
        new StickyTaskAssignor(),
        List.of(this::verifyActiveQuota),
        List.of(StickyTaskAssignorFuzzTest::stickiness)
    );

    @Test
    public void shouldConvergeToValidAssignmentsForSmallGroupsUnderRandomChanges() {
        testbed.run(Profile.SMALL);
    }

    @Test
    public void shouldConvergeToValidAssignmentsForLargeGroupsUnderRandomChanges() {
        testbed.run(Profile.LARGE);
    }

    /**
     * Stateful active tasks are assigned first and the fill phase always picks the least loaded process, so no member
     * ends above {@code ceil(stateful tasks / members)} stateful active tasks, nor above {@code ceil(tasks / members)}
     * active tasks in total. Stateless tasks have no quota of their own: they fill each member up to the total one.
     */
    private void verifyActiveQuota(final Scenario scenario, final GroupAssignment result, final boolean last) {
        final int members = scenario.memberIds().size();
        final int statefulQuota = (scenario.topology.statefulTasks().size() + members - 1) / members;
        final int quota = (scenario.topology.tasks().size() + members - 1) / members;
        for (final Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            final Set<TaskId> active = toTaskIds(entry.getValue().activeTasks());
            int stateful = 0;
            for (final TaskId task : active) {
                if (scenario.topology.statefulTasks().contains(task)) {
                    stateful++;
                }
            }
            assertTrue(stateful <= statefulQuota, entry.getKey() + " holds " + stateful + " stateful active tasks, above the quota of " + statefulQuota);
            assertTrue(active.size() <= quota, entry.getKey() + " holds " + active.size() + " active tasks, above the quota of " + quota);
        }
    }

    /**
     * Of the tasks whose previous owner is still in the group, the fraction the sticky assignor kept there. Nothing
     * is graded on the initial assignment from empty.
     */
    private static Map<String, OptionalDouble> stickiness(
        final Scenario scenario,
        final GroupAssignment result,
        final Scenario.Baseline before,
        final Map<String, Set<TaskId>> reportedTasks
    ) {
        final Map<TaskId, String> activeMemberOf = new HashMap<>();
        final Map<TaskId, String> activeProcessOf = new HashMap<>();
        final Map<TaskId, Set<String>> ownerProcessesOf = new HashMap<>();
        final Map<TaskId, Set<String>> ownerMembersOf = new HashMap<>();
        for (final Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            final String memberId = entry.getKey();
            final String processId = scenario.processOf(memberId);
            for (final TaskId task : toTaskIds(entry.getValue().activeTasks())) {
                activeMemberOf.put(task, memberId);
                activeProcessOf.put(task, processId);
                ownerProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(processId);
                ownerMembersOf.computeIfAbsent(task, t -> new HashSet<>()).add(memberId);
            }
            for (final TaskId task : toTaskIds(entry.getValue().standbyTasks())) {
                ownerProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(processId);
                ownerMembersOf.computeIfAbsent(task, t -> new HashSet<>()).add(memberId);
            }
        }

        final ActiveRetention activeRetention = activeRetention(scenario, before, activeMemberOf, activeProcessOf);
        final StandbyRetention standbyRetention = standbyRetention(scenario, before, ownerProcessesOf, ownerMembersOf);
        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        rows.put("stateful active tasks kept on process (1.0 = all)", activeRetention.statefulOnProcess());
        rows.put("stateful active tasks kept on member (1.0 = all)", activeRetention.statefulOnMember());
        rows.put("stateless active tasks kept on member (1.0 = all)", activeRetention.statelessOnMember());
        rows.put("standby tasks kept on process (1.0 = all)", standbyRetention.onProcess());
        rows.put("standby tasks kept on member (1.0 = all)", standbyRetention.onMember());
        return rows;
    }

    private record ActiveRetention(OptionalDouble statefulOnMember, OptionalDouble statefulOnProcess, OptionalDouble statelessOnMember) {
    }

    /**
     * Of the active tasks held before the rebalance by a member or process still in the group, the fraction that
     * member or process still holds as active. Tasks no longer in the topology are left out. Nothing is graded on
     * the initial assignment from empty or when no previous active owner is still in the group.
     */
    private static ActiveRetention activeRetention(
        final Scenario scenario,
        final Scenario.Baseline before,
        final Map<TaskId, String> activeMemberOf,
        final Map<TaskId, String> activeProcessOf
    ) {
        int statefulMemberHeld = 0;
        int statefulMemberKept = 0;
        int statefulProcessHeld = 0;
        int statefulProcessKept = 0;
        int statelessMemberHeld = 0;
        int statelessMemberKept = 0;
        for (final Map.Entry<String, MemberAssignment> entry : before.assignment().entrySet()) {
            final String previousMember = entry.getKey();
            final String previousProcess = before.processOfMember().get(previousMember);
            final boolean memberInGroup = scenario.memberIds().contains(previousMember);
            final boolean processInGroup = scenario.processes.containsKey(previousProcess);
            for (final TaskId task : toTaskIds(entry.getValue().activeTasks())) {
                if (!scenario.topology.tasks().contains(task)) {
                    continue;
                }
                final boolean memberKept = previousMember.equals(activeMemberOf.get(task));
                if (scenario.topology.statefulTasks().contains(task)) {
                    if (memberInGroup) {
                        statefulMemberHeld++;
                        statefulMemberKept += memberKept ? 1 : 0;
                    }
                    if (processInGroup) {
                        statefulProcessHeld++;
                        statefulProcessKept += previousProcess.equals(activeProcessOf.get(task)) ? 1 : 0;
                    }
                } else if (memberInGroup) {
                    statelessMemberHeld++;
                    statelessMemberKept += memberKept ? 1 : 0;
                }
            }
        }
        return new ActiveRetention(
            AssignmentMetrics.fraction(statefulMemberKept, statefulMemberHeld),
            AssignmentMetrics.fraction(statefulProcessKept, statefulProcessHeld),
            AssignmentMetrics.fraction(statelessMemberKept, statelessMemberHeld)
        );
    }

    private record StandbyRetention(OptionalDouble onMember, OptionalDouble onProcess) {
    }

    /**
     * Of the standby tasks held before the rebalance by a member or process still in the group, the fraction that
     * member or process still holds, as standby or promoted to active. A stateful task has at most its replicas,
     * {@code 1 + min(numStandbyReplicas, processes - 1)}, minus the one its previous active process still holds, so
     * previous standby holders beyond that are not graded: fewer standbys configured or too few processes left.
     * Nothing is graded on the initial assignment from empty or when no previous standby holder is still in the group.
     */
    private static StandbyRetention standbyRetention(
        final Scenario scenario,
        final Scenario.Baseline before,
        final Map<TaskId, Set<String>> ownerProcessesOf,
        final Map<TaskId, Set<String>> ownerMembersOf
    ) {
        final Map<TaskId, String> previousActiveProcessOf = new HashMap<>();
        final Map<TaskId, Set<String>> previousStandbyMembersOf = new HashMap<>();
        for (final Map.Entry<String, MemberAssignment> entry : before.assignment().entrySet()) {
            final String previousProcess = before.processOfMember().get(entry.getKey());
            for (final TaskId task : toTaskIds(entry.getValue().activeTasks())) {
                previousActiveProcessOf.put(task, previousProcess);
            }
            for (final TaskId task : toTaskIds(entry.getValue().standbyTasks())) {
                previousStandbyMembersOf.computeIfAbsent(task, t -> new HashSet<>()).add(entry.getKey());
            }
        }

        final int replicas = 1 + AssignmentInvariants.expectedStandbys(scenario);
        int memberHeld = 0;
        int memberKept = 0;
        int processHeld = 0;
        int processKept = 0;
        for (final Map.Entry<TaskId, Set<String>> entry : previousStandbyMembersOf.entrySet()) {
            final TaskId task = entry.getKey();
            if (!scenario.topology.statefulTasks().contains(task)) {
                continue;
            }
            final Set<String> owners = ownerProcessesOf.getOrDefault(task, Set.of());
            final Set<String> ownerMembers = ownerMembersOf.getOrDefault(task, Set.of());
            final int keepable = owners.contains(previousActiveProcessOf.get(task)) ? replicas - 1 : replicas;
            int membersInGroup = 0;
            int processesInGroup = 0;
            for (final String member : entry.getValue()) {
                final String process = before.processOfMember().get(member);
                if (scenario.memberIds().contains(member)) {
                    membersInGroup++;
                    if (ownerMembers.contains(member)) {
                        memberKept++;
                    }
                }
                if (scenario.processes.containsKey(process)) {
                    processesInGroup++;
                    if (owners.contains(process)) {
                        processKept++;
                    }
                }
            }
            memberHeld += Math.min(membersInGroup, keepable);
            processHeld += Math.min(processesInGroup, keepable);
        }
        return new StandbyRetention(AssignmentMetrics.fraction(memberKept, memberHeld), AssignmentMetrics.fraction(processKept, processHeld));
    }
}
