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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

/**
 * Refines the task assignor's target assignment into the <em>intermediate</em> assignment that the reconciler
 * ({@link CurrentAssignmentBuilder}) converges the members toward.
 *
 * <p>The assignor decides <em>where</em> a task belongs; the refiner decides <em>how</em> the group gets there. When a
 * stateful task has to move to a member that does not hold its state yet, handing it over right away would stall
 * processing while the new owner restores the state from the changelog. Instead, the refiner leaves the task with its
 * current owner and hands the new owner a warm-up task, so it can restore the state in the background. Once the warm-up
 * task has caught up -- its lag is within {@code acceptable.recovery.lag} -- a later refinement step moves the task
 * over. {@code num.warmup.replicas} bounds how many such warm-up tasks the group runs at a time.
 *
 * <p>Properties of the intermediate assignment that callers rely on:
 * <ul>
 *     <li>It is <b>derived for the whole group at once</b>, because both the warm-up budget and the rule that a
 *     process must not hold a task twice are group-wide. Callers take the individual member's slice out of the
 *     result.</li>
 *     <li>It is <b>held in memory only</b> and never persisted. The assignor's target assignment remains the persisted
 *     source of truth, and the intermediate assignment is derived again from the persisted current assignments after a
 *     coordinator failover.</li>
 *     <li>It is <b>frozen for the duration of an assignment epoch</b>: every refinement step is an epoch of its own, so
 *     the decisions of a step are taken once, when the epoch is minted, and do not change while the members reconcile
 *     towards it. See {@link StreamsGroup#refinedAssignment(int)}.</li>
 * </ul>
 * <p>
 * An implementation need not be thread-safe, and holds no per-group state: the two per-group configurations a
 * refinement depends on are passed on every call.
 * <p>
 * An internal broker configuration ({@code GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_REFINER_CLASS_CONFIG})
 * selects the implementation, defaulting to {@link NoOpAssignmentRefiner}. It exists so that tests can put a warm-up
 * assignment in front of a real client while the derivation itself is still being built, and is not a public extension
 * point. An implementation that also implements {@link org.apache.kafka.common.Configurable} is handed the broker
 * configuration, so a test implementation can be steered through broker properties.
 */
public interface AssignmentRefiner {

    /**
     * Derives the intermediate assignment for all members of the group.
     *
     * @param members
     *        All members of the group, with their current assignments, the tasks they are still to revoke, and
     *        their process IDs.
     * @param targetAssignment
     *        All members' target assignments, as computed by the task assignor.
     * @param taskOffsets
     *        The latest changelog offsets/end-offsets reported by the members via heartbeats, from which the lag
     *        of a warm-up task is derived. Not populated for a member that has not reported any offsets yet, for
     *        example right after a coordinator failover.
     * @param warmupSupport
     *        Whether each member may be handed a warm-up task, keyed by member ID. A member that is missing from the
     *        map is {@link WarmupSupport#UNKNOWN}. The refiner never places a warm-up task on a member that is not
     *        {@link WarmupSupport#SUPPORTED}, except that it keeps one that a member of unknown version already holds.
     * @param subtopologies
     *        The group's resolved subtopologies, keyed by subtopology ID, which tell whether a subtopology is
     *        stateful. Only stateful tasks are warmed up; a stateless task has no state to restore. The coordinator
     *        resolves the topology and never invokes a refiner until it is ready, so an implementation does not deal
     *        with topology readiness.
     * @param numWarmupReplicas
     *        The maximum number of warm-up tasks the group may run at a time. Never zero: with warm-up tasks
     *        disabled, the intermediate assignment is the target assignment and the refiner is not called at all.
     * @param acceptableRecoveryLag
     *        The lag at or below which a warm-up task is considered caught up.
     *
     * @return The intermediate assignment, keyed by member ID. It must hand out the same active tasks as the target
     *         assignment, only possibly to different members; an implementation that drops or duplicates one is
     *         ignored in favour of the target assignment (see {@link #preservesActiveTaskCount}).
     */
    Map<String, TasksTuple> refine(
        Map<String, StreamsGroupMember> members,
        Map<String, TasksTuple> targetAssignment,
        Map<String, MemberTaskOffsets> taskOffsets,
        Map<String, WarmupSupport> warmupSupport,
        SortedMap<String, ConfiguredSubtopology> subtopologies,
        int numWarmupReplicas,
        long acceptableRecoveryLag
    );

    static boolean preservesActiveTaskCount(
        Map<String, TasksTuple> targetAssignment,
        Map<String, TasksTuple> refinedAssignment
    ) {
        return countActiveTasks(targetAssignment) == countActiveTasks(refinedAssignment);
    }

    /**
     * The members that the assignment hands a warm-up task which they may not be given: one that is not
     * {@link WarmupSupport#SUPPORTED}, unless the member is merely {@link WarmupSupport#UNKNOWN} and its process
     * already holds that warm-up task. The latter is a warm-up task that a coordinator handed out before it failed
     * over: only a member that supports warm-up tasks can have been given one, and revoking it would throw its restore
     * progress away. It may sit on another member of the process than before, since the members of a process share
     * the state of their tasks. A member that is missing from {@code warmupSupport} is {@link WarmupSupport#UNKNOWN}.
     */
    static Set<String> membersWithUnsupportedWarmupTasks(
        Map<String, TasksTuple> assignment,
        Map<String, WarmupSupport> warmupSupport,
        Map<String, StreamsGroupMember> members
    ) {
        final Map<String, Map<String, Set<Integer>>> warmupsHeldByProcess = new HashMap<>();
        members.values().forEach(member -> member.assignedTasks().warmupTasks().forEach((subtopologyId, partitions) ->
            warmupsHeldByProcess
                .computeIfAbsent(member.processId(), processId -> new HashMap<>())
                .computeIfAbsent(subtopologyId, id -> new HashSet<>())
                .addAll(partitions)));

        final Set<String> offenders = new TreeSet<>();
        assignment.forEach((memberId, tasks) -> {
            final WarmupSupport support = warmupSupport.getOrDefault(memberId, WarmupSupport.UNKNOWN);
            if (tasks.warmupTasks().isEmpty() || support == WarmupSupport.SUPPORTED) {
                return;
            }
            final StreamsGroupMember member = members.get(memberId);
            final boolean keptFromBeforeFailover = support == WarmupSupport.UNKNOWN
                && member != null
                && tasks.warmupTasks().entrySet().stream().allMatch(entry ->
                    warmupsHeldByProcess.getOrDefault(member.processId(), Map.of())
                        .getOrDefault(entry.getKey(), Set.of()).containsAll(entry.getValue()));
            if (!keptFromBeforeFailover) {
                offenders.add(memberId);
            }
        });
        return offenders;
    }

    private static int countActiveTasks(Map<String, TasksTuple> assignment) {
        int count = 0;
        for (TasksTuple tasks : assignment.values()) {
            for (Set<Integer> partitionIds : tasks.activeTasks().values()) {
                count += partitionIds.size();
            }
        }
        return count;
    }
}
