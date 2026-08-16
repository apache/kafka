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

import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;

import java.util.Map;
import java.util.Set;

/**
 * Refines the task assignor's target assignment into the <em>intermediate</em> assignment that the reconciler
 * ({@link CurrentAssignmentBuilder}) converges the members toward.
 * <p>
 * The assignor decides <em>where</em> a task belongs; the refiner decides <em>how</em> the group gets there. When a
 * stateful task has to move to a member that does not hold its state yet, handing it over right away would stall
 * processing while the new owner restores the state from the changelog. Instead, the refiner leaves the task with its
 * current owner and hands the new owner a warm-up task, so it can restore the state in the background. Once the warm-up
 * task has caught up -- its lag is within {@code acceptable.recovery.lag} -- a later refinement step moves the task
 * over. {@code num.warmup.replicas} bounds how many such warm-up tasks the group runs at a time.
 * <p>
 * Properties of the intermediate assignment that callers rely on:
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
 */
public class AssignmentRefiner {

    private AssignmentRefiner() {
    }

    /**
     * Derives the intermediate assignment for all members of the group.
     *
     * @param members               All members of the group, with their current assignments, the tasks they are still
     *                              to revoke, and their process IDs.
     * @param targetAssignment      All members' target assignments, as computed by the task assignor.
     * @param taskOffsets           The latest changelog offsets/end-offsets reported by the members via heartbeats,
     *                              from which the lag of a warm-up task is derived. Not populated for a member that
     *                              has not reported any offsets yet, for example right after a coordinator failover.
     * @param configuredTopology    The configured topology, which tells whether a subtopology is stateful. Only
     *                              stateful tasks are warmed up; a stateless task has no state to restore.
     * @param numWarmupReplicas     The maximum number of warm-up tasks the group may run at a time. Zero disables
     *                              warm-up tasks altogether, in which case the intermediate assignment is the target
     *                              assignment.
     * @param acceptableRecoveryLag The lag at or below which a warm-up task is considered caught up.
     *
     * @return The intermediate assignment, keyed by member ID.
     */
    public static Map<String, TasksTuple> refine(
        Map<String, StreamsGroupMember> members,
        Map<String, TasksTuple> targetAssignment,
        Map<String, MemberTaskOffsets> taskOffsets,
        ConfiguredTopology configuredTopology,
        int numWarmupReplicas,
        long acceptableRecoveryLag
    ) {
        // No warm-up tasks are inserted yet, so the intermediate assignment is the target assignment.
        return targetAssignment;
    }

    public static boolean preservesActiveTaskCount(
        Map<String, TasksTuple> targetAssignment,
        Map<String, TasksTuple> refinedAssignment
    ) {
        return countActiveTasks(targetAssignment) == countActiveTasks(refinedAssignment);
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
