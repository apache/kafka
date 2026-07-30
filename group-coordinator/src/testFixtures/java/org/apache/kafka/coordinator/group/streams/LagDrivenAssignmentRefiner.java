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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * An {@link AssignmentRefiner} that holds a stateful task back with its current owner until the member the task is
 * moving to has restored it as a warm-up task. Intended for testing and as a starting point for a real refiner; it is
 * deliberately simple and is <b>not</b> the derivation the broker will ship.
 * <p>
 * For every stateful task whose target owner is not its current owner, it withholds the task from the target owner --
 * leaving it active where it already runs -- and hands the target owner a warm-up task instead, so it can restore the
 * state in the background. Once that member reports a lag within {@code acceptableRecoveryLag} for the task, the
 * withholding stops and the target assignment flows through, at which point the reconciler revokes the task from the
 * old owner and promotes the warm-up in place. {@code numWarmupReplicas} caps how many warm-up tasks it hands out at a
 * time; a migration that does not get a slot is still held back, it just runs without anybody warming it up.
 * <p>
 * Three cases are deliberately let through without warming up, because there is no state to restore first:
 * <ul>
 *     <li>a stateless task, which has no state,</li>
 *     <li>a task moving between two members of the same process, whose state directory is already local, and</li>
 *     <li>a task whose old owner was already told to revoke it, which means the hand-over is under way and reversing it
 *     would strand the task with nobody running it.</li>
 * </ul>
 * <p>
 * What it does <em>not</em> do, and a real refiner has to: rank migrations by how loaded their source and destination
 * are (this one orders them by task ID, so the warm-up budget is handed out in an arbitrary but stable order),
 * carry warm-up slots over from the previous step rather than re-deriving them, convert an existing standby into a
 * warm-up instead of spending a slot on a new one, and defer standbys that cannot be placed yet instead of dropping
 * them. It also assumes the assignor is sticky enough not to keep re-targeting a task while it is being warmed up.
 */
public class LagDrivenAssignmentRefiner implements AssignmentRefiner {

    @Override
    public Map<String, TasksTuple> refine(
        Map<String, StreamsGroupMember> members,
        Map<String, TasksTuple> targetAssignment,
        Map<String, MemberTaskOffsets> taskOffsets,
        SortedMap<String, ConfiguredSubtopology> subtopologies,
        int numWarmupReplicas,
        long acceptableRecoveryLag
    ) {
        final Map<Task, String> currentActiveOwner = new HashMap<>();
        final Set<Task> handOverStarted = new HashSet<>();
        for (StreamsGroupMember member : members.values()) {
            member.assignedTasks().activeTasksWithEpochs().forEach((subtopologyId, tasksByPartition) ->
                tasksByPartition.keySet().forEach(partitionId ->
                    currentActiveOwner.put(new Task(subtopologyId, partitionId), member.memberId())));
            // The old owner has been told to give the task up, so the target owner is only waiting for it to be
            // released. Withholding the task again now would take it away from a member that has already stopped
            // running it.
            member.tasksPendingRevocation().activeTasksWithEpochs().forEach((subtopologyId, tasksByPartition) ->
                tasksByPartition.keySet().forEach(partitionId ->
                    handOverStarted.add(new Task(subtopologyId, partitionId))));
        }

        final Map<Task, String> targetActiveOwner = new TreeMap<>();
        targetAssignment.forEach((memberId, tasks) ->
            tasks.activeTasks().forEach((subtopologyId, partitionIds) ->
                partitionIds.forEach(partitionId ->
                    targetActiveOwner.put(new Task(subtopologyId, partitionId), memberId))));

        final List<Task> withheld = new ArrayList<>();
        for (Map.Entry<Task, String> entry : targetActiveOwner.entrySet()) {
            final Task task = entry.getKey();
            final String targetOwner = entry.getValue();
            final String currentOwner = currentActiveOwner.get(task);

            if (currentOwner == null || currentOwner.equals(targetOwner)) {
                // Nobody is running the task yet, or it is already where it belongs.
                continue;
            }
            if (!isStateful(subtopologies, task) || handOverStarted.contains(task)) {
                continue;
            }
            if (processOf(members, currentOwner).equals(processOf(members, targetOwner))) {
                // The state directory is on the process already, so there is nothing to restore over the network.
                continue;
            }
            if (isCaughtUp(taskOffsets.get(targetOwner), task, acceptableRecoveryLag)) {
                continue;
            }
            withheld.add(task);
        }

        if (withheld.isEmpty()) {
            return targetAssignment;
        }

        final Map<String, PatchedTasks> patches = new HashMap<>();
        int warmupsHandedOut = 0;
        for (Task task : withheld) {
            final String targetOwner = targetActiveOwner.get(task);
            final String currentOwner = currentActiveOwner.get(task);

            patchFor(patches, targetAssignment, targetOwner).removeActive(task);
            patchFor(patches, targetAssignment, currentOwner).addActive(task);

            if (warmupsHandedOut < numWarmupReplicas) {
                patchFor(patches, targetAssignment, targetOwner).addWarmup(task);
                warmupsHandedOut++;
            }

            // A process must never run the same task twice, so any standby the target assignment placed on either of
            // the two processes involved has to give way to the roles we just handed out. Dropping a standby is always
            // safe -- a later step can place it again once the migration is done.
            final Set<String> processes = Set.of(processOf(members, currentOwner), processOf(members, targetOwner));
            members.values().stream()
                .filter(member -> processes.contains(member.processId()))
                .forEach(member -> patchFor(patches, targetAssignment, member.memberId()).removeStandby(task));
        }

        final Map<String, TasksTuple> refinedAssignment = new HashMap<>(targetAssignment);
        patches.forEach((memberId, patch) -> refinedAssignment.put(memberId, patch.toTasksTuple()));
        return refinedAssignment;
    }

    private static boolean isStateful(
        final SortedMap<String, ConfiguredSubtopology> subtopologies,
        final Task task
    ) {
        final ConfiguredSubtopology subtopology = subtopologies.get(task.subtopologyId());
        return subtopology != null && !subtopology.stateChangelogTopics().isEmpty();
    }

    private static String processOf(final Map<String, StreamsGroupMember> members, final String memberId) {
        final StreamsGroupMember member = members.get(memberId);
        // A member that the target assignment mentions but the group does not know cannot share a process with anyone,
        // so give it one nothing else matches.
        return member == null ? memberId : member.processId();
    }

    /**
     * Whether the member has restored the task closely enough to take it over as an active task. Mirrors the client's
     * own predicate, so that both ends agree on when a warm-up task is caught up: an offset that is unknown or capped
     * at {@link Long#MAX_VALUE} counts as not caught up.
     */
    private static boolean isCaughtUp(
        final MemberTaskOffsets memberTaskOffsets,
        final Task task,
        final long acceptableRecoveryLag
    ) {
        if (memberTaskOffsets == null) {
            return false;
        }
        final Long offset = offsetOf(memberTaskOffsets.taskOffsets(), task);
        final Long endOffset = offsetOf(memberTaskOffsets.taskEndOffsets(), task);
        if (offset == null || offset == Long.MAX_VALUE || endOffset == null || endOffset == Long.MAX_VALUE) {
            return false;
        }
        return endOffset - offset <= acceptableRecoveryLag;
    }

    private static Long offsetOf(final Map<String, Map<Integer, Long>> offsets, final Task task) {
        final Map<Integer, Long> byPartition = offsets.get(task.subtopologyId());
        return byPartition == null ? null : byPartition.get(task.partitionId());
    }

    private static PatchedTasks patchFor(
        final Map<String, PatchedTasks> patches,
        final Map<String, TasksTuple> targetAssignment,
        final String memberId
    ) {
        return patches.computeIfAbsent(
            memberId,
            __ -> new PatchedTasks(targetAssignment.getOrDefault(memberId, TasksTuple.EMPTY))
        );
    }

    private record Task(String subtopologyId, int partitionId) implements Comparable<Task> {
        @Override
        public int compareTo(final Task other) {
            final int bySubtopology = subtopologyId.compareTo(other.subtopologyId);
            return bySubtopology != 0 ? bySubtopology : Integer.compare(partitionId, other.partitionId);
        }
    }

    /**
     * A member's slice of the target assignment, made mutable so that a refinement step can patch it. Empty subtopology
     * entries are pruned as tasks are removed, so that a patched slice that ends up holding the same tasks as the
     * target assignment also compares equal to it -- an equal-but-differently-shaped map would read as a change and
     * make the coordinator mint a refinement step on every heartbeat.
     */
    private static final class PatchedTasks {
        private final Map<String, Set<Integer>> activeTasks;
        private final Map<String, Set<Integer>> standbyTasks;
        private final Map<String, Set<Integer>> warmupTasks;

        private PatchedTasks(final TasksTuple tasks) {
            this.activeTasks = mutableCopy(tasks.activeTasks());
            this.standbyTasks = mutableCopy(tasks.standbyTasks());
            this.warmupTasks = mutableCopy(tasks.warmupTasks());
        }

        private static Map<String, Set<Integer>> mutableCopy(final Map<String, Set<Integer>> tasks) {
            final Map<String, Set<Integer>> copy = new HashMap<>();
            tasks.forEach((subtopologyId, partitionIds) -> copy.put(subtopologyId, new HashSet<>(partitionIds)));
            return copy;
        }

        private void addActive(final Task task) {
            add(activeTasks, task);
        }

        private void removeActive(final Task task) {
            remove(activeTasks, task);
        }

        private void addWarmup(final Task task) {
            add(warmupTasks, task);
        }

        private void removeStandby(final Task task) {
            remove(standbyTasks, task);
        }

        private static void add(final Map<String, Set<Integer>> tasks, final Task task) {
            tasks.computeIfAbsent(task.subtopologyId(), __ -> new HashSet<>()).add(task.partitionId());
        }

        private static void remove(final Map<String, Set<Integer>> tasks, final Task task) {
            final Set<Integer> partitionIds = tasks.get(task.subtopologyId());
            if (partitionIds != null && partitionIds.remove(task.partitionId()) && partitionIds.isEmpty()) {
                tasks.remove(task.subtopologyId());
            }
        }

        private TasksTuple toTasksTuple() {
            return new TasksTuple(activeTasks, standbyTasks, warmupTasks);
        }
    }
}
