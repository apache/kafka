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

import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * The CurrentAssignmentBuilder class encapsulates the reconciliation engine of the streams group protocol. Given the current state of a
 * member and a desired or target assignment state, the state machine takes the necessary steps to converge them.
 */
public class CurrentAssignmentBuilder {

    /**
     * The streams group member which is reconciled.
     */
    private final StreamsGroupMember member;

    /**
     * The target assignment epoch.
     */
    private int targetAssignmentEpoch;

    /**
     * The target assignment.
     */
    private Assignment targetAssignment;

    /**
     * A function which returns the current process ID of an active task or null if the active task
     * is not assigned. The current process ID is the process ID of the current owner.
     */
    private BiFunction<String, Integer, String> currentActiveTaskProcessId;

    /**
     * A function which returns the current process IDs of a standby task or null if the standby
     * task is not assigned. The current process IDs are the process IDs of all current owners.
     */
    private BiFunction<String, Integer, Set<String>> currentStandbyTaskProcessIds;

    /**
     * A function which returns the current process IDs of a warmup task or null if the warmup task
     * is not assigned. The current process IDs are the process IDs of all current owners.
     */
    private BiFunction<String, Integer, Set<String>> currentWarmupTaskProcessIds;

    /**
     * The active tasks owned by the member. This is directly provided by the member in the StreamsGroupHeartbeat request.
     */
    private List<StreamsGroupHeartbeatRequestData.TaskIds> ownedActiveTasks;

    /**
     * The standby tasks owned by the member. This is directly provided by the member in the StreamsGroupHeartbeat request.
     */
    private List<StreamsGroupHeartbeatRequestData.TaskIds> ownedStandbyTasks;

    /**
     * The warmup tasks owned by the member. This is directly provided by the member in the StreamsGroupHeartbeat request.
     */
    private List<StreamsGroupHeartbeatRequestData.TaskIds> ownedWarmupTasks;

    /**
     * Constructs the CurrentAssignmentBuilder based on the current state of the provided streams group member.
     *
     * @param member The streams group member that must be reconciled.
     */
    public CurrentAssignmentBuilder(StreamsGroupMember member) {
        this.member = Objects.requireNonNull(member);
    }

    /**
     * Sets the target assignment epoch and the target assignment that the streams group member must be reconciled to.
     *
     * @param targetAssignmentEpoch The target assignment epoch.
     * @param targetAssignment      The target assignment.
     * @return This object.
     */
    public CurrentAssignmentBuilder withTargetAssignment(
        int targetAssignmentEpoch,
        Assignment targetAssignment
    ) {
        this.targetAssignmentEpoch = targetAssignmentEpoch;
        this.targetAssignment = Objects.requireNonNull(targetAssignment);
        return this;
    }

    /**
     * Sets a BiFunction which allows to retrieve the current process ID of an active task. This is
     * used by the state machine to determine if an active task is free or still used by another
     * member, and if there is still a task on a specific process that is not yet revoked.
     *
     * @param currentActiveTaskProcessId A BiFunction which gets the memberId of a subtopology id /
     *                                   partition id pair.
     * @return This object.
     */
    public CurrentAssignmentBuilder withCurrentActiveTaskProcessId(
        BiFunction<String, Integer, String> currentActiveTaskProcessId
    ) {
        this.currentActiveTaskProcessId = Objects.requireNonNull(currentActiveTaskProcessId);
        return this;
    }

    /**
     * Sets a BiFunction which allows to retrieve the current process IDs of a standby task. This is
     * used by the state machine to determine if there is still a task on a specific process that is
     * not yet revoked.
     *
     * @param currentStandbyTaskProcessIds A BiFunction which gets the memberIds of a subtopology
     *                                     ids / partition ids pair.
     * @return This object.
     */
    public CurrentAssignmentBuilder withCurrentStandbyTaskProcessIds(
        BiFunction<String, Integer, Set<String>> currentStandbyTaskProcessIds
    ) {
        this.currentStandbyTaskProcessIds = Objects.requireNonNull(currentStandbyTaskProcessIds);
        return this;
    }

    /**
     * Sets a BiFunction which allows to retrieve the current process IDs of a warmup task. This is
     * used by the state machine to determine if there is still a task on a specific process that is
     * not yet revoked.
     *
     * @param currentWarmupTaskProcessIds A BiFunction which gets the memberIds of a subtopology ids
     *                                    / partition ids pair.
     * @return This object.
     */
    public CurrentAssignmentBuilder withCurrentWarmupTaskProcessIds(
        BiFunction<String, Integer, Set<String>> currentWarmupTaskProcessIds
    ) {
        this.currentWarmupTaskProcessIds = Objects.requireNonNull(currentWarmupTaskProcessIds);
        return this;
    }

    protected CurrentAssignmentBuilder withOwnedAssignment(
        Assignment ownedAssignment
    ) {
        if (ownedAssignment == null) {
            this.ownedActiveTasks = null;
            this.ownedStandbyTasks = null;
            this.ownedWarmupTasks = null;
        } else {
            this.ownedActiveTasks = ownedAssignment.activeTasks().entrySet().stream()
                .map(activeTaskIds -> new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(activeTaskIds.getKey())
                    .setPartitions(new ArrayList<>(activeTaskIds.getValue())))
                .collect(Collectors.toList());
            this.ownedStandbyTasks = ownedAssignment.standbyTasks().entrySet().stream()
                .map(standbyTaskIds -> new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(standbyTaskIds.getKey())
                    .setPartitions(new ArrayList<>(standbyTaskIds.getValue())))
                .collect(Collectors.toList());
            this.ownedWarmupTasks = ownedAssignment.warmupTasks().entrySet().stream()
                .map(warmupTaskIds -> new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(warmupTaskIds.getKey())
                    .setPartitions(new ArrayList<>(warmupTaskIds.getValue())))
                .collect(Collectors.toList());
        }
        return this;
    }

    /**
     * Sets the active tasks currently owned by the member. This comes directly from the last StreamsGroupHeartbeat request. This is used to
     * determine if the member has revoked the necessary tasks.
     *
     * @param ownedActiveTasks A list of topic-tasks.
     * @return This object.
     */
    public CurrentAssignmentBuilder withOwnedActiveTasks(
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedActiveTasks
    ) {
        this.ownedActiveTasks = ownedActiveTasks;
        return this;
    }

    /**
     * Sets the standby tasks currently owned by the member. This comes directly from the last StreamsGroupHeartbeat request. This is used to
     * determine if the member has revoked the necessary tasks.
     *
     * @param ownedStandbyTasks A list of topic-tasks.
     * @return This object.
     */
    public CurrentAssignmentBuilder withOwnedStandbyTasks(
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedStandbyTasks
    ) {
        this.ownedStandbyTasks = ownedStandbyTasks;
        return this;
    }

    /**
     * Sets the warmup tasks currently owned by the member. This comes directly from the last StreamsGroupHeartbeat request. This is used to
     * determine if the member has revoked the necessary tasks.
     *
     * @param ownedWarmupTasks A list of topic-tasks.
     * @return This object.
     */
    public CurrentAssignmentBuilder withOwnedWarmupTasks(
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedWarmupTasks
    ) {
        this.ownedWarmupTasks = ownedWarmupTasks;
        return this;
    }


    /**
     * Builds the next state for the member or keep the current one if it is not possible to move forward with the current state.
     *
     * @return A new StreamsGroupMember or the current one.
     */
    public StreamsGroupMember build() {
        switch (member.state()) {
            case STABLE:
                // When the member is in the STABLE state, we verify if a newer
                // epoch (or target assignment) is available. If it is, we can
                // reconcile the member towards it. Otherwise, we return.
                if (member.memberEpoch() != targetAssignmentEpoch) {
                    return computeNextAssignment(
                        member.memberEpoch(),
                        member.assignedActiveTasks(),
                        member.assignedStandbyTasks(),
                        member.assignedWarmupTasks()
                    );
                } else {
                    return member;
                }

            case UNREVOKED_TASKS:
                // When the member is in the UNREVOKED_TASKS state, we wait
                // until the member has revoked the necessary tasks. They are
                // considered revoked when they are not anymore reported in the
                // owned tasks set in the StreamsGroupHeartbeat API.

                // If the member provides its owned tasks. We verify if it still
                // owns any of the revoked tasks. If it does, we cannot progress.
                if (
                    ownRevokedTasks(member.activeTasksPendingRevocation(), ownedActiveTasks)
                        || ownRevokedTasks(member.standbyTasksPendingRevocation(),
                        ownedStandbyTasks)
                        || ownRevokedTasks(member.warmupTasksPendingRevocation(), ownedWarmupTasks)
                ) {
                    return member;
                }

                // When the member has revoked all the pending tasks, it can
                // transition to the next epoch (current + 1) and we can reconcile
                // its state towards the latest target assignment.
                return computeNextAssignment(
                    member.memberEpoch() + 1,
                    member.assignedActiveTasks(),
                    member.assignedStandbyTasks(),
                    member.assignedWarmupTasks()
                );

            case UNRELEASED_TASKS:
                // When the member is in the UNRELEASED_TASKS, we reconcile the
                // member towards the latest target assignment. This will assign any
                // of the unreleased tasks when they become available.
                return computeNextAssignment(
                    member.memberEpoch(),
                    member.assignedActiveTasks(),
                    member.assignedStandbyTasks(),
                    member.assignedWarmupTasks()
                );

            case UNKNOWN:
                // We could only end up in this state if a new state is added in the
                // future and the group coordinator is downgraded. In this case, the
                // best option is to fence the member to force it to rejoin the group
                // without any tasks and to reconcile it again from scratch.
                if ((ownedActiveTasks == null || !ownedActiveTasks.isEmpty())
                    || (ownedStandbyTasks == null || !ownedStandbyTasks.isEmpty())
                    || (ownedWarmupTasks == null || !ownedWarmupTasks.isEmpty())) {
                    throw new FencedMemberEpochException(
                        "The streams group member is in a unknown state. "
                            + "The member must abandon all its tasks and rejoin.");
                }

                return computeNextAssignment(
                    targetAssignmentEpoch,
                    member.assignedActiveTasks(),
                    member.assignedStandbyTasks(),
                    member.assignedWarmupTasks()
                );
        }

        return member;
    }

    /**
     * Decides whether the given owned tasks contains any partition that is pending revocation.
     *
     * @param assignment The assignment that has the tasks pending revocation.
     * @param ownedTasks The set of tasks from the response.
     * @return A boolean based on the condition mentioned above.
     */
    private static boolean ownRevokedTasks(
        Map<String, Set<Integer>> assignment,
        List<StreamsGroupHeartbeatRequestData.TaskIds> ownedTasks
    ) {
        if (ownedTasks == null) {
            return true;
        }

        for (StreamsGroupHeartbeatRequestData.TaskIds owned : ownedTasks) {
            Set<Integer> tasksPendingRevocation =
                assignment.getOrDefault(owned.subtopologyId(), Collections.emptySet());

            for (Integer partitionId : owned.partitions()) {
                if (tasksPendingRevocation.contains(partitionId)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Takes the current currentAssignment and the targetAssignment, and generates three
     * collections:
     *
     * - the resultAssignedTasks: the tasks that are assigned in both the current and target
     * assignments.
     * - the resultTasksPendingRevocation: the tasks that are assigned in the current
     * assignment but not in the target assignment.
     * - the resultTasksPendingAssignment: the tasks that are assigned in the target assignment but
     * not in the current assignment, and can be assigned currently (i.e., they are not owned by
     * another member, as defined by the `isUnreleasedTask` predicate).
     */
    private boolean computeAssignmentDifference(
        Map<String, Set<Integer>> currentAssignment,
        Map<String, Set<Integer>> targetAssignment,
        Map<String, Set<Integer>> resultAssignedTasks,
        Map<String, Set<Integer>> resultTasksPendingRevocation,
        Map<String, Set<Integer>> resultTasksPendingAssignment,
        BiPredicate<String, Integer> isUnreleasedTask
    ) {
        boolean hasUnreleasedTasks = false;

        Set<String> allSubtopologyIds = new HashSet<>(targetAssignment.keySet());
        allSubtopologyIds.addAll(currentAssignment.keySet());

        for (String subtopologyId : allSubtopologyIds) {
            Set<Integer> currentPartitions = currentAssignment.getOrDefault(subtopologyId, Collections.emptySet());
            Set<Integer> targetPartitions = targetAssignment.getOrDefault(subtopologyId, Collections.emptySet());

            // Result Assigned Partitions = Current Partitions ∩ Target Partitions
            // i.e. we remove all partitions from the current assignment that are not in the target
            //         assignment
            Set<Integer> resultAssignedPartitions = new HashSet<>(currentPartitions);
            resultAssignedPartitions.retainAll(targetPartitions);

            // Result Partitions Pending Revocation = Current Partitions - Result Assigned Partitions
            // i.e. we will ask the member to revoke all partitions in its current assignment that
            //      are not in the target assignment
            Set<Integer> resultPartitionsPendingRevocation = new HashSet<>(currentPartitions);
            resultPartitionsPendingRevocation.removeAll(resultAssignedPartitions);

            // Result Partitions Pending Assignment = Target Partitions - Result Assigned Partitions - Unreleased Partitions
            // i.e. we will ask the member to assign all partitions in its target assignment,
            //      except those that are already assigned, and those that are unrelead
            Set<Integer> resultPartitionsPendingAssignment = new HashSet<>(targetPartitions);
            resultPartitionsPendingAssignment.removeAll(resultAssignedPartitions);
            hasUnreleasedTasks = resultPartitionsPendingAssignment.removeIf(partitionId ->
                isUnreleasedTask.test(subtopologyId, partitionId)
            ) || hasUnreleasedTasks;

            if (!resultAssignedPartitions.isEmpty()) {
                resultAssignedTasks.put(subtopologyId, resultAssignedPartitions);
            }

            if (!resultPartitionsPendingRevocation.isEmpty()) {
                resultTasksPendingRevocation.put(subtopologyId, resultPartitionsPendingRevocation);
            }

            if (!resultPartitionsPendingAssignment.isEmpty()) {
                resultTasksPendingAssignment.put(subtopologyId, resultPartitionsPendingAssignment);
            }
        }
        return hasUnreleasedTasks;
    }

    /**
     * Computes the next assignment.
     *
     * @param memberEpoch                The epoch of the member to use. This may be different from
     *                                   the epoch in {@link CurrentAssignmentBuilder#member}.
     * @param memberAssignedActiveTasks  The assigned active tasks of the member to use.
     * @param memberAssignedStandbyTasks The assigned standby tasks of the member to use.
     * @param memberAssignedWarmupTasks  The assigned warmup tasks of the member to use.
     * @return A new StreamsGroupMember.
     */
    private StreamsGroupMember computeNextAssignment(
        int memberEpoch,
        Map<String, Set<Integer>> memberAssignedActiveTasks,
        Map<String, Set<Integer>> memberAssignedStandbyTasks,
        Map<String, Set<Integer>> memberAssignedWarmupTasks
    ) {
        Map<String, Set<Integer>> newActiveAssignedTasks = new HashMap<>();
        Map<String, Set<Integer>> newActiveTasksPendingRevocation = new HashMap<>();
        Map<String, Set<Integer>> newActiveTasksPendingAssignment = new HashMap<>();
        Map<String, Set<Integer>> newStandbyAssignedTasks = new HashMap<>();
        Map<String, Set<Integer>> newStandbyTasksPendingRevocation = new HashMap<>();
        Map<String, Set<Integer>> newStandbyTasksPendingAssignment = new HashMap<>();
        Map<String, Set<Integer>> newWarmupAssignedTasks = new HashMap<>();
        Map<String, Set<Integer>> newWarmupTasksPendingRevocation = new HashMap<>();
        Map<String, Set<Integer>> newWarmupTasksPendingAssignment = new HashMap<>();

        boolean hasUnreleasedTasks = computeAssignmentDifference(
            memberAssignedActiveTasks,
            targetAssignment.activeTasks(),
            newActiveAssignedTasks,
            newActiveTasksPendingRevocation,
            newActiveTasksPendingAssignment,
            (subtopologyId, partitionId) ->
                currentActiveTaskProcessId.apply(subtopologyId, partitionId) != null ||
                    currentStandbyTaskProcessIds.apply(subtopologyId, partitionId)
                        .contains(member.processId()) ||
                    currentWarmupTaskProcessIds.apply(subtopologyId, partitionId)
                        .contains(member.processId())
        );

        hasUnreleasedTasks = computeAssignmentDifference(
            memberAssignedStandbyTasks,
            targetAssignment.standbyTasks(),
            newStandbyAssignedTasks,
            newStandbyTasksPendingRevocation,
            newStandbyTasksPendingAssignment,
            (subtopologyId, partitionId) ->
                Objects.equals(currentActiveTaskProcessId.apply(subtopologyId, partitionId),
                    member.processId()) ||
                    currentStandbyTaskProcessIds.apply(subtopologyId, partitionId)
                        .contains(member.processId()) ||
                    currentWarmupTaskProcessIds.apply(subtopologyId, partitionId)
                        .contains(member.processId())
        ) || hasUnreleasedTasks;

        hasUnreleasedTasks = computeAssignmentDifference(
            memberAssignedWarmupTasks,
            targetAssignment.warmupTasks(),
            newWarmupAssignedTasks,
            newWarmupTasksPendingRevocation,
            newWarmupTasksPendingAssignment,
            (subtopologyId, partitionId) ->
                Objects.equals(currentActiveTaskProcessId.apply(subtopologyId, partitionId),
                    member.processId()) ||
                    currentStandbyTaskProcessIds.apply(subtopologyId, partitionId)
                        .contains(member.processId()) ||
                    currentWarmupTaskProcessIds.apply(subtopologyId, partitionId)
                        .contains(member.processId())
        ) || hasUnreleasedTasks;

        return buildNewMember(memberEpoch, newActiveTasksPendingRevocation,
            newStandbyTasksPendingRevocation, newWarmupTasksPendingRevocation,
            newActiveAssignedTasks,
            newStandbyAssignedTasks, newWarmupAssignedTasks, newActiveTasksPendingAssignment,
            newStandbyTasksPendingAssignment, newWarmupTasksPendingAssignment, hasUnreleasedTasks);
    }

    private StreamsGroupMember buildNewMember(
        final int memberEpoch,
        final Map<String, Set<Integer>> newActiveTasksPendingRevocation,
        final Map<String, Set<Integer>> newStandbyTasksPendingRevocation,
        final Map<String, Set<Integer>> newWarmupTasksPendingRevocation,
        final Map<String, Set<Integer>> newActiveAssignedTasks,
        final Map<String, Set<Integer>> newStandbyAssignedTasks,
        final Map<String, Set<Integer>> newWarmupAssignedTasks,
        final Map<String, Set<Integer>> newActiveTasksPendingAssignment,
        final Map<String, Set<Integer>> newStandbyTasksPendingAssignment,
        final Map<String, Set<Integer>> newWarmupTasksPendingAssignment,
        final boolean hasUnreleasedTasks) {

        final boolean hasTasksToBeRevoked =
            (!newActiveTasksPendingRevocation.isEmpty() && ownRevokedTasks(
                newActiveTasksPendingRevocation, ownedActiveTasks)) ||
                (!newStandbyTasksPendingRevocation.isEmpty() && ownRevokedTasks(
                    newStandbyTasksPendingRevocation, ownedStandbyTasks)) ||
                (!newWarmupTasksPendingRevocation.isEmpty() && ownRevokedTasks(
                    newWarmupTasksPendingRevocation, ownedWarmupTasks));

        if (hasTasksToBeRevoked) {
            // If there are tasks to be revoked, the member remains in its current
            // epoch and requests the revocation of those tasks. It transitions to
            // the UNREVOKED_TASKS state to wait until the client acknowledges the
            // revocation of the tasks.
            return new StreamsGroupMember.Builder(member)
                .setState(MemberState.UNREVOKED_TASKS)
                .updateMemberEpoch(memberEpoch)
                .setAssignedActiveTasks(newActiveAssignedTasks)
                .setAssignedStandbyTasks(newStandbyAssignedTasks)
                .setAssignedWarmupTasks(newWarmupAssignedTasks)
                .setActiveTasksPendingRevocation(newActiveTasksPendingRevocation)
                .setStandbyTasksPendingRevocation(newStandbyTasksPendingRevocation)
                .setWarmupTasksPendingRevocation(newWarmupTasksPendingRevocation)
                .build();
        } else if (
            !newActiveTasksPendingAssignment.isEmpty() ||
                !newStandbyTasksPendingAssignment.isEmpty() ||
                !newWarmupTasksPendingAssignment.isEmpty()
        ) {
            // If there are tasks to be assigned, the member transitions to the
            // target epoch and requests the assignment of those tasks. Note that
            // the tasks are directly added to the assigned tasks set. The
            // member transitions to the STABLE state or to the UNRELEASED_TASKS
            // state depending on whether there are unreleased tasks or not.
            newActiveTasksPendingAssignment.forEach((subtopologyId, tasks) -> newActiveAssignedTasks
                .computeIfAbsent(subtopologyId, __ -> new HashSet<>())
                .addAll(tasks));
            newStandbyTasksPendingAssignment.forEach(
                (subtopologyId, tasks) -> newStandbyAssignedTasks
                    .computeIfAbsent(subtopologyId, __ -> new HashSet<>())
                    .addAll(tasks));
            newWarmupTasksPendingAssignment.forEach((subtopologyId, tasks) -> newWarmupAssignedTasks
                .computeIfAbsent(subtopologyId, __ -> new HashSet<>())
                .addAll(tasks));
            MemberState newState =
                hasUnreleasedTasks
                    ? MemberState.UNRELEASED_TASKS
                    : MemberState.STABLE;
            return new StreamsGroupMember.Builder(member)
                .setState(newState)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedActiveTasks(newActiveAssignedTasks)
                .setAssignedStandbyTasks(newStandbyAssignedTasks)
                .setAssignedWarmupTasks(newWarmupAssignedTasks)
                .setActiveTasksPendingRevocation(Collections.emptyMap())
                .setStandbyTasksPendingRevocation(Collections.emptyMap())
                .setWarmupTasksPendingRevocation(Collections.emptyMap())
                .build();
        } else if (hasUnreleasedTasks) {
            // If there are no tasks to be revoked nor to be assigned but some
            // tasks are not available yet, the member transitions to the target
            // epoch, to the UNRELEASED_TASKS state and waits.
            return new StreamsGroupMember.Builder(member)
                .setState(MemberState.UNRELEASED_TASKS)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedActiveTasks(newActiveAssignedTasks)
                .setAssignedStandbyTasks(newStandbyAssignedTasks)
                .setAssignedWarmupTasks(newWarmupAssignedTasks)
                .setActiveTasksPendingRevocation(Collections.emptyMap())
                .setStandbyTasksPendingRevocation(Collections.emptyMap())
                .setWarmupTasksPendingRevocation(Collections.emptyMap())
                .build();
        } else {
            // Otherwise, the member transitions to the target epoch and to the
            // STABLE state.
            return new StreamsGroupMember.Builder(member)
                .setState(MemberState.STABLE)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedActiveTasks(newActiveAssignedTasks)
                .setAssignedStandbyTasks(newStandbyAssignedTasks)
                .setAssignedWarmupTasks(newWarmupAssignedTasks)
                .setActiveTasksPendingRevocation(Collections.emptyMap())
                .setStandbyTasksPendingRevocation(Collections.emptyMap())
                .setWarmupTasksPendingRevocation(Collections.emptyMap())
                .build();
        }
    }
}
