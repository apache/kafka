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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

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
    private TasksTuple targetAssignment;

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
     * The tasks owned by the member. This may be provided by the member in the StreamsGroupHeartbeat request.
     */
    private Optional<TasksTuple> ownedTasks = Optional.empty();

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
    public CurrentAssignmentBuilder withTargetAssignment(int targetAssignmentEpoch,
                                                         TasksTuple targetAssignment) {
        this.targetAssignmentEpoch = targetAssignmentEpoch;
        this.targetAssignment = Objects.requireNonNull(targetAssignment);
        return this;
    }

    /**
     * Sets a BiFunction which allows to retrieve the current process ID of an active task. This is
     * used by the state machine to determine if an active task is free or still used by another
     * member, and if there is still a task on a specific process that is not yet revoked.
     *
     * @param currentActiveTaskProcessId A BiFunction which gets the process ID of a subtopology ID /
     *                                   partition ID pair.
     * @return This object.
     */
    public CurrentAssignmentBuilder withCurrentActiveTaskProcessId(BiFunction<String, Integer, String> currentActiveTaskProcessId) {
        this.currentActiveTaskProcessId = Objects.requireNonNull(currentActiveTaskProcessId);
        return this;
    }

    /**
     * Sets a BiFunction which allows to retrieve the current process IDs of a standby task. This is
     * used by the state machine to determine if there is still a task on a specific process that is
     * not yet revoked.
     *
     * @param currentStandbyTaskProcessIds A BiFunction which gets the process IDs of a subtopology
     *                                     ID / partition ID pair.
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
     * @param currentWarmupTaskProcessIds A BiFunction which gets the process IDs of a subtopology ID
     *                                    / partition ID pair.
     * @return This object.
     */
    public CurrentAssignmentBuilder withCurrentWarmupTaskProcessIds(BiFunction<String, Integer, Set<String>> currentWarmupTaskProcessIds) {
        this.currentWarmupTaskProcessIds = Objects.requireNonNull(currentWarmupTaskProcessIds);
        return this;
    }

    /**
     * Sets the tasks currently owned by the member. This comes directly from the last StreamsGroupHeartbeat request. This is used to
     * determine if the member has revoked the necessary tasks. Passing null into this function means that the member did not provide
     * its owned tasks in this heartbeat.
     *
     * @param ownedAssignment A collection of active, standby and warm-up tasks
     * @return This object.
     */
    protected CurrentAssignmentBuilder withOwnedAssignment(TasksTuple ownedAssignment) {
        this.ownedTasks = Optional.ofNullable(ownedAssignment);
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
                        member.assignedTasks()
                    );
                } else {
                    return member;
                }

            case UNREVOKED_TASKS:
                // When the member is in the UNREVOKED_TASKS state, we wait
                // until the member has revoked the necessary tasks. They are
                // considered revoked when they are not anymore reported in the
                // owned tasks set in the StreamsGroupHeartbeat API.

                // If the member provides its owned tasks, we verify if it still
                // owns any of the revoked tasks. If it did not provide it's
                // owned tasks, or we still own some of the revoked tasks, we
                // cannot progress.
                if (
                    ownedTasks.isEmpty() || ownedTasks.get().containsAny(member.tasksPendingRevocation())
                ) {
                    return member;
                }

                // When the member has revoked all the pending tasks, it can
                // transition to the next epoch (current + 1) and we can reconcile
                // its state towards the latest target assignment.
                return computeNextAssignment(
                    member.memberEpoch() + 1,
                    member.assignedTasks()
                );

            case UNRELEASED_TASKS:
                // When the member is in the UNRELEASED_TASKS, we reconcile the
                // member towards the latest target assignment. This will assign any
                // of the unreleased tasks when they become available.
                return computeNextAssignment(
                    member.memberEpoch(),
                    member.assignedTasks()
                );

            case UNKNOWN:
                // We could only end up in this state if a new state is added in the
                // future and the group coordinator is downgraded. In this case, the
                // best option is to fence the member to force it to rejoin the group
                // without any tasks and to reconcile it again from scratch.
                if ((ownedTasks.isEmpty() || !ownedTasks.get().isEmpty())) {
                    throw new FencedMemberEpochException(
                        "The streams group member is in a unknown state. "
                            + "The member must abandon all its tasks and rejoin.");
                }

                return computeNextAssignment(
                    targetAssignmentEpoch,
                    member.assignedTasks()
                );
        }

        return member;
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
    private boolean computeAssignmentDifference(Map<String, Set<Integer>> currentAssignment,
                                                Map<String, Set<Integer>> targetAssignment,
                                                Map<String, Set<Integer>> resultAssignedTasks,
                                                Map<String, Set<Integer>> resultTasksPendingRevocation,
                                                Map<String, Set<Integer>> resultTasksPendingAssignment,
                                                BiPredicate<String, Integer> isUnreleasedTask) {
        boolean hasUnreleasedTasks = false;

        Set<String> allSubtopologyIds = new HashSet<>(targetAssignment.keySet());
        allSubtopologyIds.addAll(currentAssignment.keySet());

        for (String subtopologyId : allSubtopologyIds) {
            hasUnreleasedTasks |= computeAssignmentDifferenceForOneSubtopology(
                subtopologyId,
                currentAssignment.getOrDefault(subtopologyId, Collections.emptySet()),
                targetAssignment.getOrDefault(subtopologyId, Collections.emptySet()),
                resultAssignedTasks,
                resultTasksPendingRevocation,
                resultTasksPendingAssignment,
                isUnreleasedTask
            );
        }
        return hasUnreleasedTasks;
    }

    private static boolean computeAssignmentDifferenceForOneSubtopology(final String subtopologyId,
                                                                        final Set<Integer> currentTasksForThisSubtopology,
                                                                        final Set<Integer> targetTasksForThisSubtopology,
                                                                        final Map<String, Set<Integer>> resultAssignedTasks,
                                                                        final Map<String, Set<Integer>> resultTasksPendingRevocation,
                                                                        final Map<String, Set<Integer>> resultTasksPendingAssignment,
                                                                        final BiPredicate<String, Integer> isUnreleasedTask) {
        // Result Assigned Tasks = Current Tasks ∩ Target Tasks
        // i.e. we remove all tasks from the current assignment that are not in the target
        //         assignment
        Set<Integer> resultAssignedTasksForThisSubtopology = new HashSet<>(currentTasksForThisSubtopology);
        resultAssignedTasksForThisSubtopology.retainAll(targetTasksForThisSubtopology);

        // Result Tasks Pending Revocation = Current Tasks - Result Assigned Tasks
        // i.e. we will ask the member to revoke all tasks in its current assignment that
        //      are not in the target assignment
        Set<Integer> resultTasksPendingRevocationForThisSubtopology = new HashSet<>(currentTasksForThisSubtopology);
        resultTasksPendingRevocationForThisSubtopology.removeAll(resultAssignedTasksForThisSubtopology);

        // Result Tasks Pending Assignment = Target Tasks - Result Assigned Tasks - Unreleased Tasks
        // i.e. we will ask the member to assign all tasks in its target assignment,
        //      except those that are already assigned, and those that are unreleased
        Set<Integer> resultTasksPendingAssignmentForThisSubtopology = new HashSet<>(targetTasksForThisSubtopology);
        resultTasksPendingAssignmentForThisSubtopology.removeAll(resultAssignedTasksForThisSubtopology);
        boolean hasUnreleasedTasks = resultTasksPendingAssignmentForThisSubtopology.removeIf(taskId ->
            isUnreleasedTask.test(subtopologyId, taskId)
        );

        if (!resultAssignedTasksForThisSubtopology.isEmpty()) {
            resultAssignedTasks.put(subtopologyId, resultAssignedTasksForThisSubtopology);
        }

        if (!resultTasksPendingRevocationForThisSubtopology.isEmpty()) {
            resultTasksPendingRevocation.put(subtopologyId, resultTasksPendingRevocationForThisSubtopology);
        }

        if (!resultTasksPendingAssignmentForThisSubtopology.isEmpty()) {
            resultTasksPendingAssignment.put(subtopologyId, resultTasksPendingAssignmentForThisSubtopology);
        }

        return hasUnreleasedTasks;
    }

    /**
     * Computes the next assignment.
     *
     * @param memberEpoch         The epoch of the member to use. This may be different from
     *                            the epoch in {@link CurrentAssignmentBuilder#member}.
     * @param memberAssignedTasks The assigned tasks of the member to use.
     * @return A new StreamsGroupMember.
     */
    private StreamsGroupMember computeNextAssignment(int memberEpoch,
                                                     TasksTuple memberAssignedTasks) {
        Map<String, Set<Integer>> newActiveAssignedTasks = new HashMap<>();
        Map<String, Set<Integer>> newActiveTasksPendingRevocation = new HashMap<>();
        Map<String, Set<Integer>> newActiveTasksPendingAssignment = new HashMap<>();
        Map<String, Set<Integer>> newStandbyAssignedTasks = new HashMap<>();
        Map<String, Set<Integer>> newStandbyTasksPendingRevocation = new HashMap<>();
        Map<String, Set<Integer>> newStandbyTasksPendingAssignment = new HashMap<>();
        Map<String, Set<Integer>> newWarmupAssignedTasks = new HashMap<>();
        Map<String, Set<Integer>> newWarmupTasksPendingRevocation = new HashMap<>();
        Map<String, Set<Integer>> newWarmupTasksPendingAssignment = new HashMap<>();

        boolean hasUnreleasedActiveTasks = computeAssignmentDifference(
            memberAssignedTasks.activeTasks(),
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

        boolean hasUnreleasedStandbyTasks = computeAssignmentDifference(
            memberAssignedTasks.standbyTasks(),
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
        );

        boolean hasUnreleasedWarmupTasks = computeAssignmentDifference(
            memberAssignedTasks.warmupTasks(),
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
        );

        return buildNewMember(
            memberEpoch,
            new TasksTuple(
                newActiveTasksPendingRevocation,
                newStandbyTasksPendingRevocation,
                newWarmupTasksPendingRevocation
            ),
            new TasksTuple(
                newActiveAssignedTasks,
                newStandbyAssignedTasks,
                newWarmupAssignedTasks
            ),
            new TasksTuple(
                newActiveTasksPendingAssignment,
                newStandbyTasksPendingAssignment,
                newWarmupTasksPendingAssignment
            ),
            hasUnreleasedActiveTasks || hasUnreleasedStandbyTasks || hasUnreleasedWarmupTasks
        );
    }

    private StreamsGroupMember buildNewMember(final int memberEpoch,
                                              final TasksTuple newTasksPendingRevocation,
                                              final TasksTuple newAssignedTasks,
                                              final TasksTuple newTasksPendingAssignment,
                                              final boolean hasUnreleasedTasks) {

        final boolean hasTasksToBeRevoked =
            (!newTasksPendingRevocation.isEmpty())
                && (ownedTasks.isEmpty() || ownedTasks.get().containsAny(newTasksPendingRevocation));

        if (hasTasksToBeRevoked) {
            // If there are tasks to be revoked, the member remains in its current
            // epoch and requests the revocation of those tasks. It transitions to
            // the UNREVOKED_TASKS state to wait until the client acknowledges the
            // revocation of the tasks.
            return new StreamsGroupMember.Builder(member)
                .setState(MemberState.UNREVOKED_TASKS)
                .updateMemberEpoch(memberEpoch)
                .setAssignedTasks(newAssignedTasks)
                .setTasksPendingRevocation(newTasksPendingRevocation)
                .build();
        } else if (!newTasksPendingAssignment.isEmpty()) {
            // If there are tasks to be assigned, the member transitions to the
            // target epoch and requests the assignment of those tasks. Note that
            // the tasks are directly added to the assigned tasks set. The
            // member transitions to the STABLE state or to the UNRELEASED_TASKS
            // state depending on whether there are unreleased tasks or not.
            MemberState newState =
                hasUnreleasedTasks
                    ? MemberState.UNRELEASED_TASKS
                    : MemberState.STABLE;
            return new StreamsGroupMember.Builder(member)
                .setState(newState)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedTasks(newAssignedTasks.merge(newTasksPendingAssignment))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build();
        } else if (hasUnreleasedTasks) {
            // If there are no tasks to be revoked nor to be assigned but some
            // tasks are not available yet, the member transitions to the target
            // epoch, to the UNRELEASED_TASKS state and waits.
            return new StreamsGroupMember.Builder(member)
                .setState(MemberState.UNRELEASED_TASKS)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedTasks(newAssignedTasks)
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build();
        } else {
            // Otherwise, the member transitions to the target epoch and to the
            // STABLE state.
            return new StreamsGroupMember.Builder(member)
                .setState(MemberState.STABLE)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedTasks(newAssignedTasks)
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build();
        }
    }
}
