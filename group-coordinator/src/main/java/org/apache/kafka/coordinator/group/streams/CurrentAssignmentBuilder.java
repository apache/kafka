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
    public CurrentAssignmentBuilder withOwnedAssignment(TasksTuple ownedAssignment) {
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
                // owns any of the revoked tasks. If it did not provide its
                // owned tasks, or we still own some of the revoked tasks, we
                // cannot progress.
                if (hasNotReleased(member.tasksPendingRevocation())) {
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
                currentAssignment.getOrDefault(subtopologyId, Set.of()),
                targetAssignment.getOrDefault(subtopologyId, Set.of()),
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
     *
     * Epoch Handling:
     * - For tasks in resultAssignedTasks and resultTasksPendingRevocation, the epoch from currentAssignment is preserved.
     * - For tasks in resultTasksPendingAssignment, the targetAssignmentEpoch is used.
     */
    private boolean computeAssignmentDifferenceWithEpoch(Map<String, Map<Integer, Integer>> currentAssignment,
                                                         Map<String, Set<Integer>> targetAssignment,
                                                         int targetAssignmentEpoch,
                                                         Map<String, Map<Integer, Integer>> resultAssignedTasks,
                                                         Map<String, Map<Integer, Integer>> resultTasksPendingRevocation,
                                                         Map<String, Map<Integer, Integer>> resultTasksPendingAssignment,
                                                         BiPredicate<String, Integer> isUnreleasedTask) {
        boolean hasUnreleasedTasks = false;

        Set<String> allSubtopologyIds = new HashSet<>(targetAssignment.keySet());
        allSubtopologyIds.addAll(currentAssignment.keySet());

        for (String subtopologyId : allSubtopologyIds) {
            hasUnreleasedTasks |= computeAssignmentDifferenceForOneSubtopologyWithEpoch(
                subtopologyId,
                currentAssignment.getOrDefault(subtopologyId, Map.of()),
                targetAssignment.getOrDefault(subtopologyId, Set.of()),
                targetAssignmentEpoch,
                resultAssignedTasks,
                resultTasksPendingRevocation,
                resultTasksPendingAssignment,
                isUnreleasedTask
            );
        }
        return hasUnreleasedTasks;
    }

    private static boolean computeAssignmentDifferenceForOneSubtopologyWithEpoch(final String subtopologyId,
                                                                                 final Map<Integer, Integer> currentTasksForThisSubtopology,
                                                                                 final Set<Integer> targetTasksForThisSubtopology,
                                                                                 final int targetAssignmentEpoch,
                                                                                 final Map<String, Map<Integer, Integer>> resultAssignedTasks,
                                                                                 final Map<String, Map<Integer, Integer>> resultTasksPendingRevocation,
                                                                                 final Map<String, Map<Integer, Integer>> resultTasksPendingAssignment,
                                                                                 final BiPredicate<String, Integer> isUnreleasedTask) {
        // Result Assigned Tasks = Current Tasks ∩ Target Tasks
        // i.e. we remove all tasks from the current assignment that are not in the target
        //         assignment
        Map<Integer, Integer> resultAssignedTasksForThisSubtopology = new HashMap<>();
        for (Map.Entry<Integer, Integer> entry : currentTasksForThisSubtopology.entrySet()) {
            if (targetTasksForThisSubtopology.contains(entry.getKey())) {
                resultAssignedTasksForThisSubtopology.put(entry.getKey(), entry.getValue());
            }
        }

        // Result Tasks Pending Revocation = Current Tasks - Result Assigned Tasks
        // i.e. we will ask the member to revoke all tasks in its current assignment that
        //      are not in the target assignment
        Map<Integer, Integer> resultTasksPendingRevocationForThisSubtopology = new HashMap<>(currentTasksForThisSubtopology);
        resultTasksPendingRevocationForThisSubtopology.keySet().removeAll(resultAssignedTasksForThisSubtopology.keySet());

        // Result Tasks Pending Assignment = Target Tasks - Result Assigned Tasks - Unreleased Tasks
        // i.e. we will ask the member to assign all tasks in its target assignment,
        //      except those that are already assigned, and those that are unreleased
        Map<Integer, Integer> resultTasksPendingAssignmentForThisSubtopology = new HashMap<>();
        for (Integer taskId : targetTasksForThisSubtopology) {
            if (!resultAssignedTasksForThisSubtopology.containsKey(taskId)) {
                resultTasksPendingAssignmentForThisSubtopology.put(taskId, targetAssignmentEpoch);
            }
        }
        boolean hasUnreleasedTasks = resultTasksPendingAssignmentForThisSubtopology.keySet().removeIf(taskId ->
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
                                                     TasksTupleWithEpochs memberAssignedTasks) {
        Map<String, Map<Integer, Integer>> newActiveAssignedTasks = new HashMap<>();
        Map<String, Map<Integer, Integer>> newActiveTasksPendingRevocation = new HashMap<>();
        Map<String, Map<Integer, Integer>> newActiveTasksPendingAssignment = new HashMap<>();
        Map<String, Set<Integer>> newStandbyAssignedTasks = new HashMap<>();
        Map<String, Set<Integer>> newStandbyTasksPendingRevocation = new HashMap<>();
        Map<String, Set<Integer>> newStandbyTasksPendingAssignment = new HashMap<>();
        Map<String, Set<Integer>> newWarmupAssignedTasks = new HashMap<>();
        Map<String, Set<Integer>> newWarmupTasksPendingRevocation = new HashMap<>();
        Map<String, Set<Integer>> newWarmupTasksPendingAssignment = new HashMap<>();

        boolean hasUnreleasedActiveTasks = computeAssignmentDifferenceWithEpoch(
            memberAssignedTasks.activeTasksWithEpochs(),
            targetAssignment.activeTasks(),
            targetAssignmentEpoch,
            newActiveAssignedTasks,
            newActiveTasksPendingRevocation,
            newActiveTasksPendingAssignment,
            (subtopologyId, partitionId) -> isUnreleasedActiveTask(memberAssignedTasks, subtopologyId, partitionId)
        );

        boolean hasUnreleasedStandbyTasks = computeAssignmentDifference(
            memberAssignedTasks.standbyTasks(),
            targetAssignment.standbyTasks(),
            newStandbyAssignedTasks,
            newStandbyTasksPendingRevocation,
            newStandbyTasksPendingAssignment,
            (subtopologyId, partitionId) -> isUnreleasedStandbyTask(memberAssignedTasks, subtopologyId, partitionId)
        );

        boolean hasUnreleasedWarmupTasks = computeAssignmentDifference(
            memberAssignedTasks.warmupTasks(),
            targetAssignment.warmupTasks(),
            newWarmupAssignedTasks,
            newWarmupTasksPendingRevocation,
            newWarmupTasksPendingAssignment,
            (subtopologyId, partitionId) -> isUnreleasedWarmupTask(memberAssignedTasks, subtopologyId, partitionId)
        );

        // A role change that does not depend on any other member takes effect right away: the target role is moved
        // from the pending assignment to the assigned tasks, so that the member is told about both halves of the
        // change in the same heartbeat and the client recycles the task instead of closing and re-creating it.
        // A demoted active task still stays in the revocation set, because the group must observe its release
        // before the task can be granted to its next owner.

        // The three calls below modify the maps passed to them in place, so they have to run before the new member is built.

        // revoked active tasks are already reflected in `newActiveTasksPendingRevocation`;
        // we only need to take care of standby task part
        demoteActiveTasksToStandby(memberAssignedTasks, newStandbyTasksPendingAssignment, newStandbyAssignedTasks);
        // convert standby tasks to warmup task
        convertReplicaRole(
            memberAssignedTasks.standbyTasks(),
            newStandbyTasksPendingRevocation,
            newWarmupTasksPendingAssignment,
            newWarmupAssignedTasks
        );
        // convert warmup tasks to standby tasks
        convertReplicaRole(
            memberAssignedTasks.warmupTasks(),
            newWarmupTasksPendingRevocation,
            newStandbyTasksPendingAssignment,
            newStandbyAssignedTasks
        );

        // A promotion to active, in contrast, has to wait for the previous owner to release the active task, so the
        // member keeps its standby or warm-up task until the active task is actually granted.
        TasksTupleWithEpochs newTasksPendingRevocation = applyPromotionsToActive(
            memberAssignedTasks,
            newActiveTasksPendingRevocation,
            newStandbyTasksPendingRevocation, // modified in-place by applyPromotionsToActive
            newWarmupTasksPendingRevocation, // modified in-place by applyPromotionsToActive
            newActiveTasksPendingAssignment,
            newStandbyAssignedTasks, // modified in-place by applyPromotionsToActive
            newWarmupAssignedTasks // modified in-place by applyPromotionsToActive
        );

        return buildNewMember(
            memberEpoch,
            newTasksPendingRevocation,
            new TasksTupleWithEpochs(
                newActiveAssignedTasks,
                newStandbyAssignedTasks,
                newWarmupAssignedTasks
            ),
            new TasksTupleWithEpochs(
                newActiveTasksPendingAssignment,
                newStandbyTasksPendingAssignment,
                newWarmupTasksPendingAssignment
            ),
            hasUnreleasedActiveTasks || hasUnreleasedStandbyTasks || hasUnreleasedWarmupTasks
        );
    }

    /**
     * An active task can only be granted once its previous owner anywhere in the group has released it -- a task has
     * exactly one active owner. It can also not be granted to a _process_ that still holds the task as a standby or
     * warm-up, because a single process must never run the same task twice. The exception is an in-place promotion: if
     * THIS MEMBER holds the standby or warm-up task, the active task supersedes it in a single step, so that the client
     * recycles the task.
     */
    private boolean isUnreleasedActiveTask(TasksTupleWithEpochs memberAssignedTasks,
                                           String subtopologyId,
                                           Integer partitionId) {
        return currentActiveTaskProcessId.apply(subtopologyId, partitionId) != null
            || heldByAnotherMemberOnThisProcess(currentStandbyTaskProcessIds, memberAssignedTasks.standbyTasks(), subtopologyId, partitionId)
            || heldByAnotherMemberOnThisProcess(currentWarmupTaskProcessIds, memberAssignedTasks.warmupTasks(), subtopologyId, partitionId);
    }

    /**
     * A standby task can only be granted once the previous holder WITHIN THE SAME PROCESS has released it -- unlike an
     * active task, the same standby task also exists on other processes, and those do not block. It can also not be
     * granted to a process that holds the task as an active or warm-up task, because a single process must never run
     * the same task twice. The exceptions are in-place role changes of THIS MEMBER's own task: demoting its active task
     * to a standby task, or converting its warm-up task into a standby task.
     */
    private boolean isUnreleasedStandbyTask(TasksTupleWithEpochs memberAssignedTasks,
                                            String subtopologyId,
                                            Integer partitionId) {
        return runByAnotherMemberOnThisProcess(memberAssignedTasks, subtopologyId, partitionId)
            || currentStandbyTaskProcessIds.apply(subtopologyId, partitionId).contains(member.processId())
            || heldByAnotherMemberOnThisProcess(currentWarmupTaskProcessIds, memberAssignedTasks.warmupTasks(), subtopologyId, partitionId);
    }

    /**
     * A warm-up task can only be granted once the previous holder WITHIN THE SAME PROCESS has released it -- as for
     * standby tasks, holders on other processes do not block. It can also not be granted to a process that holds the
     * task as an active or standby task, because a single process must never run the same task twice. The exception is
     * an in-place conversion of THIS MEMBER's own standby task into a warm-up task.
     */
    private boolean isUnreleasedWarmupTask(TasksTupleWithEpochs memberAssignedTasks,
                                           String subtopologyId,
                                           Integer partitionId) {
        return Objects.equals(currentActiveTaskProcessId.apply(subtopologyId, partitionId), member.processId())
            || heldByAnotherMemberOnThisProcess(currentStandbyTaskProcessIds, memberAssignedTasks.standbyTasks(), subtopologyId, partitionId)
            || currentWarmupTaskProcessIds.apply(subtopologyId, partitionId).contains(member.processId());
    }

    /**
     * Checks whether the task is run as an active task on this member's process by a member other than this one. The
     * member's own active task is a candidate for an in-place role change rather than a blocker.
     */
    private boolean runByAnotherMemberOnThisProcess(TasksTupleWithEpochs memberAssignedTasks,
                                                    String subtopologyId,
                                                    Integer partitionId) {
        return Objects.equals(currentActiveTaskProcessId.apply(subtopologyId, partitionId), member.processId())
            && !memberAssignedTasks.activeTasksWithEpochs().getOrDefault(subtopologyId, Map.of()).containsKey(partitionId);
    }

    /**
     * Checks whether the task is held in the given replica role on this member's process by a member other than this
     * one. A task the member holds itself is a candidate for an in-place role change rather than a blocker.
     *
     * @param currentProcessIds The process IDs currently holding the task in that role.
     * @param memberTasks       The tasks this member holds in that role.
     */
    private boolean heldByAnotherMemberOnThisProcess(BiFunction<String, Integer, Set<String>> currentProcessIds,
                                                     Map<String, Set<Integer>> memberTasks,
                                                     String subtopologyId,
                                                     Integer partitionId) {
        return currentProcessIds.apply(subtopologyId, partitionId).contains(member.processId())
            && !memberTasks.getOrDefault(subtopologyId, Set.of()).contains(partitionId);
    }

    /**
     * Demotes the member's active tasks to standby tasks in place. The active task remains pending revocation -- the
     * group must observe its release before its next owner can run it -- but the standby task is granted right away,
     * so the client recycles the task's state store instead of closing it and restoring a new standby task from the
     * changelog (cf. KAFKA-9501).
     *
     * @param memberAssignedTasks               The tasks this member currently holds.
     * @param newStandbyTasksPendingAssignment  Modified in place: the demoted task is removed, as the standby is granted
     *                                          in this step rather than left pending.
     * @param newStandbyAssignedTasks           Modified in place: the demoted task is added as an assigned standby.
     */
    private static void demoteActiveTasksToStandby(TasksTupleWithEpochs memberAssignedTasks,
                                                   Map<String, Set<Integer>> newStandbyTasksPendingAssignment,
                                                   Map<String, Set<Integer>> newStandbyAssignedTasks) {
        for (Map.Entry<String, Map<Integer, Integer>> activeTasks : memberAssignedTasks.activeTasksWithEpochs().entrySet()) {
            String subtopologyId = activeTasks.getKey();
            for (Integer partitionId : activeTasks.getValue().keySet()) {
                if (isPendingAssignment(newStandbyTasksPendingAssignment, subtopologyId, partitionId)) {
                    grantNow(newStandbyTasksPendingAssignment, newStandbyAssignedTasks, subtopologyId, partitionId);
                }
            }
        }
    }

    /**
     * Converts one of the member's replica roles into the other in place, i.e. a standby task into a warm-up task or
     * vice versa. Both roles run the same code on the client, so the conversion needs no client-side action at all
     * and hence no revocation ack either: the outgoing role is dropped in the same step in which the incoming role is
     * granted. This keeps the task on the member throughout, which is what the process-level exclusivity checks of
     * the other members rely on.
     *
     * @param memberOutgoingTasks                The tasks this member holds in the role it gives up.
     * @param newOutgoingTasksPendingRevocation  Modified in place: the converted task is removed, since the conversion
     *                                           needs no client-side revocation ack.
     * @param newIncomingTasksPendingAssignment  Modified in place: the converted task is removed, as it is granted in
     *                                           this step rather than left pending.
     * @param newIncomingAssignedTasks           Modified in place: the converted task is added, granting the new role.
     */
    private static void convertReplicaRole(Map<String, Set<Integer>> memberOutgoingTasks,
                                           Map<String, Set<Integer>> newOutgoingTasksPendingRevocation,
                                           Map<String, Set<Integer>> newIncomingTasksPendingAssignment,
                                           Map<String, Set<Integer>> newIncomingAssignedTasks) {
        for (Map.Entry<String, Set<Integer>> outgoingTasks : memberOutgoingTasks.entrySet()) {
            String subtopologyId = outgoingTasks.getKey();
            for (Integer partitionId : outgoingTasks.getValue()) {
                if (isPendingAssignment(newIncomingTasksPendingAssignment, subtopologyId, partitionId)) {
                    grantNow(newIncomingTasksPendingAssignment, newIncomingAssignedTasks, subtopologyId, partitionId);
                    removeFromTaskSet(newOutgoingTasksPendingRevocation, subtopologyId, partitionId);
                }
            }
        }
    }

    /**
     * Applies promotions to active to the freshly computed reconciliation result. A task the member holds as a standby
     * or warm-up that the target wants it to own as an active task is promoted in place rather than
     * revoked-then-reassigned, so its recyclable state store survives instead of being closed and restored from
     * the changelog (cf. KAFKA-9501). Such a standby or warm-up is never routed through the (ack-based) revocation path.
     * <p>
     * Must be called before the caller builds the member's new assigned tasks and pending revocation, because four of
     * the maps below are modified in place here rather than being returned.
     *
     * @param memberAssignedTasks              The tasks this member currently holds; the standby and warm-up sets are
     *                                         the promotion candidates.
     * @param newActiveTasksPendingRevocation  Not modified; passed through into the returned pending revocation.
     * @param newStandbyTasksPendingRevocation Modified in place: promoted standby tasks are removed, since a promotion
     *                                         needs no client-side revocation ack.
     * @param newWarmupTasksPendingRevocation  Modified in place: as above, for warm-up tasks.
     * @param newActiveTasksPendingAssignment  Not modified; consulted to see whether the active task is granted in
     *                                         this step, which decides whether the standby or warm-up is kept.
     * @param newStandbyAssignedTasks          Modified in place: a standby whose promotion is not granted in this step
     *                                         is added back, so the member keeps it for now.
     * @param newWarmupAssignedTasks           Modified in place: as above, for warm-up tasks.
     * @return the member's pending revocation after the promoted standby and warm-up tasks have been removed from it.
     */
    private TasksTupleWithEpochs applyPromotionsToActive(TasksTupleWithEpochs memberAssignedTasks,
                                                         Map<String, Map<Integer, Integer>> newActiveTasksPendingRevocation,
                                                         Map<String, Set<Integer>> newStandbyTasksPendingRevocation,
                                                         Map<String, Set<Integer>> newWarmupTasksPendingRevocation,
                                                         Map<String, Map<Integer, Integer>> newActiveTasksPendingAssignment,
                                                         Map<String, Set<Integer>> newStandbyAssignedTasks,
                                                         Map<String, Set<Integer>> newWarmupAssignedTasks) {
        // If we promote a standby or warm-up to active, that task does not need an explicit client-side revocation.
        // Thus, it can be removed from the revocation set -- we don't expect a client ack back. We need to remove it
        // regardless of whether the promotion happens now, or is still pending on the active task revocation.
        dropPromotedTasksFromRevocation(memberAssignedTasks.standbyTasks(), newStandbyTasksPendingRevocation);
        dropPromotedTasksFromRevocation(memberAssignedTasks.warmupTasks(), newWarmupTasksPendingRevocation);

        TasksTupleWithEpochs newTasksPendingRevocation = new TasksTupleWithEpochs(
            newActiveTasksPendingRevocation,
            newStandbyTasksPendingRevocation,
            newWarmupTasksPendingRevocation
        );
        boolean hasTasksToBeRevoked = !newTasksPendingRevocation.isEmpty()
            && hasNotReleased(newTasksPendingRevocation);

        // keep the standby or warm-up task and don't revoke it if we are not ready for the promotion to active
        //  - this member still needs to complete its own revocation of other tasks
        //    (which must complete before any assignment can happen)
        //  - the active task was not released by its previous owner yet

        // keep standby tasks on this member and wait on active to be released, for later in-place promotion
        keepPromotedTasksUntilGranted(
            memberAssignedTasks.standbyTasks(),
            newStandbyAssignedTasks,
            newActiveTasksPendingAssignment,
            hasTasksToBeRevoked
        );
        // keep warm-up tasks on this member and wait on active to be released, for later in-place promotion
        keepPromotedTasksUntilGranted(
            memberAssignedTasks.warmupTasks(),
            newWarmupAssignedTasks,
            newActiveTasksPendingAssignment,
            hasTasksToBeRevoked
        );

        return newTasksPendingRevocation;
    }

    /**
     * Takes the tasks that are being promoted to active out of the revocation set of the role they are promoted from.
     */
    private void dropPromotedTasksFromRevocation(Map<String, Set<Integer>> memberTasks,
                                                 Map<String, Set<Integer>> newTasksPendingRevocation) {
        for (Map.Entry<String, Set<Integer>> standbyOrWarmup : memberTasks.entrySet()) {
            String subtopologyId = standbyOrWarmup.getKey();
            Set<Integer> targetActiveTasks = targetAssignment.activeTasks().getOrDefault(subtopologyId, Set.of());
            for (Integer partitionId : standbyOrWarmup.getValue()) {
                if (targetActiveTasks.contains(partitionId)) {
                    removeFromTaskSet(newTasksPendingRevocation, subtopologyId, partitionId);
                }
            }
        }
    }

    /**
     * Keeps the tasks that are being promoted to active assigned in the role they are promoted from, for as long as
     * the active task is not granted in this step.
     */
    private void keepPromotedTasksUntilGranted(Map<String, Set<Integer>> memberTasks,
                                               Map<String, Set<Integer>> newAssignedTasks,
                                               Map<String, Map<Integer, Integer>> newActiveTasksPendingAssignment,
                                               boolean hasTasksToBeRevoked) {
        for (Map.Entry<String, Set<Integer>> standbyOrWarmup : memberTasks.entrySet()) {
            String subtopologyId = standbyOrWarmup.getKey();
            Set<Integer> targetActiveTasks = targetAssignment.activeTasks().getOrDefault(subtopologyId, Set.of());
            Map<Integer, Integer> grantedActiveTasks = newActiveTasksPendingAssignment.getOrDefault(subtopologyId, Map.of());
            for (Integer partitionId : standbyOrWarmup.getValue()) {
                boolean promotedThisStep = !hasTasksToBeRevoked && grantedActiveTasks.containsKey(partitionId);
                if (targetActiveTasks.contains(partitionId) && !promotedThisStep) {
                    newAssignedTasks.computeIfAbsent(subtopologyId, __ -> new HashSet<>()).add(partitionId);
                }
            }
        }
    }

    private static boolean isPendingAssignment(Map<String, Set<Integer>> tasksPendingAssignment,
                                               String subtopologyId,
                                               Integer partitionId) {
        return tasksPendingAssignment.getOrDefault(subtopologyId, Set.of()).contains(partitionId);
    }

    /**
     * Grants a task in this step instead of leaving it pending, by moving it from the pending assignment to the
     * assigned tasks.
     */
    private static void grantNow(Map<String, Set<Integer>> tasksPendingAssignment,
                                 Map<String, Set<Integer>> assignedTasks,
                                 String subtopologyId,
                                 Integer partitionId) {
        removeFromTaskSet(tasksPendingAssignment, subtopologyId, partitionId);
        assignedTasks.computeIfAbsent(subtopologyId, __ -> new HashSet<>()).add(partitionId);
    }

    /**
     * Removes a single subtopology/partition from a task set, dropping the subtopology entry entirely if it
     * becomes empty (so the resulting map matches what the difference helpers would have produced).
     */
    private static void removeFromTaskSet(Map<String, Set<Integer>> tasks, String subtopologyId, Integer partitionId) {
        Set<Integer> partitions = tasks.get(subtopologyId);
        if (partitions != null && partitions.remove(partitionId) && partitions.isEmpty()) {
            tasks.remove(subtopologyId);
        }
    }

    /**
     * Checks whether the member has not yet confirmed the release of the given tasks pending revocation.
     * Revocation is ack-based: it is confirmed only once the member reports its currently owned tasks in a
     * heartbeat and none of the given tasks appear among them.
     *
     * @param tasksPendingRevocation The tasks whose revocation we are waiting for.
     * @return true if the release of any of those tasks is not yet confirmed.
     */
    private boolean hasNotReleased(TasksTupleWithEpochs tasksPendingRevocation) {
        // Note that {@code ownedTasks} being empty means the member did not report its owned tasks in this
        // heartbeat -- it is an {@link Optional}, not an empty task set. Without such a report we cannot prove
        // the tasks were released, so we conservatively treat them as still held.
        return ownedTasks.isEmpty() || ownedTasks.get().containsAny(tasksPendingRevocation);
    }

    private StreamsGroupMember buildNewMember(final int memberEpoch,
                                              final TasksTupleWithEpochs newTasksPendingRevocation,
                                              final TasksTupleWithEpochs newAssignedTasks,
                                              final TasksTupleWithEpochs newTasksPendingAssignment,
                                              final boolean hasUnreleasedTasks) {

        final boolean hasTasksToBeRevoked =
            !newTasksPendingRevocation.isEmpty()
                && hasNotReleased(newTasksPendingRevocation);

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
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();
        } else if (hasUnreleasedTasks) {
            // If there are no tasks to be revoked nor to be assigned but some
            // tasks are not available yet, the member transitions to the target
            // epoch, to the UNRELEASED_TASKS state and waits.
            return new StreamsGroupMember.Builder(member)
                .setState(MemberState.UNRELEASED_TASKS)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedTasks(newAssignedTasks)
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();
        } else {
            // Otherwise, the member transitions to the target epoch and to the
            // STABLE state.
            return new StreamsGroupMember.Builder(member)
                .setState(MemberState.STABLE)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedTasks(newAssignedTasks)
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();
        }
    }
}
