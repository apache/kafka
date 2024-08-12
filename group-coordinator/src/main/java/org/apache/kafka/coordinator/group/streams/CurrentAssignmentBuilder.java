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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

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
     * A function which returns the current epoch of a topic-partition or -1 if the topic-partition is not assigned. The current epoch is
     * the epoch of the current owner.
     */
    private BiFunction<String, Integer, Integer> currentTaskEpoch;

    /**
     * The active tasks owned by the streams. This is directly provided by the member in the StreamsHeartbeat request.
     */
    private List<StreamsGroupHeartbeatRequestData.TaskIds> ownedActiveTasks;

    /**
     * The standby tasks owned by the streams. This is directly provided by the member in the StreamsHeartbeat request.
     */
    private List<StreamsGroupHeartbeatRequestData.TaskIds> ownedStandbyTasks;

    /**
     * The warmup tasks owned by the streams. This is directly provided by the member in the StreamsHeartbeat request.
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
     * Sets a BiFunction which allows to retrieve the current epoch of a partition. This is used by the state machine to determine if a
     * partition is free or still used by another member.
     *
     * @param currentTaskEpoch A BiFunction which gets the epoch of a topic id / tasks id pair.
     * @return This object.
     */
    public CurrentAssignmentBuilder withCurrentActiveTaskEpoch(
        BiFunction<String, Integer, Integer> currentTaskEpoch
    ) {
        this.currentTaskEpoch = Objects.requireNonNull(currentTaskEpoch);
        return this;
    }

    /**
     * Sets the active tasks currently owned by the member. This comes directly from the last StreamsHeartbeat request. This is used to
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
     * Sets the standby tasks currently owned by the member. This comes directly from the last StreamsHeartbeat request. This is used to
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
     * Sets the warmup tasks currently owned by the member. This comes directly from the last StreamsHeartbeat request. This is used to
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
                // owned tasks set in the StreamsHeartbeat API.

                // If the member provides its owned tasks. We verify if it still
                // owns any of the revoked tasks. If it does, we cannot progress.
                if (ownsRevokedTasks(member.activeTasksPendingRevocation())) {
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
                if (ownedActiveTasks == null || !ownedActiveTasks.isEmpty()) {
                    throw new FencedMemberEpochException("The streams group member is in a unknown state. "
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
     * Decides whether the current ownedTopicTasks contains any partition that is pending revocation.
     *
     * @param assignment The assignment that has the tasks pending revocation.
     * @return A boolean based on the condition mentioned above.
     */
    private boolean ownsRevokedTasks(
        Map<String, Set<Integer>> assignment
    ) {
        if (ownedActiveTasks == null) {
            return true;
        }

        for (StreamsGroupHeartbeatRequestData.TaskIds activeTasks : ownedActiveTasks) {
            Set<Integer> tasksPendingRevocation =
                assignment.getOrDefault(activeTasks.subtopology(), Collections.emptySet());

            for (Integer partitionId : activeTasks.partitions()) {
                if (tasksPendingRevocation.contains(partitionId)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Computes the next assignment.
     *
     * @param memberEpoch                The epoch of the member to use. This may be different from the epoch in
     *                                   {@link CurrentAssignmentBuilder#member}.
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
        boolean hasUnreleasedTasks = false;
        Map<String, Set<Integer>> newAssignedTasks = new HashMap<>();
        Map<String, Set<Integer>> newTasksPendingRevocation = new HashMap<>();
        Map<String, Set<Integer>> newTasksPendingAssignment = new HashMap<>();

        Set<String> allSubtopologyIds = new HashSet<>(targetAssignment.activeTasks().keySet());
        allSubtopologyIds.addAll(memberAssignedActiveTasks.keySet());

        for (String subtopologyId : allSubtopologyIds) {
            Set<Integer> target = targetAssignment.activeTasks()
                .getOrDefault(subtopologyId, Collections.emptySet());
            Set<Integer> currentAssignedTasks = memberAssignedActiveTasks
                .getOrDefault(subtopologyId, Collections.emptySet());

            // New Assigned Tasks = Previous Assigned Tasks ∩ Target
            Set<Integer> assignedTasks = new HashSet<>(currentAssignedTasks);
            assignedTasks.retainAll(target);

            // Tasks Pending Revocation = Previous Assigned Tasks - New Assigned Tasks
            Set<Integer> tasksPendingRevocation = new HashSet<>(currentAssignedTasks);
            tasksPendingRevocation.removeAll(assignedTasks);

            // Tasks Pending Assignment = Target - New Assigned Tasks - Unreleased Tasks
            Set<Integer> tasksPendingAssignment = new HashSet<>(target);
            tasksPendingAssignment.removeAll(assignedTasks);
            hasUnreleasedTasks = tasksPendingAssignment.removeIf(partitionId ->
                currentTaskEpoch.apply(subtopologyId, partitionId) != -1
            ) || hasUnreleasedTasks;

            if (!assignedTasks.isEmpty()) {
                newAssignedTasks.put(subtopologyId, assignedTasks);
            }

            if (!tasksPendingRevocation.isEmpty()) {
                newTasksPendingRevocation.put(subtopologyId, tasksPendingRevocation);
            }

            if (!tasksPendingAssignment.isEmpty()) {
                newTasksPendingAssignment.put(subtopologyId, tasksPendingAssignment);
            }
        }

        if (!newTasksPendingRevocation.isEmpty() && ownsRevokedTasks(newTasksPendingRevocation)) {
            // If there are tasks to be revoked, the member remains in its current
            // epoch and requests the revocation of those tasks. It transitions to
            // the UNREVOKED_TASKS state to wait until the client acknowledges the
            // revocation of the tasks.
            return new StreamsGroupMember.Builder(member)
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
                .updateMemberEpoch(memberEpoch)
                .setAssignedActiveTasks(newAssignedTasks)
                .setActiveTasksPendingRevocation(newTasksPendingRevocation)
                .setAssignedStandbyTasks(memberAssignedStandbyTasks)
                .setAssignedWarmupTasks(memberAssignedWarmupTasks)
                .build();
        } else if (!newTasksPendingAssignment.isEmpty()) {
            // If there are tasks to be assigned, the member transitions to the
            // target epoch and requests the assignment of those tasks. Note that
            // the tasks are directly added to the assigned tasks set. The
            // member transitions to the STABLE state or to the UNRELEASED_TASKS
            // state depending on whether there are unreleased tasks or not.
            newTasksPendingAssignment.forEach((subtopologyId, tasks) -> newAssignedTasks
                .computeIfAbsent(subtopologyId, __ -> new HashSet<>())
                .addAll(tasks));
            org.apache.kafka.coordinator.group.streams.MemberState newState =
                hasUnreleasedTasks ? org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS
                    : org.apache.kafka.coordinator.group.streams.MemberState.STABLE;
            return new StreamsGroupMember.Builder(member)
                .setState(newState)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedActiveTasks(newAssignedTasks)
                .setActiveTasksPendingRevocation(Collections.emptyMap())
                .setAssignedStandbyTasks(memberAssignedStandbyTasks)
                .setAssignedWarmupTasks(memberAssignedWarmupTasks)
                .build();
        } else if (hasUnreleasedTasks) {
            // If there are no tasks to be revoked nor to be assigned but some
            // tasks are not available yet, the member transitions to the target
            // epoch, to the UNRELEASED_TASKS state and waits.
            return new StreamsGroupMember.Builder(member)
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedActiveTasks(newAssignedTasks)
                .setActiveTasksPendingRevocation(Collections.emptyMap())
                .setAssignedStandbyTasks(memberAssignedStandbyTasks)
                .setAssignedWarmupTasks(memberAssignedWarmupTasks)
                .build();
        } else {
            // Otherwise, the member transitions to the target epoch and to the
            // STABLE state.
            return new StreamsGroupMember.Builder(member)
                .setState(MemberState.STABLE)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedActiveTasks(newAssignedTasks)
                .setActiveTasksPendingRevocation(Collections.emptyMap())
                .setAssignedStandbyTasks(memberAssignedStandbyTasks)
                .setAssignedWarmupTasks(memberAssignedWarmupTasks)
                .build();
        }
    }
}
