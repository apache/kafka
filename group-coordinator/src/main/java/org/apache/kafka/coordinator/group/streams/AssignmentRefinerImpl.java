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

import org.apache.kafka.coordinator.group.streams.assignor.TaskId;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * The {@link AssignmentRefiner} being built out to replace {@link NoOpAssignmentRefiner} as the broker's default
 * once the derivation is complete.
 *
 * <p>{@link #refine} is still a stub -- it returns the target assignment unchanged, exactly like
 * {@link NoOpAssignmentRefiner} -- while the derivation is built out incrementally across several changes. The
 * methods below are its building blocks: indexing the current assignment, and deciding which migrations can
 * complete immediately versus which have to stage behind a warm-up task. None of them are called from
 * {@link #refine} yet.
 */
public class AssignmentRefinerImpl implements AssignmentRefiner {

    @Override
    public Map<String, TasksTuple> refine(
        Map<String, StreamsGroupMember> members,
        Map<String, TasksTuple> targetAssignment,
        Map<String, MemberTaskOffsets> taskOffsets,
        SortedMap<String, ConfiguredSubtopology> subtopologies,
        int numWarmupReplicas,
        long acceptableRecoveryLag
    ) {
        return targetAssignment;
    }

    /**
     * Indexes the members' current assignment by task, so that the case analysis can look up what a task's situation is
     * without scanning the group again for every task. This is a single pass over the members' task entries.
     *
     * <p>Only stateful tasks are indexed. A stateless task has no state to restore, so it is never staged and never
     * consulted here; it simply flows through from the target assignment.
     *
     * @param members
     *        All members of the group.
     * @param taskOffsets
     *        The latest changelog offsets/end-offsets reported by the members.
     * @param subtopologies
     *        The resolved subtopologies, which tell whether a subtopology is stateful.
     * @param acceptableRecoveryLag
     *        The lag at or below which a replica counts as caught up.
     *
     * @return The current assignment, indexed by task.
     */
    static CurrentAssignmentIndex indexCurrentAssignment(
        final Map<String, StreamsGroupMember> members,
        final Map<String, MemberTaskOffsets> taskOffsets,
        final SortedMap<String, ConfiguredSubtopology> subtopologies,
        final long acceptableRecoveryLag
    ) {
        final Map<TaskId, String> activeOwner = new HashMap<>();
        final Map<TaskId, Set<String>> processesRevoking = new HashMap<>();
        final Map<TaskId, List<TaskCopy>> taskCopies = new HashMap<>();

        for (final StreamsGroupMember member : members.values()) {
            final MemberTaskOffsets offsets = taskOffsets.getOrDefault(member.memberId(), MemberTaskOffsets.EMPTY);

            forEachStatefulActiveTask(
                member.assignedTasks().activeTasksWithEpochs(),
                subtopologies,
                task -> activeOwner.put(task, member.memberId())
            );

            // The member has been told to give this task up and has stopped running it, so the member is
            // deliberately not recorded as the task's active owner. Recording the member as the owner would make the
            // case analysis try to keep the task there, undoing a hand-over that is already under way.
            //
            // The task does still occupy the member's process until the revocation completes, and that is what stops
            // a standby of the same task being placed there. The block applies to the whole process, not just this
            // one member, so the process is what gets recorded.
            forEachStatefulActiveTask(
                member.tasksPendingRevocation().activeTasksWithEpochs(),
                subtopologies,
                task -> processesRevoking.computeIfAbsent(task, __ -> new HashSet<>()).add(member.processId())
            );

            forEachStatefulTask(
                member.assignedTasks().standbyTasks(),
                subtopologies,
                task -> addTaskCopy(taskCopies, task, member, TaskRole.STANDBY, offsets, acceptableRecoveryLag)
            );

            forEachStatefulTask(
                member.assignedTasks().warmupTasks(),
                subtopologies,
                task -> addTaskCopy(taskCopies, task, member, TaskRole.WARMUP, offsets, acceptableRecoveryLag)
            );
        }

        return new CurrentAssignmentIndex(activeOwner, processesRevoking, taskCopies);
    }

    /**
     * Decides, for every stateful task whose active role is not already where the target assignment wants it, whether
     * the migration has to be staged behind a warm-up task or can be completed in this step.
     *
     * <p>A task is <b>staged</b> only when all of the following hold: somebody runs it today, the target owner is
     * somebody else, and there is a warming improvement left to achieve. Everything else completes right away, which
     * needs no patch at all -- the target assignment already places the task on its target owner, and the previous
     * owner's slice already omits it. That is why the two outcomes are so lopsided: staging is the exception, and the
     * result is proportional to how far the current assignment has diverged from the target rather than to the group's
     * size.
     *
     * @param currentAssignment
     *        The indexed current assignment, from {@link #indexCurrentAssignment}.
     * @param targetAssignment
     *        All members' target assignments, as computed by the task assignor.
     * @param members
     *        All members of the group, used to resolve which process a member runs in.
     * @param subtopologies
     *        The resolved subtopologies, which tell whether a subtopology is stateful.
     *
     * @return What was decided for the tasks that are not already in place.
     */
    static TaskDecisions analyzeTasks(
        final CurrentAssignmentIndex currentAssignment,
        final Map<String, TasksTuple> targetAssignment,
        final Map<String, StreamsGroupMember> members,
        final SortedMap<String, ConfiguredSubtopology> subtopologies
    ) {
        // Only a task the target assignment still contains needs a decision, so its owners alone drive the loop. A
        // task the target assignment dropped -- after a topology change, say -- belongs in nobody's slice, and its
        // current holders revoke it the ordinary way, so walking the current assignment's tasks too would only turn
        // up tasks to skip.
        //
        // The map is sorted, which gives the canonical task order that makes a derivation reproducible and leaves the
        // budget pass that follows a deterministic tie-break to fall back on.
        final SortedMap<TaskId, String> targetOwners = statefulActiveOwners(targetAssignment, subtopologies);

        final List<StagedMigration> stagedMigrations = new ArrayList<>();
        final List<TaskGrant> grantedTasks = new ArrayList<>();

        for (final Map.Entry<TaskId, String> targetOwnerByTask : targetOwners.entrySet()) {
            final TaskId task = targetOwnerByTask.getKey();
            final String targetOwner = targetOwnerByTask.getValue();

            final String currentOwner = currentAssignment.activeOwner().get(task);
            if (targetOwner.equals(currentOwner)) {
                // The task already runs where it belongs.
                continue;
            }

            // The target assignment can still name a member the group has already removed: it is only recomputed when
            // the assignor runs again, which the assignment interval can defer, and a member can be fenced in the
            // meantime. Such a member cannot restore anything, so nothing may be staged into it.
            final StreamsGroupMember targetMember = members.get(targetOwner);
            if (targetMember == null) {
                if (currentOwner != null) {
                    // Keep the task where it runs until the assignor names a member that still exists. Leaving it out
                    // instead would make its current owner revoke it, so it would stop being processed for no gain.
                    stagedMigrations.add(new StagedMigration(
                        task,
                        currentOwner,
                        targetOwner,
                        Optional.empty(),
                        Optional.empty()
                    ));
                }
                // A task that nobody runs and whose target owner is gone is left to the next assignor run: granting it
                // to a member that is no longer in the group would achieve nothing.
                continue;
            }

            final String targetProcessId = targetMember.processId();

            // The task moves now, for either of two reasons. Nobody runs it -- it is new, its owner left, or a
            // hand-over is in flight and the previous owner has already released it -- so there is no running task to
            // protect and the target owner takes it even cold; preferring a warmer owner would be a placement
            // decision, and placement is the assignor's job. Or somebody runs it but no achievable warming improvement
            // remains. The current owner, when there is one, is necessarily a member of the group, because the index it
            // comes from was built from the members themselves.
            if (currentOwner == null
                || isReady(currentAssignment, task, members.get(currentOwner).processId(), targetProcessId)) {
                grantedTasks.add(new TaskGrant(task, targetOwner));
                continue;
            }

            stagedMigrations.add(new StagedMigration(
                task,
                currentOwner,
                targetOwner,
                Optional.of(targetProcessId),
                findCopyOnProcess(currentAssignment, task, targetProcessId)
            ));
        }

        return new TaskDecisions(List.copyOf(stagedMigrations), List.copyOf(grantedTasks));
    }

    /**
     * Whether no achievable warming improvement remains for handing the task over to its target owner.
     *
     * <p>This is deliberately weaker than "the target owner has caught up". Warming up is only worth staging when it
     * can actually shorten the hand-over, and there are two situations where it cannot -- one per clause of the
     * predicate:
     * <ul>
     *     <li><b>The task is moving between two members of one process.</b> A process must not hold the same task
     *     twice, so there is no way to warm the target owner up while the current owner still runs it. No condition
     *     on the state applies here: staging such a move would park it forever, so it has to count as ready.</li>
     *     <li><b>The target owner's process already holds a caught-up copy of the task.</b> Here <em>caught up</em>
     *     carries the weight: a copy that is still catching up leaves a genuine improvement to wait for, so the task
     *     stays staged, that copy keeps consuming, and a later step grants the task once the copy is hot. Planting a
     *     warm-up task on the target owner is no help either way, because its process would then hold the task
     *     twice. The copy sits either on the target owner itself, which promotes it in place, or on a sibling
     *     member, which has to release the task first so that the target owner can reopen it from the state
     *     directory.</li>
     * </ul>
     *
     * <p>The two are not variants of one another, even though both turn on members sharing a process. The first is
     * about the <em>current owner</em> sharing one with the target owner; the second about some <em>copy holder</em>
     * doing so. They also cannot both apply: if the current owner is on the target owner's process then that process
     * runs the task, so by one-task-per-process it holds no copy of the task for the second clause to find. The
     * second therefore only ever decides a move that crosses process boundaries, and the order the two are tested in
     * makes no difference to the outcome.
     *
     * <p>Only the in-place promotion is warm for every store type. The other two paths -- the move within one
     * process, and the sibling releasing the task -- are warm only for a store that persists to disk, where the
     * releasing member's clean close leaves a checkpoint behind for the incoming member to reopen from. <b>An
     * in-memory store is rebuilt from the changelog in full:</b> its state lives on the releasing member's heap and
     * is dropped when the task closes, and no hand-over of a running task between threads of one process exists to
     * carry it across. Worse, the lag that made the task look ready was measured on the member that is about to
     * close, so for an in-memory store it says nothing about what the incoming member then has to restore. This
     * predicate cannot fix that; it would take a client-side cross-thread task hand-over. The broker cannot even see
     * the difference, because the topology metadata carries changelog topics but not how a store is backed.
     *
     * <p>What bounds the damage is that a warm-up task the refiner plants always targets the target owner itself, so
     * every migration the refiner stages resolves through the in-place promotion. The other paths arise only out of a
     * layout the refiner inherited.
     */
    private static boolean isReady(
        final CurrentAssignmentIndex currentAssignment,
        final TaskId task,
        final String currentProcessId,
        final String targetProcessId
    ) {
        if (targetProcessId.equals(currentProcessId)) {
            return true;
        }
        return currentAssignment.taskCopies().getOrDefault(task, List.of()).stream()
            .anyMatch(holder -> holder.processId().equals(targetProcessId) && holder.caughtUp());
    }

    /**
     * Whether the member has restored the task closely enough to take it over as an active task. Mirrors the client's
     * own predicate, so that both ends agree on when a warm-up task is caught up.
     *
     * <p>The lag is the distance between the reported end offset and the reported offset, and a lag that is not known
     * is never within the threshold: an offset missing on either side, or capped at {@link Long#MAX_VALUE} to say that
     * the restore has not started, counts as not caught up. A slightly negative lag does count, because the offset is a
     * position while the end offset is the last offset, so a fully restored task reports a lag of -1.
     *
     * @param memberTaskOffsets
     *        The offsets the member reported, {@link MemberTaskOffsets#EMPTY} if it reported none.
     * @param task
     *        The task to check.
     * @param acceptableRecoveryLag
     *        The lag at or below which the task counts as caught up.
     */
    static boolean isCaughtUp(
        final MemberTaskOffsets memberTaskOffsets,
        final TaskId task,
        final long acceptableRecoveryLag
    ) {
        final Long offset = offsetOf(memberTaskOffsets.taskOffsets(), task);
        final Long endOffset = offsetOf(memberTaskOffsets.taskEndOffsets(), task);
        if (offset == null || endOffset == null || offset == Long.MAX_VALUE || endOffset == Long.MAX_VALUE) {
            return false;
        }
        return endOffset - offset <= acceptableRecoveryLag;
    }

    private static Long offsetOf(final Map<String, Map<Integer, Long>> offsets, final TaskId task) {
        final Map<Integer, Long> byPartition = offsets.get(task.subtopologyId());
        return byPartition == null ? null : byPartition.get(task.partition());
    }

    /**
     * The replica of the task that the given process already holds, if any.
     *
     * <p>There is at most one, so no tie-break between roles is needed: a process holds a given task in at most one
     * role, on at most one of its members. The reconciler enforces that -- {@code isUnreleasedActiveTask},
     * {@code isUnreleasedStandbyTask} and {@code isUnreleasedWarmupTask} in {@link CurrentAssignmentBuilder} each
     * block a role for as long as the process holds the task in any role.
     */
    private static Optional<TaskCopy> findCopyOnProcess(
        final CurrentAssignmentIndex currentAssignment,
        final TaskId task,
        final String processId
    ) {
        return currentAssignment.taskCopies().getOrDefault(task, List.of()).stream()
            .filter(holder -> holder.processId().equals(processId))
            .findFirst();
    }

    /**
     * Inverts the target assignment into a lookup from stateful task to the member that is to run it as an active task.
     */
    private static SortedMap<TaskId, String> statefulActiveOwners(
        final Map<String, TasksTuple> targetAssignment,
        final SortedMap<String, ConfiguredSubtopology> subtopologies
    ) {
        final SortedMap<TaskId, String> owners = new TreeMap<>();
        targetAssignment.forEach((memberId, tasks) ->
            forEachStatefulTask(tasks.activeTasks(), subtopologies, task -> owners.put(task, memberId)));
        return owners;
    }

    private static void addTaskCopy(
        final Map<TaskId, List<TaskCopy>> taskCopies,
        final TaskId task,
        final StreamsGroupMember member,
        final TaskRole role,
        final MemberTaskOffsets offsets,
        final long acceptableRecoveryLag
    ) {
        taskCopies.computeIfAbsent(task, __ -> new ArrayList<>()).add(new TaskCopy(
            member.memberId(),
            member.processId(),
            role,
            isCaughtUp(offsets, task, acceptableRecoveryLag)
        ));
    }

    private static void forEachStatefulActiveTask(
        final Map<String, Map<Integer, Integer>> activeTasksWithEpochs,
        final SortedMap<String, ConfiguredSubtopology> subtopologies,
        final Consumer<TaskId> action
    ) {
        activeTasksWithEpochs.forEach((subtopologyId, partitionsWithEpochs) -> {
            if (isStateful(subtopologies, subtopologyId)) {
                partitionsWithEpochs.keySet()
                    .forEach(partitionId -> action.accept(new TaskId(subtopologyId, partitionId)));
            }
        });
    }

    private static void forEachStatefulTask(
        final Map<String, Set<Integer>> tasks,
        final SortedMap<String, ConfiguredSubtopology> subtopologies,
        final Consumer<TaskId> action
    ) {
        tasks.forEach((subtopologyId, partitionIds) -> {
            if (isStateful(subtopologies, subtopologyId)) {
                partitionIds.forEach(partitionId -> action.accept(new TaskId(subtopologyId, partitionId)));
            }
        });
    }

    private static boolean isStateful(
        final SortedMap<String, ConfiguredSubtopology> subtopologies,
        final String subtopologyId
    ) {
        final ConfiguredSubtopology subtopology = subtopologies.get(subtopologyId);
        return subtopology != null && !subtopology.stateChangelogTopics().isEmpty();
    }

    /**
     * The members' current assignment, indexed by task. Only stateful tasks appear.
     *
     * @param activeOwner
     *        The member running each task as an active task. A task that only sits in some member's pending revocation
     *        has no entry here, because an in-flight removal is a decision that has already been taken rather than a
     *        placement to preserve.
     * @param processesRevoking
     *        The processes that were told to revoke each task's active role. Recorded per process rather than per
     *        member because what matters about it is physical ownership: it blocks a standby task of the same task
     *        anywhere on that process.
     * @param taskCopies
     *        The standby and warm-up holders of each task.
     */
    record CurrentAssignmentIndex(
        Map<TaskId, String> activeOwner,
        Map<TaskId, Set<String>> processesRevoking,
        Map<TaskId, List<TaskCopy>> taskCopies
    ) {
    }

    /**
     * A copy of a task that exists on some member: which member holds it, in which role, and whether that member has
     * restored it far enough to take the task over as an active task.
     *
     * <p>Only the {@link TaskRole#STANDBY} and {@link TaskRole#WARMUP} copies are recorded. The active copy is tracked
     * separately, as {@link CurrentAssignmentIndex#activeOwner()}.
     *
     * @param memberId
     *        The member the copy is on.
     * @param processId
     *        The process that member runs in.
     * @param role
     *        The role the member holds the task in.
     * @param caughtUp
     *        Whether the member's reported lag for the task is within the acceptable recovery lag.
     */
    record TaskCopy(String memberId, String processId, TaskRole role, boolean caughtUp) {
    }

    /**
     * A migration that this refinement step holds back: the task keeps running on its current owner instead of moving
     * to the member the target assignment wants it on.
     *
     * <p>Whether the target owner <em>also</em> gets a warm-up task, so that it restores the state in the background,
     * is a separate and later decision. The warm-up budget is finite, and a migration that cannot be funded is still
     * held back here -- the task simply waits on its current owner with nothing warming up, until a slot frees up.
     *
     * @param task
     *        The task being migrated.
     * @param currentOwner
     *        The member the task keeps running on for now.
     * @param targetOwner
     *        The member the target assignment moves the task to.
     * @param targetProcessId
     *        The process the target owner runs in, or empty if the target assignment names a member the group no longer
     *        has. Empty means the migration can never be warmed up and must not be given a warm-up task or a budget
     *        slot; the task just stays with its current owner until the assignor names a member that still exists.
     * @param copyOnTargetProcess
     *        The replica of the task that the target owner's process already holds, if any. When there is none, the
     *        migration is a candidate for a fresh warm-up task. When there is one, the process is already restoring the
     *        task and must not be handed a second copy of it.
     */
    record StagedMigration(
        TaskId task,
        String currentOwner,
        String targetOwner,
        Optional<String> targetProcessId,
        Optional<TaskCopy> copyOnTargetProcess
    ) {
    }

    /**
     * A task that is granted to its target owner in this refinement step, rather than held back: the intermediate
     * assignment says what the target assignment says for it.
     *
     * <p>That happens either because no achievable warming improvement remains -- the target owner's process already
     * holds the task's state, or the move is within a single process, where warming up is impossible -- or because
     * nobody runs the task at all, which covers a brand-new task as much as one whose owner departed. It notably does
     * <b>not</b> happen because the warm-up budget ran out: a migration that cannot be funded stays a
     * {@link StagedMigration} and its task keeps running on its current owner.
     *
     * <p>Granting is the refiner's decision that the hand-over may proceed, not the hand-over itself. The reconciler
     * still serializes it, so a task granted here can still spend a step in {@code UNRELEASED_TASKS} while its
     * previous owner revokes it.
     *
     * <p>The member that ran the task before, if any, is deliberately not recorded here: it is
     * {@link CurrentAssignmentIndex#activeOwner()} for the task, so repeating it would only be a second copy that
     * could disagree.
     *
     * @param task
     *        The task moving.
     * @param targetOwner
     *        The member the task moves to.
     */
    record TaskGrant(
        TaskId task,
        String targetOwner
    ) {
    }

    /**
     * What the case analysis decided for the tasks whose active role is not already where the target assignment wants
     * it.
     *
     * <p>Tasks that need no decision are deliberately not listed: neither the ones already in place, nor the ones the
     * target assignment no longer contains. That keeps the result proportional to how far the current assignment has
     * diverged from the target rather than to the group's task count.
     *
     * @param stagedMigrations
     *        The migrations held back behind a warm-up task, in canonical task order.
     * @param grantedTasks
     *        The tasks whose active role moves in this step, in canonical task order.
     */
    record TaskDecisions(
        List<StagedMigration> stagedMigrations,
        List<TaskGrant> grantedTasks
    ) {
    }
}
