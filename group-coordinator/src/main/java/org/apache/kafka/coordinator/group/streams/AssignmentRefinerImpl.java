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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Derives the intermediate assignment which is the target assignment with the migration of a stateful task held back
 * behind a warm-up task, so that the task keeps running on its current owner while its target owner restores the
 * state.
 *
 * <p>{@link #refine} returns the target assignment unchanged, like {@link NoOpAssignmentRefiner} for now,
 * because this class is WIP is not used yet.
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
     * <p>Only stateful tasks are indexed because a stateless task has no state to restore.
     *
     * <p>The index covers the tasks a member has been granted. A task the member has been told to give up is left
     * out, even though the member may still be running it until the revocation is acknowledged: recording that member
     * as the task's holder would make the case analysis try to keep the task there, undoing a hand-over that is
     * already under way. The process it occupies until the revocation completes needs no tracking either, because the
     * reconciler refuses to grant a role for a task the process still holds.
     *
     * @param members
     *        All members of the group.
     * @param taskOffsets
     *        The latest changelog offsets/end-offsets reported by the members.
     * @param subtopologies
     *        The resolved subtopologies, which tell whether a subtopology is stateful.
     * @param acceptableRecoveryLag
     *        The lag at or below which a copy counts as caught up.
     *
     * @return The current assignment, indexed by task.
     */
    static CurrentAssignmentIndex indexCurrentAssignment(
        final Map<String, StreamsGroupMember> members,
        final Map<String, MemberTaskOffsets> taskOffsets,
        final SortedMap<String, ConfiguredSubtopology> subtopologies,
        final long acceptableRecoveryLag
    ) {
        final Map<TaskId, ActiveHolder> activeHolder = new HashMap<>();
        final Map<TaskId, List<TaskCopy>> taskCopies = new HashMap<>();

        for (final StreamsGroupMember member : members.values()) {
            final MemberTaskOffsets offsets = taskOffsets.getOrDefault(member.memberId(), MemberTaskOffsets.EMPTY);

            forEachStatefulActiveTask(
                member.assignedTasks().activeTasksWithEpochs(),
                subtopologies,
                task -> activeHolder.put(task, new ActiveHolder(member.memberId(), !isRestoring(offsets, task)))
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

        return new CurrentAssignmentIndex(activeHolder, taskCopies);
    }

    /**
     * Whether the member is still restoring an active task, rather than processing it.
     *
     * <p>A client reports changelog offsets for every task it is restoring and stops reporting them once the task is
     * running, so a reported offset for an active task means the restore is still under way.
     *
     * <p><b>A member the coordinator has not heard from reports nothing, so its active tasks all read as running.</b>
     * That is every member right after a coordinator failover -- the reported offsets are in-memory state that does not
     * survive one -- and a newly joined member until its first report. It is the safe direction: the task is treated as
     * something to "protect", so at worst a migration is staged that could have been granted outright, and the next
     * report corrects it.
     */
    private static boolean isRestoring(final MemberTaskOffsets memberTaskOffsets, final TaskId task) {
        return offsetOf(memberTaskOffsets.taskOffsets(), task) != null;
    }

    /**
     * Indexes how loaded each process is, for the order in which the warmup budget pass funds warm-up tasks.
     * The load of a process is its stateful task count over the number of members it runs -- the same shape as the task
     * assignor's own {@code ProcessState.load()}, so that both layers rank processes comparably.
     *
     * <p><b>Only stateful tasks are counted.</b>
     *
     * <p>A process running nothing but stateless tasks therefore has a load of zero, which is the right answer:
     * the target assignment has already chosen every target owner, and this order only decides which of those
     * migrations is funded first, never where a task goes.
     *
     * <p>The count covers the tasks a member has been granted ({@link StreamsGroupMember#assignedTasks()}).
     * A task on its way out ({@link StreamsGroupMember#tasksPendingRevocation()}) is no part of the load.
     *
     * @param members
     *        All members of the group.
     * @param subtopologies
     *        The resolved subtopologies, which tell whether a subtopology is stateful.
     *
     * @return The load of every process running at least one member, indexed by process ID.
     */
    static Map<String, ProcessLoad> indexProcessLoad(
        final Map<String, StreamsGroupMember> members,
        final SortedMap<String, ConfiguredSubtopology> subtopologies
    ) {
        final Map<String, Integer> memberCounts = new HashMap<>();
        final Map<String, Integer> statefulTaskCounts = new HashMap<>();

        for (final StreamsGroupMember member : members.values()) {
            final String processId = member.processId();
            memberCounts.merge(processId, 1, Integer::sum);

            final Consumer<TaskId> count = task -> statefulTaskCounts.merge(processId, 1, Integer::sum);
            forEachStatefulActiveTask(member.assignedTasks().activeTasksWithEpochs(), subtopologies, count);
            forEachStatefulTask(member.assignedTasks().standbyTasks(), subtopologies, count);
            forEachStatefulTask(member.assignedTasks().warmupTasks(), subtopologies, count);
        }

        final Map<String, ProcessLoad> processLoad = new HashMap<>(memberCounts.size());
        memberCounts.forEach((processId, memberCount) ->
            processLoad.put(processId, new ProcessLoad(statefulTaskCounts.getOrDefault(processId, 0), memberCount)));
        return processLoad;
    }

    /**
     * Decides, for every stateful task which does not match its target assignment owner yet, whether
     * the migration has to be staged behind a warm-up task or can be completed in this step.
     *
     * <p>A task is <b>staged</b> only when all of the following hold: somebody is <em>processing</em> it today, the
     * target owner is somebody else, and the target is still not caught up. Everything else completes the task
     * migration right away (no target assignment patch needed).
     *
     * <p>A task is only staged if the old owner <em>processes</em> the task, but not when it's only restoring it.
     * For the restore case, we migrate the task to the target owner right away.
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
        // The map is sorted, which gives the canonical task order that makes a derivation reproducible and leaves the
        // warmup budget pass that follows a deterministic tie-break to fall back on.
        final SortedMap<TaskId, String> targetOwners = statefulActiveOwners(targetAssignment, subtopologies);

        final List<StagedMigration> stagedMigrations = new ArrayList<>();
        final List<TaskGrant> grantedTasks = new ArrayList<>();

        for (final Map.Entry<TaskId, String> targetOwnerByTask : targetOwners.entrySet()) {
            final TaskId task = targetOwnerByTask.getKey();
            final String targetOwner = targetOwnerByTask.getValue();

            final ActiveHolder holder = currentAssignment.activeHolder().get(task);
            if (holder != null && targetOwner.equals(holder.memberId())) {
                // The task is already on its target owner (either processing or still restoring)
                continue;
            }

            // Because of assignment offloading and member fencing, the target assignment could contain a member which
            // was removed from the group in the meantime. For this case, all previously owned tasks of this member
            // (which did not get move to a new owner) will be "dandling" which will be fixed by the next assignor run.
            // Furthermore, we stage all tasks the assignor moves to this member on their old owners to keep them
            // "online".
            final StreamsGroupMember targetMember = members.get(targetOwner);
            if (targetMember == null) {
                if (holder != null) {
                    stagedMigrations.add(new StagedMigration(
                        task,
                        holder.memberId(),
                        targetOwner,
                        Optional.empty(),
                        Optional.empty()
                    ));
                }
                continue;
            }

            final String targetProcessId = targetMember.processId();

            // The task moves now, for any of three reasons.
            //   1. Nobody holds it.
            //   2. Somebody holds it but is still restoring it.
            //   3. Somebody is processing it and the target owner is caught up
            if (holder == null
                || !holder.processing()
                || isReady(currentAssignment, task, members.get(holder.memberId()).processId(), targetProcessId)) {
                grantedTasks.add(new TaskGrant(task, targetOwner));
                continue;
            }

            stagedMigrations.add(new StagedMigration(
                task,
                holder.memberId(),
                targetOwner,
                Optional.of(targetProcessId),
                findCopyOnProcess(currentAssignment, task, targetProcessId)
            ));
        }

        return new TaskDecisions(List.copyOf(stagedMigrations), List.copyOf(grantedTasks));
    }

    /**
     * Whether a task is ready for migration to its target owner.
     *
     * <p>The general case is, the warmup is caught up on the target <b>member</b>, but there are two more cases we need
     * to consider:
     * <b>(1) The task is moving between two members of one process:</b> A process cannot hold two copies of a task at
     * the same time, so we cannot put a warmup but can only migrate the task right away.
     * <b>(2) The task is moved to a different process, but a sibling member of the target member holds a caught-up copy
     * of the task:</b> We migrate the task right away.
     *
     * <p>Both cases work out fine for persistent state store, but for in-memory stores we get a cold migration.
     * Closing that gap takes a client-side cross-thread task hand-over (https://issues.apache.org/jira/browse/KAFKA-21090).
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
     * Decides which of the staged migrations get a warm-up task, under the warmup budget.
     *
     * <p>There is different scenarios:
     * <ul>
     *     <li>A warm-up task already restoring keeps its warm-up slot if the target assignment didn't change, and the
     *     warm-up task is not caught up yet. It could also get revoked if the warmup budget was reduced and keeping
     *     the warmup would now exceed the budget.
     *     <li>A <b>fresh plant</b> puts a warm-up task on a target owner whose process holds nothing for the task,
     *     and spends a warm-up slot.</li>
     *     <li>When the target owner <em>itself</em> already holds a standby of the task we can <b>borrow</b> it,
     *     and no warmup budget is used: that standby warms-up the task anyway.</li>
     *     <li>If a target member's <em>sibling</em> hold a standby, we cannot borrow but, but need to move the
     *     standby to its new owner, and putting a warmup on the target member, spending a warm-up slot.
     *     (Cf case (2) of {@link #isReady(CurrentAssignmentIndex, TaskId, String, String)} </li>
     * </ul>
     *
     * <p>Everything else <b>parks</b> -- the task keeps running on its current owner with nothing warming up, and a
     * later refinement step picks it up once a warm-up slot frees.
     *
     * @param decisions
     *        What the case analysis decided, from {@link #analyzeTasks}.
     * @param members
     *        All members of the group, used to resolve which process a task's current owner runs in.
     * @param processLoad
     *        The load of each process, from {@link #indexProcessLoad}.
     * @param numWarmupReplicas
     *        How many copies beyond the target assignment may exist at once, group-wide.
     *
     * @return Which warm-up tasks the intermediate assignment places, and how each staged migration is warmed.
     */
    static WarmupPlan planWarmups(
        final TaskDecisions decisions,
        final Map<String, StreamsGroupMember> members,
        final Map<String, ProcessLoad> processLoad,
        final int numWarmupReplicas
    ) {
        if (numWarmupReplicas == 0) {
            return WarmupPlan.EMPTY;
        }

        final SortedMap<TaskId, String> warmupTasks = new TreeMap<>();
        final SortedSet<TaskId> borrowedMigrations = new TreeSet<>();
        final SortedSet<TaskId> parkedMigrations = new TreeSet<>();

        final List<FundingCandidate> keptWarmups = new ArrayList<>();
        final List<FundingCandidate> newWarmupCandidates = new ArrayList<>();

        for (final StagedMigration migration : decisions.stagedMigrations()) {
            final Warming warming = warmingOf(migration);
            switch (warming) {
                case PARK -> parkedMigrations.add(migration.task());
                case BORROW -> borrowedMigrations.add(migration.task());
                case KEEP -> keptWarmups.add(fundingCandidate(migration, members, warming));
                // Both put a warm-up task on the target owner and both cost a warm-up slot, so they share one
                // candidate list -- but a plant is funded ahead of a sibling move (see comparePriority).
                case PLANT, SIBLING_MOVE ->
                    newWarmupCandidates.add(fundingCandidate(migration, members, warming));
            }
        }

        // Warm-up tasks already restoring are funded first. If {@code num.warmup.replicas} config was reduced, we might
        // be over warmup budget and have to give up some warmup tasks. Evicting in reverse funding order
        // keeps which ones deterministic rather than dependent on iteration order.
        // Note: revocation of warmup task happens implicitly by not adding them to the assignment patch again
        keptWarmups.sort((left, right) -> comparePriority(left, right, processLoad, Map.of()));
        for (int i = 0; i < keptWarmups.size(); i++) {
            final FundingCandidate keptWarmup = keptWarmups.get(i);
            if (i < numWarmupReplicas) {
                warmupTasks.put(keptWarmup.task(), keptWarmup.targetOwner());
            } else {
                parkedMigrations.add(keptWarmup.task());
            }
        }

        // A warm-up task new to its target process raises that process's load, so we need to update it while we go,
        // and find a new `best` from scratch each time. A sibling move relocates the copy the process already holds,
        // which the load counts where that copy sits today, so it leaves the load unchanged.
        // note: this nested-loop is bounded by the number of unused warm-up slots; so while it's O(unused * candidate)
        // it's effectively not quadratic (we can consider `unused` a constant)
        final Map<String, Integer> newWarmupsByProcess = new HashMap<>();
        int used = Math.min(keptWarmups.size(), numWarmupReplicas);

        while (used < numWarmupReplicas && !newWarmupCandidates.isEmpty()) {
            int best = 0;
            for (int candidate = 1; candidate < newWarmupCandidates.size(); candidate++) {
                final int comparison = comparePriority(
                    newWarmupCandidates.get(candidate),
                    newWarmupCandidates.get(best),
                    processLoad,
                    newWarmupsByProcess
                );
                if (comparison < 0) {
                    best = candidate;
                }
            }

            final FundingCandidate funded = newWarmupCandidates.remove(best);
            warmupTasks.put(funded.task(), funded.targetOwner());
            newWarmupsByProcess.merge(funded.targetProcessId(), funded.newWarmupsOnTargetProcess(), Integer::sum);
            used++;
        }

        // Unfunded task migrations are parked, until warmup budget frees up again later.
        // For SIBLING_MOVE, we can apply an optimization and convert to a BORROW, which does not require a warm-up slot
        newWarmupCandidates.forEach(candidate -> {
            if (candidate.warming() == Warming.SIBLING_MOVE) {
                borrowedMigrations.add(candidate.task());
            } else {
                parkedMigrations.add(candidate.task());
            }
        });

        return new WarmupPlan(
            Collections.unmodifiableSortedMap(warmupTasks),
            Collections.unmodifiableSortedSet(borrowedMigrations),
            Collections.unmodifiableSortedSet(parkedMigrations)
        );
    }

    /**
     * Builds the {@link FundingCandidate} for a staged migration.
     */
    private static FundingCandidate fundingCandidate(
        final StagedMigration migration,
        final Map<String, StreamsGroupMember> members,
        final Warming warming
    ) {
        return new FundingCandidate(
            migration.task(),
            migration.targetOwner(),
            migration.targetProcessId().orElseThrow(),
            members.get(migration.currentOwner()).processId(),
            warming
        );
    }

    /**
     * How a staged migration is to be warmed, which is decided entirely by what the target owner's process already
     * holds for the task -- and, when it holds a standby, by whether that standby sits on the target owner itself.
     */
    private static Warming warmingOf(final StagedMigration migration) {
        if (migration.targetProcessId().isEmpty()) {
            // The target assignment names a member the group no longer has, so there is nowhere to warm up and no
            // warm-up slot may be spent. The task simply stays with its current owner.
            return Warming.PARK;
        }

        final Optional<TaskCopy> copyOnTargetProcess = migration.copyOnTargetProcess();
        if (copyOnTargetProcess.isEmpty()) {
            return Warming.PLANT;
        }
        if (copyOnTargetProcess.get().role() == TaskRole.WARMUP) {
            return Warming.KEEP;
        }
        if (copyOnTargetProcess.get().memberId().equals(migration.targetOwner())) {
            // The promotion takes this copy over in place, so it warms the migration for nothing.
            return Warming.BORROW;
        }
        // A standby on a sibling warms nothing the promotion can take over, so it has to move onto the target owner,
        // and that competes for a warm-up slot to pay for the redundancy backfill which follows it across.
        return Warming.SIBLING_MOVE;
    }

    /**
     * Funding priority by warming category: a fresh plant (0) before a sibling move (1). All other warmings rank 0,
     * which is harmless because only plants and sibling moves are ever ordered against each other.
     */
    private static int fundingRank(final Warming warming) {
        return warming == Warming.SIBLING_MOVE ? 1 : 0;
    }

    /**
     * Orders two migrations competing for the same warm-up slot, most deserving first.
     *
     * <p>A fresh plant is funded before a sibling move. The two differ only in what happens when they are unfunded:
     * a plant parks and makes no progress at all, while a sibling move falls back to borrowing the standby where it
     * sits and still warms.
     *
     * <p>Among candidates of the same warming the target process's load comes first: a lightly loaded target process
     * restores faster, so its warm-up slot recycles sooner and the group converges quicker, and spreading the
     * warm-up tasks spreads the restore traffic with them.
     * The current owner's process load breaks the tie in the opposite direction -- of two migrations that could be
     * funded, the one that relieves the busier process is worth more, which also ranks all of that process's pending
     * migrations together so its relief arrives in one batch.
     * The task itself breaks a full tie, purely so that the same inputs always produce the same assignment.
     */
    private static int comparePriority(
        final FundingCandidate left,
        final FundingCandidate right,
        final Map<String, ProcessLoad> processLoad,
        final Map<String, Integer> newWarmupsByProcess
    ) {
        final int byWarming = Integer.compare(fundingRank(left.warming()), fundingRank(right.warming()));
        if (byWarming != 0) {
            return byWarming;
        }

        final int byTargetProcessLoad = Double.compare(
            targetProcessLoad(left, processLoad, newWarmupsByProcess),
            targetProcessLoad(right, processLoad, newWarmupsByProcess)
        );
        if (byTargetProcessLoad != 0) {
            return byTargetProcessLoad;
        }

        final int byCurrentProcessLoad = Double.compare(
            currentProcessLoad(right, processLoad, newWarmupsByProcess),
            currentProcessLoad(left, processLoad, newWarmupsByProcess)
        );
        if (byCurrentProcessLoad != 0) {
            return byCurrentProcessLoad;
        }

        return left.task().compareTo(right.task());
    }

    private static double targetProcessLoad(
        final FundingCandidate candidate,
        final Map<String, ProcessLoad> processLoad,
        final Map<String, Integer> newWarmupsByProcess
    ) {
        return processLoad.get(candidate.targetProcessId())
            .loadWith(newWarmupsByProcess.getOrDefault(candidate.targetProcessId(), 0));
    }

    private static double currentProcessLoad(
        final FundingCandidate candidate,
        final Map<String, ProcessLoad> processLoad,
        final Map<String, Integer> newWarmupsByProcess
    ) {
        return processLoad.get(candidate.currentProcessId())
            .loadWith(newWarmupsByProcess.getOrDefault(candidate.currentProcessId(), 0));
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
     * The copy of the task that the given process already holds, if any.
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

    /**
     * Whether the subtopology has state the coordinator can reason about, which here means state with a changelog.
     *
     * <p>A changelog is the <em>only</em> signal of state that reaches the coordinator -- the topology metadata carries
     * changelog topics and nothing about stores -- so the two are not merely equal in effect, the broker has no way to
     * tell them apart. A store configured without logging is therefore invisible here, and that is also the right
     * outcome: without a changelog there is nothing to restore, so such a task can never be warmed up and is treated
     * exactly like a stateless task. This is narrower than "stateful" client-side, where a task can have state and no
     * changelog.
     */
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
     * @param activeHolder
     *        The member holding each task as an active task. A task that only sits in some member's pending revocation
     *        has no entry here, because an in-flight removal is a decision that has already been taken rather than a
     *        placement to preserve.
     * @param taskCopies
     *        The standby and warm-up holders of each task.
     */
    record CurrentAssignmentIndex(
        Map<TaskId, ActiveHolder> activeHolder,
        Map<TaskId, List<TaskCopy>> taskCopies
    ) {
    }

    /**
     * How much stateful work a process is carrying, for the warm-up funding order.
     *
     * <p>The count and the divisor are kept separately because the funding order needs the quotient while the
     * accounting needs the count: a warm-up task funded within one pass raises its target process's load before the
     * next pick is made, which {@link #loadWith(int)} does without disturbing the index itself.
     *
     * @param statefulTaskCount
     *        How many stateful tasks the process has been granted, counting every role. See
     *        {@link #indexProcessLoad} for why stateless tasks are left out.
     * @param memberCount
     *        How many members the process runs. Each member is one stream thread, so this is the process's
     *        capacity for running tasks.
     */
    record ProcessLoad(int statefulTaskCount, int memberCount) {

        double load() {
            return loadWith(0);
        }

        /**
         * The load this process would carry with the given number of warm-up tasks added to it.
         */
        double loadWith(final int newWarmupTasks) {
            return (double) (statefulTaskCount + newWarmupTasks) / memberCount;
        }
    }

    /**
     * The member holding a task as an active task, and whether it is processing the task or still restoring it.
     *
     * <p>Only a member that is <em>processing</em> a task has something a staged migration could protect, so the
     * flag is recorded alongside the member ID. The identity matters for a member that is only restoring too: it is
     * what tells the case analysis the task is already in the right place, and what lets a task be kept where it is
     * when the target assignment names a member the group no longer has.
     *
     * @param memberId
     *        The member holding the task.
     * @param processing
     *        Whether the member is processing the task, as opposed to still restoring it. See {@link #isRestoring}
     *        for how this is determined, and for why a member the coordinator has not heard from reads as processing.
     */
    record ActiveHolder(String memberId, boolean processing) {
    }

    /**
     * A copy of a task that exists on some member: which member holds it, in which role, and whether that member
     * has restored it far enough to take the task over as an active task.
     *
     * <p>Only the {@link TaskRole#STANDBY} and {@link TaskRole#WARMUP} copies are recorded here. A member holding the
     * task as an active task is tracked as {@link CurrentAssignmentIndex#activeHolder()} instead, whether it is
     * processing the task or still restoring it.
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
     * held back here -- the task simply waits on its current owner with nothing warming up, until a warm-up slot frees
     * up.
     *
     * @param task
     *        The task being migrated.
     * @param currentOwner
     *        The member the task stays with for now. Normally the member processing it; when the target assignment
     *        names a member the group no longer has, it can also be one that is still restoring the task.
     * @param targetOwner
     *        The member the target assignment moves the task to.
     * @param targetProcessId
     *        The process the target owner runs in, or empty if the target assignment names a member the group no
     *        longer has. Empty means the migration can never be warmed up: it gets no warm-up task, no warm-up slot
     *        is spent on it, and the task stays with its current owner until the assignor names a member that still
     *        exists.
     * @param copyOnTargetProcess
     *        The copy of the task that the target owner's process already holds, if any. When there is none, the
     *        migration is a candidate for a fresh warm-up task. When there is one, the process is already restoring
     *        the task and must not be handed a second copy of it -- and which member holds it then decides what
     *        warming the migration costs, since only a copy on the target owner itself can be promoted in place.
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
     * <p>That happens for any of three reasons: no achievable warming improvement remains -- the target owner's
     * process already holds the task's state, or the move is within a single process, where warming up is impossible;
     * or nobody holds the task at all, which covers a brand-new task as much as one whose owner departed; or the
     * member holding it is still restoring it, so nothing is being processed and staging would protect nothing. A
     * migration the warm-up budget could not fund is not among them: it stays a {@link StagedMigration}, with its
     * task still running on its current owner.
     *
     * <p>Granting is the refiner's decision that the hand-over may proceed, not the hand-over itself. The reconciler
     * still serializes it, so a task granted here can still spend a step in {@code UNRELEASED_TASKS} while its
     * previous owner revokes it.
     *
     * <p>Applying a grant needs no patch, so the member that held the task before is not recorded: the intermediate
     * assignment is the target assignment plus patches, and the target assignment already both places the task on
     * its target owner and omits it from its previous one. Only a migration that is <em>delayed</em> has to patch
     * the target assignment.
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
     * <p>Only the tasks that need a decision are listed, which leaves out both the ones already in place and the
     * ones the target assignment no longer contains. That keeps the result proportional to how far the current
     * assignment has diverged from the target rather than to the group's task count.
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

    /**
     * What a staged migration needs from the warm-up budget. This is decided entirely by what the target owner's
     * process already holds for the task, so the five values are mutually exclusive and cover every staged
     * migration.
     */
    private enum Warming {
        /** Nothing can warm this migration and no warm-up slot may be spent on it. */
        PARK,

        /** A standby on the target owner itself already warms it, for free. */
        BORROW,

        /** A warm-up is already restoring for it, and keeps the warm-up slot it was funded with. */
        KEEP,

        /** Its target owner's process holds nothing for the task: a warm-up must be planted. Costs a warm-up slot. */
        PLANT,

        /**
         * Its target owner's process holds a standby of the task, but on one of its <em>other</em> members.
         * Costs a warm-up slot.
         */
        SIBLING_MOVE
    }

    /**
     * A staged migration competing for a warm-up slot, with the parts of the funding order that can be resolved
     * ahead of the comparisons.
     *
     * @param task
     *        The task being migrated.
     * @param targetOwner
     *        The member the warm-up task goes on, if this migration is funded. Always the migration's target
     *        owner, so that the warm-up can be promoted in place once it has caught up.
     * @param targetProcessId
     *        The process that member runs in, whose load the funding order reads and the accounting raises.
     * @param currentProcessId
     *        The process still running the task, whose load the funding order reads (descending) as its secondary key.
     * @param warming
     *        What this migration needs from the budget, decided once when the migration is classified. The funding
     *        order reads it, as does the fall-back: a {@link Warming#SIBLING_MOVE} that does not get a warm-up slot
     *        falls back to borrowing the standby where it sits, everything else parks.
     */
    private record FundingCandidate(
        TaskId task,
        String targetOwner,
        String targetProcessId,
        String currentProcessId,
        Warming warming
    ) {

        /**
         * How many warm-up tasks funding this migration adds to the target process, which is what raises that
         * process's load for the picks that follow. A plant adds one, to a process that holds no copy of the task.
         * A sibling move adds none: the copy it moves onto the target owner is one the process already holds, and
         * the process load counts it where it sits today.
         */
        int newWarmupsOnTargetProcess() {
            return warming == Warming.PLANT ? 1 : 0;
        }
    }

    /**
     * Which warm-up tasks the intermediate assignment places, and how each staged migration is being warmed.
     *
     * <p>Every migration the case analysis staged appears in exactly one of these: its task is either a key of
     * {@code warmupTasks}, or in {@code borrowedMigrations}, or in {@code parkedMigrations}.
     *
     * @param warmupTasks
     *        The member holding a warm-up task of each task, which is always that task's target owner. A warm-up
     *        task planted in this step and one that has been restoring for several look alike here; nothing
     *        downstream needs the difference, and {@link CurrentAssignmentIndex#taskCopies()} still tells them
     *        apart.
     * @param borrowedMigrations
     *        The migrations warmed for free by a standby the target owner's process already holds, which it keeps where
     *        it is. They spend no warm-up slot, on the condition that the standby filter withholds the copy the
     *        target assignment relocates in its place. Either the standby sits on the target owner itself, where it
     *        is borrowed outright because the promotion can take it over in place, or it sits on a sibling member and
     *        the migration competed for a warm-up slot to move it across, did not get one, and settles for warming
     *        through the sibling.
     * @param parkedMigrations
     *        The migrations with nothing warming them: the budget was spent, or the target assignment names a
     *        member the group no longer has. Their tasks keep running on their current owners (or temporarily not at
     *        all), and a later refinement step picks them up.
     */
    record WarmupPlan(
        SortedMap<TaskId, String> warmupTasks,
        SortedSet<TaskId> borrowedMigrations,
        SortedSet<TaskId> parkedMigrations
    ) {

        static final WarmupPlan EMPTY = new WarmupPlan(
            Collections.emptySortedMap(),
            Collections.emptySortedSet(),
            Collections.emptySortedSet()
        );
    }
}
