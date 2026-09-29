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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;

/**
 * Derives the intermediate assignment which is the target assignment with the migration of a stateful task held back
 * behind a warm-up task, so that the task keeps running on its current owner while its target owner restores the
 * state.
 *
 * <p>{@link #refine} runs the derivation in five passes: index what the members hold today and how loaded their
 * processes are, decide per task whether its migration completes now or waits behind a warm-up task, spend the
 * warm-up budget on the migrations that wait, hold back the standby placements that collide with those decisions,
 * and assemble the result as the target assignment plus the patches all of that implies.
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
        if (numWarmupReplicas == 0) {
            return targetAssignment;
        }

        final CurrentAssignmentIndex currentAssignment =
            indexCurrentAssignment(members, taskOffsets, subtopologies, acceptableRecoveryLag);
        final Map<String, ProcessLoad> processLoad = indexProcessLoad(members, subtopologies);
        final TaskDecisions decisions =
            analyzeTasks(currentAssignment, targetAssignment, members, subtopologies, processLoad);
        final WarmupPlan warmupPlan =
            planWarmups(decisions, currentAssignment, members, processLoad, numWarmupReplicas);
        final SortedMap<String, SortedSet<TaskId>> withheldStandbys =
            filterStandbys(targetAssignment, currentAssignment, decisions, warmupPlan, members, subtopologies);

        return assemble(targetAssignment, currentAssignment, decisions, warmupPlan, withheldStandbys);
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
     * <p>A member also reports offsets for tasks it holds no role for, from the state directories an earlier
     * incarnation left on disk. Those reports are indexed per process as {@link CurrentAssignmentIndex#onDiskByProcess}
     * and are what lets the case analysis hand such a task straight back to the process that can reopen it.
     *
     * @param members
     *        All members of the group.
     * @param taskOffsets
     *        The latest changelog offsets/end-offsets reported by the members.
     * @param subtopologies
     *        The resolved subtopologies, which tell whether a subtopology is stateful.
     * @param acceptableRecoveryLag
     *        The lag at or below which a task's state counts as caught up.
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
        final Map<String, Set<TaskId>> reportedByProcess = new HashMap<>();
        final Map<String, Set<TaskId>> heldByProcess = new HashMap<>();

        for (final StreamsGroupMember member : members.values()) {
            final MemberTaskOffsets offsets = taskOffsets.getOrDefault(member.memberId(), MemberTaskOffsets.EMPTY);
            final Set<TaskId> heldOnProcess = heldByProcess.computeIfAbsent(member.processId(), __ -> new HashSet<>());

            forEachStatefulActiveTask(
                member.assignedTasks().activeTasksWithEpochs(),
                subtopologies,
                task -> {
                    activeHolder.put(task, new ActiveHolder(
                        member.memberId(),
                        isRestoring(offsets, task),
                        isCaughtUp(offsets, task, acceptableRecoveryLag)
                    ));
                    heldOnProcess.add(task);
                }
            );

            forEachStatefulTask(
                member.assignedTasks().standbyTasks(),
                subtopologies,
                task -> {
                    addTaskCopy(taskCopies, task, member, TaskRole.STANDBY, offsets, acceptableRecoveryLag);
                    heldOnProcess.add(task);
                }
            );

            forEachStatefulTask(
                member.assignedTasks().warmupTasks(),
                subtopologies,
                task -> {
                    addTaskCopy(taskCopies, task, member, TaskRole.WARMUP, offsets, acceptableRecoveryLag);
                    heldOnProcess.add(task);
                }
            );

            forEachStatefulReportedTask(
                offsets.taskOffsets(),
                subtopologies,
                task -> reportedByProcess.computeIfAbsent(member.processId(), __ -> new HashSet<>()).add(task)
            );
        }

        final Map<String, Set<TaskId>> onDiskByProcess = new HashMap<>(reportedByProcess.size());
        reportedByProcess.forEach((processId, reported) -> {
            reported.removeAll(heldByProcess.getOrDefault(processId, Set.of()));
            if (!reported.isEmpty()) {
                onDiskByProcess.put(processId, reported);
            }
        });

        return new CurrentAssignmentIndex(activeHolder, taskCopies, onDiskByProcess);
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
     * Indexes how loaded each process is, so that the derivation can send work to the process with the most room:
     * the case analysis picks between equally good copies to promote, and the warmup budget pass orders which
     * warm-up tasks it funds.
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
     * <p>A migration is <b>staged</b> when there is caught-up state for the task away from the target owner's
     * process: (1) either the old owner of the task keeps running it, or (2) caught-up copy which we temporarily
     * promote as active is running it.
     *
     * <p>Only an active-running or caught-up copy is ever promoted.
     *
     * @param currentAssignment
     *        The indexed current assignment, from {@link #indexCurrentAssignment}.
     * @param targetAssignment
     *        All members' target assignments, as computed by the task assignor.
     * @param members
     *        All members of the group, used to resolve which process a member runs in.
     * @param subtopologies
     *        The resolved subtopologies, which tell whether a subtopology is stateful.
     * @param processLoad
     *        The load of each process, from {@link #indexProcessLoad}, used to pick between equally good copies to
     *        promote.
     *
     * @return What was decided for the tasks that are not already in place.
     */
    static TaskDecisions analyzeTasks(
        final CurrentAssignmentIndex currentAssignment,
        final Map<String, TasksTuple> targetAssignment,
        final Map<String, StreamsGroupMember> members,
        final SortedMap<String, ConfiguredSubtopology> subtopologies,
        final Map<String, ProcessLoad> processLoad
    ) {
        // The map is sorted, which gives the canonical task order that makes a derivation reproducible and leaves the
        // warmup budget pass that follows a deterministic tie-break to fall back on.
        final SortedMap<TaskId, String> targetOwners = statefulActiveOwners(targetAssignment, subtopologies);
        final Map<TaskId, Set<String>> targetStandbyHolders = statefulStandbyHolders(targetAssignment, subtopologies);

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
            // (which were not moved to a new owner) will be dangling, which the next assignor run fixes.
            // Furthermore, we stage all tasks the assignor moves to this member on their old owners to keep them
            // "online". A task nobody holds as an active task is put on a caught-up copy holder instead, which is the
            // only way it runs at all before the next assignor run.
            final StreamsGroupMember targetMember = members.get(targetOwner);
            if (targetMember == null) {
                final Optional<String> currentOwner = holder != null
                    ? Optional.of(holder.memberId())
                    : bestCopyToPromote(currentAssignment, task, targetStandbyHolders, processLoad);
                currentOwner.ifPresent(owner -> stagedMigrations.add(
                    stagedMigration(currentAssignment, task, owner, targetOwner, Optional.empty())));
                continue;
            }

            final String targetProcessId = targetMember.processId();
            final Optional<String> currentProcessId = Optional.ofNullable(holder)
                .map(activeHolder -> members.get(activeHolder.memberId()).processId());

            if (isReady(currentAssignment, task, currentProcessId, targetProcessId)) {
                grantedTasks.add(new TaskGrant(task, targetOwner));
                continue;
            }

            // Which member the task keeps running on until the target owner is warm. 'Empty' hands the task to the
            // target owner in this step instead, cold unless its own process still has the state on disk.
            final Optional<String> currentOwner;
            if (holder != null && holder.hot()) {
                currentOwner = Optional.of(holder.memberId());
            } else if (holder == null && onDisk(currentAssignment, targetProcessId, task)) {
                // The target owner's process still has this task's state on disk and reopens it, so the task goes
                // there rather than onto a copy holder: the usual way into this branch is a member that restarted
                // inside the session timeout and got its own tasks back, where that state is exact. A stale disk
                // state is not told apart from a fresh one -- a process reports an end offset only for a task it
                // is restoring -- and the copy-holder detour would cost a second restore, a slot and a hand-over.
                currentOwner = Optional.empty();
            } else {
                currentOwner = bestCopyToPromote(currentAssignment, task, targetStandbyHolders, processLoad);
            }

            if (currentOwner.isEmpty()) {
                grantedTasks.add(new TaskGrant(task, targetOwner));
            } else {
                stagedMigrations.add(stagedMigration(
                    currentAssignment, task, currentOwner.get(), targetOwner, Optional.of(targetProcessId)));
            }
        }

        return new TaskDecisions(List.copyOf(stagedMigrations), List.copyOf(grantedTasks));
    }

    /**
     * The caught-up copy of the task that is the best one to promote to an active task, if the group holds one.
     *
     * <p>A copy the target assignment does not name as a standby holder of the task is promoted first, which leaves
     * the target assignment's own standby placements intact for as long as possible. Among equals, the least loaded
     * process wins, so that the member taking over the task is the one with the most room to run it.
     *
     * <p>If there was a caught-up copy on the target owner's process, it makes the task ready to move, so the case
     * analysis grants it upfront. Thus, all candidates that reach this step always sit on another process, and we
     * don't need to filter on processId.
     */
    private static Optional<String> bestCopyToPromote(
        final CurrentAssignmentIndex currentAssignment,
        final TaskId task,
        final Map<TaskId, Set<String>> targetStandbyHolders,
        final Map<String, ProcessLoad> processLoad
    ) {
        final Set<String> designatedStandbyHolders = targetStandbyHolders.getOrDefault(task, Set.of());
        return currentAssignment.taskCopies().getOrDefault(task, List.of()).stream()
            .filter(TaskCopy::caughtUp)
            .min(Comparator
                .comparingInt((TaskCopy copy) -> designatedStandbyHolders.contains(copy.memberId()) ? 1 : 0)
                .thenComparingDouble(copy -> processLoad.get(copy.processId()).load())
                .thenComparing(TaskCopy::memberId))
            .map(TaskCopy::memberId);
    }

    /**
     * Whether the process reports state for the task on disk without holding a copy of it.
     */
    private static boolean onDisk(
        final CurrentAssignmentIndex currentAssignment,
        final String processId,
        final TaskId task
    ) {
        return currentAssignment.onDiskByProcess().getOrDefault(processId, Set.of()).contains(task);
    }

    /**
     * Builds the staged migration that keeps the task on {@code currentOwner}. An empty {@code targetProcessId} means
     * the migration can never be warmed up, which is the case when the target assignment names a member the group no
     * longer has.
     */
    private static StagedMigration stagedMigration(
        final CurrentAssignmentIndex currentAssignment,
        final TaskId task,
        final String currentOwner,
        final String targetOwner,
        final Optional<String> targetProcessId
    ) {
        return new StagedMigration(
            task,
            currentOwner,
            targetOwner,
            targetProcessId,
            targetProcessId.flatMap(processId -> findCopyOnProcess(currentAssignment, task, processId))
        );
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
        final Optional<String> currentProcessId,
        final String targetProcessId
    ) {
        if (currentProcessId.isPresent() && targetProcessId.equals(currentProcessId.get())) {
            return true;
        }
        return currentAssignment.taskCopies().getOrDefault(task, List.of()).stream()
            .anyMatch(holder -> holder.processId().equals(targetProcessId) && holder.caughtUp());
    }

    /**
     * Decides which of the staged migrations get a warm-up task, under the warmup budget.
     *
     * <p>There are several scenarios:
     * <ul>
     *     <li>A warm-up task already restoring keeps its warm-up slot if the target assignment didn't change, and the
     *     warm-up task is not caught up yet. It could also get revoked if the warmup budget was reduced and keeping
     *     the warmup would now exceed the budget.</li>
     *     <li>A <b>fresh plant</b> puts a warm-up task on a target owner whose process holds nothing for the task,
     *     and spends a warm-up slot.</li>
     *     <li>When the target owner <em>itself</em> already holds a standby of the task we can <b>borrow</b> it,
     *     and no warmup budget is used: that standby warms-up the task anyway.</li>
     *     <li>If a target member's <em>sibling</em> holds a standby, we cannot borrow, but need to move the
     *     standby to its new owner, and putting a warmup on the target member, spending a warm-up slot.
     *     (Cf case (2) of {@link #isReady(CurrentAssignmentIndex, TaskId, Optional, String)})</li>
     * </ul>
     *
     * <p>Everything else <b>parks</b> -- the task keeps running on its current owner with nothing warming up, and a
     * later refinement step picks it up once a warm-up slot frees.
     *
     * <p>A warm-up task only makes progress on a member that is not restoring an active task, thus we only want to
     * assign a new warm-up to such a member with the lowest priority. If a member with an existing warm-up (and
     * non-zero warm-up restore progress) is active-restoring, we keep the warm-up there and accept the warm-up stall,
     * to avoid throwing away restore work. See {@link #fundingTier}.
     *
     * @param decisions
     *        What the case analysis decided, from {@link #analyzeTasks}.
     * @param currentAssignment
     *        The indexed current assignment, from {@link #indexCurrentAssignment}.
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
        final CurrentAssignmentIndex currentAssignment,
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

        final Set<String> restoringAnActive = membersRestoringAnActive(currentAssignment, decisions, members);
        final Map<String, ProcessLoad> loadAfterGrants =
            loadAfterGrants(processLoad, currentAssignment, decisions, members);
        final List<FundingCandidate> candidates = new ArrayList<>();

        for (final StagedMigration migration : decisions.stagedMigrations()) {
            final Warming warming = warmingOf(migration);
            switch (warming) {
                case PARK -> parkedMigrations.add(migration.task());
                case BORROW -> borrowedMigrations.add(migration.task());
                // The rest all put a warm-up task on the target owner and all cost a warm-up slot, so they compete
                // in one list -- a warm-up task kept from an earlier step included, because a kept one that cannot
                // make progress gives way to a fresh plant that can (see fundingTier).
                case KEEP, PLANT, SIBLING_MOVE ->
                    candidates.add(fundingCandidate(migration, members, warming, restoringAnActive));
            }
        }

        // Funding a warm-up task changes the order of what is left -- a plant raises its target process's load
        // before the next pick, which spreads concurrent restores instead of stacking them on whichever process
        // started out emptiest -- so the best candidate is found from scratch each time rather than sorted once.
        // note: this nested-loop is bounded by the warm-up budget; so while it's O(budget * candidate)
        // it's effectively not quadratic (we can consider `budget` a constant)
        final Map<String, Integer> newWarmupsByProcess = new HashMap<>();
        int used = 0;

        while (used < numWarmupReplicas && !candidates.isEmpty()) {
            int best = 0;
            for (int candidate = 1; candidate < candidates.size(); candidate++) {
                final int comparison = comparePriority(
                    candidates.get(candidate),
                    candidates.get(best),
                    loadAfterGrants,
                    newWarmupsByProcess
                );
                if (comparison < 0) {
                    best = candidate;
                }
            }

            final FundingCandidate funded = candidates.remove(best);
            warmupTasks.put(funded.task(), funded.targetOwner());
            newWarmupsByProcess.merge(funded.targetProcessId(), funded.newWarmupsOnTargetProcess(), Integer::sum);
            used++;
        }

        // Unfunded task migrations are parked, until warmup budget frees up again later.
        // For SIBLING_MOVE, we can apply an optimization and convert to a BORROW, which does not require a warm-up slot
        candidates.forEach(candidate -> {
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
     * {@link #indexProcessLoad}'s counts with this step's grants applied: minus one for the process running the
     * task today, plus one for the target owner's process unless that process already holds a copy of the task,
     * since the grant then promotes that copy and starts no restore.
     */
    private static Map<String, ProcessLoad> loadAfterGrants(
        final Map<String, ProcessLoad> processLoad,
        final CurrentAssignmentIndex currentAssignment,
        final TaskDecisions decisions,
        final Map<String, StreamsGroupMember> members
    ) {
        if (decisions.grantedTasks().isEmpty()) {
            return processLoad;
        }

        final Map<String, Integer> byProcess = new HashMap<>();
        for (final TaskGrant grant : decisions.grantedTasks()) {
            final ActiveHolder holder = currentAssignment.activeHolder().get(grant.task());
            if (holder != null) {
                byProcess.merge(members.get(holder.memberId()).processId(), -1, Integer::sum);
            }
            final String targetProcessId = members.get(grant.targetOwner()).processId();
            if (findCopyOnProcess(currentAssignment, grant.task(), targetProcessId).isEmpty()) {
                byProcess.merge(targetProcessId, 1, Integer::sum);
            }
        }

        final Map<String, ProcessLoad> updatedLoad = new HashMap<>(processLoad);
        byProcess.forEach((processId, change) -> updatedLoad.computeIfPresent(
            processId,
            (__, load) -> new ProcessLoad(load.statefulTaskCount() + change, load.memberCount())
        ));
        return updatedLoad;
    }

    /**
     * Builds the {@link FundingCandidate} for a staged migration.
     */
    private static FundingCandidate fundingCandidate(
        final StagedMigration migration,
        final Map<String, StreamsGroupMember> members,
        final Warming warming,
        final Set<String> restoringAnActive
    ) {
        return new FundingCandidate(
            migration.task(),
            migration.targetOwner(),
            migration.targetProcessId().orElseThrow(),
            members.get(migration.currentOwner()).processId(),
            warming,
            fundingTier(migration, warming, restoringAnActive)
        );
    }

    /**
     * The round of funding a candidate competes in, lowest (highest priority) first: an in-flight warm-up task (0),
     * a new one (1), and on a member that is restoring an active task -- where a warm-up task makes no progress,
     * because that restore pauses every warm-up partition the member holds -- an in-flight one that has restored
     * nothing (2) and a new one (3). An in-flight warm-up task that has restored something stays at 0, since dropping
     * it would discard that work to start another restore from scratch.
     */
    private static int fundingTier(
        final StagedMigration migration,
        final Warming warming,
        final Set<String> restoringAnActive
    ) {
        final boolean canProgress = !restoringAnActive.contains(migration.targetOwner());
        if (warming == Warming.KEEP) {
            final boolean restoredSomething =
                migration.copyOnTargetProcess().map(TaskCopy::restoreStarted).orElse(false);
            return canProgress || restoredSomething ? 0 : 2;
        }
        return canProgress ? 1 : 3;
    }

    /**
     * Members that are restoring an active task: the ones already restoring one, plus the ones this step grants a task
     * they have to restore.
     */
    private static Set<String> membersRestoringAnActive(
        final CurrentAssignmentIndex currentAssignment,
        final TaskDecisions decisions,
        final Map<String, StreamsGroupMember> members
    ) {
        final Set<String> restoringAnActive = new HashSet<>();
        currentAssignment.activeHolder().forEach((task, holder) -> {
            if (holder.restoring()) {
                restoringAnActive.add(holder.memberId());
            }
        });

        for (final TaskGrant grant : decisions.grantedTasks()) {
            if (startsARestore(currentAssignment, grant, members)) {
                restoringAnActive.add(grant.targetOwner());
            }
        }

        return restoringAnActive;
    }

    /**
     * Whether granting the task makes its new owner restore it, as opposed to taking over state its own process
     * already holds -- where the reconciler relabels a copy, or a sibling hands the task over through the state
     * directory, and no changelog reading happens at all.
     */
    private static boolean startsARestore(
        final CurrentAssignmentIndex currentAssignment,
        final TaskGrant grant,
        final Map<String, StreamsGroupMember> members
    ) {
        final String targetProcessId = members.get(grant.targetOwner()).processId();
        if (findCopyOnProcess(currentAssignment, grant.task(), targetProcessId).isPresent()) {
            return false;
        }

        final ActiveHolder holder = currentAssignment.activeHolder().get(grant.task());
        return holder == null || !targetProcessId.equals(members.get(holder.memberId()).processId());
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
        final int byTier = Integer.compare(left.tier(), right.tier());
        if (byTier != 0) {
            return byTier;
        }

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
     * Decides which of the target assignment's standby placements this step has to hold back.
     *
     * <p>Nothing is invented or dropped permanently: every placement comes from the target assignment, and one held
     * back here is emitted by a later step once its reason is gone. A placement of task {@code t} on member {@code m}
     * of process {@code p} is withheld when:
     * <ol>
     *     <li>a staged migration keeps {@code t} running as an active task on {@code p}, and a process cannot hold
     *     {@code t} twice.</li>
     *     <li>{@code t}'s migration onto {@code p} borrowed an existing standby on {@code p}. To not run
     *     {@code num.standby.replicas + 1} standbys, we hold back one assignment of the standby to a new owner.</li>
     * </ol>
     *
     * @param targetAssignment
     *        All members' target assignments, as computed by the task assignor.
     * @param currentAssignment
     *        The indexed current assignment, from {@link #indexCurrentAssignment}.
     * @param decisions
     *        What the case analysis decided, from {@link #analyzeTasks}.
     * @param warmupPlan
     *        How each staged migration is being warmed, from {@link #planWarmups}.
     * @param members
     *        All members of the group, used to resolve which process a member runs in.
     * @param subtopologies
     *        The resolved subtopologies, which tell whether a subtopology is stateful.
     *
     * @return The standby placements to withhold, as the tasks to drop from each member's target assignment, in
     *         canonical order. A member with nothing withheld does not appear.
     */
    static SortedMap<String, SortedSet<TaskId>> filterStandbys(
        final Map<String, TasksTuple> targetAssignment,
        final CurrentAssignmentIndex currentAssignment,
        final TaskDecisions decisions,
        final WarmupPlan warmupPlan,
        final Map<String, StreamsGroupMember> members,
        final SortedMap<String, ConfiguredSubtopology> subtopologies
    ) {
        final StandbyConflicts conflicts = indexStandbyConflicts(
            targetAssignment,
            currentAssignment,
            decisions,
            warmupPlan,
            members,
            subtopologies
        );
        final SortedMap<String, SortedSet<TaskId>> withheld = new TreeMap<>();

        targetAssignment.forEach((memberId, tasks) -> {
            final StreamsGroupMember member = members.get(memberId);
            if (member == null) {
                // The target assignment can name a member the group has already removed. Its tasks reach nobody, so
                // there is nothing to hold back and no process to resolve it against.
                return;
            }

            forEachStatefulTask(tasks.standbyTasks(), subtopologies, task -> {
                if (isStandbyWithheld(memberId, member.processId(), task, conflicts)) {
                    withheld.computeIfAbsent(memberId, __ -> new TreeSet<>()).add(task);
                }
            });
        });

        return Collections.unmodifiableSortedMap(withheld);
    }

    /**
     * Builds the {@link StandbyConflicts} lookups, ie, each standby task which cannot be relocated yet, because
     * (1) its target process owns the corresponding active task from a staged migration, or (2) the standby must stay
     * on its current owner to fulfill its role of being borrowed.
     */
    private static StandbyConflicts indexStandbyConflicts(
        final Map<String, TasksTuple> targetAssignment,
        final CurrentAssignmentIndex currentAssignment,
        final TaskDecisions decisions,
        final WarmupPlan warmupPlan,
        final Map<String, StreamsGroupMember> members,
        final SortedMap<String, ConfiguredSubtopology> subtopologies
    ) {
        // Conflicts from staged migrations
        final Map<TaskId, String> activeStagedOnProcess = new HashMap<>();
        for (final StagedMigration migration : decisions.stagedMigrations()) {
            activeStagedOnProcess.put(migration.task(), members.get(migration.currentOwner()).processId());
        }

        // Conflicts from borrows
        //
        // If we have more than one standby (ie, `num.standby.replicas >= 2`), we need to ensure to only hold back
        // one standby task migration (for the single borrow of the active task migration)
        // If there is a parallel sibling-move for the same standby task on a different process we leave it alone;
        // we are looking for a process with a new standby being assigned to (there might be multiple, so we pick one
        // deterministically based on memberId order)
        final BinaryOperator<String> firstInMemberOrder = (left, right) -> left.compareTo(right) <= 0 ? left : right;
        final Map<TaskId, String> standbyKeptOnMember = new HashMap<>();
        final Set<TaskId> alreadyUndelivered = new HashSet<>();
        targetAssignment.forEach((targetMemberId, tasks) ->
            forEachStatefulTask(tasks.standbyTasks(), subtopologies, task -> {
                if (warmupPlan.borrowedMigrations().contains(task)) {
                    final StreamsGroupMember targetMember = members.get(targetMemberId);
                    final String currentOwnerProcessId = activeStagedOnProcess.get(task);
                    if (targetMember == null // dropped out of the group
                        || targetMember.processId().equals(currentOwnerProcessId)) { // stage migration; tracked above
                        alreadyUndelivered.add(task);
                    } else if (findCopyOnProcess(currentAssignment, task, targetMember.processId()).isEmpty()) { // only withhold if not a sibling move
                        standbyKeptOnMember.merge(task, targetMemberId, firstInMemberOrder);
                    }
                }
            }));
        alreadyUndelivered.forEach(standbyKeptOnMember::remove);

        return new StandbyConflicts(activeStagedOnProcess, standbyKeptOnMember);
    }

    /**
     * Whether this step has to hold the standby placement back.
     */
    private static boolean isStandbyWithheld(
        final String memberId,
        final String processId,
        final TaskId task,
        final StandbyConflicts conflicts
    ) {
        // Rule 1: a process cannot hold `task` twice, so a standby on the process a staged migration keeps the
        // active running on waits for that migration to complete.
        if (processId.equals(conflicts.activeStagedOnProcess().get(task))) {
            return true;
        }

        // Rule 2: a borrowed migration keeps its existing copy as one of the group's entitled replicas, so one
        // relocated placement of that standby waits, which keeps the group at `num.standby.replicas`.
        return memberId.equals(conflicts.standbyKeptOnMember().get(task));
    }

    /**
     * Builds the intermediate assignment: the target assignment, with a patch applied.
     *
     * <p><b>A granted task needs no patch:</b> The target assignment already places the task on its new owner and
     * already omits it from the old one, so letting it through unchanged <em>is</em> the grant.
     * Only a migration this step holds back has to be written down.
     *
     * <p>Five kinds of patches.
     * (Note: multiple patches might apply at once, eg, a regular warmup plant is the first three patches combined):
     * <ul>
     *     <li>Keep the active task on its old owner.</li>
     *     <li>Withhold the active task from its new owner.</li>
     *     <li>Place a warm-up task.</li>
     *     <li>Keep a standby task on its old owner.</li>
     *     <li>Withhold a standby task from its new owner.</li>
     * </ul>
     *
     * @param targetAssignment
     *        All members' target assignments, as computed by the task assignor.
     * @param currentAssignment
     *        The indexed current assignment, from {@link #indexCurrentAssignment}, used to find where a borrowed
     *        copy sits.
     * @param decisions
     *        What the case analysis decided, from {@link #analyzeTasks}.
     * @param warmupPlan
     *        How each staged migration is being warmed, from {@link #planWarmups}.
     * @param withheldStandbys
     *        The standby placements to hold back, from {@link #filterStandbys}.
     *
     * @return The intermediate assignment, keyed by member ID. The target assignment itself when nothing diverges.
     */
    static Map<String, TasksTuple> assemble(
        final Map<String, TasksTuple> targetAssignment,
        final CurrentAssignmentIndex currentAssignment,
        final TaskDecisions decisions,
        final WarmupPlan warmupPlan,
        final SortedMap<String, SortedSet<TaskId>> withheldStandbys
    ) {
        final Map<String, PatchedTasks> patches = new HashMap<>();

        for (final StagedMigration migration : decisions.stagedMigrations()) {
            final TaskId task = migration.task();
            patchFor(patches, targetAssignment, migration.currentOwner()).addActive(task);
            patchFor(patches, targetAssignment, migration.targetOwner()).removeActive(task);

            if (warmupPlan.borrowedMigrations().contains(task)) {
                findCopyOnProcess(currentAssignment, task, migration.targetProcessId().orElseThrow())
                    .ifPresent(copy -> patchFor(patches, targetAssignment, copy.memberId()).addStandby(task));
            }
        }

        warmupPlan.warmupTasks().forEach((task, memberId) ->
            patchFor(patches, targetAssignment, memberId).addWarmup(task));

        withheldStandbys.forEach((memberId, tasks) -> {
            final PatchedTasks patch = patchFor(patches, targetAssignment, memberId);
            tasks.forEach(patch::removeStandby);
        });

        if (patches.isEmpty()) {
            return targetAssignment;
        }

        final Map<String, TasksTuple> intermediateAssignment = new HashMap<>(targetAssignment);
        patches.forEach((memberId, patch) -> intermediateAssignment.put(memberId, patch.toTasksTuple()));
        return Collections.unmodifiableMap(intermediateAssignment);
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

    /**
     * Whether the member has restored any of the task: it reports a position for it, and that position is not the
     * cap the client reports before a restore has begun.
     */
    private static boolean hasRestoredSomething(final MemberTaskOffsets memberTaskOffsets, final TaskId task) {
        final Long offset = offsetOf(memberTaskOffsets.taskOffsets(), task);
        return offset != null && offset != Long.MAX_VALUE;
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

    /**
     * Inverts the target assignment into a lookup from stateful task to the members that are to hold it as a standby.
     */
    private static Map<TaskId, Set<String>> statefulStandbyHolders(
        final Map<String, TasksTuple> targetAssignment,
        final SortedMap<String, ConfiguredSubtopology> subtopologies
    ) {
        final Map<TaskId, Set<String>> holders = new HashMap<>();
        targetAssignment.forEach((memberId, tasks) ->
            forEachStatefulTask(tasks.standbyTasks(), subtopologies,
                task -> holders.computeIfAbsent(task, __ -> new HashSet<>()).add(memberId)));
        return holders;
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
            isCaughtUp(offsets, task, acceptableRecoveryLag),
            hasRestoredSomething(offsets, task)
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

    private static void forEachStatefulReportedTask(
        final Map<String, Map<Integer, Long>> reportedOffsets,
        final SortedMap<String, ConfiguredSubtopology> subtopologies,
        final Consumer<TaskId> action
    ) {
        reportedOffsets.forEach((subtopologyId, offsetsByPartition) -> {
            if (isStateful(subtopologies, subtopologyId)) {
                offsetsByPartition.keySet()
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
     * @param onDiskByProcess
     *        Per process, the tasks it reports state for while holding no copy of them: state in its state directory
     *        that no role it has been granted accounts for. How far behind that state is cannot be measured, because
     *        a member reports an end offset only for a task it is restoring. A process with no such task has no
     *        entry.
     */
    record CurrentAssignmentIndex(
        Map<TaskId, ActiveHolder> activeHolder,
        Map<TaskId, List<TaskCopy>> taskCopies,
        Map<String, Set<TaskId>> onDiskByProcess
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
     * The member holding a task as an active task, and how usable the state it holds is.
     *
     * <p>Only a {@link #hot()} holder has state a staged migration can use, so the two flags it is derived
     * from are recorded alongside the member ID.
     *
     * <p>The identity matters even for a holder that is not hot: it is what tells the case analysis the task is
     * already in the right place, and what lets a task be kept where it is when the target assignment names a member
     * the group no longer has.
     *
     * @param memberId
     *        The member holding the task.
     * @param restoring
     *        Whether the member is still restoring the task, as opposed to processing it. See {@link #isRestoring}
     *        for how this is determined, and for why a member the coordinator has not heard from reads as processing.
     * @param caughtUp
     *        Whether the member has restored the task to within {@code acceptable.recovery.lag}. Only meaningful
     *        while it is {@code restoring}: a member that is processing the task reports no offsets for it, so this
     *        reads false there.
     */
    record ActiveHolder(String memberId, boolean restoring, boolean caughtUp) {

        /**
         * Whether the holder's state is usable for running the task right now: it is either processing the task
         * already, or has restored it to within {@code acceptable.recovery.lag}.
         */
        boolean hot() {
            return !restoring || caughtUp;
        }
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
     * @param restoreStarted
     *        Whether the member has restored any of the task.
     */
    record TaskCopy(String memberId, String processId, TaskRole role, boolean caughtUp, boolean restoreStarted) {
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
     *        The member the task runs on for now: the member holding it as an active task, or, when that member is
     *        too far behind to run it or there is none, the caught-up copy holder promoted to run it. When the target
     *        assignment names a member the group no longer has, it can also be a holder that is still restoring the
     *        task, which is better than taking the task away from a restore that is part-way through.
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
     * process already holds caught-up state for the task, or the move is within a single process, where warming up
     * is impossible; or the target owner's process has the task's state on disk and reopens it from there; or
     * nothing in the group can run the task at all, neither the member holding it nor any copy of it being caught
     * up, so the target owner restores it from the changelog. A migration the warm-up budget could not fund is not
     * among them: it stays a {@link StagedMigration}, with its task still running on its current owner.
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
        Warming warming,
        int tier
    ) {

        /**
         * How many warm-up tasks funding this migration adds to the target process, which is what raises that
         * process's load for the picks that follow. A plant adds one, to a process that holds no copy of the task.
         * A sibling move adds none: the copy it moves onto the target owner is one the process already holds, and
         * the process load counts it where it sits today. A kept warm-up task adds none either, for the same
         * reason -- it is the copy that process is already restoring.
         */
        int newWarmupsOnTargetProcess() {
            return warming == Warming.PLANT ? 1 : 0;
        }
    }

    /**
     * The per-task lookups {@link #filterStandbys} consults, each built once so that a rule is a map lookup rather
     * than a fresh scan of the group.
     *
     * @param activeStagedOnProcess
     *        The process each staged migration keeps its task running on.
     * @param standbyKeptOnMember
     *        For each migration warmed by a standby that stays where it is, the one member whose relocated placement
     *        of that task waits in the borrowed copy's stead. Tasks warmed some other way are absent, as are those
     *        with no placement left to hold back.
     */
    private record StandbyConflicts(
        Map<TaskId, String> activeStagedOnProcess,
        Map<TaskId, String> standbyKeptOnMember
    ) {
    }

    /**
     * One member's tasks in the target assignment, made mutable so that the assembly can patch them.
     *
     * <p><b>A subtopology whose partition set becomes empty is dropped, and that is load-bearing rather than
     * tidiness.</b> The coordinator decides whether a refinement step is due by asking whether a member's assigned
     * tasks still match what it already holds, and that comparison is a plain map equality: a subtopology key mapped
     * to an empty set is <em>not</em> equal to the same map without the key. A patch that removed a member's last
     * task for some subtopology and left the key behind would therefore compare unequal forever, and the group would
     * mint a fresh refinement step on every heartbeat without anything changing. Pruning is what makes a patch that
     * ends up holding the target assignment's tasks read as the target assignment.
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

        private void addActive(final TaskId task) {
            add(activeTasks, task);
        }

        private void removeActive(final TaskId task) {
            remove(activeTasks, task);
        }

        private void addStandby(final TaskId task) {
            add(standbyTasks, task);
        }

        private void removeStandby(final TaskId task) {
            remove(standbyTasks, task);
        }

        private void addWarmup(final TaskId task) {
            add(warmupTasks, task);
        }

        private TasksTuple toTasksTuple() {
            return new TasksTuple(activeTasks, standbyTasks, warmupTasks);
        }

        private static Map<String, Set<Integer>> mutableCopy(final Map<String, Set<Integer>> tasks) {
            final Map<String, Set<Integer>> copy = new HashMap<>();
            tasks.forEach((subtopologyId, partitionIds) -> copy.put(subtopologyId, new HashSet<>(partitionIds)));
            return copy;
        }

        private static void add(final Map<String, Set<Integer>> tasks, final TaskId task) {
            tasks.computeIfAbsent(task.subtopologyId(), __ -> new HashSet<>()).add(task.partition());
        }

        private static void remove(final Map<String, Set<Integer>> tasks, final TaskId task) {
            final Set<Integer> partitionIds = tasks.get(task.subtopologyId());
            if (partitionIds != null && partitionIds.remove(task.partition()) && partitionIds.isEmpty()) {
                tasks.remove(task.subtopologyId());
            }
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
