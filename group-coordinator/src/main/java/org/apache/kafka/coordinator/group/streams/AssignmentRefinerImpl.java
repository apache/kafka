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
     * <p>A task a member has been told to give up is deliberately not indexed at all, even though the member may still
     * be physically running it until the revocation is acknowledged. Recording that member as the task's holder would
     * make the case analysis try to keep the task there, undoing a hand-over that is already under way; and the
     * process it occupies until the revocation completes needs no tracking here either, because the reconciler already
     * refuses to grant a role for a task the process still holds.
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
     * Whether the member is still restoring the task it holds as an active task, rather than processing it.
     *
     * <p>A client reports changelog offsets for every task it is restoring and stops reporting them once the task is
     * running, so a reported offset for an active task means the restore is still under way. The value does not matter,
     * only that something was reported: the {@link Long#MAX_VALUE} "restore not started" cap counts as restoring too.
     *
     * <p><b>A member the coordinator has not heard from reports nothing, so its active tasks all read as running.</b>
     * That is every member right after a coordinator failover -- the reported offsets are in-memory state that does not
     * survive one -- and a newly joined member until its first report. It is the safe direction: the task is treated as
     * something to protect, so at worst a migration is staged that could have been granted outright, and the next
     * report corrects it. Reading the absence the other way would leave every active task in the group unprotected at
     * once after a failover.
     */
    private static boolean isRestoring(final MemberTaskOffsets memberTaskOffsets, final TaskId task) {
        return offsetOf(memberTaskOffsets.taskOffsets(), task) != null;
    }

    /**
     * Indexes how loaded each process is, for the order in which the budget pass funds warm-up tasks. The load of a
     * process is its stateful task count over the number of members it runs -- the same shape as the task assignor's
     * own {@code ProcessState.load()}, so that both layers rank processes comparably.
     *
     * <p><b>Only stateful tasks are counted</b>, which is narrower than what the assignor measures. Standby and
     * warm-up tasks exist only for stateful tasks anyway, so in practice this comes down to leaving stateless active
     * tasks out, for two reasons. Where the assignor spreads stateless tasks evenly, they add the same amount to
     * every process's load and so cannot change the ranking at all. Where it does not spread them evenly, only
     * stateful work competes for the changelog reads a warm-up needs, so counting stateless tasks would rank a
     * process busy with work that does not compete as though it were a poor place to restore.
     *
     * <p>A process running nothing but stateless tasks therefore has a load of zero, which is the right answer
     * here. That it holds no state to take over is beside the point: the target assignment has already chosen every
     * destination, and this order only decides which of those migrations is funded first, never where a task goes.
     *
     * <p>Only {@link StreamsGroupMember#assignedTasks()} is counted -- {@link
     * StreamsGroupMember#tasksPendingRevocation()} is deliberately not read, and the two are disjoint, so nothing on
     * its way out is counted. Counting a task the member has been told to give up would overstate the load the
     * process is about to carry, and would double-count the commonest shape of all: a member being demoted from
     * active to standby holds the task as a pending active revocation and as an already-granted standby at once.
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
     * Decides, for every stateful task whose active role is not already where the target assignment wants it, whether
     * the migration has to be staged behind a warm-up task or can be completed in this step.
     *
     * <p>A task is <b>staged</b> only when all of the following hold: somebody is <em>processing</em> it today, the
     * target owner is somebody else, and there is a warming improvement left to achieve. Everything else completes
     * right away, which needs no patch at all -- the target assignment already places the task on its target owner, and
     * the previous owner's slice already omits it. That is why the two outcomes are so lopsided: staging is the
     * exception, and the result is proportional to how far the current assignment has diverged from the target rather
     * than to the group's size.
     *
     * <p>Note it takes <em>processing</em>, not merely holding the active task. A member that has been granted an
     * active task but is still restoring it processes nothing, so staging a migration away from it would protect
     * nothing while wasting the restore work and possibly a warm-up slot; such a task is granted to its target
     * straight away. What this deliberately does not do is compare how far along the two members are -- picking the
     * better-placed of two candidates is a placement decision, and placement is the assignor's job.
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

            final ActiveHolder holder = currentAssignment.activeHolder().get(task);
            if (holder != null && targetOwner.equals(holder.memberId())) {
                // The task is already where it belongs, whether it is processing yet or still restoring.
                continue;
            }

            // The target assignment can still name a member the group has already removed: it is only recomputed when
            // the assignor runs again, which the assignment interval can defer, and a member can be fenced in the
            // meantime. Such a member cannot restore anything, so nothing may be staged into it.
            final StreamsGroupMember targetMember = members.get(targetOwner);
            if (targetMember == null) {
                if (holder != null) {
                    // Keep the task with whoever holds it until the assignor names a member that still exists. Leaving
                    // it out instead would make that member revoke it -- discarding a restore part-way through, or
                    // stopping processing outright -- for no gain, since the task has nowhere else to go.
                    stagedMigrations.add(new StagedMigration(
                        task,
                        holder.memberId(),
                        targetOwner,
                        Optional.empty(),
                        Optional.empty()
                    ));
                }
                // A task nobody holds and whose target owner is gone is left to the next assignor run: granting it to
                // a member that is no longer in the group would achieve nothing.
                continue;
            }

            final String targetProcessId = targetMember.processId();

            // The task moves now, for any of three reasons. Nobody holds it -- it is new, its owner left, or a
            // hand-over is in flight and the previous owner has already released it -- so there is nothing to protect
            // and the target owner takes it even cold; preferring a warmer owner would be a placement decision, and
            // placement is the assignor's job. Or somebody holds it but is still restoring it, so nothing is being
            // processed and staging would protect nothing. Or somebody is processing it but no achievable warming
            // improvement remains. The holder, when there is one, is necessarily a member of the group, because the
            // index it comes from was built from the members themselves.
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
     * predicate cannot fix that; it would take a client-side cross-thread task hand-over
     * (https://issues.apache.org/jira/browse/KAFKA-21090). The broker cannot even see the difference, because the
     * topology metadata carries changelog topics but not how a store is backed.
     *
     * <p>The damage is bounded, though, because <b>the refiner never creates one of those two paths -- it only ever
     * inherits them.</b> Every warm-up it plants sits on the target owner itself, so every migration it warms ends in
     * the in-place promotion, which is warm for every store type. It even pays to keep that true: where the
     * destination process holds a copy of the task only on a <em>sibling</em> of the target owner, the budget pass
     * spends a slot to move that copy onto the target owner rather than borrow it where it sits. So the only way to
     * reach one of the two cold paths is through this predicate granting the task outright -- nothing was warmed, and
     * the layout was already there when the refiner looked.
     *
     * <p>Note that includes a copy on a sibling member that is <em>already</em> caught up: the task is granted here,
     * in this step, before the budget pass ever sees the migration, so nothing gets the chance to move that copy onto
     * the target owner first. Doing so would spend a slot to buy an in-place promotion -- which is worth it for an
     * in-memory store and pure waste for a store that persists to disk, since that one reopens warm from the state
     * directory anyway. The broker cannot tell the two apart, so this grants immediately and converges fast. It is a
     * deliberate boundary rather than an oversight, and the design document carries it as an open question.
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
     * Decides which of the staged migrations get a warm-up task, under the standing cap on how many replicas beyond
     * the target assignment may exist at once.
     *
     * <p>Wherever a warm-up ends up it sits on the target owner <em>itself</em>, so that it is promoted in place once
     * it has caught up rather than closed on one member and reopened on another. What varies is the price of getting
     * it there:
     * <ul>
     *     <li>A <b>fresh plant</b> puts a warm-up task on a target owner whose process holds nothing for the task,
     *     and spends a slot.</li>
     *     <li>A <b>borrow</b> spends nothing, and applies when the target owner <em>itself</em> already holds a
     *     standby of the task: that copy warms the migration as a side effect of being a standby, and is the very
     *     replica the promotion then takes over in place. The target assignment must be relocating it elsewhere --
     *     an assignor does not leave a standby on the process it hands the active to -- so withholding that
     *     relocation keeps the replica count exactly where the target assignment wants it. That withholding is the
     *     standby filter's job, not this one's.</li>
     *     <li>A standby on a <b>sibling</b> member of the target owner's process competes for a slot like a fresh
     *     plant, because free warming is not on offer there. The sibling cannot promote in place, so the copy has to
     *     move onto the target owner as a warm-up, and the redundancy it was providing while it sat on the sibling
     *     then has to be backfilled by the relocation the target assignment already wants -- three copies against
     *     the target assignment's two, which is one slot. Unfunded, it falls back to being borrowed where it sits.</li>
     * </ul>
     *
     * <p>Paying a slot for that move is worth it because a process holds a task at most once, so the sibling has to
     * release the task before the target owner can hold anything at all, and only a store that persists to disk
     * survives the release: the sibling's clean close leaves a checkpoint behind for the incoming member to reopen
     * from, whereas an in-memory store lives on the sibling's heap and is dropped. Moving the copy onto the target
     * owner pays that cost <em>during</em> the warming phase, where it merely delays convergence. Leaving it on the
     * sibling pays it at the hand-over instead, where it stalls processing -- which is the one thing staging exists
     * to prevent.
     *
     * <p>A migration already being warmed keeps its slot ahead of any fresh plant: dropping a restore part-way
     * through to start another one elsewhere would throw away the very work the budget exists to buy. What counts
     * as already being warmed is a warm-up that a <em>still-staged</em> migration justifies, which is why this
     * reads the case analysis and not the current assignment. A task being granted this step is no longer staged,
     * so its warm-up is not kept and its slot is free again within this same pass. The budget is recounted from
     * zero on every call for the same reason: a warm-up whose task the assignor has since re-targeted elsewhere
     * must not go on holding a slot it no longer earns.
     *
     * <p>Everything else <b>parks</b> -- the task keeps running on its current owner with nothing warming up, and a
     * later refinement step picks it up once a slot frees. Parking is never destructive: no state is discarded
     * because the budget ran out.
     *
     * @param decisions
     *        What the case analysis decided, from {@link #analyzeTasks}.
     * @param members
     *        All members of the group, used to resolve which process a task's current owner runs in.
     * @param processLoad
     *        The load of each process, from {@link #indexProcessLoad}.
     * @param maxWarmupReplicas
     *        How many replicas beyond the target assignment may exist at once, group-wide.
     *
     * @return Which warm-up tasks the intermediate assignment places, and how each staged migration is warmed.
     */
    static WarmupPlan planWarmups(
        final TaskDecisions decisions,
        final Map<String, StreamsGroupMember> members,
        final Map<String, ProcessLoad> processLoad,
        final int maxWarmupReplicas
    ) {
        // A budget of zero means the group does not stage migrations at all, so there is nothing to fund. The
        // caller short-circuits to the target assignment long before this, which is where that contract lives --
        // including that it disables the budget-free borrows too. This is only the guard for a direct call.
        if (maxWarmupReplicas == 0) {
            return WarmupPlan.EMPTY;
        }

        final SortedMap<TaskId, String> warmupTasks = new TreeMap<>();
        final SortedSet<TaskId> borrowedMigrations = new TreeSet<>();
        final SortedSet<TaskId> parkedMigrations = new TreeSet<>();

        final List<FundingCandidate> keptWarmers = new ArrayList<>();
        final List<FundingCandidate> plantCandidates = new ArrayList<>();

        for (final StagedMigration migration : decisions.stagedMigrations()) {
            switch (warmingOf(migration)) {
                case PARK -> parkedMigrations.add(migration.task());
                case BORROW -> borrowedMigrations.add(migration.task());
                case KEEP -> keptWarmers.add(fundingCandidate(migration, members, processLoad));
                case PLANT -> plantCandidates.add(fundingCandidate(migration, members, processLoad));
            }
        }

        // Warm-ups in flight are funded first, but a budget that has shrunk below their number -- a config change,
        // since nothing else can lower it -- has to give some up. Evicting in reverse funding order keeps which
        // ones deterministic rather than dependent on iteration order.
        keptWarmers.sort((left, right) -> comparePriority(left, right, processLoad, Map.of()));
        for (int i = 0; i < keptWarmers.size(); i++) {
            final FundingCandidate keptWarmer = keptWarmers.get(i);
            if (i < maxWarmupReplicas) {
                warmupTasks.put(keptWarmer.task(), keptWarmer.targetOwner());
            } else {
                parkedMigrations.add(keptWarmer.task());
            }
        }

        // Fresh plants take whatever the kept warmers left. Each one funded raises its destination's load before
        // the next pick, which spreads concurrent restores across processes instead of stacking them all on
        // whichever process happened to start out lightest -- so this picks repeatedly rather than sorting once.
        final Map<String, Integer> plantsByProcess = new HashMap<>();
        int used = Math.min(keptWarmers.size(), maxWarmupReplicas);

        while (used < maxWarmupReplicas && !plantCandidates.isEmpty()) {
            int best = 0;
            for (int candidate = 1; candidate < plantCandidates.size(); candidate++) {
                final int comparison = comparePriority(
                    plantCandidates.get(candidate),
                    plantCandidates.get(best),
                    processLoad,
                    plantsByProcess
                );
                if (comparison < 0) {
                    best = candidate;
                }
            }

            final FundingCandidate funded = plantCandidates.remove(best);
            warmupTasks.put(funded.task(), funded.targetOwner());
            plantsByProcess.merge(funded.targetProcessId(), 1, Integer::sum);
            used++;
        }

        // A candidate that missed out is borrowed where it sits when the target owner's process holds a standby on a
        // sibling: warming through the sibling is worth more than not warming at all, and is what the migration would
        // have done anyway had the slot never been on offer. Everything else has nothing to fall back on and parks.
        plantCandidates.forEach(candidate -> {
            if (candidate.borrowable()) {
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
     * Resolves the parts of a staged migration the funding order needs, once, so that the repeated comparisons do
     * not each redo the lookups.
     *
     * <p>The source load is resolved here rather than compared lazily because it cannot change during the pass:
     * funding a warm-up adds a task to its <em>destination</em> process, while the source keeps running the active
     * task either way.
     */
    private static FundingCandidate fundingCandidate(
        final StagedMigration migration,
        final Map<String, StreamsGroupMember> members,
        final Map<String, ProcessLoad> processLoad
    ) {
        final String sourceProcessId = members.get(migration.currentOwner()).processId();
        return new FundingCandidate(
            migration.task(),
            migration.targetOwner(),
            migration.targetProcessId().orElseThrow(),
            processLoad.get(sourceProcessId).load(),
            isBorrowableFromSibling(migration)
        );
    }

    /**
     * How a staged migration is to be warmed, which is decided entirely by what the target owner's process already
     * holds for the task -- and, when it holds a standby, by whether that standby sits on the target owner itself.
     */
    private static Warming warmingOf(final StagedMigration migration) {
        if (migration.targetProcessId().isEmpty()) {
            // The target assignment names a member the group no longer has, so there is nowhere to warm up and no
            // slot may be spent. The task simply stays with its current owner.
            return Warming.PARK;
        }

        final Optional<TaskCopy> copyOnTargetProcess = migration.copyOnTargetProcess();
        if (copyOnTargetProcess.isEmpty()) {
            return Warming.PLANT;
        }
        if (copyOnTargetProcess.get().role() == TaskRole.WARMUP) {
            return Warming.KEEP;
        }
        // A standby on the target owner itself is borrowed outright, since the promotion takes it over in place. One
        // on a sibling warms nothing the promotion can take over, so it has to move onto the target owner, and that
        // competes for a slot to pay for the redundancy backfill which follows it across.
        return copyOnTargetProcess.get().memberId().equals(migration.targetOwner())
            ? Warming.BORROW
            : Warming.PLANT;
    }

    /**
     * Whether the migration can still be warmed for free if it does not get a slot, by leaving a standby the target
     * owner's process holds on one of its <em>other</em> members where it is. Such a standby goes on consuming from
     * the changelog wherever it sits, so it warms the destination process either way; what the slot buys is moving it
     * onto the target owner, so that the hand-over becomes an in-place promotion instead of a release and reopen.
     *
     * <p>A standby on the target owner itself is not covered here: that one is borrowed outright and never competes
     * for a slot, so it never reaches the point of needing a fallback.
     */
    private static boolean isBorrowableFromSibling(final StagedMigration migration) {
        return migration.copyOnTargetProcess()
            .filter(copy -> copy.role() == TaskRole.STANDBY)
            .filter(copy -> !copy.memberId().equals(migration.targetOwner()))
            .isPresent();
    }

    /**
     * Orders two migrations competing for the same warm-up slot, most deserving first.
     *
     * <p>The destination's load comes first: a lightly loaded destination restores faster, so its slot recycles
     * sooner and the group converges quicker, and spreading the plants spreads the restore traffic with them. The
     * source's load breaks the tie in the opposite direction -- of two migrations that could be funded, the one
     * that relieves the busier process is worth more, which also has the effect of ranking all of a hot process's
     * pending migrations together so its relief arrives in one batch. The task itself breaks a full tie, purely so
     * that the same inputs always produce the same assignment.
     *
     * <p>None of this decides <em>where</em> a task goes -- the target assignment has already done that. It only
     * decides which migrations get to start warming first, so being approximately right is enough.
     *
     * <p>{@code plantsByProcess} is empty when ordering warm-ups that are already in flight, since the load index
     * counts those already; for fresh plants it carries what this pass has funded so far, so that each plant raises
     * its destination before the next pick.
     */
    private static int comparePriority(
        final FundingCandidate left,
        final FundingCandidate right,
        final Map<String, ProcessLoad> processLoad,
        final Map<String, Integer> plantsByProcess
    ) {
        final int byDestinationLoad = Double.compare(
            destinationLoad(left, processLoad, plantsByProcess),
            destinationLoad(right, processLoad, plantsByProcess)
        );
        if (byDestinationLoad != 0) {
            return byDestinationLoad;
        }

        final int bySourceLoad = Double.compare(right.sourceLoad(), left.sourceLoad());
        if (bySourceLoad != 0) {
            return bySourceLoad;
        }

        return left.task().compareTo(right.task());
    }

    private static double destinationLoad(
        final FundingCandidate candidate,
        final Map<String, ProcessLoad> processLoad,
        final Map<String, Integer> plantsByProcess
    ) {
        return processLoad.get(candidate.targetProcessId())
            .loadWith(plantsByProcess.getOrDefault(candidate.targetProcessId(), 0));
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

    /**
     * Whether the subtopology has state the coordinator can reason about, which here means state with a changelog.
     *
     * <p>A changelog is the <em>only</em> signal of state that reaches the coordinator -- the topology metadata carries
     * changelog topics and nothing about stores -- so the two are not merely equal in effect, the broker has no way to
     * tell them apart. A store configured without logging is therefore invisible here, and that is also the right
     * outcome: without a changelog there is nothing to restore, so such a task can never be warmed up and is treated
     * exactly like a stateless one. Note this is narrower than what "stateful" means client-side, where a task can
     * have state and no changelog.
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
     * <p>The count and the divisor are kept apart rather than stored pre-divided because the funding order needs
     * the quotient while the accounting needs the count: each warm-up funded within one pass has to raise its
     * destination's load before the next pick is made, which {@link #loadWith(int)} does without disturbing the
     * index itself.
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
        double loadWith(final int extraWarmupTasks) {
            return (double) (statefulTaskCount + extraWarmupTasks) / memberCount;
        }
    }

    /**
     * The member holding a task as an active task, and whether it is processing the task or still restoring it.
     *
     * <p>Only a member that is <em>processing</em> a task has something a staged migration could protect, so the two
     * are kept apart rather than collapsed into a plain member ID. The identity is still needed for a member that is
     * only restoring, though: it is what tells the case analysis the task is already in the right place, and what lets
     * a task be kept where it is when the target assignment names a member the group no longer has.
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
     * A copy of a task that exists on some member: which member holds it, in which role, and whether that member has
     * restored it far enough to take the task over as an active task.
     *
     * <p>Only the {@link TaskRole#STANDBY} and {@link TaskRole#WARMUP} copies are recorded. A member holding the task
     * as an active task is tracked separately, as {@link CurrentAssignmentIndex#activeHolder()}, whether it is
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
     * held back here -- the task simply waits on its current owner with nothing warming up, until a slot frees up.
     *
     * @param task
     *        The task being migrated.
     * @param currentOwner
     *        The member the task stays with for now. Normally the member processing it; when the target assignment
     *        names a member the group no longer has, it can also be one that is still restoring the task.
     * @param targetOwner
     *        The member the target assignment moves the task to.
     * @param targetProcessId
     *        The process the target owner runs in, or empty if the target assignment names a member the group no longer
     *        has. Empty means the migration can never be warmed up and must not be given a warm-up task or a budget
     *        slot; the task just stays with its current owner until the assignor names a member that still exists.
     * @param copyOnTargetProcess
     *        The replica of the task that the target owner's process already holds, if any. When there is none, the
     *        migration is a candidate for a fresh warm-up task. When there is one, the process is already restoring the
     *        task and must not be handed a second copy of it -- and which member holds it then decides what warming
     *        the migration costs, since only a copy on the target owner itself can be promoted in place.
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
     * <p>That happens for any of three reasons: no achievable warming improvement remains -- the target owner's process
     * already holds the task's state, or the move is within a single process, where warming up is impossible; or nobody
     * holds the task at all, which covers a brand-new task as much as one whose owner departed; or the member holding
     * it is still restoring it, so nothing is being processed and staging would protect nothing. It notably does
     * <b>not</b> happen because the warm-up budget ran out: a migration that cannot be funded stays a
     * {@link StagedMigration} and its task keeps running on its current owner.
     *
     * <p>Granting is the refiner's decision that the hand-over may proceed, not the hand-over itself. The reconciler
     * still serializes it, so a task granted here can still spend a step in {@code UNRELEASED_TASKS} while its
     * previous owner revokes it.
     *
     * <p>The member that held the task before is deliberately not recorded here, because applying a grant needs no
     * patch at all: the intermediate assignment is the target assignment plus patches, and the target assignment
     * already both places the task on its new owner and omits it from the old one. Only a migration that is
     * <em>delayed</em> has to patch the target assignment.
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

    /**
     * What a staged migration needs from the budget, which is what the classification pass sorts them by.
     */
    private enum Warming {
        /** Nothing can warm this migration and no slot may be spent on it. */
        PARK,

        /** A standby on the target owner itself already warms it, for free. */
        BORROW,

        /** A warm-up is already restoring for it, and keeps the slot it was funded with. */
        KEEP,

        /** It needs a warm-up placed on its target owner, which costs a slot. */
        PLANT
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
     * @param sourceLoad
     *        The load of the process still running the task, which cannot change during a funding pass.
     * @param borrowable
     *        Whether missing out on a slot leaves the migration warmed anyway, because a standby on a sibling member
     *        of the target owner's process can be borrowed where it sits. Such a candidate never parks.
     */
    private record FundingCandidate(
        TaskId task,
        String targetOwner,
        String targetProcessId,
        double sourceLoad,
        boolean borrowable
    ) {
    }

    /**
     * Which warm-up tasks the intermediate assignment places, and how each staged migration is being warmed.
     *
     * <p>Every migration the case analysis staged appears in exactly one of these: its task is either a key of
     * {@code warmupTasks}, or in {@code borrowedMigrations}, or in {@code parkedMigrations}.
     *
     * @param warmupTasks
     *        The member holding a warm-up task of each task, which is always that task's target owner -- whether
     *        the warm-up was planted in this step or has been restoring for several. The two are deliberately not
     *        distinguished, because nothing downstream needs the difference and it stays recoverable from
     *        {@link CurrentAssignmentIndex#taskCopies()}.
     * @param borrowedMigrations
     *        The migrations warmed for free by a standby the target owner's process already holds, which it keeps
     *        where it is. They spend no slot, on the condition that the standby filter withholds the replica the
     *        target assignment relocates in its place. Either the standby sits on the target owner itself, where it
     *        is borrowed outright because the promotion can take it over in place, or it sits on a sibling member
     *        and the migration competed for a slot to move it across, did not get one, and settles for warming
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
