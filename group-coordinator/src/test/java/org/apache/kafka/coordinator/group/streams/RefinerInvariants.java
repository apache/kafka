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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * The properties of an intermediate assignment that {@link RefinerFuzzSimulator} checks after every refiner call.
 *
 * <p>The generic checks hold for any refiner: they are what the reconciler and the clients rely on, whatever the
 * refiner is trying to achieve. The warm-up checks are specific to {@link AssignmentRefinerImpl} and follow from its
 * design: it only ever holds a migration back behind a warm-up task, never invents a placement, and spends the
 * warm-up budget before it parks anything.
 *
 * <p>The checks are deliberately written against the refiner's inputs and output only, without reusing any of the
 * refiner's own helpers, so that they are an independent statement of what the output must look like.
 */
final class RefinerInvariants {

    private static final int MAX_EXAMPLES = 5;

    private RefinerInvariants() {
    }

    /**
     * The inputs of one refiner call.
     */
    record RefineCall(
        Map<String, StreamsGroupMember> members,
        Map<String, TasksTuple> targetAssignment,
        Map<String, MemberTaskOffsets> taskOffsets,
        SortedMap<String, ConfiguredSubtopology> subtopologies,
        int numWarmupReplicas,
        long acceptableRecoveryLag
    ) {

        Map<String, TasksTuple> refine(final AssignmentRefiner refiner) {
            return refiner.refine(members, targetAssignment, taskOffsets, subtopologies, numWarmupReplicas,
                acceptableRecoveryLag);
        }

        /**
         * The same inputs, with every map rebuilt in reverse iteration order.
         */
        RefineCall reordered() {
            final Map<String, StreamsGroupMember> reorderedMembers = new LinkedHashMap<>();
            reversedKeys(members).forEach(memberId -> reorderedMembers.put(memberId, members.get(memberId)));

            final Map<String, TasksTuple> reorderedTarget = new LinkedHashMap<>();
            reversedKeys(targetAssignment).forEach(memberId -> {
                final TasksTuple tasks = targetAssignment.get(memberId);
                reorderedTarget.put(memberId, new TasksTuple(
                    reversed(tasks.activeTasks()),
                    reversed(tasks.standbyTasks()),
                    reversed(tasks.warmupTasks())
                ));
            });

            final Map<String, MemberTaskOffsets> reorderedOffsets = new LinkedHashMap<>();
            reversedKeys(taskOffsets).forEach(memberId -> {
                final MemberTaskOffsets offsets = taskOffsets.get(memberId);
                reorderedOffsets.put(memberId, new MemberTaskOffsets(
                    reversedOffsets(offsets.taskOffsets()),
                    reversedOffsets(offsets.taskEndOffsets())
                ));
            });

            return new RefineCall(reorderedMembers, reorderedTarget, reorderedOffsets, subtopologies,
                numWarmupReplicas, acceptableRecoveryLag);
        }
    }

    /**
     * Checks the invariants every refiner must keep and, if asked to, the ones specific to the warm-up refiner,
     * {@link AssignmentRefinerImpl}.
     *
     * @return A description of every violation, empty if there is none.
     */
    static List<String> check(final RefineCall call, final Map<String, TasksTuple> refined, final boolean warmup) {
        final Violations violations = new Violations();
        final Facts facts = new Facts(call, refined);

        checkActiveTasksPreserved(facts, violations);
        checkProcessExclusivity(facts, violations);
        checkWarmupBudget(facts, violations);
        checkStatelessTasksUntouched(facts, violations);
        checkNoEmptyEntries(refined, violations);
        checkNoInventedPlacements(facts, violations);

        if (warmup) {
            checkWarmupsOnlyOnTargetOwners(facts, violations);
            checkHeldBackTasksStayWhereStateIs(facts, violations);
            checkNoColdGrantOfHotTask(facts, violations);
            checkParkingOnlyWhenBudgetIsSpent(facts, violations);
            checkIdentityWhenNothingDiverges(facts, violations);
            checkStandbyCount(facts, violations);
        }

        return violations.list;
    }

    /**
     * Checks that the refiner returns the same result for the same inputs, whatever order they iterate in.
     */
    static List<String> checkDeterminism(
        final AssignmentRefiner refiner,
        final RefineCall call,
        final Map<String, TasksTuple> refined
    ) {
        final Map<String, TasksTuple> again = call.reordered().refine(refiner);
        if (!again.equals(refined)) {
            return List.of("determinism: the same inputs in a different iteration order gave " + again
                + " instead of " + refined);
        }
        return List.of();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Generic invariants

    /**
     * The intermediate assignment hands out exactly the target assignment's active tasks, each exactly once. This is
     * stronger than {@link AssignmentRefiner#preservesActiveTaskCount}, which only compares the counts.
     */
    private static void checkActiveTasksPreserved(final Facts facts, final Violations violations) {
        final Set<TaskId> allTasks = new HashSet<>(facts.targetActiveOwner.keySet());
        allTasks.addAll(facts.refinedActiveOwners.keySet());
        for (final TaskId task : allTasks) {
            final List<String> owners = facts.refinedActiveOwners.getOrDefault(task, List.of());
            if (!facts.targetActiveOwner.containsKey(task)) {
                violations.add("active-preserved", task + " is active on " + owners + " but not in the target");
            } else if (owners.size() != 1) {
                violations.add("active-preserved", task + " is active on " + owners + " instead of exactly once");
            }
        }
    }

    /**
     * A process holds a task at most once, in one role, on one member. Members the group no longer has belong to no
     * process and are left out.
     */
    private static void checkProcessExclusivity(final Facts facts, final Violations violations) {
        final Map<String, Map<TaskId, List<String>>> holdersByProcess = new TreeMap<>();
        facts.refined.forEach((memberId, tasks) -> {
            final StreamsGroupMember member = facts.call.members().get(memberId);
            if (member == null) {
                return;
            }
            final Map<TaskId, List<String>> holders =
                holdersByProcess.computeIfAbsent(member.processId(), __ -> new HashMap<>());
            forEachTask(tasks.activeTasks(), task -> holders.computeIfAbsent(task, __ -> new ArrayList<>())
                .add(memberId + ":active"));
            forEachTask(tasks.standbyTasks(), task -> holders.computeIfAbsent(task, __ -> new ArrayList<>())
                .add(memberId + ":standby"));
            forEachTask(tasks.warmupTasks(), task -> holders.computeIfAbsent(task, __ -> new ArrayList<>())
                .add(memberId + ":warmup"));
        });
        holdersByProcess.forEach((processId, holders) -> holders.forEach((task, roles) -> {
            if (roles.size() > 1) {
                violations.add("process-exclusivity", "process " + processId + " holds " + task + " as " + roles);
            }
        }));
    }

    /**
     * At most {@code num.warmup.replicas} warm-up tasks, all of stateful tasks, on members of the group.
     */
    private static void checkWarmupBudget(final Facts facts, final Violations violations) {
        if (facts.refinedWarmups.size() > facts.call.numWarmupReplicas()) {
            violations.add("warmup-budget", facts.refinedWarmups.size() + " warm-up tasks exceed the budget of "
                + facts.call.numWarmupReplicas());
        }
        facts.refinedWarmups.forEach((task, memberId) -> {
            if (!facts.isStateful(task)) {
                violations.add("warmup-budget", "warm-up task of stateless task " + task + " on " + memberId);
            }
            if (!facts.call.members().containsKey(memberId)) {
                violations.add("warmup-budget", "warm-up task " + task + " on " + memberId + ", who left the group");
            }
        });
    }

    /**
     * A stateless task has no state to warm up, so the refiner places it exactly where the target assignment does.
     * And the refiner only speaks for members of the target assignment and of the group.
     */
    private static void checkStatelessTasksUntouched(final Facts facts, final Violations violations) {
        final Set<String> memberIds = new TreeSet<>(facts.call.targetAssignment().keySet());
        memberIds.addAll(facts.refined.keySet());
        for (final String memberId : memberIds) {
            if (!facts.call.targetAssignment().containsKey(memberId) && !facts.call.members().containsKey(memberId)) {
                violations.add("stateless-untouched", "the result names " + memberId
                    + ", who is neither in the target assignment nor in the group");
            }
            final TasksTuple target = facts.call.targetAssignment().getOrDefault(memberId, TasksTuple.EMPTY);
            final TasksTuple refined = facts.refined.getOrDefault(memberId, TasksTuple.EMPTY);
            if (!facts.stateless(refined.activeTasks()).equals(facts.stateless(target.activeTasks()))
                || !facts.stateless(refined.standbyTasks()).equals(facts.stateless(target.standbyTasks()))) {
                violations.add("stateless-untouched", memberId + " has stateless tasks " + refined
                    + " where the target assignment has " + target);
            }
        }
    }

    /**
     * No subtopology maps to an empty set of partitions. The coordinator compares a member's assignment with a plain
     * map equality, so an empty entry would never compare equal and mint a refinement step on every heartbeat.
     */
    private static void checkNoEmptyEntries(final Map<String, TasksTuple> refined, final Violations violations) {
        refined.forEach((memberId, tasks) -> {
            for (final Map<String, Set<Integer>> byRole
                : List.of(tasks.activeTasks(), tasks.standbyTasks(), tasks.warmupTasks())) {
                byRole.forEach((subtopologyId, partitions) -> {
                    if (partitions.isEmpty()) {
                        violations.add("no-empty-entries", memberId + " has an empty entry for " + subtopologyId);
                    }
                });
            }
        });
    }

    /**
     * Every active or standby placement comes from the target assignment, or keeps something the member already
     * holds.
     */
    private static void checkNoInventedPlacements(final Facts facts, final Violations violations) {
        facts.refined.forEach((memberId, tasks) -> {
            final TasksTuple target = facts.call.targetAssignment().getOrDefault(memberId, TasksTuple.EMPTY);
            forEachTask(tasks.activeTasks(), task -> {
                if (!contains(target.activeTasks(), task) && !facts.holdsInAnyRole(memberId, task)) {
                    violations.add("no-invented-placements", "active " + task + " on " + memberId
                        + ", which neither the target assignment places there nor the member holds");
                }
            });
            forEachTask(tasks.standbyTasks(), task -> {
                if (!contains(target.standbyTasks(), task) && !facts.holdsCopy(memberId, task)) {
                    violations.add("no-invented-placements", "standby " + task + " on " + memberId
                        + ", which neither the target assignment places there nor the member holds a copy of");
                }
            });
        });
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Warm-up refiner invariants

    /**
     * A warm-up task sits on the task's target owner, and only while the task is still active elsewhere.
     */
    private static void checkWarmupsOnlyOnTargetOwners(final Facts facts, final Violations violations) {
        facts.refinedWarmups.forEach((task, memberId) -> {
            if (!memberId.equals(facts.targetActiveOwner.get(task))) {
                violations.add("warmup-on-target-owner", "warm-up task " + task + " on " + memberId
                    + " instead of on its target owner " + facts.targetActiveOwner.get(task));
            }
            if (facts.refinedActiveOwners.getOrDefault(task, List.of()).contains(memberId)) {
                violations.add("warmup-on-target-owner", "warm-up task " + task + " on " + memberId
                    + ", which also runs it as an active task");
            }
        });
    }

    /**
     * A migration is only ever held back on a member that has the task's state: the member running the task today,
     * or a caught-up copy of it that is promoted in the meantime.
     */
    private static void checkHeldBackTasksStayWhereStateIs(final Facts facts, final Violations violations) {
        facts.refinedActiveOwners.forEach((task, owners) -> {
            final String targetOwner = facts.targetActiveOwner.get(task);
            for (final String owner : owners) {
                if (owner.equals(targetOwner)) {
                    continue;
                }
                if (!facts.call.members().containsKey(owner)) {
                    violations.add("held-back-on-state", task + " is held back on " + owner
                        + ", who left the group");
                } else if (!owner.equals(facts.currentActiveHolder.get(task)) && !facts.hasCaughtUpCopy(owner, task)) {
                    violations.add("held-back-on-state", task + " is held back on " + owner
                        + ", which neither runs it nor holds a caught-up copy of it");
                }
            }
        });
    }

    /**
     * The budget never turns into a cold hand-over: a task whose current owner can run it, and whose target owner's
     * process has nothing to take it over from, stays with its current owner.
     */
    private static void checkNoColdGrantOfHotTask(final Facts facts, final Violations violations) {
        facts.targetActiveOwner.forEach((task, targetOwner) -> {
            final String holder = facts.currentActiveHolder.get(task);
            final StreamsGroupMember targetMember = facts.call.members().get(targetOwner);
            if (!facts.isStateful(task) || holder == null || holder.equals(targetOwner) || targetMember == null) {
                return;
            }
            final String targetProcessId = targetMember.processId();
            final boolean sameProcess = facts.processOf(holder).equals(targetProcessId);
            if (!sameProcess && facts.isHot(holder, task) && !facts.hasCaughtUpCopyOnProcess(targetProcessId, task)
                && !facts.isOnDisk(targetProcessId, task)
                && !facts.refinedActiveOwners.getOrDefault(task, List.of()).contains(holder)) {
                violations.add("no-cold-grant", task + " was taken from " + holder
                    + ", who can run it, and handed to " + facts.refinedActiveOwners.get(task)
                    + ", whose process holds no caught-up state for it");
            }
        });
    }

    /**
     * A staged migration that nothing on the target owner's process warms up is parked, and the refiner only parks
     * one once the warm-up budget is spent.
     */
    private static void checkParkingOnlyWhenBudgetIsSpent(final Facts facts, final Violations violations) {
        if (facts.refinedWarmups.size() >= facts.call.numWarmupReplicas()) {
            return;
        }
        final List<TaskId> parked = new ArrayList<>();
        facts.targetActiveOwner.forEach((task, targetOwner) -> {
            final StreamsGroupMember targetMember = facts.call.members().get(targetOwner);
            if (!facts.isStateful(task) || targetMember == null
                || facts.refinedActiveOwners.getOrDefault(task, List.of()).contains(targetOwner)) {
                return;
            }
            if (!facts.hasCopyOnProcess(targetMember.processId(), task) && !facts.refinedWarmups.containsKey(task)) {
                parked.add(task);
            }
        });
        if (!parked.isEmpty()) {
            violations.add("park-only-when-budget-spent", "migrations of " + parked + " are parked while only "
                + facts.refinedWarmups.size() + " of " + facts.call.numWarmupReplicas() + " warm-up slots are used");
        }
    }

    /**
     * When every stateful active task already runs on its target owner, there is nothing to refine.
     */
    private static void checkIdentityWhenNothingDiverges(final Facts facts, final Violations violations) {
        for (final Map.Entry<TaskId, String> entry : facts.targetActiveOwner.entrySet()) {
            if (facts.isStateful(entry.getKey())
                && !entry.getValue().equals(facts.currentActiveHolder.get(entry.getKey()))) {
                return;
            }
        }
        if (!normalized(facts.refined).equals(normalized(facts.call.targetAssignment()))) {
            violations.add("identity", "nothing diverges from the target assignment, but the result differs from it");
        }
    }

    /**
     * A task never has more standby tasks than the target assignment gives it, plus the one standby a borrow keeps
     * in place when there is no relocated placement left to hold back in its stead (KSTREAMS-9377).
     */
    private static void checkStandbyCount(final Facts facts, final Violations violations) {
        final Map<TaskId, Integer> targetStandbys = standbyCounts(facts.call.targetAssignment());
        standbyCounts(facts.refined).forEach((task, count) -> {
            final int allowed = targetStandbys.getOrDefault(task, 0) + 1;
            if (count > allowed) {
                violations.add("standby-count", task + " has " + count + " standby tasks, more than " + allowed);
            }
        });
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Helpers

    /**
     * What the checks need to know about one refiner call, indexed once.
     */
    private static final class Facts {

        private final RefineCall call;
        private final Map<String, TasksTuple> refined;
        private final Map<TaskId, String> targetActiveOwner = new HashMap<>();
        private final Map<TaskId, List<String>> refinedActiveOwners = new HashMap<>();
        private final Map<TaskId, String> refinedWarmups = new HashMap<>();
        private final Map<TaskId, String> currentActiveHolder = new HashMap<>();
        private final Map<TaskId, List<String>> currentCopyHolders = new HashMap<>();
        private final Map<String, Set<TaskId>> heldByProcess = new HashMap<>();
        private final Map<String, Set<TaskId>> reportedByProcess = new HashMap<>();

        private Facts(final RefineCall call, final Map<String, TasksTuple> refined) {
            this.call = call;
            this.refined = refined;
            call.targetAssignment().forEach((memberId, tasks) ->
                forEachTask(tasks.activeTasks(), task -> targetActiveOwner.put(task, memberId)));
            refined.forEach((memberId, tasks) -> {
                forEachTask(tasks.activeTasks(), task ->
                    refinedActiveOwners.computeIfAbsent(task, __ -> new ArrayList<>()).add(memberId));
                forEachTask(tasks.warmupTasks(), task -> refinedWarmups.put(task, memberId));
            });
            call.members().forEach((memberId, member) -> {
                final Set<TaskId> held = heldByProcess.computeIfAbsent(member.processId(), __ -> new HashSet<>());
                forEachTask(member.assignedTasks().activeTasks(), task -> {
                    currentActiveHolder.put(task, memberId);
                    held.add(task);
                });
                final Consumer<TaskId> copy = task -> {
                    currentCopyHolders.computeIfAbsent(task, __ -> new ArrayList<>()).add(memberId);
                    held.add(task);
                };
                forEachTask(member.assignedTasks().standbyTasks(), copy);
                forEachTask(member.assignedTasks().warmupTasks(), copy);
                offsetsOf(memberId).taskOffsets().forEach((subtopologyId, partitions) -> partitions.keySet().forEach(
                    partition -> reportedByProcess.computeIfAbsent(member.processId(), __ -> new HashSet<>())
                        .add(new TaskId(subtopologyId, partition))));
            });
        }

        private boolean isStateful(final TaskId task) {
            final ConfiguredSubtopology subtopology = call.subtopologies().get(task.subtopologyId());
            return subtopology != null && !subtopology.stateChangelogTopics().isEmpty();
        }

        private Map<String, Set<Integer>> stateless(final Map<String, Set<Integer>> tasks) {
            final Map<String, Set<Integer>> stateless = new TreeMap<>();
            tasks.forEach((subtopologyId, partitions) -> {
                if (!isStateful(new TaskId(subtopologyId, 0)) && !partitions.isEmpty()) {
                    stateless.put(subtopologyId, new TreeSet<>(partitions));
                }
            });
            return stateless;
        }

        private String processOf(final String memberId) {
            return call.members().get(memberId).processId();
        }

        private boolean holdsInAnyRole(final String memberId, final TaskId task) {
            final StreamsGroupMember member = call.members().get(memberId);
            return member != null && (contains(member.assignedTasks().activeTasks(), task) || holdsCopy(memberId, task));
        }

        private boolean holdsCopy(final String memberId, final TaskId task) {
            final StreamsGroupMember member = call.members().get(memberId);
            return member != null && (contains(member.assignedTasks().standbyTasks(), task)
                || contains(member.assignedTasks().warmupTasks(), task));
        }

        private boolean hasCaughtUpCopy(final String memberId, final TaskId task) {
            return holdsCopy(memberId, task) && isCaughtUp(memberId, task);
        }

        private boolean hasCopyOnProcess(final String processId, final TaskId task) {
            return currentCopyHolders.getOrDefault(task, List.of()).stream()
                .anyMatch(memberId -> processOf(memberId).equals(processId));
        }

        private boolean hasCaughtUpCopyOnProcess(final String processId, final TaskId task) {
            return currentCopyHolders.getOrDefault(task, List.of()).stream()
                .anyMatch(memberId -> processOf(memberId).equals(processId) && isCaughtUp(memberId, task));
        }

        /**
         * Whether a member of the process reports state for the task while no member of it holds the task.
         */
        private boolean isOnDisk(final String processId, final TaskId task) {
            return reportedByProcess.getOrDefault(processId, Set.of()).contains(task)
                && !heldByProcess.getOrDefault(processId, Set.of()).contains(task);
        }

        /**
         * A member processing an active task reports no offsets for it, one still restoring it does.
         */
        private boolean isHot(final String memberId, final TaskId task) {
            final boolean restoring = offset(offsetsOf(memberId).taskOffsets(), task) != null;
            return !restoring || isCaughtUp(memberId, task);
        }

        private boolean isCaughtUp(final String memberId, final TaskId task) {
            final MemberTaskOffsets offsets = offsetsOf(memberId);
            final Long offset = offset(offsets.taskOffsets(), task);
            final Long endOffset = offset(offsets.taskEndOffsets(), task);
            if (offset == null || endOffset == null || offset == Long.MAX_VALUE || endOffset == Long.MAX_VALUE) {
                return false;
            }
            return endOffset - offset <= call.acceptableRecoveryLag();
        }

        private MemberTaskOffsets offsetsOf(final String memberId) {
            return call.taskOffsets().getOrDefault(memberId, MemberTaskOffsets.EMPTY);
        }
    }

    private static final class Violations {

        private final List<String> list = new ArrayList<>();
        private final Map<String, Integer> countByInvariant = new TreeMap<>();

        private void add(final String invariant, final String description) {
            final int count = countByInvariant.merge(invariant, 1, Integer::sum);
            if (count <= MAX_EXAMPLES) {
                list.add(invariant + ": " + description);
            }
        }
    }

    private static Long offset(final Map<String, Map<Integer, Long>> offsets, final TaskId task) {
        final Map<Integer, Long> byPartition = offsets.get(task.subtopologyId());
        return byPartition == null ? null : byPartition.get(task.partition());
    }

    private static boolean contains(final Map<String, Set<Integer>> tasks, final TaskId task) {
        return tasks.getOrDefault(task.subtopologyId(), Set.of()).contains(task.partition());
    }

    private static void forEachTask(final Map<String, Set<Integer>> tasks, final Consumer<TaskId> action) {
        tasks.forEach((subtopologyId, partitions) ->
            partitions.forEach(partition -> action.accept(new TaskId(subtopologyId, partition))));
    }

    private static Map<TaskId, Integer> standbyCounts(final Map<String, TasksTuple> assignment) {
        final Map<TaskId, Integer> counts = new HashMap<>();
        assignment.values().forEach(tasks -> forEachTask(tasks.standbyTasks(), task -> counts.merge(task, 1, Integer::sum)));
        return counts;
    }

    /**
     * The assignment without the members that hold nothing, for comparing two assignments that only differ in
     * whether an empty member is listed.
     */
    private static Map<String, TasksTuple> normalized(final Map<String, TasksTuple> assignment) {
        final Map<String, TasksTuple> normalized = new TreeMap<>();
        assignment.forEach((memberId, tasks) -> {
            if (!tasks.isEmpty()) {
                normalized.put(memberId, tasks);
            }
        });
        return normalized;
    }

    private static <K extends Comparable<K>> List<K> reversedKeys(final Map<K, ?> map) {
        final List<K> keys = new ArrayList<>(new TreeSet<>(map.keySet()));
        Collections.reverse(keys);
        return keys;
    }

    private static Map<String, Set<Integer>> reversed(final Map<String, Set<Integer>> tasks) {
        final Map<String, Set<Integer>> reversed = new LinkedHashMap<>();
        reversedKeys(tasks).forEach(subtopologyId -> {
            final List<Integer> partitions = new ArrayList<>(new TreeSet<>(tasks.get(subtopologyId)));
            Collections.reverse(partitions);
            reversed.put(subtopologyId, new LinkedHashSet<>(partitions));
        });
        return reversed;
    }

    private static Map<String, Map<Integer, Long>> reversedOffsets(final Map<String, Map<Integer, Long>> offsets) {
        final Map<String, Map<Integer, Long>> reversed = new LinkedHashMap<>();
        reversedKeys(offsets).forEach(subtopologyId -> {
            final Map<Integer, Long> byPartition = offsets.get(subtopologyId);
            final Map<Integer, Long> reversedByPartition = new LinkedHashMap<>();
            reversedKeys(byPartition).forEach(partition -> reversedByPartition.put(partition, byPartition.get(partition)));
            reversed.put(subtopologyId, reversedByPartition);
        });
        return reversed;
    }
}
