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

import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.TopologyDescriber;
import org.apache.kafka.coordinator.group.streams.RefinerFuzzMetrics.ScenarioMetrics;
import org.apache.kafka.coordinator.group.streams.RefinerFuzzMetrics.ScenarioResult;
import org.apache.kafka.coordinator.group.streams.RefinerFuzzScenario.Event;
import org.apache.kafka.coordinator.group.streams.RefinerInvariants.RefineCall;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentConfigsImpl;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberMetadataAndStateImpl;
import org.apache.kafka.coordinator.group.streams.assignor.StickyTaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskId;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Runs one {@link RefinerFuzzScenario} under one refiner, from the settled initial group until the group has
 * converged on its final target assignment, and checks and measures the refiner along the way.
 *
 * <p>The simulation proceeds in ticks. Each tick is one heartbeat of every member, in member ID order, and one step
 * of the clients' restoration. Within a tick:
 * <ol>
 *     <li>the scheduled events are applied;</li>
 *     <li>the clients restore and process for one tick, and every member reports its task offsets;</li>
 *     <li>the coordinator derives the intermediate assignment the way {@code GroupMetadataManager} does: afresh when
 *     a new target assignment arrived or the coordinator failed over; otherwise, once the whole group is reconciled
 *     to the current assignment epoch, as a refinement step that mints a new epoch if it changes any member's
 *     assignment; and in between, the intermediate assignment stays frozen for the epoch;</li>
 *     <li>every member is reconciled toward its slice of the intermediate assignment by the real
 *     {@link CurrentAssignmentBuilder}, reporting as owned exactly what it was assigned, so a revocation is
 *     acknowledged one heartbeat after it was asked for;</li>
 *     <li>the tick is measured.</li>
 * </ol>
 *
 * <p>The clients are modelled per process, since the members of a process share its state directory: every stateful
 * task a process has held has a restore position against the task's changelog. Changelogs are compacted, so a copy
 * never has more than the task's compacted size left to restore, however far behind it is. An active task restores
 * until it has restored the whole changelog and then processes, which is what makes its changelog grow. A standby or
 * warm-up task
 * restores continuously, except on a member that is restoring an active task, where the changelog reader leaves it
 * paused. State a process no longer holds a role for stays on disk and is reported as an offset without an end
 * offset. What the members report follows the client: a processing active task reports nothing, a copy that has not
 * started restoring reports an offset of {@link Long#MAX_VALUE}, and a fully restored copy reports a lag of -1.
 */
final class RefinerFuzzSimulator {

    private static final int MAX_RECORDED_VIOLATIONS = 20;

    private final RefinerFuzzScenario scenario;
    private final AssignmentRefiner refiner;
    private final boolean checkWarmupInvariants;
    private final PrintStream trace;

    private final SortedMap<String, StreamsGroupMember> members = new TreeMap<>();
    private final Map<String, Integer> nextMemberIndex = new TreeMap<>();
    private Map<String, TasksTuple> targetAssignment = Map.of();
    private Map<String, TasksTuple> intermediateAssignment = Map.of();
    private int assignmentEpoch;
    private int numStandbyReplicas;
    private int numWarmupReplicas;
    private boolean assignorPending;

    private final Map<TaskId, Long> changelogEnd = new HashMap<>();
    private final SortedMap<String, Map<TaskId, StateCopy>> stateByProcess = new TreeMap<>();
    private final Map<String, MemberTaskOffsets> taskOffsets = new TreeMap<>();
    private final Set<String> silentMembers = new TreeSet<>();

    private int tick;
    private final List<String> violations = new ArrayList<>();
    private boolean dumped;
    private long coldHandOvers;
    private long unavoidableColdHandOvers;
    private long diskOnlyColdHandOvers;
    private long notProcessingTaskTicks;
    private int refinementSteps;
    private int maxConcurrentWarmups;
    private int maxExtraCopies;
    private long wastedWarmups;
    private int refineCalls;
    private long refineTotalNanos;
    private long refineMaxNanos;

    /**
     * @param scenario              The scenario to run.
     * @param refiner               The refiner under test, a fresh instance for this run.
     * @param checkWarmupInvariants Whether to check the invariants specific to the warm-up refiner, on top of the
     *                              generic ones.
     * @param trace                 Where to print a step-by-step trace of the run, or {@code null} for none.
     */
    RefinerFuzzSimulator(
        final RefinerFuzzScenario scenario,
        final AssignmentRefiner refiner,
        final boolean checkWarmupInvariants,
        final PrintStream trace
    ) {
        this.scenario = scenario;
        this.refiner = refiner;
        this.checkWarmupInvariants = checkWarmupInvariants;
        this.trace = trace;
    }

    ScenarioResult run() {
        initialize();

        int eventIndex = 0;
        boolean converged = false;
        for (tick = 1; tick <= scenario.maxTicks && !converged; tick++) {
            boolean derive = false;
            while (eventIndex < scenario.events.size() && scenario.events.get(eventIndex).tick() == tick) {
                derive |= apply(scenario.events.get(eventIndex++));
            }

            advanceClients();
            reportOffsets();
            deriveIntermediateAssignment(derive);

            final Map<TaskId, String> activeBefore = activeHolders();
            final Set<Placement> warmupsBefore = warmups();
            heartbeatAll();
            measure(activeBefore, warmupsBefore);
            if (trace != null) {
                trace.println("t" + tick + " current assignment: " + describeMembers());
            }

            converged = tick >= scenario.lastEventTick && isConverged();
        }
        final int ticks = tick - 1;

        if (!converged) {
            recordViolations("liveness", null, null, List.of("liveness: the group did not converge on its target "
                + "assignment within " + scenario.maxTicks + " ticks"));
        }
        if (trace != null) {
            trace.println((converged ? "converged" : "did not converge") + " after " + ticks + " ticks");
        }

        return new ScenarioResult(scenario.seed, new ScenarioMetrics(
            coldHandOvers,
            unavoidableColdHandOvers,
            diskOnlyColdHandOvers,
            notProcessingTaskTicks,
            ticks,
            converged,
            refinementSteps,
            maxConcurrentWarmups,
            maxExtraCopies,
            wastedWarmups,
            refineCalls,
            refineTotalNanos / 1_000_000.0,
            refineMaxNanos / 1_000_000.0
        ), List.copyOf(violations));
    }

    /**
     * Derives the intermediate assignment afresh when asked to; otherwise, once the group is reconciled, runs a
     * refinement step, which mints a new assignment epoch if it changes what any member is assigned.
     */
    private void deriveIntermediateAssignment(final boolean derive) {
        if (derive) {
            intermediateAssignment = refineAndCheck("derive");
        } else if (!assignorPending && isGroupReconciled()) {
            final Map<String, TasksTuple> candidate = refineAndCheck("refine");
            if (changesAnyMember(candidate)) {
                assignmentEpoch++;
                refinementSteps++;
                intermediateAssignment = candidate;
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Setup and events

    /**
     * The group starts settled on the sticky assignor's assignment, with every copy of every task caught up.
     */
    private void initialize() {
        numStandbyReplicas = scenario.initialStandbyReplicas;
        numWarmupReplicas = scenario.initialWarmupBudget;
        changelogEnd.putAll(scenario.initialEndOffsets);
        scenario.initialProcesses.forEach((processId, memberCount) -> {
            for (int member = 0; member < memberCount; member++) {
                addMember(processId);
            }
        });
        silentMembers.clear();

        targetAssignment = runStickyAssignor();
        assignmentEpoch = 1;
        intermediateAssignment = targetAssignment;
        for (final String memberId : new ArrayList<>(members.keySet())) {
            final TasksTuple tasks = targetAssignment.getOrDefault(memberId, TasksTuple.EMPTY);
            final StreamsGroupMember member = new StreamsGroupMember.Builder(members.get(memberId))
                .setMemberEpoch(assignmentEpoch)
                .setPreviousMemberEpoch(0)
                .setState(MemberState.STABLE)
                .setAssignedTasks(withEpochs(tasks, assignmentEpoch))
                .build();
            members.put(memberId, member);
            final Map<TaskId, StateCopy> states = statesOf(member.processId());
            final Consumer<TaskId> caughtUp = task -> states.put(task, new StateCopy(changelogEnd.get(task) + 1, true));
            forEachStatefulTask(tasks.activeTasks(), caughtUp);
            forEachStatefulTask(tasks.standbyTasks(), caughtUp);
        }
        if (trace != null) {
            trace.println(scenario.describe());
            trace.println("t0 initial target assignment: " + format(targetAssignment));
        }
    }

    /**
     * Applies one event, returning whether the intermediate assignment has to be derived afresh.
     */
    private boolean apply(final Event event) {
        if (trace != null) {
            trace.println("t" + tick + " event " + event);
        }
        switch (event.kind()) {
            case ADD_PROCESS -> {
                for (int member = 0; member < event.value(); member++) {
                    addMember(event.processId());
                }
                assignorPending = true;
            }
            case REMOVE_PROCESS -> {
                membersOf(event.processId()).forEach(this::removeMember);
                stateByProcess.remove(event.processId());
                assignorPending = true;
            }
            case RESTART_PROCESS -> {
                final List<String> restarted = membersOf(event.processId());
                restarted.forEach(this::removeMember);
                restarted.forEach(__ -> addMember(event.processId()));
                assignorPending = true;
            }
            case ADD_MEMBER -> {
                addMember(event.processId());
                assignorPending = true;
            }
            case REMOVE_MEMBER -> {
                final List<String> processMembers = membersOf(event.processId());
                removeMember(processMembers.get(processMembers.size() - 1));
                assignorPending = true;
            }
            case SET_STANDBY_REPLICAS -> {
                numStandbyReplicas = event.value();
                assignorPending = true;
            }
            case SET_WARMUP_BUDGET -> numWarmupReplicas = event.value();
            case FAILOVER -> {
                // The reported offsets and the intermediate assignment are in-memory state that does not survive a
                // failover; the members report their offsets again from their next heartbeat on.
                taskOffsets.clear();
                silentMembers.addAll(members.keySet());
                return true;
            }
            case STICKY_TARGET -> {
                setTargetAssignment(runStickyAssignor());
                return true;
            }
            case SYNTHETIC_TARGET -> {
                setTargetAssignment(syntheticTargetAssignment(new Random(event.seed())));
                return true;
            }
        }
        return false;
    }

    private void setTargetAssignment(final Map<String, TasksTuple> newTargetAssignment) {
        targetAssignment = newTargetAssignment;
        assignmentEpoch++;
        assignorPending = false;
        if (trace != null) {
            trace.println("t" + tick + " target assignment (epoch " + assignmentEpoch + "): " + format(targetAssignment));
        }
    }

    private void addMember(final String processId) {
        final int index = nextMemberIndex.merge(processId, 1, Integer::sum) - 1;
        final String memberId = String.format(Locale.ROOT, "%s-m%02d", processId, index);
        members.put(memberId, new StreamsGroupMember.Builder(memberId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setProcessId(processId)
            .setRebalanceTimeoutMs(1500)
            .setTopologyEpoch(0)
            .setClientTags(Map.of())
            .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build());
        // A new member reports its offsets from its second heartbeat on.
        silentMembers.add(memberId);
    }

    private void removeMember(final String memberId) {
        members.remove(memberId);
        taskOffsets.remove(memberId);
        silentMembers.remove(memberId);
    }

    private List<String> membersOf(final String processId) {
        final List<String> processMembers = new ArrayList<>();
        members.forEach((memberId, member) -> {
            if (member.processId().equals(processId)) {
                processMembers.add(memberId);
            }
        });
        return processMembers;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Target assignments

    private Map<String, TasksTuple> runStickyAssignor() {
        final Map<String, MemberMetadataAndStateImpl> memberSpecs = new TreeMap<>();
        members.forEach((memberId, member) -> {
            final MemberTaskOffsets offsets = taskOffsets.getOrDefault(memberId, MemberTaskOffsets.EMPTY);
            memberSpecs.put(memberId, new MemberMetadataAndStateImpl(
                Optional.empty(),
                Optional.empty(),
                member.processId(),
                Map.of(),
                member.assignedTasks().activeTasks(),
                member.assignedTasks().standbyTasks(),
                member.assignedTasks().warmupTasks(),
                offsets.taskOffsets(),
                offsets.taskEndOffsets()
            ));
        });
        final GroupAssignment assignment = new StickyTaskAssignor().assign(
            new GroupSpecImpl(memberSpecs, new AssignmentConfigsImpl(numStandbyReplicas, List.of())),
            new ScenarioTopologyDescriber(scenario.subtopologies)
        );
        final Map<String, TasksTuple> target = new TreeMap<>();
        assignment.members().forEach((memberId, memberAssignment) -> target.put(memberId,
            new TasksTuple(memberAssignment.activeTasks(), memberAssignment.standbyTasks(), Map.of())));
        return Collections.unmodifiableMap(target);
    }

    /**
     * A random target assignment with the structure the assignor guarantees: every task active on exactly one
     * member, and up to {@code num.standby.replicas} standby tasks of every stateful task, each on a process of its
     * own that does not run the task.
     */
    private Map<String, TasksTuple> syntheticTargetAssignment(final Random random) {
        final List<String> memberIds = new ArrayList<>(members.keySet());
        final SortedMap<String, List<String>> membersByProcess = new TreeMap<>();
        members.forEach((memberId, member) ->
            membersByProcess.computeIfAbsent(member.processId(), __ -> new ArrayList<>()).add(memberId));

        final Map<String, Map<String, Set<Integer>>> active = new TreeMap<>();
        final Map<String, Map<String, Set<Integer>>> standby = new TreeMap<>();
        scenario.subtopologies.forEach((subtopologyId, subtopology) -> {
            for (int partition = 0; partition < subtopology.numberOfTasks(); partition++) {
                final String owner = memberIds.get(random.nextInt(memberIds.size()));
                add(active, owner, subtopologyId, partition);
                if (subtopology.stateChangelogTopics().isEmpty()) {
                    continue;
                }
                final List<String> otherProcesses = new ArrayList<>(membersByProcess.keySet());
                otherProcesses.remove(members.get(owner).processId());
                Collections.shuffle(otherProcesses, random);
                for (int replica = 0; replica < Math.min(numStandbyReplicas, otherProcesses.size()); replica++) {
                    final List<String> candidates = membersByProcess.get(otherProcesses.get(replica));
                    add(standby, candidates.get(random.nextInt(candidates.size())), subtopologyId, partition);
                }
            }
        });

        final Map<String, TasksTuple> target = new TreeMap<>();
        for (final String memberId : memberIds) {
            target.put(memberId, new TasksTuple(
                active.getOrDefault(memberId, Map.of()),
                standby.getOrDefault(memberId, Map.of()),
                Map.of()
            ));
        }
        return Collections.unmodifiableMap(target);
    }

    private static void add(
        final Map<String, Map<String, Set<Integer>>> assignment,
        final String memberId,
        final String subtopologyId,
        final int partition
    ) {
        assignment.computeIfAbsent(memberId, __ -> new TreeMap<>())
            .computeIfAbsent(subtopologyId, __ -> new TreeSet<>())
            .add(partition);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Clients

    /**
     * One tick of restoring and processing on every client.
     */
    private void advanceClients() {
        final Set<String> restoringAnActive = new TreeSet<>();
        for (final StreamsGroupMember member : members.values()) {
            final Map<TaskId, StateCopy> states = statesOf(member.processId());
            for (final TaskId task : runningActives(member)) {
                final StateCopy state = states.computeIfAbsent(task, __ -> new StateCopy(compactedStart(task), true));
                state.started = true;
                final long end = changelogEnd.get(task);
                if (state.position > end) {
                    // Processing: the task writes to its changelog, and its state keeps up with it.
                    final long newEnd = end + scenario.writeRates.get(task);
                    changelogEnd.put(task, newEnd);
                    state.position = newEnd + 1;
                } else {
                    restoringAnActive.add(member.memberId());
                    restore(task, state);
                }
            }
        }

        for (final StreamsGroupMember member : members.values()) {
            final Map<TaskId, StateCopy> states = statesOf(member.processId());
            final boolean paused = restoringAnActive.contains(member.memberId());
            for (final TaskId task : copies(member)) {
                final StateCopy state = states.get(task);
                if (state == null) {
                    states.put(task, new StateCopy(compactedStart(task), false));
                } else if (!paused) {
                    state.started = true;
                    restore(task, state);
                }
            }
        }
    }

    /**
     * One tick of restoring. A changelog is compacted, so however far behind a copy is, it never has more than the
     * task's compacted size left to restore.
     */
    private void restore(final TaskId task, final StateCopy state) {
        final long end = changelogEnd.get(task);
        state.position = Math.min(end + 1, Math.max(state.position, compactedStart(task)) + scenario.restoreRate);
    }

    /**
     * Where a copy that has nothing of the task yet starts restoring from.
     */
    private long compactedStart(final TaskId task) {
        return Math.max(0, changelogEnd.get(task) - scenario.compactedSizes.get(task));
    }

    private void reportOffsets() {
        final Map<String, Set<TaskId>> heldByProcess = heldByProcess();
        final Set<String> reportedDormantFor = new TreeSet<>();
        for (final StreamsGroupMember member : members.values()) {
            final String memberId = member.memberId();
            if (silentMembers.remove(memberId)) {
                continue;
            }
            final Map<TaskId, StateCopy> states = statesOf(member.processId());
            final Map<String, Map<Integer, Long>> offsets = new HashMap<>();
            final Map<String, Map<Integer, Long>> endOffsets = new HashMap<>();

            for (final TaskId task : runningActives(member)) {
                final StateCopy state = states.get(task);
                if (state.position <= changelogEnd.get(task)) {
                    put(offsets, task, state.position);
                    put(endOffsets, task, changelogEnd.get(task));
                }
            }
            for (final TaskId task : copies(member)) {
                final StateCopy state = states.get(task);
                put(offsets, task, state.started ? state.position : Long.MAX_VALUE);
                put(endOffsets, task, changelogEnd.get(task));
            }
            // State on disk that no member of the process holds a role for is reported by one member of the process.
            if (reportedDormantFor.add(member.processId())) {
                final Set<TaskId> held = heldByProcess.getOrDefault(member.processId(), Set.of());
                states.forEach((task, state) -> {
                    if (!held.contains(task) && state.started) {
                        put(offsets, task, state.position);
                    }
                });
            }
            taskOffsets.put(memberId, new MemberTaskOffsets(offsets, endOffsets));
        }
    }

    private Set<TaskId> runningActives(final StreamsGroupMember member) {
        final Set<TaskId> tasks = new HashSet<>();
        forEachStatefulTask(member.assignedTasks().activeTasks(), tasks::add);
        forEachStatefulTask(member.tasksPendingRevocation().activeTasks(), tasks::add);
        return tasks;
    }

    private Set<TaskId> copies(final StreamsGroupMember member) {
        final Set<TaskId> tasks = new HashSet<>();
        forEachStatefulTask(member.assignedTasks().standbyTasks(), tasks::add);
        forEachStatefulTask(member.assignedTasks().warmupTasks(), tasks::add);
        forEachStatefulTask(member.tasksPendingRevocation().standbyTasks(), tasks::add);
        forEachStatefulTask(member.tasksPendingRevocation().warmupTasks(), tasks::add);
        tasks.removeAll(runningActives(member));
        return tasks;
    }

    /**
     * The tasks each process holds in any role, including the ones pending revocation.
     */
    private Map<String, Set<TaskId>> heldByProcess() {
        final Map<String, Set<TaskId>> held = new HashMap<>();
        for (final StreamsGroupMember member : members.values()) {
            final Set<TaskId> onProcess = held.computeIfAbsent(member.processId(), __ -> new HashSet<>());
            onProcess.addAll(runningActives(member));
            onProcess.addAll(copies(member));
        }
        return held;
    }

    /**
     * The processes holding each task as a standby or warm-up task, including the ones pending revocation.
     */
    private Map<TaskId, Set<String>> copyProcesses() {
        final Map<TaskId, Set<String>> processes = new HashMap<>();
        for (final StreamsGroupMember member : members.values()) {
            copies(member).forEach(task -> processes.computeIfAbsent(task, __ -> new HashSet<>()).add(member.processId()));
        }
        return processes;
    }

    private Map<TaskId, StateCopy> statesOf(final String processId) {
        return stateByProcess.computeIfAbsent(processId, __ -> new HashMap<>());
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Coordinator

    private Map<String, TasksTuple> refineAndCheck(final String reason) {
        final RefineCall call = new RefineCall(
            Collections.unmodifiableMap(new HashMap<>(members)),
            targetAssignment,
            Collections.unmodifiableMap(new HashMap<>(taskOffsets)),
            scenario.subtopologies,
            numWarmupReplicas,
            scenario.acceptableRecoveryLag
        );

        final long start = System.nanoTime();
        final Map<String, TasksTuple> refined = call.refine(refiner);
        final long elapsed = System.nanoTime() - start;
        refineCalls++;
        refineTotalNanos += elapsed;
        refineMaxNanos = Math.max(refineMaxNanos, elapsed);

        final List<String> found = new ArrayList<>(RefinerInvariants.check(call, refined, checkWarmupInvariants));
        found.addAll(RefinerInvariants.checkDeterminism(refiner, call, refined));
        if (!found.isEmpty()) {
            recordViolations(reason, call, refined, found);
        }
        if (trace != null) {
            trace.printf(Locale.ROOT, "t%d %s (epoch %d, %.3f ms): %s%n    I=%s%n", tick, reason, assignmentEpoch,
                elapsed / 1_000_000.0, describeRefinement(refined), format(refined));
        }

        // What the coordinator does with a refiner that loses or duplicates an active task.
        if (!AssignmentRefiner.preservesActiveTaskCount(targetAssignment, refined)) {
            return targetAssignment;
        }
        return refined;
    }

    private boolean isGroupReconciled() {
        return members.values().stream().allMatch(member -> member.isReconciledTo(assignmentEpoch));
    }

    private boolean changesAnyMember(final Map<String, TasksTuple> candidate) {
        return members.values().stream().anyMatch(member ->
            !candidate.getOrDefault(member.memberId(), TasksTuple.EMPTY).sameTasks(member.assignedTasks()));
    }

    private void heartbeatAll() {
        final TaskProcessIndex index = new TaskProcessIndex(members.values());
        for (final String memberId : new ArrayList<>(members.keySet())) {
            final StreamsGroupMember member = members.get(memberId);
            final TasksTupleWithEpochs assigned = member.assignedTasks();
            final StreamsGroupMember updated = new CurrentAssignmentBuilder(member)
                .withTargetAssignment(assignmentEpoch, intermediateAssignment.getOrDefault(memberId, TasksTuple.EMPTY))
                .withCurrentActiveTaskProcessId(index::activeProcessId)
                .withCurrentStandbyTaskProcessIds(index::standbyProcessIds)
                .withCurrentWarmupTaskProcessIds(index::warmupProcessIds)
                .withOwnedAssignment(new TasksTuple(assigned.activeTasks(), assigned.standbyTasks(), assigned.warmupTasks()))
                .build();
            if (!updated.equals(member)) {
                index.remove(member);
                index.add(updated);
                members.put(memberId, updated);
            }
        }
    }

    private boolean isConverged() {
        if (assignorPending || !members.keySet().containsAll(targetAssignment.keySet())) {
            return false;
        }
        return members.values().stream().allMatch(member ->
            member.isReconciledTo(assignmentEpoch)
                && member.tasksPendingRevocation().isEmpty()
                && targetAssignment.getOrDefault(member.memberId(), TasksTuple.EMPTY).sameTasks(member.assignedTasks()));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Measurements

    private void measure(final Map<TaskId, String> activeBefore, final Set<Placement> warmupsBefore) {
        final Map<TaskId, String> activeNow = activeHolders();
        final Map<TaskId, Set<String>> copyProcesses = copyProcesses();
        activeNow.forEach((task, memberId) -> {
            final String previousHolder = activeBefore.get(task);
            if (!memberId.equals(previousHolder) && !isWarm(members.get(memberId).processId(), task)) {
                countColdHandOver(task, previousHolder, memberId, copyProcesses);
            }
        });

        for (final TaskId task : scenario.statefulTasks) {
            final String holder = activeNow.get(task);
            final StateCopy state = holder == null ? null : statesOf(members.get(holder).processId()).get(task);
            if (state == null || state.position <= changelogEnd.get(task)) {
                notProcessingTaskTicks++;
            }
        }

        final Set<Placement> warmupsNow = warmups();
        maxConcurrentWarmups = Math.max(maxConcurrentWarmups, warmupsNow.size());
        for (final Placement warmup : warmupsBefore) {
            if (!warmupsNow.contains(warmup) && !tookOver(warmup.memberId(), warmup.task())) {
                wastedWarmups++;
            }
        }

        maxExtraCopies = Math.max(maxExtraCopies, extraCopies());
    }

    private void countColdHandOver(
        final TaskId task,
        final String previousHolder,
        final String memberId,
        final Map<TaskId, Set<String>> copyProcesses
    ) {
        coldHandOvers++;
        final String kind;
        if (stateByProcess.keySet().stream().noneMatch(processId -> isWarm(processId, task))) {
            unavoidableColdHandOvers++;
            kind = "unavoidable";
        } else if (!isHeldWarm(previousHolder, task, copyProcesses)) {
            diskOnlyColdHandOvers++;
            kind = "disk only";
        } else {
            kind = "avoidable";
        }
        if (trace != null) {
            trace.println("t" + tick + " cold hand-over (" + kind + ") of " + task + " from " + previousHolder
                + " to " + memberId + ", warm on " + warmProcesses(task));
        }
    }

    /**
     * Whether the process holds the task's state within {@code acceptable.recovery.lag}, as far as the simulation
     * knows, which may be further than the members have reported.
     */
    private boolean isWarm(final String processId, final TaskId task) {
        final Map<TaskId, StateCopy> states = stateByProcess.get(processId);
        final StateCopy state = states == null ? null : states.get(task);
        return state != null && state.started
            && changelogEnd.get(task) - state.position <= scenario.acceptableRecoveryLag;
    }

    private List<String> warmProcesses(final TaskId task) {
        final List<String> warm = new ArrayList<>();
        stateByProcess.keySet().forEach(processId -> {
            if (isWarm(processId, task)) {
                warm.add(processId);
            }
        });
        return warm;
    }

    /**
     * Whether the task's previous active holder, or a member holding a copy of it, has its state warm.
     */
    private boolean isHeldWarm(
        final String previousHolder,
        final TaskId task,
        final Map<TaskId, Set<String>> copyProcesses
    ) {
        final StreamsGroupMember previous = previousHolder == null ? null : members.get(previousHolder);
        if (previous != null && isWarm(previous.processId(), task)) {
            return true;
        }
        return copyProcesses.getOrDefault(task, Set.of()).stream().anyMatch(processId -> isWarm(processId, task));
    }

    private boolean tookOver(final String memberId, final TaskId task) {
        final StreamsGroupMember member = members.get(memberId);
        return member != null && (contains(member.assignedTasks().activeTasks(), task)
            || contains(member.assignedTasks().standbyTasks(), task));
    }

    private int extraCopies() {
        final Map<TaskId, Integer> copies = new HashMap<>();
        final Consumer<TaskId> count = task -> copies.merge(task, 1, Integer::sum);
        for (final StreamsGroupMember member : members.values()) {
            forEachStatefulTask(member.assignedTasks().activeTasks(), count);
            forEachStatefulTask(member.assignedTasks().standbyTasks(), count);
            forEachStatefulTask(member.assignedTasks().warmupTasks(), count);
        }
        final Map<TaskId, Integer> expected = new HashMap<>();
        for (final TasksTuple tasks : targetAssignment.values()) {
            forEachStatefulTask(tasks.activeTasks(), task -> expected.merge(task, 1, Integer::sum));
            forEachStatefulTask(tasks.standbyTasks(), task -> expected.merge(task, 1, Integer::sum));
        }
        int extra = 0;
        for (final Map.Entry<TaskId, Integer> entry : copies.entrySet()) {
            extra += Math.max(0, entry.getValue() - expected.getOrDefault(entry.getKey(), 0));
        }
        return extra;
    }

    private Map<TaskId, String> activeHolders() {
        final Map<TaskId, String> holders = new HashMap<>();
        members.forEach((memberId, member) ->
            forEachStatefulTask(member.assignedTasks().activeTasks(), task -> holders.put(task, memberId)));
        return holders;
    }

    private Set<Placement> warmups() {
        final Set<Placement> warmups = new HashSet<>();
        members.forEach((memberId, member) ->
            forEachStatefulTask(member.assignedTasks().warmupTasks(), task -> warmups.add(new Placement(memberId, task))));
        return warmups;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Reporting

    private void recordViolations(
        final String reason,
        final RefineCall call,
        final Map<String, TasksTuple> refined,
        final List<String> found
    ) {
        for (final String violation : found) {
            if (violations.size() < MAX_RECORDED_VIOLATIONS) {
                violations.add("t" + tick + " " + reason + " (epoch " + assignmentEpoch + "): " + violation);
            }
        }
        if (!dumped && call != null) {
            dumped = true;
            violations.add("state at the first violation:\n" + dump(call, refined));
        }
        if (trace != null) {
            found.forEach(violation -> trace.println("t" + tick + " VIOLATION " + violation));
        }
    }

    private String describeMembers() {
        final StringBuilder builder = new StringBuilder("{");
        members.forEach((memberId, member) -> {
            builder.append(' ').append(memberId).append('(').append(member.state()).append('@')
                .append(member.memberEpoch()).append(")=").append(format(member.assignedTasks()));
            if (!member.tasksPendingRevocation().isEmpty()) {
                builder.append(" pending ").append(format(member.tasksPendingRevocation()));
            }
        });
        return builder.append(" }").toString();
    }

    private String describeRefinement(final Map<String, TasksTuple> refined) {
        int held = 0;
        int warmupCount = 0;
        for (final Map.Entry<String, TasksTuple> entry : refined.entrySet()) {
            final TasksTuple target = targetAssignment.getOrDefault(entry.getKey(), TasksTuple.EMPTY);
            for (final Map.Entry<String, Set<Integer>> active : entry.getValue().activeTasks().entrySet()) {
                for (final int partition : active.getValue()) {
                    if (!target.activeTasks().getOrDefault(active.getKey(), Set.of()).contains(partition)) {
                        held++;
                    }
                }
            }
            warmupCount += entry.getValue().warmupTasks().values().stream().mapToInt(Set::size).sum();
        }
        return held + " held back, " + warmupCount + " warm-up tasks of " + numWarmupReplicas;
    }

    private String dump(final RefineCall call, final Map<String, TasksTuple> refined) {
        final StringBuilder builder = new StringBuilder();
        builder.append("  budget=").append(call.numWarmupReplicas())
            .append(" acceptableRecoveryLag=").append(call.acceptableRecoveryLag()).append('\n');
        final Set<String> memberIds = new TreeSet<>(call.members().keySet());
        memberIds.addAll(call.targetAssignment().keySet());
        memberIds.addAll(refined.keySet());
        for (final String memberId : memberIds) {
            final StreamsGroupMember member = call.members().get(memberId);
            builder.append("  ").append(memberId);
            if (member == null) {
                builder.append(" (left the group)");
            } else {
                builder.append('@').append(member.processId())
                    .append(" C=").append(format(member.assignedTasks()))
                    .append(" pending=").append(format(member.tasksPendingRevocation()))
                    .append(" offsets=").append(call.taskOffsets().getOrDefault(memberId, MemberTaskOffsets.EMPTY));
            }
            builder.append("\n      F=").append(format(call.targetAssignment().getOrDefault(memberId, TasksTuple.EMPTY)))
                .append("\n      I=").append(format(refined.getOrDefault(memberId, TasksTuple.EMPTY)))
                .append('\n');
        }
        return builder.toString();
    }

    static String format(final Map<String, TasksTuple> assignment) {
        final StringBuilder builder = new StringBuilder("{");
        new TreeMap<>(assignment).forEach((memberId, tasks) ->
            builder.append(' ').append(memberId).append('=').append(format(tasks)));
        return builder.append(" }").toString();
    }

    static String format(final TasksTuple tasks) {
        return "A" + formatTasks(tasks.activeTasks()) + " S" + formatTasks(tasks.standbyTasks())
            + " W" + formatTasks(tasks.warmupTasks());
    }

    private static String format(final TasksTupleWithEpochs tasks) {
        return "A" + formatTasks(tasks.activeTasks()) + " S" + formatTasks(tasks.standbyTasks())
            + " W" + formatTasks(tasks.warmupTasks());
    }

    private static String formatTasks(final Map<String, Set<Integer>> tasks) {
        final SortedSet<TaskId> sorted = new TreeSet<>();
        tasks.forEach((subtopologyId, partitions) ->
            partitions.forEach(partition -> sorted.add(new TaskId(subtopologyId, partition))));
        return sorted.toString();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Helpers

    private void forEachStatefulTask(final Map<String, Set<Integer>> tasks, final Consumer<TaskId> action) {
        tasks.forEach((subtopologyId, partitions) -> {
            final ConfiguredSubtopology subtopology = scenario.subtopologies.get(subtopologyId);
            if (subtopology != null && !subtopology.stateChangelogTopics().isEmpty()) {
                partitions.forEach(partition -> action.accept(new TaskId(subtopologyId, partition)));
            }
        });
    }

    private static boolean contains(final Map<String, Set<Integer>> tasks, final TaskId task) {
        return tasks.getOrDefault(task.subtopologyId(), Set.of()).contains(task.partition());
    }

    private static void put(final Map<String, Map<Integer, Long>> offsets, final TaskId task, final long offset) {
        offsets.computeIfAbsent(task.subtopologyId(), __ -> new HashMap<>()).put(task.partition(), offset);
    }

    private static TasksTupleWithEpochs withEpochs(final TasksTuple tasks, final int epoch) {
        final Map<String, Map<Integer, Integer>> activeWithEpochs = new TreeMap<>();
        tasks.activeTasks().forEach((subtopologyId, partitions) -> {
            final Map<Integer, Integer> byPartition = new TreeMap<>();
            partitions.forEach(partition -> byPartition.put(partition, epoch));
            activeWithEpochs.put(subtopologyId, byPartition);
        });
        return new TasksTupleWithEpochs(activeWithEpochs, tasks.standbyTasks(), tasks.warmupTasks());
    }

    /**
     * How far a process has restored a task's changelog: the next offset it would restore, so that a fully restored
     * copy sits at the changelog's end offset plus one.
     */
    private static final class StateCopy {

        private long position;
        private boolean started;

        private StateCopy(final long position, final boolean started) {
            this.position = position;
            this.started = started;
        }
    }

    /**
     * Which processes hold each task in each role, across the members' assigned tasks and their tasks pending
     * revocation. This is what {@link StreamsGroup} maintains for the reconciler.
     */
    private static final class TaskProcessIndex {

        private final Map<TaskId, Map<String, Integer>> active = new HashMap<>();
        private final Map<TaskId, Map<String, Integer>> standby = new HashMap<>();
        private final Map<TaskId, Map<String, Integer>> warmup = new HashMap<>();

        private TaskProcessIndex(final Iterable<StreamsGroupMember> members) {
            members.forEach(this::add);
        }

        private void add(final StreamsGroupMember member) {
            update(member, 1);
        }

        private void remove(final StreamsGroupMember member) {
            update(member, -1);
        }

        private void update(final StreamsGroupMember member, final int delta) {
            for (final TasksTupleWithEpochs tasks : List.of(member.assignedTasks(), member.tasksPendingRevocation())) {
                update(active, tasks.activeTasks(), member.processId(), delta);
                update(standby, tasks.standbyTasks(), member.processId(), delta);
                update(warmup, tasks.warmupTasks(), member.processId(), delta);
            }
        }

        private static void update(
            final Map<TaskId, Map<String, Integer>> index,
            final Map<String, Set<Integer>> tasks,
            final String processId,
            final int delta
        ) {
            tasks.forEach((subtopologyId, partitions) -> partitions.forEach(partition -> {
                final Map<String, Integer> counts =
                    index.computeIfAbsent(new TaskId(subtopologyId, partition), __ -> new HashMap<>());
                if (counts.merge(processId, delta, Integer::sum) == 0) {
                    counts.remove(processId);
                }
            }));
        }

        private String activeProcessId(final String subtopologyId, final int partition) {
            final Map<String, Integer> counts = active.get(new TaskId(subtopologyId, partition));
            return counts == null || counts.isEmpty() ? null : counts.keySet().iterator().next();
        }

        private Set<String> standbyProcessIds(final String subtopologyId, final int partition) {
            return Set.copyOf(standby.getOrDefault(new TaskId(subtopologyId, partition), Map.of()).keySet());
        }

        private Set<String> warmupProcessIds(final String subtopologyId, final int partition) {
            return Set.copyOf(warmup.getOrDefault(new TaskId(subtopologyId, partition), Map.of()).keySet());
        }
    }

    private record Placement(String memberId, TaskId task) {
    }

    private record ScenarioTopologyDescriber(SortedMap<String, ConfiguredSubtopology> configuredSubtopologies)
        implements TopologyDescriber {

        @Override
        public List<String> subtopologies() {
            return List.copyOf(configuredSubtopologies.keySet());
        }

        @Override
        public int maxNumInputPartitions(final String subtopologyId) {
            return configuredSubtopologies.get(subtopologyId).numberOfTasks();
        }

        @Override
        public boolean isStateful(final String subtopologyId) {
            return !configuredSubtopologies.get(subtopologyId).stateChangelogTopics().isEmpty();
        }
    }
}
