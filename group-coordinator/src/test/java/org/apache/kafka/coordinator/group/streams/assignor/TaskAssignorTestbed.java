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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.api.streams.assignor.TopologyDescriber;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Randomized testbed for {@link TaskAssignor} implementations. Each scenario generates a random topology and group
 * from a seed, runs the assignor until the assignment is stable, then applies random rebalance events (processes
 * joining, leaving, restarting or changing tags, all processes of one tag value leaving at once, members joining
 * or leaving, the standby count changing, subtopologies growing, appearing or disappearing) and converges again.
 * Every assignment is checked against {@link AssignmentInvariants} and the assignor-specific checks given by the
 * caller; the converged result of each rebalance is graded by {@link AssignmentMetrics}, and the summary is printed
 * so two versions of an assignor can be compared on the same scenarios.
 * <p>
 * Runs are deterministic: all scenarios derive from {@link #DEFAULT_BASE_SEED}. Set
 * {@code STREAMS_ASSIGNOR_FUZZ_BASE_SEED=<seed>} to run a different set of scenarios, or
 * {@code STREAMS_ASSIGNOR_FUZZ_SEED=<seed>} to replay the single scenario named by a failure. From an IDE the
 * system properties {@code streams.assignor.fuzz.base.seed} and {@code streams.assignor.fuzz.seed} work as well;
 * Gradle does not forward {@code -D} to the test JVM.
 */
final class TaskAssignorTestbed {

    /**
     * An assignor-specific check run on every assignment, in addition to the invariants. {@code last} is true for
     * the final assignment of a rebalance, the converged one or the last attempt, so expensive checks can run once.
     */
    @FunctionalInterface
    interface AssignmentCheck {
        void check(Scenario scenario, GroupAssignment result, boolean last);
    }

    static final long DEFAULT_BASE_SEED = 42L;
    static final List<String> TAG_KEYS = List.of("region", "zone", "rack");
    static final String HOST_TAG = "host";

    private static final String SEED_PROPERTY = "streams.assignor.fuzz.seed";
    private static final String SEED_ENVIRONMENT_VARIABLE = "STREAMS_ASSIGNOR_FUZZ_SEED";
    private static final String BASE_SEED_PROPERTY = "streams.assignor.fuzz.base.seed";
    private static final String BASE_SEED_ENVIRONMENT_VARIABLE = "STREAMS_ASSIGNOR_FUZZ_BASE_SEED";
    private static final int MAX_EVENTS = 8;
    private static final int MAX_CONVERGENCE_ITERATIONS = 10;
    private static final long RESTORED_OFFSET = 100L;
    /** Mean of {@link Profile#LARGE}'s membersPerProcess draw, used to size the group from the task count. */
    private static final int EXPECTED_MEMBERS_PER_LARGE_PROCESS = 9;

    /**
     * How large the generated groups are. Each bound is drawn per scenario (or per process) from the given random.
     */
    enum Profile {
        /**
         * Hits the logical corners: fewer processes than copies, tag values exhausted, quota boundaries, load ties.
         * Small enough that a failure prints every assignment.
         */
        SMALL {
            int scenarios() {
                return 300;
            }

            boolean fullHistory() {
                return true;
            }

            int processes(final Random random, final int tasks) {
                return random.nextInt(8) + 1;
            }

            int membersPerProcess(final Random random) {
                return random.nextInt(4) + 1;
            }

            int subtopologies(final Random random) {
                return random.nextInt(6) + 1;
            }

            int partitions(final Random random) {
                return random.nextInt(8) + 1;
            }

            boolean stateful(final Random random) {
                return random.nextBoolean();
            }

            int tagKeyCount(final Random random) {
                return random.nextInt(TAG_KEYS.size() + 1);
            }

            int valuesPerTag(final Random random) {
                return random.nextInt(3) + 2;
            }

            boolean hostTag(final Random random) {
                return false;
            }

            int standbyReplicas(final Random random) {
                return random.nextInt(3);
            }
        },
        /**
         * Production shape: tens to over a hundred processes with a few large ones, sized so that members hold a
         * few active tasks each and the quota leaves room for balancing decisions; one or two tags with about three
         * values each so tag dimensions rarely run out, standbys mostly 1. Occasionally adds a host tag whose value
         * is unique per process.
         */
        LARGE {
            int scenarios() {
                return 30;
            }

            boolean fullHistory() {
                return false;
            }

            int processes(final Random random, final int tasks) {
                final int draw = random.nextInt(10);
                final int tasksPerMember = draw < 2 ? 2 : draw < 5 ? 3 : draw < 8 ? 4 : draw < 9 ? 6 : 8;
                return Math.max(2, tasks / tasksPerMember / EXPECTED_MEMBERS_PER_LARGE_PROCESS);
            }

            int membersPerProcess(final Random random) {
                return random.nextInt(10) == 0 ? random.nextInt(33) + 32 : random.nextInt(8) + 1;
            }

            int subtopologies(final Random random) {
                return random.nextInt(17) + 4;
            }

            int partitions(final Random random) {
                return random.nextInt(113) + 16;
            }

            boolean stateful(final Random random) {
                return random.nextInt(10) < 6;
            }

            int tagKeyCount(final Random random) {
                return random.nextInt(2) + 1;
            }

            int valuesPerTag(final Random random) {
                final int draw = random.nextInt(10);
                return draw < 6 ? 3 : draw < 8 ? 2 : 4;
            }

            boolean hostTag(final Random random) {
                return random.nextInt(5) == 0;
            }

            int standbyReplicas(final Random random) {
                final int draw = random.nextInt(10);
                return draw < 2 ? 0 : draw < 8 ? 1 : 2;
            }
        };

        abstract int scenarios();

        /** Whether the history records every assignment, or only the metrics of each converged result. */
        abstract boolean fullHistory();

        /** How many processes to generate for a topology of the given size. */
        abstract int processes(Random random, int tasks);

        abstract int membersPerProcess(Random random);

        abstract int subtopologies(Random random);

        abstract int partitions(Random random);

        abstract boolean stateful(Random random);

        abstract int tagKeyCount(Random random);

        abstract int valuesPerTag(Random random);

        abstract boolean hostTag(Random random);

        abstract int standbyReplicas(Random random);
    }

    private final TaskAssignor assignor;
    private final List<AssignmentCheck> checks;

    TaskAssignorTestbed(final TaskAssignor assignor, final List<AssignmentCheck> checks) {
        this.assignor = assignor;
        this.checks = List.copyOf(checks);
    }

    /**
     * Runs all scenarios of the profile and prints two summaries: the first assignment of each group, computed from
     * an empty assignment, and the rebalances after the random events, computed from the previous assignment.
     */
    void run(final Profile profile) {
        final Long fixedSeed = readSeed(SEED_PROPERTY, SEED_ENVIRONMENT_VARIABLE);
        final Long fixedBaseSeed = readSeed(BASE_SEED_PROPERTY, BASE_SEED_ENVIRONMENT_VARIABLE);
        final long baseSeed;
        if (fixedSeed != null) {
            baseSeed = fixedSeed;
        } else {
            // Offset per profile so the runs do not replay the same scenarios.
            final long base = fixedBaseSeed != null ? fixedBaseSeed : DEFAULT_BASE_SEED;
            baseSeed = base + profile.ordinal() * 1_000_000L;
        }
        final int scenarios = fixedSeed != null ? 1 : profile.scenarios();
        final String title = assignor.name() + ", " + profile + ", " + scenarios + " scenarios, base seed " + baseSeed;
        final AssignmentMetrics.Summary fromEmpty = new AssignmentMetrics.Summary(title + ", from empty assignment");
        final AssignmentMetrics.Summary fromPrevious = new AssignmentMetrics.Summary(title + ", from previous assignment");
        for (int i = 0; i < scenarios; i++) {
            runScenario(baseSeed + i, profile, fromEmpty, fromPrevious);
        }
        System.out.println(fromEmpty);
        System.out.println(fromPrevious);
    }

    private static Long readSeed(final String property, final String environmentVariable) {
        final Long fromProperty = Long.getLong(property);
        if (fromProperty != null) {
            return fromProperty;
        }
        final String fromEnvironment = System.getenv(environmentVariable);
        return fromEnvironment == null ? null : Long.valueOf(fromEnvironment);
    }

    private void runScenario(
        final long seed,
        final Profile profile,
        final AssignmentMetrics.Summary fromEmpty,
        final AssignmentMetrics.Summary fromPrevious
    ) {
        final Random random = new Random(seed);
        final Scenario scenario = Scenario.generate(random, profile);
        try {
            converge(scenario, fromEmpty, scenario.baseline());
            final int events = random.nextInt(MAX_EVENTS) + 1;
            for (int i = 0; i < events; i++) {
                final Scenario.Baseline before = scenario.baseline();
                scenario.applyRandomEvent(random);
                converge(scenario, fromPrevious, before);
            }
        } catch (final AssertionError | RuntimeException e) {
            throw new AssertionError(
                "Fuzz scenario failed. Reproduce with " + SEED_ENVIRONMENT_VARIABLE + "=" + seed + " (or -D" + SEED_PROPERTY + "=" + seed
                    + ") in the " + profile + " test\n" + scenario.history,
                e
            );
        }
    }

    /**
     * Runs the assignor, feeding each result back as the previous assignment, until the result stops changing or
     * {@link #MAX_CONVERGENCE_ITERATIONS} is reached. Not reaching a fixed point is recorded, not failed: the
     * coordinator runs the assignor once per rebalance, so it only means the next rebalance will move tasks again.
     * {@code before} is the state before the event that triggered this rebalance; the last result is graded against it.
     */
    private void converge(final Scenario scenario, final AssignmentMetrics.Summary summary, final Scenario.Baseline before) {
        // Reported in the first heartbeat after a restart and gone once the members hold an assignment again.
        final Map<String, Set<TaskId>> restoredTasks = scenario.restoredTasksByProcess();
        for (int iteration = 1; iteration <= MAX_CONVERGENCE_ITERATIONS; iteration++) {
            final GroupAssignment result = assignor.assign(scenario.groupSpec(), scenario.topology);
            final boolean stable = result.members().equals(scenario.previousAssignment);
            final boolean last = stable || iteration == MAX_CONVERGENCE_ITERATIONS;
            try {
                AssignmentInvariants.assertValid(scenario, result);
                for (final AssignmentCheck check : checks) {
                    check.check(scenario, result, last);
                }
            } catch (final AssertionError e) {
                scenario.history.append("  iteration ").append(iteration).append(" input: ").append(format(scenario.previousAssignment)).append('\n')
                    .append("  iteration ").append(iteration).append(" FAILED: ").append(format(result.members())).append('\n');
                throw e;
            }
            if (scenario.profile.fullHistory()) {
                scenario.history.append("  iteration ").append(iteration).append(": ").append(format(result.members())).append('\n');
            }
            scenario.feedBack(result);
            if (last) {
                final AssignmentMetrics.Metrics metrics = AssignmentMetrics.compute(scenario, result, before, restoredTasks);
                summary.add(metrics);
                if (stable) {
                    summary.addConvergence(iteration);
                    scenario.history.append("  converged after ").append(iteration).append(": ").append(metrics).append('\n');
                } else {
                    summary.addNotConverged();
                    scenario.history.append("  not converged within ").append(iteration).append(": ").append(metrics).append('\n');
                }
                return;
            }
        }
    }

    static Set<TaskId> toTaskIds(final Map<String, Set<Integer>> tasks) {
        final Set<TaskId> taskIds = new HashSet<>();
        tasks.forEach((subtopology, partitions) -> partitions.forEach(partition -> taskIds.add(new TaskId(subtopology, partition))));
        return taskIds;
    }

    static String format(final Map<String, MemberAssignment> members) {
        final StringBuilder builder = new StringBuilder();
        for (final Map.Entry<String, MemberAssignment> entry : new TreeMap<>(members).entrySet()) {
            builder.append(entry.getKey())
                .append(" A").append(new TreeSet<>(toTaskIds(entry.getValue().activeTasks())))
                .append(" S").append(new TreeSet<>(toTaskIds(entry.getValue().standbyTasks())))
                .append("; ");
        }
        return builder.toString();
    }

    // ---- Random scenario: topology, group and events ----

    record Subtopology(String id, int partitions, boolean stateful) {
    }

    record Topology(List<Subtopology> specs, Set<TaskId> tasks, Set<TaskId> statefulTasks) implements TopologyDescriber {

        static Topology generate(final Random random, final Profile profile) {
            final List<Subtopology> specs = new ArrayList<>();
            final int count = profile.subtopologies(random);
            for (int i = 0; i < count; i++) {
                specs.add(new Subtopology("s" + i, profile.partitions(random), profile.stateful(random)));
            }
            return of(specs);
        }

        static Topology of(final List<Subtopology> specs) {
            final Set<TaskId> tasks = new HashSet<>();
            final Set<TaskId> statefulTasks = new HashSet<>();
            for (final Subtopology subtopology : specs) {
                for (int partition = 0; partition < subtopology.partitions; partition++) {
                    final TaskId task = new TaskId(subtopology.id, partition);
                    tasks.add(task);
                    if (subtopology.stateful) {
                        statefulTasks.add(task);
                    }
                }
            }
            return new Topology(List.copyOf(specs), tasks, statefulTasks);
        }

        @Override
        public List<String> subtopologies() {
            return specs.stream().map(Subtopology::id).toList();
        }

        @Override
        public int maxNumInputPartitions(final String subtopologyId) throws NoSuchElementException {
            return find(subtopologyId).partitions;
        }

        @Override
        public boolean isStateful(final String subtopologyId) {
            return find(subtopologyId).stateful;
        }

        private Subtopology find(final String subtopologyId) {
            return specs.stream()
                .filter(subtopology -> subtopology.id.equals(subtopologyId))
                .findFirst()
                .orElseThrow();
        }

        @Override
        public String toString() {
            return specs.toString();
        }
    }

    static final class ProcessSpec {
        final List<String> members = new ArrayList<>();
        final Map<String, String> tags;

        ProcessSpec(final Map<String, String> tags) {
            this.tags = tags;
        }

        @Override
        public String toString() {
            return members + " " + tags;
        }
    }

    /** A group under test: its topology, processes and members, and the assignment they currently hold. */
    static final class Scenario {
        final Profile profile;
        final List<String> tagKeys;
        Topology topology;
        final Map<String, ProcessSpec> processes = new LinkedHashMap<>();
        final StringBuilder history = new StringBuilder();
        Map<String, MemberAssignment> previousAssignment = Map.of();
        int numStandbyReplicas;

        private final int valuesPerTag;
        private final Map<String, String> memberToProcess = new HashMap<>();
        private final Map<String, Map<String, Map<Integer, Long>>> restoredOffsets = new HashMap<>();
        private int nextProcess;
        private int nextGeneration;
        private int nextSubtopology;

        /**
         * What the members held before the event that triggered a rebalance, and on which process, so that
         * stickiness can follow tasks across restarts.
         */
        record Baseline(Map<String, MemberAssignment> assignment, Map<String, String> processOfMember) {
        }

        private Scenario(
            final Profile profile,
            final Topology topology,
            final List<String> tagKeys,
            final int valuesPerTag
        ) {
            this.profile = profile;
            this.topology = topology;
            this.tagKeys = tagKeys;
            this.valuesPerTag = valuesPerTag;
            this.nextSubtopology = topology.specs().size();
        }

        static Scenario generate(final Random random, final Profile profile) {
            final Topology topology = Topology.generate(random, profile);
            final List<String> tagKeys = new ArrayList<>(TAG_KEYS.subList(0, profile.tagKeyCount(random)));
            if (profile.hostTag(random)) {
                tagKeys.add(HOST_TAG);
            }
            final Scenario scenario = new Scenario(profile, topology, List.copyOf(tagKeys), profile.valuesPerTag(random));
            scenario.numStandbyReplicas = profile.standbyReplicas(random);
            final int processCount = profile.processes(random, topology.tasks().size());
            for (int i = 0; i < processCount; i++) {
                scenario.addProcess(random);
            }
            scenario.history.append("topology ").append(topology)
                .append(", standbyReplicas=").append(scenario.numStandbyReplicas)
                .append(", tags=").append(tagKeys).append(" with ").append(scenario.valuesPerTag).append(" values")
                .append(", processes ").append(scenario.processes).append('\n');
            return scenario;
        }

        /** A rebalance event; returns false when it is not applicable to the group as it is, so another is drawn. */
        @FunctionalInterface
        private interface Event {
            boolean apply(Random random);
        }

        private final List<Event> events = List.of(
            this::addProcessEvent,
            this::dropProcessEvent,
            this::restartProcessEvent,
            this::addMemberEvent,
            this::dropMemberEvent,
            this::retagProcess,
            this::dropTagValue,
            this::expandPartitions,
            this::addSubtopology,
            this::removeSubtopology,
            this::changeStandbyReplicas
        );

        /** Applies one random event that changes the group; a drawn event that would change nothing is redrawn. */
        void applyRandomEvent(final Random random) {
            boolean applied = false;
            while (!applied) {
                applied = events.get(random.nextInt(events.size())).apply(random);
            }
        }

        private boolean addProcessEvent(final Random random) {
            final String processId = addProcess(random);
            history.append("event: add process ").append(processId).append(' ').append(processes.get(processId)).append('\n');
            return true;
        }

        private boolean dropProcessEvent(final Random random) {
            if (processes.size() == 1) {
                return false;
            }
            final String processId = randomProcess(random);
            removeProcess(processId);
            history.append("event: drop process ").append(processId).append('\n');
            return true;
        }

        private boolean restartProcessEvent(final Random random) {
            final String processId = randomProcess(random);
            restartProcess(processId);
            history.append("event: restart process ").append(processId).append(" -> ").append(processes.get(processId)).append('\n');
            return true;
        }

        private boolean addMemberEvent(final Random random) {
            final String memberId = addMember(randomProcess(random));
            history.append("event: add member ").append(memberId).append('\n');
            return true;
        }

        private boolean dropMemberEvent(final Random random) {
            final String processId = randomProcess(random);
            final ProcessSpec process = processes.get(processId);
            if (process.members.size() == 1) {
                return false;
            }
            final String memberId = process.members.get(random.nextInt(process.members.size()));
            removeMember(processId, memberId);
            history.append("event: drop member ").append(memberId).append('\n');
            return true;
        }

        private boolean retagProcess(final Random random) {
            final String processId = randomProcess(random);
            final String key = randomTagKey(random);
            if (key == null) {
                return false;
            }
            final String value = key + "-" + random.nextInt(valuesPerTag);
            if (value.equals(processes.get(processId).tags.put(key, value))) {
                return false;
            }
            history.append("event: retag process ").append(processId).append(" -> ").append(processes.get(processId)).append('\n');
            return true;
        }

        private boolean changeStandbyReplicas(final Random random) {
            final int standbyReplicas = profile.standbyReplicas(random);
            if (standbyReplicas == numStandbyReplicas) {
                return false;
            }
            numStandbyReplicas = standbyReplicas;
            history.append("event: standbyReplicas=").append(numStandbyReplicas).append('\n');
            return true;
        }

        private String addProcess(final Random random) {
            final String processId = "p" + nextProcess++;
            final Map<String, String> tags = new HashMap<>();
            for (final String key : tagKeys) {
                if (key.equals(HOST_TAG)) {
                    tags.put(key, processId);
                } else if (random.nextInt(10) != 0) {
                    // A process occasionally misses a tag, as a client without that config would.
                    tags.put(key, key + "-" + random.nextInt(valuesPerTag));
                }
            }
            processes.put(processId, new ProcessSpec(tags));
            final int members = profile.membersPerProcess(random);
            for (int i = 0; i < members; i++) {
                addMember(processId);
            }
            return processId;
        }

        private String addMember(final String processId) {
            final ProcessSpec process = processes.get(processId);
            final String memberId = processId + "-m" + process.members.size() + "g" + nextGeneration++;
            process.members.add(memberId);
            memberToProcess.put(memberId, processId);
            return memberId;
        }

        private void removeMember(final String processId, final String memberId) {
            processes.get(processId).members.remove(memberId);
            memberToProcess.remove(memberId);
            restoredOffsets.remove(memberId);
            final Map<String, MemberAssignment> remaining = new HashMap<>(previousAssignment);
            remaining.remove(memberId);
            previousAssignment = remaining;
        }

        private void removeProcess(final String processId) {
            final ProcessSpec process = processes.remove(processId);
            for (final String memberId : process.members) {
                memberToProcess.remove(memberId);
                restoredOffsets.remove(memberId);
            }
            final Map<String, MemberAssignment> remaining = new HashMap<>(previousAssignment);
            remaining.keySet().removeAll(process.members);
            previousAssignment = remaining;
        }

        /**
         * The process comes back with fresh member ids and no target assignment, but its state directories still
         * hold the stateful tasks it owned, which it reports as task offsets.
         */
        private void restartProcess(final String processId) {
            final ProcessSpec process = processes.get(processId);
            final List<String> oldMembers = new ArrayList<>(process.members);
            final Map<String, MemberAssignment> remaining = new HashMap<>(previousAssignment);
            process.members.clear();
            for (final String oldMember : oldMembers) {
                memberToProcess.remove(oldMember);
                restoredOffsets.remove(oldMember);
                final MemberAssignment previous = remaining.remove(oldMember);
                final String newMember = addMember(processId);
                if (previous != null) {
                    final Map<String, Map<Integer, Long>> offsets = new HashMap<>();
                    addOffsets(offsets, previous.activeTasks());
                    addOffsets(offsets, previous.standbyTasks());
                    restoredOffsets.put(newMember, offsets);
                }
            }
            previousAssignment = remaining;
        }

        Baseline baseline() {
            return new Baseline(previousAssignment, Map.copyOf(memberToProcess));
        }

        /** The tasks each process currently reports from its state directories, as a restarted process does. */
        Map<String, Set<TaskId>> restoredTasksByProcess() {
            final Map<String, Set<TaskId>> restoredTasksByProcess = new HashMap<>();
            restoredOffsets.forEach((memberId, offsets) ->
                restoredTasksByProcess.computeIfAbsent(memberToProcess.get(memberId), p -> new HashSet<>()).addAll(toTaskIds(toPartitions(offsets)))
            );
            return restoredTasksByProcess;
        }

        private static Map<String, Set<Integer>> toPartitions(final Map<String, Map<Integer, Long>> offsets) {
            final Map<String, Set<Integer>> partitions = new HashMap<>();
            offsets.forEach((subtopology, perPartition) -> partitions.put(subtopology, perPartition.keySet()));
            return partitions;
        }

        /** Only stateful tasks leave state on disk, so only they are reported. */
        private void addOffsets(final Map<String, Map<Integer, Long>> offsets, final Map<String, Set<Integer>> tasks) {
            tasks.forEach((subtopology, partitions) -> partitions.forEach(partition -> {
                if (topology.statefulTasks().contains(new TaskId(subtopology, partition))) {
                    offsets.computeIfAbsent(subtopology, s -> new HashMap<>()).put(partition, RESTORED_OFFSET);
                }
            }));
        }

        private String randomProcess(final Random random) {
            final List<String> ids = new ArrayList<>(processes.keySet());
            return ids.get(random.nextInt(ids.size()));
        }

        /** A configured tag key other than the per-process host tag, or null if there is none. */
        private String randomTagKey(final Random random) {
            final List<String> keys = tagKeys.stream().filter(key -> !key.equals(HOST_TAG)).toList();
            return keys.isEmpty() ? null : keys.get(random.nextInt(keys.size()));
        }

        /**
         * Correlated failure: every process sharing one value of a tag leaves at once, as when a zone goes down.
         * Not applicable without tags, or when the value covers the whole group.
         */
        private boolean dropTagValue(final Random random) {
            final String key = randomTagKey(random);
            if (key == null) {
                return false;
            }
            final List<String> values = processes.values().stream().map(process -> process.tags.get(key)).filter(Objects::nonNull).distinct().toList();
            if (values.isEmpty()) {
                return false;
            }
            final String value = values.get(random.nextInt(values.size()));
            final List<String> victims = processes.entrySet().stream()
                .filter(entry -> value.equals(entry.getValue().tags.get(key)))
                .map(Map.Entry::getKey)
                .toList();
            if (victims.size() == processes.size()) {
                return false;
            }
            victims.forEach(this::removeProcess);
            history.append("event: drop all processes with ").append(key).append('=').append(value).append(' ').append(victims).append('\n');
            return true;
        }

        /** An input topic gets more partitions: the subtopology grows by up to its current size. */
        private boolean expandPartitions(final Random random) {
            final List<Subtopology> specs = new ArrayList<>(topology.specs());
            final int index = random.nextInt(specs.size());
            final Subtopology old = specs.get(index);
            specs.set(index, new Subtopology(old.id, old.partitions + 1 + random.nextInt(old.partitions), old.stateful));
            topology = Topology.of(specs);
            history.append("event: expand ").append(old.id).append(" from ").append(old.partitions).append(" to ").append(specs.get(index).partitions).append(" partitions\n");
            return true;
        }

        /** A topology update adds a subtopology; its tasks have no previous owner. */
        private boolean addSubtopology(final Random random) {
            final List<Subtopology> specs = new ArrayList<>(topology.specs());
            final Subtopology added = new Subtopology("s" + nextSubtopology++, profile.partitions(random), profile.stateful(random));
            specs.add(added);
            topology = Topology.of(specs);
            history.append("event: add subtopology ").append(added).append('\n');
            return true;
        }

        /**
         * A topology update removes a subtopology. The members still report its tasks as owned until the next
         * assignment, so the assignor sees tasks that no longer exist.
         */
        private boolean removeSubtopology(final Random random) {
            if (topology.specs().size() == 1) {
                return false;
            }
            final List<Subtopology> specs = new ArrayList<>(topology.specs());
            final Subtopology removed = specs.remove(random.nextInt(specs.size()));
            topology = Topology.of(specs);
            history.append("event: remove subtopology ").append(removed.id).append('\n');
            return true;
        }

        GroupSpecImpl groupSpec() {
            return groupSpec(tagKeys);
        }

        /** The group with the given rack-aware tags configured; the members still carry their client tags. */
        GroupSpecImpl groupSpec(final List<String> rackAwareAssignmentTags) {
            final Map<String, MemberMetadataAndStateImpl> members = new HashMap<>();
            processes.forEach((processId, process) -> {
                for (final String memberId : process.members) {
                    final MemberAssignment previous = previousAssignment.get(memberId);
                    members.put(memberId, new MemberMetadataAndStateImpl(
                        Optional.empty(),
                        Optional.empty(),
                        processId,
                        process.tags,
                        previous == null ? Map.of() : previous.activeTasks(),
                        previous == null ? Map.of() : previous.standbyTasks(),
                        Map.of(),
                        restoredOffsets.getOrDefault(memberId, Map.of()),
                        Map.of()
                    ));
                }
            });
            return new GroupSpecImpl(
                members,
                AssignmentConfigsImpl.DEFAULT
                    .withNumStandbyReplicas(numStandbyReplicas)
                    .withRackAwareAssignmentTags(rackAwareAssignmentTags)
            );
        }

        void feedBack(final GroupAssignment result) {
            previousAssignment = new HashMap<>(result.members());
            restoredOffsets.clear();
        }

        Set<String> memberIds() {
            return memberToProcess.keySet();
        }

        String processOf(final String memberId) {
            final String processId = memberToProcess.get(memberId);
            if (processId == null) {
                throw new AssertionError("assignment names unknown member " + memberId);
            }
            return processId;
        }
    }
}
