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
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Randomized testbed for {@link TaskAssignor} implementations. Each scenario generates a random topology and group
 * from a seed, runs the assignor until the assignment converges, then applies random changes (a process joining,
 * leaving or restarting, a member leaving, a subtopology being added) and converges again. Every assignment is checked
 * against {@link AssignmentInvariants} and the assignor-specific checks given by the caller; the converged result of
 * each rebalance is graded by {@link AssignmentMetrics} and the assignor-specific graders given by the caller, and
 * the summary is printed so two assignors, or two versions of one, can be compared on the same scenarios. When the
 * scenario has client tags, the same input is also assigned with {@code rackAwareAssignmentTags} empty, and the
 * tag-related rows of that tag-blind result are reported next to the real ones.
 * <p>
 * Runs are deterministic: {@link #run} derives all scenarios from {@link #DEFAULT_BASE_SEED}, so a test can assert
 * fixed bounds on the summaries. {@link #runFromEnvironment}, for a fuzz test's {@code main}, prints the summaries
 * only: set {@code STREAMS_ASSIGNOR_FUZZ_BASE_SEED=<seed>} to run a different set of scenarios, or
 * {@code STREAMS_ASSIGNOR_FUZZ_SEED=<seed>} to replay the single scenario named by a failure. The system properties
 * {@code streams.assignor.fuzz.base.seed} and {@code streams.assignor.fuzz.seed} work as well.
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

    /**
     * Assignor-specific metrics graded on the converged result of every rebalance, next to {@link AssignmentMetrics}.
     * Every call returns the same rows in the same order; an empty value is not graded for this rebalance.
     * {@code reportedTasks} are the stateful tasks each process reported offsets for when the rebalance started,
     * as a restarted process does for the state on its disk.
     */
    @FunctionalInterface
    interface AssignmentGrader {
        Map<String, OptionalDouble> grade(
            Scenario scenario,
            GroupAssignment result,
            Scenario.Baseline before,
            Map<String, Set<TaskId>> reportedTasks
        );
    }

    static final long DEFAULT_BASE_SEED = 42L;
    static final List<String> TAG_KEYS = List.of("region", "zone", "rack");
    /**
     * A tag whose value is unique per process, as a host name is. Replicas of one task never share a process, so
     * this key cannot separate them any further and the tag metrics report it as n/a; it checks that the assignor
     * copes with such a key and that it does not disturb the other keys.
     */
    private static final String HOST_TAG = "host";

    private static final String SEED_PROPERTY = "streams.assignor.fuzz.seed";
    private static final String SEED_ENVIRONMENT_VARIABLE = "STREAMS_ASSIGNOR_FUZZ_SEED";
    private static final String BASE_SEED_PROPERTY = "streams.assignor.fuzz.base.seed";
    private static final String BASE_SEED_ENVIRONMENT_VARIABLE = "STREAMS_ASSIGNOR_FUZZ_BASE_SEED";
    private static final int MAX_CHANGES = 8;
    private static final int MAX_CONVERGENCE_ITERATIONS = 10;
    private static final long REPORTED_OFFSET = 100L;
    /** Mean of {@link Profile#LARGE}'s membersPerProcess draw, used to size the group from the task count. */
    private static final int EXPECTED_MEMBERS_PER_LARGE_PROCESS = 9;

    /**
     * How large the generated groups are. Each bound is drawn per scenario (or per process) from the given random.
     */
    enum Profile {
        /**
         * Hits the logical corners: fewer processes than replicas, tag values exhausted, uneven shares, load ties.
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
         * few active tasks each, which leaves balancing decisions room; one or two tags with about three
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

        /** Whether the scenario adds {@link #HOST_TAG} to its tag keys. */
        abstract boolean hostTag(Random random);

        abstract int standbyReplicas(Random random);
    }

    private final TaskAssignor assignor;
    private final List<AssignmentCheck> checks;
    private final List<AssignmentGrader> graders;

    TaskAssignorTestbed(final TaskAssignor assignor, final List<AssignmentCheck> checks, final List<AssignmentGrader> graders) {
        this.assignor = assignor;
        this.checks = List.copyOf(checks);
        final List<AssignmentGrader> allGraders = new ArrayList<>();
        allGraders.add(AssignmentMetrics::grade);
        allGraders.addAll(graders);
        this.graders = List.copyOf(allGraders);
    }

    /** The two summaries of a run: the initial assignment of each group, and the rebalances after the random changes. */
    record Summaries(AssignmentMetrics.Summary fromEmpty, AssignmentMetrics.Summary fromPrevious) {
    }

    /**
     * Runs all scenarios of the profile from {@link #DEFAULT_BASE_SEED} and prints two summaries: the initial
     * assignment of each group, computed from an empty assignment, and the rebalances after the random changes,
     * computed from the previous assignment.
     */
    Summaries run(final Profile profile) {
        return run(profile, baseSeed(DEFAULT_BASE_SEED, profile), profile.scenarios());
    }

    /** Like {@link #run(Profile)}, but from the seeds set in the environment, see the class javadoc. */
    void runFromEnvironment(final Profile profile) {
        final Long fixedSeed = readSeed(SEED_PROPERTY, SEED_ENVIRONMENT_VARIABLE);
        final Long fixedBaseSeed = readSeed(BASE_SEED_PROPERTY, BASE_SEED_ENVIRONMENT_VARIABLE);
        if (fixedSeed != null) {
            run(profile, fixedSeed, 1);
        } else {
            run(profile, baseSeed(fixedBaseSeed != null ? fixedBaseSeed : DEFAULT_BASE_SEED, profile), profile.scenarios());
        }
    }

    /** Offset per profile so the runs do not replay the same scenarios. */
    private static long baseSeed(final long base, final Profile profile) {
        return base + profile.ordinal() * 1_000_000L;
    }

    private Summaries run(final Profile profile, final long baseSeed, final int scenarios) {
        final String title = assignor.name() + ", " + profile + ", " + scenarios + " scenarios, base seed " + baseSeed;
        final AssignmentMetrics.Summary fromEmpty = new AssignmentMetrics.Summary(title + ", from empty assignment");
        final AssignmentMetrics.Summary fromPrevious = new AssignmentMetrics.Summary(title + ", from previous assignment");
        for (int i = 0; i < scenarios; i++) {
            runScenario(baseSeed + i, profile, fromEmpty, fromPrevious);
        }
        System.out.println(fromEmpty);
        System.out.println(fromPrevious);
        return new Summaries(fromEmpty, fromPrevious);
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
            final int changes = random.nextInt(MAX_CHANGES) + 1;
            for (int i = 0; i < changes; i++) {
                final Scenario.Baseline before = scenario.baseline();
                scenario.applyRandomChange(random);
                converge(scenario, fromPrevious, before);
            }
        } catch (final AssertionError | RuntimeException e) {
            throw new AssertionError(
                "Fuzz scenario failed. Reproduce with " + SEED_ENVIRONMENT_VARIABLE + "=" + seed + " (or -D" + SEED_PROPERTY + "=" + seed
                    + ") in the " + profile + " run of the fuzz test's main\n" + scenario.history,
                e
            );
        }
    }

    /**
     * One rebalance: the assignor runs on the scenario's input until its output reproduces itself, and the converged
     * result is graded against {@code before}, the state before the change that triggered the rebalance. When the
     * scenario has client tags, the same input is then assigned tag-blind through the same loop and the tag-related
     * rows of that result are added, so the effect and the cost of rack-aware standby task assignment can be read
     * off the same rebalance. The scenario ends up holding the tag-aware result.
     */
    private void converge(final Scenario scenario, final AssignmentMetrics.Summary summary, final Scenario.Baseline before) {
        // Reported in the first heartbeat after a restart and gone once the members hold an assignment again.
        final Map<String, Set<TaskId>> reportedTasks = scenario.reportedTasksByProcess();
        final Scenario.Input input = scenario.input();
        final Converged real = converge(scenario, before, reportedTasks, scenario.tagKeys, "");
        final Map<String, OptionalDouble> metrics = new LinkedHashMap<>(real.metrics());
        if (scenario.tagKeys.isEmpty()) {
            metrics.putAll(AssignmentMetrics.tagBlindRows(metrics, false));
        } else {
            scenario.restore(input);
            final Converged tagBlind = converge(scenario, before, reportedTasks, List.of(), " tag-blind");
            metrics.putAll(AssignmentMetrics.tagBlindRows(tagBlind.metrics(), true));
            scenario.feedBack(real.result());
        }
        summary.add(metrics);
        scenario.history.append(real.converged() ? "  converged after " + real.rounds() + " rounds: " : "  not converged within " + MAX_CONVERGENCE_ITERATIONS + " rounds: ")
            .append(AssignmentMetrics.format(metrics)).append('\n');
    }

    /** The result of a convergence loop, {@code rounds} being the assignments that changed something before it. */
    private record Converged(GroupAssignment result, boolean converged, int rounds, Map<String, OptionalDouble> metrics) {
    }

    /**
     * Runs the assignor, feeding each result back as the previous assignment, until the result converges or
     * {@link #MAX_CONVERGENCE_ITERATIONS} is reached. Not converging is graded, not failed: the coordinator runs the
     * assignor once per rebalance, so it only means the next rebalance will move tasks again. Every result passes
     * {@link AssignmentInvariants} and the registered checks; the last one is graded. {@code label} marks the run in
     * the history.
     */
    private Converged converge(
        final Scenario scenario,
        final Scenario.Baseline before,
        final Map<String, Set<TaskId>> reportedTasks,
        final List<String> rackAwareAssignmentTags,
        final String label
    ) {
        long firstAssignmentNanos = 0;
        for (int iteration = 1; ; iteration++) {
            GroupAssignment result = null;
            final boolean converged;
            final boolean last;
            try {
                final GroupSpecImpl groupSpec = scenario.groupSpec(rackAwareAssignmentTags);
                final long start = System.nanoTime();
                result = assignor.assign(groupSpec, scenario.topology);
                if (iteration == 1) {
                    firstAssignmentNanos = System.nanoTime() - start;
                }
                AssignmentInvariants.assertValid(scenario, result);
                converged = result.members().equals(scenario.previousAssignment);
                last = converged || iteration == MAX_CONVERGENCE_ITERATIONS;
                for (final AssignmentCheck check : checks) {
                    check.check(scenario, result, last);
                }
            } catch (final AssertionError | RuntimeException e) {
                scenario.history.append("  iteration ").append(iteration).append(label).append(" input: ").append(format(scenario.previousAssignment)).append('\n')
                    .append("  iteration ").append(iteration).append(label).append(" FAILED: ").append(result == null ? String.valueOf(e) : format(result.members())).append('\n');
                throw e;
            }
            if (scenario.profile.fullHistory()) {
                scenario.history.append("  iteration ").append(iteration).append(label).append(": ").append(format(result.members())).append('\n');
            }
            if (last) {
                final int rounds = iteration - 1;
                final Map<String, OptionalDouble> metrics = new LinkedHashMap<>();
                for (final AssignmentGrader grader : graders) {
                    metrics.putAll(grader.grade(scenario, result, before, reportedTasks));
                }
                metrics.putAll(AssignmentMetrics.convergenceRows(rounds, converged, firstAssignmentNanos));
                scenario.feedBack(result);
                return new Converged(result, converged, rounds, metrics);
            }
            scenario.feedBack(result);
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
        /** Offsets a restarted process reports for the stateful tasks on its disk, keyed by its new member ids. */
        private final Map<String, Map<String, Map<Integer, Long>>> reportedOffsets = new HashMap<>();
        private int nextProcess;
        private int nextGeneration;
        private int nextSubtopology;

        /**
         * What the members held before the change that triggered a rebalance, and on which process, so that
         * task movement can be told apart from the tasks of a process that left.
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

        /** A change that triggers a rebalance; returns false when it is not applicable to the group as it is, so another is drawn. */
        @FunctionalInterface
        private interface Change {
            boolean apply(Random random);
        }

        /**
         * The changes a group sees in production: scale out, scale in or a crash, a rolling restart, a stream thread
         * dying, and a deployment that adds a subtopology.
         */
        private final List<Change> changes = List.of(
            this::addProcessChange,
            this::dropProcessChange,
            this::restartProcessChange,
            this::dropMemberChange,
            this::addSubtopology
        );

        /** Applies one random change to the group; a drawn change that would alter nothing is redrawn. */
        void applyRandomChange(final Random random) {
            boolean applied = false;
            while (!applied) {
                applied = changes.get(random.nextInt(changes.size())).apply(random);
            }
        }

        private boolean addProcessChange(final Random random) {
            final String processId = addProcess(random);
            history.append("change: add process ").append(processId).append(' ').append(processes.get(processId)).append('\n');
            return true;
        }

        private boolean dropProcessChange(final Random random) {
            if (processes.size() == 1) {
                return false;
            }
            final String processId = randomProcess(random);
            removeProcess(processId);
            history.append("change: drop process ").append(processId).append('\n');
            return true;
        }

        private boolean restartProcessChange(final Random random) {
            final String processId = randomProcess(random);
            restartProcess(processId);
            history.append("change: restart process ").append(processId).append(" -> ").append(processes.get(processId)).append('\n');
            return true;
        }

        private boolean dropMemberChange(final Random random) {
            final String processId = randomProcess(random);
            final ProcessSpec process = processes.get(processId);
            if (process.members.size() == 1) {
                return false;
            }
            final String memberId = process.members.get(random.nextInt(process.members.size()));
            removeMember(processId, memberId);
            history.append("change: drop member ").append(memberId).append('\n');
            return true;
        }

        private String addProcess(final Random random) {
            final String processId = "p" + nextProcess++;
            final Map<String, String> tags = new HashMap<>();
            for (final String key : tagKeys) {
                if (key.equals(HOST_TAG)) {
                    // The process id stands in for the host name: one value per process, never missing.
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
            reportedOffsets.remove(memberId);
            final Map<String, MemberAssignment> remaining = new HashMap<>(previousAssignment);
            remaining.remove(memberId);
            previousAssignment = remaining;
        }

        private void removeProcess(final String processId) {
            final ProcessSpec process = processes.remove(processId);
            for (final String memberId : process.members) {
                memberToProcess.remove(memberId);
                reportedOffsets.remove(memberId);
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
                reportedOffsets.remove(oldMember);
                final MemberAssignment previous = remaining.remove(oldMember);
                final String newMember = addMember(processId);
                if (previous != null) {
                    final Map<String, Map<Integer, Long>> offsets = new HashMap<>();
                    addOffsets(offsets, previous.activeTasks());
                    addOffsets(offsets, previous.standbyTasks());
                    reportedOffsets.put(newMember, offsets);
                }
            }
            previousAssignment = remaining;
        }

        Baseline baseline() {
            return new Baseline(previousAssignment, Map.copyOf(memberToProcess));
        }

        /** What the members hold and report when the assignor runs, so the same input can be assigned again. */
        record Input(Map<String, MemberAssignment> previousAssignment, Map<String, Map<String, Map<Integer, Long>>> reportedOffsets) {
        }

        Input input() {
            return new Input(previousAssignment, Map.copyOf(reportedOffsets));
        }

        void restore(final Input input) {
            previousAssignment = input.previousAssignment();
            reportedOffsets.clear();
            reportedOffsets.putAll(input.reportedOffsets());
        }

        /** The stateful tasks each process currently reports offsets for, as a restarted process does for the state on its disk. */
        Map<String, Set<TaskId>> reportedTasksByProcess() {
            final Map<String, Set<TaskId>> reportedTasksByProcess = new HashMap<>();
            reportedOffsets.forEach((memberId, offsets) ->
                reportedTasksByProcess.computeIfAbsent(memberToProcess.get(memberId), p -> new HashSet<>()).addAll(toTaskIds(toPartitions(offsets)))
            );
            return reportedTasksByProcess;
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
                    offsets.computeIfAbsent(subtopology, s -> new HashMap<>()).put(partition, REPORTED_OFFSET);
                }
            }));
        }

        private String randomProcess(final Random random) {
            final List<String> ids = new ArrayList<>(processes.keySet());
            return ids.get(random.nextInt(ids.size()));
        }

        /** A deployment adds a subtopology; its tasks have no previous owner. */
        private boolean addSubtopology(final Random random) {
            final List<Subtopology> specs = new ArrayList<>(topology.specs());
            final Subtopology added = new Subtopology("s" + nextSubtopology++, profile.partitions(random), profile.stateful(random));
            specs.add(added);
            topology = Topology.of(specs);
            history.append("change: add subtopology ").append(added).append('\n');
            return true;
        }

        /** The spec the assignor sees; {@code rackAwareAssignmentTags} is a parameter so the same input can be assigned tag-blind. */
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
                        reportedOffsets.getOrDefault(memberId, Map.of()),
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
            reportedOffsets.clear();
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
