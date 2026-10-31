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
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredInternalTopic;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One randomized, but fully reproducible, situation a refiner is run through by {@link RefinerFuzzSimulator}: a
 * topology, a group that starts out settled on the sticky assignor's assignment with all state caught up, and a
 * schedule of events that disturb it.
 *
 * <p>Everything here is derived from the seed alone, so a scenario is the same for every refiner that runs it. The
 * events are fixed up front; what the assignor makes of them at run time can still differ between refiners, because
 * the sticky assignor looks at the group's current assignment, which is exactly what a refiner shapes.
 *
 * <p>The event schedule is made of episodes. Each episode that changes the membership or the standby configuration
 * ends with a new target assignment, either the sticky assignor's or a synthetic one. Some episodes compute the
 * target assignment <em>before</em> a member leaves, and the assignor only catches up a few ticks later; this is how
 * the refiner gets to see a target assignment that names a member the group no longer has, as happens with an
 * offloaded assignor. The last episode always ends with a target assignment computed against the final membership,
 * so every scenario can converge.
 */
final class RefinerFuzzScenario {

    /**
     * How big a scenario can get. Each size is drawn uniformly from its range.
     */
    enum Profile {
        CI(1, 3, 1, 12, 1, 6, 1, 3, 4, 2),
        LARGE(1, 5, 1, 64, 1, 30, 1, 4, 4, 4),
        /** The size of a big production group: 1000 members on 20 processes, and 10000 tasks, half of them stateful. */
        XLARGE(100, 100, 100, 100, 20, 20, 50, 50, 2, 4);

        final int minSubtopologies;
        final int maxSubtopologies;
        final int minPartitions;
        final int maxPartitions;
        final int minProcesses;
        final int maxProcesses;
        final int minMembersPerProcess;
        final int maxMembersPerProcess;
        /** One in how many subtopologies is stateless. */
        final int statelessOneIn;
        final int maxMidRunEpisodes;

        Profile(
            final int minSubtopologies,
            final int maxSubtopologies,
            final int minPartitions,
            final int maxPartitions,
            final int minProcesses,
            final int maxProcesses,
            final int minMembersPerProcess,
            final int maxMembersPerProcess,
            final int statelessOneIn,
            final int maxMidRunEpisodes
        ) {
            this.minSubtopologies = minSubtopologies;
            this.maxSubtopologies = maxSubtopologies;
            this.minPartitions = minPartitions;
            this.maxPartitions = maxPartitions;
            this.minProcesses = minProcesses;
            this.maxProcesses = maxProcesses;
            this.minMembersPerProcess = minMembersPerProcess;
            this.maxMembersPerProcess = maxMembersPerProcess;
            this.statelessOneIn = statelessOneIn;
            this.maxMidRunEpisodes = maxMidRunEpisodes;
        }

        int subtopologies(final Random random) {
            return draw(random, minSubtopologies, maxSubtopologies);
        }

        int partitions(final Random random) {
            return draw(random, minPartitions, maxPartitions);
        }

        int processes(final Random random) {
            return draw(random, minProcesses, maxProcesses);
        }

        int membersPerProcess(final Random random) {
            return draw(random, minMembersPerProcess, maxMembersPerProcess);
        }

        private static int draw(final Random random, final int min, final int max) {
            return min + random.nextInt(max - min + 1);
        }
    }

    enum EventKind {
        /** A new process joins with {@code value} members. */
        ADD_PROCESS,
        /** All members of the process leave, and its state is gone for good. */
        REMOVE_PROCESS,
        /** All members of the process leave and rejoin under new member IDs; its state stays on disk. */
        RESTART_PROCESS,
        /** One more member joins the process. */
        ADD_MEMBER,
        /** The process's most recently added member leaves. */
        REMOVE_MEMBER,
        /** {@code num.standby.replicas} changes to {@code value}. */
        SET_STANDBY_REPLICAS,
        /** {@code num.warmup.replicas} changes to {@code value}. */
        SET_WARMUP_BUDGET,
        /** The coordinator fails over: reported offsets and the intermediate assignment are lost. */
        FAILOVER,
        /** The sticky assignor computes a new target assignment. */
        STICKY_TARGET,
        /** A random, but structurally valid, target assignment derived from {@code seed}. */
        SYNTHETIC_TARGET
    }

    record Event(int tick, EventKind kind, String processId, int value, long seed) {

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder("t").append(tick).append(':').append(kind);
            if (processId != null) {
                builder.append('(').append(processId).append(')');
            }
            if (kind == EventKind.ADD_PROCESS || kind == EventKind.SET_STANDBY_REPLICAS
                || kind == EventKind.SET_WARMUP_BUDGET) {
                builder.append('=').append(value);
            }
            return builder.toString();
        }
    }

    private static final int MAX_COMPACTED_SIZE = 30_000;
    private static final int[] WARMUP_BUDGETS = {1, 1, 2, 2, 3, 5, 20};
    private static final long[] ACCEPTABLE_RECOVERY_LAGS = {0L, 100L, 10_000L};

    final long seed;
    final Profile profile;
    final SortedMap<String, ConfiguredSubtopology> subtopologies;
    final SortedSet<TaskId> statefulTasks;
    final SortedMap<TaskId, Long> initialEndOffsets;
    final SortedMap<TaskId, Long> compactedSizes;
    final SortedMap<TaskId, Long> writeRates;
    final long restoreRate;
    final long acceptableRecoveryLag;
    final int initialStandbyReplicas;
    final int initialWarmupBudget;
    final SortedMap<String, Integer> initialProcesses;
    final List<Event> events;
    final int lastEventTick;
    final int maxTicks;

    private RefinerFuzzScenario(final long seed, final Profile profile) {
        this.seed = seed;
        this.profile = profile;
        final Random random = new Random(seed);

        this.subtopologies = generateTopology(random, profile);
        this.statefulTasks = new TreeSet<>();
        subtopologies.forEach((subtopologyId, subtopology) -> {
            if (!subtopology.stateChangelogTopics().isEmpty()) {
                for (int partition = 0; partition < subtopology.numberOfTasks(); partition++) {
                    statefulTasks.add(new TaskId(subtopologyId, partition));
                }
            }
        });

        this.initialEndOffsets = new TreeMap<>();
        this.compactedSizes = new TreeMap<>();
        this.writeRates = new TreeMap<>();
        for (final TaskId task : statefulTasks) {
            // Some changelogs are empty, which makes a fresh copy caught up straight away.
            final long size = random.nextInt(5) == 0 ? 0L : (long) random.nextInt(MAX_COMPACTED_SIZE);
            compactedSizes.put(task, size);
            // Changelogs that have already been compacted a lot and ones that have not.
            initialEndOffsets.put(task, size + random.nextInt(MAX_COMPACTED_SIZE));
            writeRates.put(task, (long) random.nextInt(200));
        }
        // Always faster than any changelog grows, so that every warm-up task can catch up.
        this.restoreRate = 2_000L + random.nextInt(8_000);
        this.acceptableRecoveryLag = ACCEPTABLE_RECOVERY_LAGS[random.nextInt(ACCEPTABLE_RECOVERY_LAGS.length)];
        this.initialWarmupBudget = WARMUP_BUDGETS[random.nextInt(WARMUP_BUDGETS.length)];

        this.initialProcesses = new TreeMap<>();
        final int processCount = profile.processes(random);
        for (int process = 0; process < processCount; process++) {
            initialProcesses.put(processId(process), profile.membersPerProcess(random));
        }
        this.initialStandbyReplicas = random.nextInt(Math.min(2, processCount - 1) + 1);

        this.events = Collections.unmodifiableList(new EventPlanner(random).plan());
        this.lastEventTick = events.isEmpty() ? 0 : events.get(events.size() - 1).tick();
        // Generous: with a budget of one warm-up task the migrations restore one after the other, each taking at most
        // a full restore of the compacted changelog plus a few heartbeats for the hand-over.
        final long ticksPerMigration = MAX_COMPACTED_SIZE / restoreRate + 10;
        this.maxTicks = (int) (lastEventTick + 100 + ticksPerMigration * Math.max(1, statefulTasks.size()));
    }

    static RefinerFuzzScenario generate(final long seed, final Profile profile) {
        return new RefinerFuzzScenario(seed, profile);
    }

    static String processId(final int index) {
        return String.format("p%02d", index);
    }

    int taskCount() {
        return subtopologies.values().stream().mapToInt(ConfiguredSubtopology::numberOfTasks).sum();
    }

    String describe() {
        return "seed=" + seed
            + " profile=" + profile
            + " subtopologies=" + subtopologies.size()
            + " tasks=" + taskCount()
            + " stateful=" + statefulTasks.size()
            + " processes=" + initialProcesses
            + " standbys=" + initialStandbyReplicas
            + " budget=" + initialWarmupBudget
            + " lag=" + acceptableRecoveryLag
            + " restoreRate=" + restoreRate
            + " events=" + events;
    }

    private static SortedMap<String, ConfiguredSubtopology> generateTopology(final Random random, final Profile profile) {
        final SortedMap<String, ConfiguredSubtopology> subtopologies = new TreeMap<>();
        final int subtopologyCount = profile.subtopologies(random);
        for (int index = 0; index < subtopologyCount; index++) {
            final String subtopologyId = "s" + index;
            final int partitions = profile.partitions(random);
            final boolean stateful = random.nextInt(profile.statelessOneIn) != 0;
            final Map<String, ConfiguredInternalTopic> changelogs = stateful
                ? Map.of(subtopologyId + "-changelog",
                    new ConfiguredInternalTopic(subtopologyId + "-changelog", partitions, Optional.empty(), Map.of()))
                : Map.of();
            subtopologies.put(subtopologyId, new ConfiguredSubtopology(
                partitions,
                Set.of(subtopologyId + "-input"),
                Map.of(),
                Set.of(),
                changelogs
            ));
        }
        return Collections.unmodifiableSortedMap(subtopologies);
    }

    /**
     * Plans the event schedule, tracking the membership it implies, so that every event names a process that
     * exists at that point and the group never runs out of members.
     */
    private final class EventPlanner {

        private final Random random;
        private final SortedMap<String, Integer> membersByProcess;
        private final List<Event> planned = new ArrayList<>();
        private int nextProcessIndex;

        private EventPlanner(final Random random) {
            this.random = random;
            this.membersByProcess = new TreeMap<>(initialProcesses);
            this.nextProcessIndex = initialProcesses.size();
        }

        private List<Event> plan() {
            // The disturbance the scenario is about, right after the initial settled state.
            int tick = 1;
            tick = planChangeEpisode(tick);

            final int midRunEpisodes = random.nextInt(profile.maxMidRunEpisodes + 1);
            for (int episode = 0; episode < midRunEpisodes; episode++) {
                tick += 1 + random.nextInt(40);
                switch (random.nextInt(4)) {
                    case 0 -> add(tick, EventKind.FAILOVER, null, 0);
                    case 1 -> add(tick, EventKind.SET_WARMUP_BUDGET, null,
                        WARMUP_BUDGETS[random.nextInt(WARMUP_BUDGETS.length)]);
                    default -> tick = planChangeEpisode(tick);
                }
            }
            return planned;
        }

        /**
         * One to three changes to the group, followed by a new target assignment. Returns the last tick used.
         */
        private int planChangeEpisode(final int tick) {
            final int changes = 1 + random.nextInt(3);
            for (int change = 0; change < changes; change++) {
                planChange(tick);
            }

            final boolean stale = random.nextInt(5) == 0 && canRemoveMember();
            if (!stale) {
                addTarget(tick);
                return tick;
            }
            // The assignor computes the target assignment before a member leaves, and only catches up later.
            addTarget(tick);
            removeSomeMember(tick);
            final int catchUpTick = tick + 1 + random.nextInt(5);
            add(catchUpTick, EventKind.STICKY_TARGET, null, 0);
            return catchUpTick;
        }

        private void planChange(final int tick) {
            switch (random.nextInt(7)) {
                case 0 -> {
                    if (membersByProcess.size() < profile.maxProcesses + 2) {
                        final String processId = processId(nextProcessIndex++);
                        final int members = profile.membersPerProcess(random);
                        membersByProcess.put(processId, members);
                        add(tick, EventKind.ADD_PROCESS, processId, members);
                    }
                }
                case 1 -> {
                    if (membersByProcess.size() > 1) {
                        final String processId = pickProcess();
                        membersByProcess.remove(processId);
                        add(tick, EventKind.REMOVE_PROCESS, processId, 0);
                    }
                }
                case 2 -> add(tick, EventKind.RESTART_PROCESS, pickProcess(), 0);
                case 3 -> {
                    final String processId = pickProcess();
                    membersByProcess.merge(processId, 1, Integer::sum);
                    add(tick, EventKind.ADD_MEMBER, processId, 0);
                }
                case 4 -> removeSomeMember(tick);
                case 5 -> add(tick, EventKind.SET_STANDBY_REPLICAS, null,
                    random.nextInt(Math.min(2, membersByProcess.size() - 1) + 1));
                default -> add(tick, EventKind.FAILOVER, null, 0);
            }
        }

        private boolean canRemoveMember() {
            return membersByProcess.values().stream().anyMatch(members -> members > 1) || membersByProcess.size() > 1;
        }

        private void removeSomeMember(final int tick) {
            final List<String> candidates = new ArrayList<>();
            membersByProcess.forEach((processId, members) -> {
                if (members > 1) {
                    candidates.add(processId);
                }
            });
            if (!candidates.isEmpty()) {
                final String processId = candidates.get(random.nextInt(candidates.size()));
                membersByProcess.merge(processId, -1, Integer::sum);
                add(tick, EventKind.REMOVE_MEMBER, processId, 0);
            } else if (membersByProcess.size() > 1) {
                final String processId = pickProcess();
                membersByProcess.remove(processId);
                add(tick, EventKind.REMOVE_PROCESS, processId, 0);
            }
        }

        private void addTarget(final int tick) {
            if (random.nextInt(10) < 7) {
                add(tick, EventKind.STICKY_TARGET, null, 0);
            } else {
                planned.add(new Event(tick, EventKind.SYNTHETIC_TARGET, null, 0, random.nextLong()));
            }
        }

        private String pickProcess() {
            final List<String> processes = new ArrayList<>(membersByProcess.keySet());
            return processes.get(random.nextInt(processes.size()));
        }

        private void add(final int tick, final EventKind kind, final String processId, final int value) {
            planned.add(new Event(tick, kind, processId, value, 0L));
        }
    }
}
