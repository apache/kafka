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
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.ProcessSpec;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.Scenario;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;

import static org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.toTaskIds;

/**
 * Grades the assignment of a rebalance on what every assignor is compared on: balance across members and processes,
 * the distribution of each stateful task's replicas over the client tags, stickiness since the assignment before the
 * trigger, and convergence. Each row is named after its metric. Nothing here is asserted; the {@link Summary} over a
 * run is printed so two assignors, or two versions of one, can be compared on the same scenarios.
 */
final class AssignmentMetrics {

    private AssignmentMetrics() {
    }

    /** The rows of one rebalance except the convergence rows, in the order the summary prints them. */
    static Map<String, OptionalDouble> grade(final Scenario scenario, final GroupAssignment result, final Scenario.Baseline before) {
        final Map<String, Integer> statefulActivePerMember = new HashMap<>();
        final Map<String, Integer> activePerMember = new HashMap<>();
        final Map<String, Map<String, Integer>> activePerSubtopologyPerMember = new HashMap<>();
        final Map<String, Integer> totalPerMember = new HashMap<>();
        final Map<TaskId, String> activeMemberOf = new HashMap<>();
        final Map<TaskId, Set<String>> ownerProcessesOf = new HashMap<>();
        final Map<TaskId, Set<String>> standbyProcessesOf = new HashMap<>();
        for (final Map.Entry<String, MemberAssignment> entry : result.members().entrySet()) {
            final String memberId = entry.getKey();
            final String processId = scenario.processOf(memberId);
            final Set<TaskId> active = toTaskIds(entry.getValue().activeTasks());
            final Set<TaskId> standby = toTaskIds(entry.getValue().standbyTasks());
            int stateful = 0;
            for (final TaskId task : active) {
                if (scenario.topology.statefulTasks().contains(task)) {
                    stateful++;
                }
                ownerProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(processId);
                activeMemberOf.put(task, memberId);
                activePerSubtopologyPerMember.computeIfAbsent(task.subtopologyId(), s -> new HashMap<>()).merge(memberId, 1, Integer::sum);
            }
            for (final TaskId task : standby) {
                ownerProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(processId);
                standbyProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(processId);
            }
            statefulActivePerMember.put(memberId, stateful);
            activePerMember.put(memberId, active.size());
            totalPerMember.put(memberId, active.size() + standby.size());
        }

        // The load of a process is its tasks divided by its members.
        final List<Double> statefulActiveLoads = new ArrayList<>();
        final List<Double> activeLoads = new ArrayList<>();
        final List<Double> loads = new ArrayList<>();
        for (final ProcessSpec process : scenario.processes.values()) {
            int statefulActive = 0;
            int active = 0;
            int tasks = 0;
            for (final String memberId : process.members) {
                statefulActive += statefulActivePerMember.get(memberId);
                active += activePerMember.get(memberId);
                tasks += totalPerMember.get(memberId);
            }
            final double capacity = process.members.size();
            statefulActiveLoads.add(statefulActive / capacity);
            activeLoads.add(active / capacity);
            loads.add(tasks / capacity);
        }

        // Members without a task of the subtopology count as 0. A skew of 1 is unavoidable when the partitions
        // do not divide evenly over the members, so only the excess over that is graded.
        final int members = result.members().size();
        int subtopologyExcessSkew = 0;
        for (final Map<String, Integer> perMember : activePerSubtopologyPerMember.values()) {
            final int min = perMember.size() < members ? 0 : perMember.values().stream().min(Integer::compare).orElseThrow();
            final int max = perMember.values().stream().max(Integer::compare).orElseThrow();
            final int partitions = perMember.values().stream().mapToInt(Integer::intValue).sum();
            final int unavoidable = partitions % members == 0 ? 0 : 1;
            subtopologyExcessSkew = Math.max(subtopologyExcessSkew, max - min - unavoidable);
        }

        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        rows.put("Stateful active balance per member", OptionalDouble.of(range(statefulActivePerMember.values())));
        rows.put("Active balance per member", OptionalDouble.of(range(activePerMember.values())));
        rows.put("Task balance per member", OptionalDouble.of(range(totalPerMember.values())));
        rows.put("Stateful active load balance per process", OptionalDouble.of(loadRange(statefulActiveLoads)));
        rows.put("Active load balance per process", OptionalDouble.of(loadRange(activeLoads)));
        rows.put("Load balance per process", OptionalDouble.of(loadRange(loads)));
        rows.put("Subtopology balance", OptionalDouble.of(subtopologyExcessSkew));
        rows.putAll(rackRows(scenario, ownerProcessesOf));
        rows.putAll(stickinessRows(scenario, before, activeMemberOf, standbyProcessesOf));
        return rows;
    }

    /**
     * Rows of the convergence loop: {@code rounds} is the number of assignments that changed something before the
     * output reproduced itself, empty when the cap was reached without that. The time is that of the loop's first
     * assignment, the one the coordinator runs; the first scenarios of a run include JIT warm-up.
     */
    static Map<String, OptionalDouble> convergenceRows(final int rounds, final boolean converged, final long firstAssignmentNanos) {
        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        rows.put("Rounds to converge", converged ? OptionalDouble.of(rounds) : OptionalDouble.empty());
        rows.put("Not converged", OptionalDouble.of(converged ? 0 : 1));
        rows.put("Assignment time (ms)", OptionalDouble.of(firstAssignmentNanos / 1_000_000.0));
        return rows;
    }

    /**
     * Two rows per key position in priority order, key 1 being the first key of {@code rack.aware.assignment.tags}.
     * A row is empty when the position has no key, the key has a single value or one value per process, so the
     * assignor has no choice, or there are no standby or no stateful tasks; every rebalance returns the same rows.
     * Only stateful tasks are graded, since a stateless task has a single replica.
     * <ul>
     * <li>tasks lost when one value of key i fails: the fraction of stateful tasks whose replicas are all on one value
     * of the key, or on processes without it; when that value fails, the task loses its state.</li>
     * <li>distinct tag values of key i: the average over stateful tasks of the values their replicas are on, out of
     * {@code min(replicas, values of the key in the group)}, the most they could be on.</li>
     * </ul>
     */
    private static Map<String, OptionalDouble> rackRows(final Scenario scenario, final Map<TaskId, Set<String>> ownerProcessesOf) {
        final int positions = TaskAssignorTestbed.TAG_KEYS.size();
        final List<OptionalDouble> tasksLost = new ArrayList<>(Collections.nCopies(positions, OptionalDouble.empty()));
        final List<OptionalDouble> distinctTagValues = new ArrayList<>(Collections.nCopies(positions, OptionalDouble.empty()));
        final Set<TaskId> statefulTasks = scenario.topology.statefulTasks();
        if (scenario.numStandbyReplicas > 0 && !statefulTasks.isEmpty()) {
            final Map<String, Set<String>> valuesInGroup = tagValuesInGroup(scenario);
            for (int i = 0; i < Math.min(positions, scenario.tagKeys.size()); i++) {
                final String key = scenario.tagKeys.get(i);
                final int values = valuesInGroup.getOrDefault(key, Set.of()).size();
                if (values <= 1 || values >= scenario.processes.size()) {
                    continue;
                }
                int lost = 0;
                double distinctShares = 0;
                for (final TaskId task : statefulTasks) {
                    final Set<String> holders = ownerProcessesOf.get(task);
                    final int distinct = distinctValues(scenario, holders, key).size();
                    lost += distinct <= 1 ? 1 : 0;
                    distinctShares += (double) distinct / Math.min(holders.size(), values);
                }
                tasksLost.set(i, OptionalDouble.of((double) lost / statefulTasks.size()));
                distinctTagValues.set(i, OptionalDouble.of(distinctShares / statefulTasks.size()));
            }
        }
        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        for (int i = 0; i < positions; i++) {
            rows.put("Tasks lost when one value of key " + (i + 1) + " fails", tasksLost.get(i));
        }
        for (int i = 0; i < positions; i++) {
            rows.put("Distinct tag values of key " + (i + 1), distinctTagValues.get(i));
        }
        return rows;
    }

    private static OptionalDouble fraction(final int part, final int whole) {
        return whole == 0 ? OptionalDouble.empty() : OptionalDouble.of((double) part / whole);
    }

    /** The distinct values of {@code key} among the holders that carry it. */
    private static Set<String> distinctValues(final Scenario scenario, final Set<String> holders, final String key) {
        final Set<String> values = new HashSet<>();
        for (final String processId : holders) {
            final String value = scenario.processes.get(processId).tags.get(key);
            if (value != null) {
                values.add(value);
            }
        }
        return values;
    }

    private static Map<String, Set<String>> tagValuesInGroup(final Scenario scenario) {
        final Map<String, Set<String>> valuesInGroup = new HashMap<>();
        for (final ProcessSpec process : scenario.processes.values()) {
            process.tags.forEach((key, value) -> valuesInGroup.computeIfAbsent(key, k -> new HashSet<>()).add(value));
        }
        return valuesInGroup;
    }

    /**
     * Stickiness since the assignment before the trigger, over the tasks some member held then; tasks of a
     * subtopology just added have no state anywhere and are left out. Every row is empty for the initial assignment.
     * A process had state of a task when it held a replica or a warm-up task of it before the trigger.
     * <ul>
     * <li>the three stateful active rows put every stateful task in one level, so they add up to 1: its active task
     * stays on its member; moves to another member of a process with state, so it is revoked and granted again but
     * not restored; or moves to a process without state, a full restore.</li>
     * <li>active tasks kept on member: the same as the first level, over stateful and stateless tasks.</li>
     * <li>standby tasks placed without state: the standby tasks of stateful tasks on a process without state, each a
     * full restore. A standby task moved within its process keeps its persistent state, so it is not counted.</li>
     * </ul>
     */
    private static Map<String, OptionalDouble> stickinessRows(
        final Scenario scenario,
        final Scenario.Baseline before,
        final Map<TaskId, String> activeMemberOf,
        final Map<TaskId, Set<String>> standbyProcessesOf
    ) {
        final Map<TaskId, String> previousActiveMemberOf = new HashMap<>();
        final Map<TaskId, Set<String>> processesWithStateOf = new HashMap<>();
        for (final Map.Entry<String, MemberAssignment> entry : before.assignment().entrySet()) {
            final String previousMember = entry.getKey();
            final Set<TaskId> previousActive = toTaskIds(entry.getValue().activeTasks());
            previousActive.forEach(task -> previousActiveMemberOf.put(task, previousMember));
            final Set<TaskId> previousTasks = new HashSet<>(previousActive);
            previousTasks.addAll(toTaskIds(entry.getValue().standbyTasks()));
            previousTasks.addAll(toTaskIds(before.warmupTasks().getOrDefault(previousMember, Map.of())));
            for (final TaskId task : previousTasks) {
                processesWithStateOf.computeIfAbsent(task, t -> new HashSet<>()).add(before.processOfMember().get(previousMember));
            }
        }
        int activeKept = 0;
        int statefulHeld = 0;
        int statefulKept = 0;
        int statefulMovedWithState = 0;
        int statefulMovedWithoutState = 0;
        int standbysWithoutState = 0;
        for (final Map.Entry<TaskId, String> entry : previousActiveMemberOf.entrySet()) {
            final TaskId task = entry.getKey();
            final String activeMember = activeMemberOf.get(task);
            final boolean kept = entry.getValue().equals(activeMember);
            activeKept += kept ? 1 : 0;
            if (!scenario.topology.statefulTasks().contains(task)) {
                continue;
            }
            final Set<String> processesWithState = processesWithStateOf.get(task);
            statefulHeld++;
            if (kept) {
                statefulKept++;
            } else if (processesWithState.contains(scenario.processOf(activeMember))) {
                statefulMovedWithState++;
            } else {
                statefulMovedWithoutState++;
            }
            for (final String processId : standbyProcessesOf.getOrDefault(task, Set.of())) {
                if (!processesWithState.contains(processId)) {
                    standbysWithoutState++;
                }
            }
        }
        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        rows.put("Stateful active tasks kept on member", fraction(statefulKept, statefulHeld));
        rows.put("Stateful active tasks moved to a process with state", fraction(statefulMovedWithState, statefulHeld));
        rows.put("Stateful active tasks moved to a process without state", fraction(statefulMovedWithoutState, statefulHeld));
        rows.put("Active tasks kept on member", fraction(activeKept, previousActiveMemberOf.size()));
        rows.put("Standby tasks placed without state", before.assignment().isEmpty() ? OptionalDouble.empty() : OptionalDouble.of(standbysWithoutState));
        return rows;
    }

    /** {@code max - min} of the values, 0 when there are none. */
    private static int range(final Iterable<Integer> values) {
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for (final int value : values) {
            max = Math.max(max, value);
            min = Math.min(min, value);
        }
        return max == Integer.MIN_VALUE ? 0 : max - min;
    }

    /** {@code max - min} of the process loads, 0 when there are none. */
    private static double loadRange(final List<Double> loads) {
        return loads.isEmpty() ? 0 : Collections.max(loads) - Collections.min(loads);
    }

    /** The graded rows of one rebalance, for the scenario history. */
    static String format(final Map<String, OptionalDouble> rows) {
        final StringBuilder builder = new StringBuilder();
        rows.forEach((name, value) -> value.ifPresent(v -> builder.append(name).append('=').append(String.format("%.3f", v)).append("; ")));
        return builder.toString();
    }

    /** Aggregates the graded rows of every rebalance in a run into one table, in the order the graders return them. */
    static final class Summary {
        private final String title;
        private final Map<String, Stat> rows = new LinkedHashMap<>();

        Summary(final String title) {
            this.title = title;
        }

        void add(final Map<String, OptionalDouble> graded) {
            graded.forEach((name, value) -> {
                final Stat stat = rows.computeIfAbsent(name, n -> new Stat());
                value.ifPresent(stat::add);
            });
        }

        @Override
        public String toString() {
            int width = "metric".length();
            for (final String name : rows.keySet()) {
                width = Math.max(width, name.length());
            }
            final String nameFormat = "  %-" + width + "s ";
            final StringBuilder builder = new StringBuilder("Fuzz metrics: ").append(title).append('\n')
                .append(String.format(nameFormat, "metric")).append(String.format("%8s %8s %8s %8s%n", "avg", "min", "max", "n"));
            rows.forEach((name, stat) -> builder.append(String.format(nameFormat, name)).append(stat).append('\n'));
            return builder.toString();
        }
    }

    static final class Stat {
        private int count;
        private double sum;
        private double min = Double.MAX_VALUE;
        private double max = -Double.MAX_VALUE;

        void add(final double value) {
            count++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }

        @Override
        public String toString() {
            if (count == 0) {
                return String.format("%8s %8s %8s %8d", "n/a", "n/a", "n/a", 0);
            }
            return String.format("%8.3f %8.3f %8.3f %8d", sum / count, min, max, count);
        }
    }
}
