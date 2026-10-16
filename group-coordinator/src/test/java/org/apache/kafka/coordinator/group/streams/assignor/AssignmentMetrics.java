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
 * the distribution of each stateful task's replicas over the client tags, stickiness and restores since the
 * assignment before the change, and convergence. Nothing here is asserted; the {@link Summary} over a run is printed
 * so two assignors, or two versions of one, can be compared on the same scenarios.
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
            }
            statefulActivePerMember.put(memberId, stateful);
            activePerMember.put(memberId, active.size());
            totalPerMember.put(memberId, active.size() + standby.size());
        }

        double maxActiveLoad = 0;
        double minActiveLoad = Double.MAX_VALUE;
        double maxLoad = 0;
        double minLoad = Double.MAX_VALUE;
        for (final ProcessSpec process : scenario.processes.values()) {
            int active = 0;
            int tasks = 0;
            for (final String memberId : process.members) {
                active += activePerMember.get(memberId);
                tasks += totalPerMember.get(memberId);
            }
            final double activeLoad = (double) active / process.members.size();
            maxActiveLoad = Math.max(maxActiveLoad, activeLoad);
            minActiveLoad = Math.min(minActiveLoad, activeLoad);
            final double load = (double) tasks / process.members.size();
            maxLoad = Math.max(maxLoad, load);
            minLoad = Math.min(minLoad, load);
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
        rows.put("stateful active tasks per member (max-min)", OptionalDouble.of(range(statefulActivePerMember.values())));
        rows.put("active tasks per member (max-min)", OptionalDouble.of(range(activePerMember.values())));
        rows.put("total tasks per member (max-min)", OptionalDouble.of(range(totalPerMember.values())));
        // The load of a process is its tasks divided by its members.
        rows.put("active load per process (max-min)", OptionalDouble.of(maxActiveLoad - minActiveLoad));
        rows.put("process load (max-min)", OptionalDouble.of(maxLoad - minLoad));
        rows.put("subtopology skew above unavoidable (max-min)", OptionalDouble.of(subtopologyExcessSkew));
        rows.putAll(rackRows(scenario, ownerProcessesOf));
        rows.putAll(stickinessRows(scenario, before, activeMemberOf, ownerProcessesOf));
        return rows;
    }

    /**
     * Rows of the convergence loop: {@code rounds} is the number of assignments that changed something before the
     * output reproduced itself, empty when the cap was reached without that. The time is that of the loop's first
     * assignment, the one the coordinator runs; the first scenarios of a run include JIT warm-up.
     */
    static Map<String, OptionalDouble> convergenceRows(final int rounds, final boolean converged, final long firstAssignmentNanos) {
        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        rows.put("rounds to converge", converged ? OptionalDouble.of(rounds) : OptionalDouble.empty());
        rows.put("not converged", OptionalDouble.of(converged ? 0 : 1));
        rows.put("assignment time (ms)", OptionalDouble.of(firstAssignmentNanos / 1_000_000.0));
        return rows;
    }

    /**
     * One row per configured tag key in priority order, empty when the key is not graded, so every rebalance
     * returns the same rows. Only stateful tasks are graded: a stateless task has a single replica.
     */
    private static Map<String, OptionalDouble> rackRows(final Scenario scenario, final Map<TaskId, Set<String>> ownerProcessesOf) {
        final TagDistribution distribution = tagDistribution(scenario, ownerProcessesOf);
        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        for (final String key : TaskAssignorTestbed.TAG_KEYS) {
            rows.put("distinct tag values [" + key + "] (achieved / achievable)", graded(distribution.distinctValues(), key));
        }
        for (final String key : TaskAssignorTestbed.TAG_KEYS) {
            rows.put("ideal distribution through [" + key + "]", graded(distribution.idealThroughKey(), key));
        }
        for (final String key : TaskAssignorTestbed.TAG_KEYS) {
            rows.put("all replicas on one tag value [" + key + "]", graded(distribution.allReplicasOnOneValue(), key));
        }
        return rows;
    }

    private static OptionalDouble graded(final Map<String, Double> perKey, final String key) {
        final Double value = perKey.get(key);
        return value == null ? OptionalDouble.empty() : OptionalDouble.of(value);
    }

    private static OptionalDouble fraction(final int part, final int whole) {
        return whole == 0 ? OptionalDouble.empty() : OptionalDouble.of((double) part / whole);
    }

    /** Per graded tag key: the three distribution rows. Keys are the graded keys in priority order. */
    private record TagDistribution(
        Map<String, Double> distinctValues,
        Map<String, Double> idealThroughKey,
        Map<String, Double> allReplicasOnOneValue
    ) {
        static final TagDistribution NONE = new TagDistribution(Map.of(), Map.of(), Map.of());
    }

    /**
     * Distribution of each stateful task's replicas over the graded tag keys: the configured keys in priority order,
     * without keys that have a single value in the group and without keys that have a value per process, as a host
     * name does, since replicas of one task never share a process and so always reach distinct values on them. A key
     * that some processes do not carry is graded. For a task {@code t} and key
     * {@code k}, {@code d} is the number of distinct values among the processes holding a replica of {@code t}
     * (a missing value adds nothing), and {@code d*} = min(replicas of {@code t}, values of {@code k} in the group)
     * is the largest {@code d} any placement can reach. Nothing is graded without standby tasks or without stateful
     * tasks, since every task then has a single replica.
     * <ul>
     * <li>distinct tag values: sum of {@code max(d - 1, 0)} over sum of {@code d* - 1}, over the tasks with
     * {@code d* > 1}; the active task's own value is not a score, so replicas sharing one value, or carrying none,
     * score 0.</li>
     * <li>ideal distribution through key: the fraction of tasks that reach the KIP-708 ideal distribution on this
     * key and on every higher-priority graded key, see {@link #idealDistribution}.</li>
     * <li>all replicas on one tag value: of the tasks with {@code d* > 1}, the fraction with {@code d <= 1}; they
     * lose every replica when that one value fails although the group allowed otherwise.</li>
     * </ul>
     */
    private static TagDistribution tagDistribution(final Scenario scenario, final Map<TaskId, Set<String>> ownerProcessesOf) {
        if (scenario.numStandbyReplicas == 0 || scenario.topology.statefulTasks().isEmpty()) {
            return TagDistribution.NONE;
        }
        final Map<String, Set<String>> valuesInGroup = tagValuesInGroup(scenario);
        final List<String> keys = new ArrayList<>();
        for (final String key : scenario.tagKeys) {
            final int values = valuesInGroup.getOrDefault(key, Set.of()).size();
            if (values > 1 && values < scenario.processes.size()) {
                keys.add(key);
            }
        }
        if (keys.isEmpty()) {
            return TagDistribution.NONE;
        }
        final Map<String, Double> distinctValues = new LinkedHashMap<>();
        final Map<String, Double> allReplicasOnOneValue = new LinkedHashMap<>();
        for (final String key : keys) {
            final int available = valuesInGroup.get(key).size();
            int achieved = 0;
            int achievable = 0;
            int gradedTasks = 0;
            int exposedTasks = 0;
            for (final TaskId task : scenario.topology.statefulTasks()) {
                final Set<String> holders = ownerProcessesOf.getOrDefault(task, Set.of());
                final int best = Math.min(holders.size(), available);
                if (best <= 1) {
                    continue;
                }
                final int distinct = distinctValues(scenario, holders, key).size();
                achieved += Math.max(distinct - 1, 0);
                achievable += best - 1;
                gradedTasks++;
                if (distinct <= 1) {
                    exposedTasks++;
                }
            }
            fraction(achieved, achievable).ifPresent(value -> distinctValues.put(key, value));
            fraction(exposedTasks, gradedTasks).ifPresent(value -> allReplicasOnOneValue.put(key, value));
        }
        return new TagDistribution(distinctValues, idealDistributionThroughKey(scenario, keys, valuesInGroup, ownerProcessesOf), allReplicasOnOneValue);
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
     * A task's replicas reach the KIP-708 ideal distribution on a key when every holder carries the key, the
     * replicas cover as many distinct values as the replicas or the values in the group allow, and no value holds
     * two replicas more than another. A holder without the key is of unknown location, so it never counts.
     */
    private static boolean idealDistribution(final Scenario scenario, final Set<String> holders, final String key, final int available) {
        final Map<String, Integer> replicasPerValue = new HashMap<>();
        for (final String processId : holders) {
            final String value = scenario.processes.get(processId).tags.get(key);
            if (value == null) {
                return false;
            }
            replicasPerValue.merge(value, 1, Integer::sum);
        }
        return replicasPerValue.size() == Math.min(holders.size(), available) && range(replicasPerValue.values()) <= 1;
    }

    /**
     * For each graded key in priority order, the fraction of the stateful tasks that reach the ideal distribution
     * on it and on every higher-priority graded key. The cascade shows which key the assignor gives up first; the
     * tag key priority says the lowest-priority key should go first.
     */
    private static Map<String, Double> idealDistributionThroughKey(
        final Scenario scenario,
        final List<String> keys,
        final Map<String, Set<String>> valuesInGroup,
        final Map<TaskId, Set<String>> ownerProcessesOf
    ) {
        final int[] idealThrough = new int[keys.size()];
        for (final TaskId task : scenario.topology.statefulTasks()) {
            final Set<String> holders = ownerProcessesOf.getOrDefault(task, Set.of());
            for (int i = 0; i < keys.size(); i++) {
                if (!idealDistribution(scenario, holders, keys.get(i), valuesInGroup.get(keys.get(i)).size())) {
                    break;
                }
                idealThrough[i]++;
            }
        }
        final Map<String, Double> result = new LinkedHashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            result.put(keys.get(i), (double) idealThrough[i] / scenario.topology.statefulTasks().size());
        }
        return result;
    }

    /**
     * Stickiness and restores since the assignment before the change; nothing is graded on the initial assignment.
     * <ul>
     * <li>stateful active tasks kept on member: of the stateful active tasks whose previous member is still in the
     * group, the fraction that member still holds as active. A restarted process comes back with new member ids, so
     * its tasks are not counted.</li>
     * <li>replicas placed without state: replicas, active or standby, of a stateful task on a process that held no
     * replica or warm-up task of it before, whatever the reason; each one is a full restore. Tasks nobody held before
     * the change (a subtopology just added) have no state anywhere and nothing to restore, so they are left out;
     * without this, a rebalance that adds a subtopology would dominate the average.</li>
     * </ul>
     */
    private static Map<String, OptionalDouble> stickinessRows(
        final Scenario scenario,
        final Scenario.Baseline before,
        final Map<TaskId, String> activeMemberOf,
        final Map<TaskId, Set<String>> ownerProcessesOf
    ) {
        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        if (before.assignment().isEmpty()) {
            rows.put("stateful active tasks kept on member (1.0 = all)", OptionalDouble.empty());
            rows.put("replicas placed without state", OptionalDouble.empty());
            return rows;
        }
        int held = 0;
        int kept = 0;
        final Map<TaskId, Set<String>> previousOwnerProcessesOf = new HashMap<>();
        for (final Map.Entry<String, MemberAssignment> entry : before.assignment().entrySet()) {
            final String previousMember = entry.getKey();
            final boolean memberInGroup = scenario.memberIds().contains(previousMember);
            final Set<TaskId> previousActive = toTaskIds(entry.getValue().activeTasks());
            for (final TaskId task : previousActive) {
                if (memberInGroup && scenario.topology.statefulTasks().contains(task)) {
                    held++;
                    kept += previousMember.equals(activeMemberOf.get(task)) ? 1 : 0;
                }
            }
            final Set<TaskId> previousTasks = new HashSet<>(previousActive);
            previousTasks.addAll(toTaskIds(entry.getValue().standbyTasks()));
            previousTasks.addAll(toTaskIds(before.warmupTasks().getOrDefault(previousMember, Map.of())));
            for (final TaskId task : previousTasks) {
                previousOwnerProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(before.processOfMember().get(previousMember));
            }
        }
        int placedWithoutState = 0;
        for (final TaskId task : scenario.topology.statefulTasks()) {
            final Set<String> previousOwners = previousOwnerProcessesOf.get(task);
            if (previousOwners == null) {
                continue;
            }
            for (final String processId : ownerProcessesOf.getOrDefault(task, Set.of())) {
                if (!previousOwners.contains(processId)) {
                    placedWithoutState++;
                }
            }
        }
        rows.put("stateful active tasks kept on member (1.0 = all)", fraction(kept, held));
        rows.put("replicas placed without state", OptionalDouble.of(placedWithoutState));
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
