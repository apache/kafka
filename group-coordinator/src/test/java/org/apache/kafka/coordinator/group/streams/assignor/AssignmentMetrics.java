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
 * Grades the converged assignment of a rebalance on what every assignor is compared on: balance across members and
 * processes, the distribution of each stateful task's replicas over the client tags, and task movement since the
 * assignment before the change. What only one assignor aims for is graded by the
 * {@link TaskAssignorTestbed.AssignmentGrader} its fuzz test registers. Nothing here is asserted; the {@link Summary}
 * over a run is printed so two assignors, or two versions of one, can be compared on the same scenarios.
 */
final class AssignmentMetrics {

    private AssignmentMetrics() {
    }

    /** Rows also reported for the tag-blind run of the same input, see {@link #tagBlindRows}. */
    private static final List<String> TAG_BLIND_ROW_PREFIXES = List.of(
        "distinct tag values",
        "all replicas on one tag value",
        "process load (max-min)",
        "standby load per process (max-min)",
        "standby tasks moved"
    );

    /** The {@link TaskAssignorTestbed.AssignmentGrader} the testbed runs for every assignor. */
    static Map<String, OptionalDouble> grade(
        final Scenario scenario,
        final GroupAssignment result,
        final Scenario.Baseline before,
        final Map<String, Set<TaskId>> reportedTasks
    ) {
        final Map<String, Integer> statefulActivePerMember = new HashMap<>();
        final Map<String, Integer> statelessActivePerMember = new HashMap<>();
        final Map<String, Integer> activePerMember = new HashMap<>();
        final Map<String, Map<String, Integer>> activePerSubtopologyPerMember = new HashMap<>();
        final Map<String, Integer> totalPerMember = new HashMap<>();
        final Map<String, Integer> standbyPerProcess = new HashMap<>();
        final Map<TaskId, Set<String>> ownerProcessesOf = new HashMap<>();
        final Map<TaskId, String> activeProcessOf = new HashMap<>();
        final Map<TaskId, Set<String>> standbyProcessesOf = new HashMap<>();
        for (final String processId : scenario.processes.keySet()) {
            standbyPerProcess.put(processId, 0);
        }
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
                activeProcessOf.put(task, processId);
                activePerSubtopologyPerMember.computeIfAbsent(task.subtopologyId(), s -> new HashMap<>()).merge(memberId, 1, Integer::sum);
            }
            for (final TaskId task : standby) {
                ownerProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(processId);
                standbyProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(processId);
            }
            statefulActivePerMember.put(memberId, stateful);
            statelessActivePerMember.put(memberId, active.size() - stateful);
            activePerMember.put(memberId, active.size());
            totalPerMember.put(memberId, active.size() + standby.size());
            standbyPerProcess.merge(processId, standby.size(), Integer::sum);
        }

        double maxLoad = 0;
        double minLoad = Double.MAX_VALUE;
        double maxStandbyLoad = 0;
        double minStandbyLoad = Double.MAX_VALUE;
        for (final Map.Entry<String, ProcessSpec> entry : scenario.processes.entrySet()) {
            final ProcessSpec process = entry.getValue();
            int tasks = 0;
            for (final String memberId : process.members) {
                tasks += totalPerMember.get(memberId);
            }
            final double load = (double) tasks / process.members.size();
            maxLoad = Math.max(maxLoad, load);
            minLoad = Math.min(minLoad, load);
            final double standbyLoad = (double) standbyPerProcess.get(entry.getKey()) / process.members.size();
            maxStandbyLoad = Math.max(maxStandbyLoad, standbyLoad);
            minStandbyLoad = Math.min(minStandbyLoad, standbyLoad);
        }

        // Members without a task of the subtopology count as 0. A skew of 1 is unavoidable when the partitions
        // do not divide evenly over the members, so only the excess over that is graded.
        final int members = result.members().size();
        int subtopologyExcessSkew = 0;
        int skewedSubtopologies = 0;
        for (final Map<String, Integer> perMember : activePerSubtopologyPerMember.values()) {
            final int min = perMember.size() < members ? 0 : perMember.values().stream().min(Integer::compare).orElseThrow();
            final int max = perMember.values().stream().max(Integer::compare).orElseThrow();
            final int partitions = perMember.values().stream().mapToInt(Integer::intValue).sum();
            final int unavoidable = partitions % members == 0 ? 0 : 1;
            final int excess = max - min - unavoidable;
            subtopologyExcessSkew = Math.max(subtopologyExcessSkew, excess);
            if (excess > 0) {
                skewedSubtopologies++;
            }
        }

        final int statefulTasks = scenario.topology.statefulTasks().size();
        final int tasks = scenario.topology.tasks().size();
        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        rows.put("stateful active tasks per member (max-min)", OptionalDouble.of(range(statefulActivePerMember.values())));
        rows.put("stateless active tasks per member (max-min)", OptionalDouble.of(range(statelessActivePerMember.values())));
        rows.put("active tasks per member (max-min)", OptionalDouble.of(range(activePerMember.values())));
        rows.put("stateful active tasks above even share", OptionalDouble.of(aboveEvenShare(statefulActivePerMember.values(), statefulTasks, members)));
        rows.put("stateless active tasks above even share", OptionalDouble.of(aboveEvenShare(statelessActivePerMember.values(), tasks - statefulTasks, members)));
        rows.put("active tasks above even share", OptionalDouble.of(aboveEvenShare(activePerMember.values(), tasks, members)));
        rows.put("total tasks per member (max-min)", OptionalDouble.of(range(totalPerMember.values())));
        // The load of a process is its tasks divided by its members.
        rows.put("process load (max-min)", OptionalDouble.of(maxLoad - minLoad));
        rows.put("standby load per process (max-min)", OptionalDouble.of(maxStandbyLoad - minStandbyLoad));
        rows.put("subtopology skew above unavoidable (max-min)", OptionalDouble.of(subtopologyExcessSkew));
        rows.put("subtopologies skewed above unavoidable", OptionalDouble.of(skewedSubtopologies));
        rows.putAll(rackRows(scenario, ownerProcessesOf));
        rows.putAll(movementRows(scenario, before, ownerProcessesOf, activeProcessOf, standbyProcessesOf, reportedTasks));
        return rows;
    }

    /** How far the most loaded member is above the even share {@code ceil(tasks / members)}; 0 when nobody is. */
    private static int aboveEvenShare(final Iterable<Integer> perMember, final int tasks, final int members) {
        final int evenShare = (tasks + members - 1) / members;
        int max = 0;
        for (final int value : perMember) {
            max = Math.max(max, value);
        }
        return Math.max(0, max - evenShare);
    }

    /**
     * The rows of {@link #grade} to report again for the tag-blind run of the same input, renamed with a
     * {@code (tag-blind)} suffix. Empty when the scenario has no tags, so every rebalance returns the same rows.
     */
    static Map<String, OptionalDouble> tagBlindRows(final Map<String, OptionalDouble> graded, final boolean applicable) {
        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        graded.forEach((name, value) -> {
            if (TAG_BLIND_ROW_PREFIXES.stream().anyMatch(name::startsWith)) {
                rows.put(name + " (tag-blind)", applicable ? value : OptionalDouble.empty());
            }
        });
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

    static OptionalDouble fraction(final int part, final int whole) {
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
     * without the host tag and without keys that have a single value in the group. For a task {@code t} and key
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
            if (!key.equals(TaskAssignorTestbed.HOST_TAG) && valuesInGroup.getOrDefault(key, Set.of()).size() > 1) {
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
     * Task movement between the assignment before the change and this one, over the processes present in both;
     * replicas of a departed process had to move and are not counted. Nothing is graded on the initial assignment.
     * <ul>
     * <li>stateful active tasks moved: the previous process is still in the group and the active task is elsewhere,
     * so the assignor chose the movement.</li>
     * <li>standby tasks moved: per stateful task, the smaller of the previous holders that lost their replica and
     * the processes that newly hold a standby task; an active and a standby task swapping roles on one process is
     * not a movement, nor is a standby task dropped after {@code numStandbyReplicas} was lowered.</li>
     * <li>replicas placed without state: replicas, active or standby, on a process that held no replica of the task
     * before and reported no offsets for it, whatever the reason; each one is a full restore. Tasks nobody held
     * before the change (a subtopology just added) have no state anywhere and nothing to restore, so they are left
     * out; without this, a rebalance that adds a subtopology would dominate the average.</li>
     * </ul>
     */
    private static Map<String, OptionalDouble> movementRows(
        final Scenario scenario,
        final Scenario.Baseline before,
        final Map<TaskId, Set<String>> ownerProcessesOf,
        final Map<TaskId, String> activeProcessOf,
        final Map<TaskId, Set<String>> standbyProcessesOf,
        final Map<String, Set<TaskId>> reportedTasks
    ) {
        final Map<String, OptionalDouble> rows = new LinkedHashMap<>();
        if (before.assignment().isEmpty()) {
            rows.put("stateful active tasks moved", OptionalDouble.empty());
            rows.put("standby tasks moved", OptionalDouble.empty());
            rows.put("replicas placed without state", OptionalDouble.empty());
            return rows;
        }
        final Map<TaskId, String> previousActiveProcessOf = new HashMap<>();
        final Map<TaskId, Set<String>> previousOwnerProcessesOf = new HashMap<>();
        final Set<TaskId> previouslyHeld = new HashSet<>();
        for (final Map.Entry<String, MemberAssignment> entry : before.assignment().entrySet()) {
            final String previousProcess = before.processOfMember().get(entry.getKey());
            final Set<TaskId> previousTasks = toTaskIds(entry.getValue().activeTasks());
            previousTasks.addAll(toTaskIds(entry.getValue().standbyTasks()));
            previouslyHeld.addAll(previousTasks);
            if (!scenario.processes.containsKey(previousProcess)) {
                continue;
            }
            for (final TaskId task : toTaskIds(entry.getValue().activeTasks())) {
                previousActiveProcessOf.put(task, previousProcess);
            }
            for (final TaskId task : previousTasks) {
                previousOwnerProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(previousProcess);
            }
        }
        int activeMoved = 0;
        int standbyMoved = 0;
        int placedWithoutState = 0;
        for (final TaskId task : scenario.topology.statefulTasks()) {
            final Set<String> previousOwners = previousOwnerProcessesOf.getOrDefault(task, Set.of());
            final Set<String> owners = ownerProcessesOf.getOrDefault(task, Set.of());
            final String previousActive = previousActiveProcessOf.get(task);
            if (previousActive != null && !previousActive.equals(activeProcessOf.get(task))) {
                activeMoved++;
            }
            final Set<String> lost = new HashSet<>(previousOwners);
            lost.removeAll(owners);
            final Set<String> newStandbyHolders = new HashSet<>(standbyProcessesOf.getOrDefault(task, Set.of()));
            newStandbyHolders.removeAll(previousOwners);
            standbyMoved += Math.min(lost.size(), newStandbyHolders.size());
            if (!previouslyHeld.contains(task)) {
                continue;
            }
            for (final String processId : owners) {
                if (!previousOwners.contains(processId) && !reportedTasks.getOrDefault(processId, Set.of()).contains(task)) {
                    placedWithoutState++;
                }
            }
        }
        rows.put("stateful active tasks moved", OptionalDouble.of(activeMoved));
        rows.put("standby tasks moved", OptionalDouble.of(standbyMoved));
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
            final StringBuilder builder = new StringBuilder("Fuzz metrics: ").append(title).append('\n')
                .append(String.format("  %-44s %8s %8s %8s %8s%n", "metric", "avg", "min", "max", "n"));
            rows.forEach((name, stat) -> row(builder, name, stat));
            return builder.toString();
        }

        private static void row(final StringBuilder builder, final String name, final Stat stat) {
            builder.append("  ").append(String.format("%-44s ", name)).append(stat).append('\n');
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
