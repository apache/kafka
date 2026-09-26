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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorTestbed.toTaskIds;

/**
 * Grades the converged assignment of a rebalance: balance across members and processes, stickiness and task
 * movement relative to the assignment held before the rebalance, and rack diversity of the copies of each stateful
 * task. Nothing here is asserted; the {@link Summary} over a run is printed so two versions of an assignor can be
 * compared on the same scenarios.
 */
final class AssignmentMetrics {

    private AssignmentMetrics() {
    }

    /**
     * @param activeSpreadPerMember          active tasks per member, max - min
     * @param statefulActiveSpreadPerMember  stateful active tasks per member, max - min
     * @param subtopologyExcessSpread        active tasks of one subtopology per member, max - min above the unavoidable spread, worst subtopology
     * @param totalSpreadPerMember           active + standby tasks per member, max - min
     * @param processLoadSpread              (active + standby) / members per process, max - min
     * @param standbyLoadSpread              standby tasks / members per process, max - min
     * @param processStickiness              fraction of the active tasks held before the rebalance that stayed on their process
     * @param memberStickiness               fraction of the active tasks held before the rebalance that stayed on their member, if it is still in the group
     * @param stateMoves                     stateful task copies placed on a process that held no copy before
     * @param stateReuse                     fraction of the tasks reported from state directories that were placed on that process
     * @param copiesLost                     stateful tasks whose every copy sat on a process the event removed, for multi-process failures
     * @param diversityPerTag                rack diversity per configured tag, relative to the best spread of that tag alone
     */
    record Metrics(
        int activeSpreadPerMember,
        int statefulActiveSpreadPerMember,
        int subtopologyExcessSpread,
        int totalSpreadPerMember,
        double processLoadSpread,
        double standbyLoadSpread,
        OptionalDouble processStickiness,
        OptionalDouble memberStickiness,
        OptionalInt stateMoves,
        OptionalDouble stateReuse,
        OptionalInt copiesLost,
        Map<String, Double> diversityPerTag
    ) {
    }

    /**
     * @param before        what the members held before the event that triggered the rebalance
     * @param restoredTasks the tasks each process reported from its state directories when the rebalance started
     */
    static Metrics compute(
        final Scenario scenario,
        final GroupAssignment result,
        final Scenario.Baseline before,
        final Map<String, Set<TaskId>> restoredTasks
    ) {
        final Map<String, Integer> activePerMember = new HashMap<>();
        final Map<String, Integer> statefulActivePerMember = new HashMap<>();
        final Map<String, Map<String, Integer>> activePerSubtopologyPerMember = new HashMap<>();
        final Map<String, Integer> totalPerMember = new HashMap<>();
        final Map<String, Integer> standbyPerProcess = new HashMap<>();
        final Map<TaskId, String> activeMemberOf = new HashMap<>();
        final Map<TaskId, String> activeProcessOf = new HashMap<>();
        final Map<TaskId, Set<String>> ownerProcessesOf = new HashMap<>();
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
                activeMemberOf.put(task, memberId);
                activeProcessOf.put(task, processId);
                ownerProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(processId);
                activePerSubtopologyPerMember.computeIfAbsent(task.subtopologyId(), s -> new HashMap<>()).merge(memberId, 1, Integer::sum);
            }
            for (final TaskId task : standby) {
                ownerProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(processId);
            }
            activePerMember.put(memberId, active.size());
            statefulActivePerMember.put(memberId, stateful);
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

        // Members without a task of the subtopology count as 0. A spread of 1 is unavoidable when the partitions
        // do not divide evenly over the members, so only the excess over that is graded.
        final int members = result.members().size();
        int subtopologyExcessSpread = 0;
        for (final Map<String, Integer> perMember : activePerSubtopologyPerMember.values()) {
            final int min = perMember.size() < members ? 0 : perMember.values().stream().min(Integer::compare).orElseThrow();
            final int max = perMember.values().stream().max(Integer::compare).orElseThrow();
            final int partitions = perMember.values().stream().mapToInt(Integer::intValue).sum();
            final int unavoidable = partitions % members == 0 ? 0 : 1;
            subtopologyExcessSpread = Math.max(subtopologyExcessSpread, max - min - unavoidable);
        }

        final Movement movement = movement(scenario, before, activeMemberOf, activeProcessOf, ownerProcessesOf);
        return new Metrics(
            spread(activePerMember.values()),
            spread(statefulActivePerMember.values()),
            subtopologyExcessSpread,
            spread(totalPerMember.values()),
            maxLoad - minLoad,
            maxStandbyLoad - minStandbyLoad,
            movement.processStickiness(),
            movement.memberStickiness(),
            movement.stateMoves(),
            stateReuse(restoredTasks, ownerProcessesOf),
            copiesLost(scenario, before),
            diversityPerTag(scenario, ownerProcessesOf)
        );
    }

    private record Movement(
        OptionalDouble processStickiness,
        OptionalDouble memberStickiness,
        OptionalInt stateMoves
    ) {
    }

    /**
     * Compares against the assignment held before the event, over the tasks whose process is still in the group.
     * Tasks of a process that left had to move, so they are not counted; tasks of a restarted process are, since
     * its state directories still hold them. Member stickiness further skips tasks whose member left, since the
     * restarted process comes back with new member ids. Tasks of a removed subtopology are not counted either.
     */
    private static Movement movement(
        final Scenario scenario,
        final Scenario.Baseline before,
        final Map<TaskId, String> activeMemberOf,
        final Map<TaskId, String> activeProcessOf,
        final Map<TaskId, Set<String>> ownerProcessesOf
    ) {
        if (before.assignment().isEmpty()) {
            return new Movement(OptionalDouble.empty(), OptionalDouble.empty(), OptionalInt.empty());
        }
        int processCandidates = 0;
        int stuckOnProcess = 0;
        int memberCandidates = 0;
        int stuckOnMember = 0;
        for (final Map.Entry<String, MemberAssignment> entry : before.assignment().entrySet()) {
            final String previousMember = entry.getKey();
            final String previousProcess = before.processOfMember().get(previousMember);
            if (!scenario.processes.containsKey(previousProcess)) {
                continue;
            }
            final boolean memberStillInGroup = scenario.memberIds().contains(previousMember);
            for (final TaskId task : toTaskIds(entry.getValue().activeTasks())) {
                if (!scenario.topology.tasks().contains(task)) {
                    continue;
                }
                processCandidates++;
                if (previousProcess.equals(activeProcessOf.get(task))) {
                    stuckOnProcess++;
                }
                if (memberStillInGroup) {
                    memberCandidates++;
                    if (previousMember.equals(activeMemberOf.get(task))) {
                        stuckOnMember++;
                    }
                }
            }
        }
        return new Movement(
            processCandidates == 0 ? OptionalDouble.empty() : OptionalDouble.of((double) stuckOnProcess / processCandidates),
            memberCandidates == 0 ? OptionalDouble.empty() : OptionalDouble.of((double) stuckOnMember / memberCandidates),
            OptionalInt.of(stateMoves(scenario, before, ownerProcessesOf))
        );
    }

    /**
     * Copies of a stateful task, active or standby, dropped from a process still in the group and placed on a
     * process that held no copy before, so the state has to be rebuilt there. Swapping active and standby between
     * two owners is not a move, nor is a dropped copy with no new one elsewhere (fewer standbys configured).
     */
    private static int stateMoves(
        final Scenario scenario,
        final Scenario.Baseline before,
        final Map<TaskId, Set<String>> ownerProcessesOf
    ) {
        final Map<TaskId, Set<String>> previousOwnerProcessesOf = new HashMap<>();
        for (final Map.Entry<String, MemberAssignment> entry : before.assignment().entrySet()) {
            final String previousProcess = before.processOfMember().get(entry.getKey());
            if (!scenario.processes.containsKey(previousProcess)) {
                continue;
            }
            final Set<TaskId> previousTasks = toTaskIds(entry.getValue().activeTasks());
            previousTasks.addAll(toTaskIds(entry.getValue().standbyTasks()));
            previousTasks.retainAll(scenario.topology.statefulTasks());
            for (final TaskId task : previousTasks) {
                previousOwnerProcessesOf.computeIfAbsent(task, t -> new HashSet<>()).add(previousProcess);
            }
        }
        int stateMoves = 0;
        for (final Map.Entry<TaskId, Set<String>> entry : previousOwnerProcessesOf.entrySet()) {
            final Set<String> current = ownerProcessesOf.getOrDefault(entry.getKey(), Set.of());
            final Set<String> dropped = new HashSet<>(entry.getValue());
            dropped.removeAll(current);
            final Set<String> added = new HashSet<>(current);
            added.removeAll(entry.getValue());
            stateMoves += Math.min(dropped.size(), added.size());
        }
        return stateMoves;
    }

    /**
     * Of the tasks the processes reported from their state directories when the rebalance started, the fraction
     * that ended up on the reporting process as active or standby. Nothing is graded when nothing was reported.
     */
    private static OptionalDouble stateReuse(
        final Map<String, Set<TaskId>> restoredTasks,
        final Map<TaskId, Set<String>> ownerProcessesOf
    ) {
        int reported = 0;
        int reused = 0;
        for (final Map.Entry<String, Set<TaskId>> entry : restoredTasks.entrySet()) {
            for (final TaskId task : entry.getValue()) {
                reported++;
                if (ownerProcessesOf.getOrDefault(task, Set.of()).contains(entry.getKey())) {
                    reused++;
                }
            }
        }
        return reported == 0 ? OptionalDouble.empty() : OptionalDouble.of((double) reused / reported);
    }

    /**
     * Stateful tasks that lost every copy, active and standbys, to the processes the event removed: the state has
     * to be rebuilt from the changelog. What rack-aware standby placement is meant to prevent. This grades the
     * placement held before the event. Graded only when several processes left at once, as in a zone failure, and
     * standbys are configured; a single process leaving cannot take every copy once standbys are placed.
     */
    private static OptionalInt copiesLost(final Scenario scenario, final Scenario.Baseline before) {
        final Set<String> gone = new HashSet<>(before.processOfMember().values());
        gone.removeAll(scenario.processes.keySet());
        if (gone.size() < 2 || scenario.numStandbyReplicas == 0) {
            return OptionalInt.empty();
        }
        final Map<TaskId, Boolean> survives = new HashMap<>();
        for (final Map.Entry<String, MemberAssignment> entry : before.assignment().entrySet()) {
            final boolean alive = !gone.contains(before.processOfMember().get(entry.getKey()));
            for (final TaskId task : toTaskIds(entry.getValue().activeTasks())) {
                if (scenario.topology.statefulTasks().contains(task)) {
                    survives.merge(task, alive, Boolean::logicalOr);
                }
            }
            for (final TaskId task : toTaskIds(entry.getValue().standbyTasks())) {
                survives.merge(task, alive, Boolean::logicalOr);
            }
        }
        return OptionalInt.of((int) survives.values().stream().filter(alive -> !alive).count());
    }

    /**
     * For each stateful task and configured tag: distinct tag values among the processes holding a copy of the
     * task, divided by the best achievable for that tag alone (the configured copies, capped by the distinct values
     * present in the group). Tags are bounded separately, so 1.0 on every tag at once may be unreachable; compare
     * assignors against each other rather than against 1.0. Averaged per tag over all tasks; a tag with at most one
     * value in the group is left out, as is the host tag whose value is unique per process, and nothing is graded
     * without standbys since every task then has a single copy.
     */
    private static Map<String, Double> diversityPerTag(final Scenario scenario, final Map<TaskId, Set<String>> ownerProcessesOf) {
        final Map<String, Double> diversityPerTag = new LinkedHashMap<>();
        if (scenario.tagKeys.isEmpty() || scenario.numStandbyReplicas == 0) {
            return diversityPerTag;
        }
        final Map<String, Set<String>> valuesInGroup = new HashMap<>();
        for (final ProcessSpec process : scenario.processes.values()) {
            process.tags.forEach((key, value) -> valuesInGroup.computeIfAbsent(key, k -> new HashSet<>()).add(value));
        }
        for (final String key : scenario.tagKeys) {
            final int available = valuesInGroup.getOrDefault(key, Set.of()).size();
            if (available <= 1 || key.equals(TaskAssignorTestbed.HOST_TAG)) {
                continue;
            }
            int tasks = 0;
            double score = 0;
            for (final TaskId task : scenario.topology.statefulTasks()) {
                final Set<String> owners = ownerProcessesOf.getOrDefault(task, Set.of());
                final Set<String> ownerValues = new HashSet<>();
                for (final String processId : owners) {
                    final String value = scenario.processes.get(processId).tags.get(key);
                    if (value != null) {
                        ownerValues.add(value);
                    }
                }
                tasks++;
                score += (double) ownerValues.size() / Math.min(1 + scenario.numStandbyReplicas, available);
            }
            if (tasks > 0) {
                diversityPerTag.put(key, score / tasks);
            }
        }
        return diversityPerTag;
    }

    private static int spread(final Iterable<Integer> values) {
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for (final int value : values) {
            max = Math.max(max, value);
            min = Math.min(min, value);
        }
        return max == Integer.MIN_VALUE ? 0 : max - min;
    }

    /** Aggregates the metrics of every rebalance in a run into one table. */
    static final class Summary {
        private final String title;
        private final Stat activeSpread = new Stat();
        private final Stat statefulActiveSpread = new Stat();
        private final Stat subtopologyExcessSpread = new Stat();
        private final Stat totalSpread = new Stat();
        private final Stat processLoadSpread = new Stat();
        private final Stat standbyLoadSpread = new Stat();
        private final Stat processStickiness = new Stat();
        private final Stat memberStickiness = new Stat();
        private final Stat stateMoves = new Stat();
        private final Stat stateReuse = new Stat();
        private final Stat copiesLost = new Stat();
        private final Map<String, Stat> diversityPerTag = new TreeMap<>();
        private final Stat convergenceIterations = new Stat();
        private int rebalances;
        private int notConverged;

        Summary(final String title) {
            this.title = title;
        }

        void add(final Metrics metrics) {
            rebalances++;
            activeSpread.add(metrics.activeSpreadPerMember);
            statefulActiveSpread.add(metrics.statefulActiveSpreadPerMember);
            subtopologyExcessSpread.add(metrics.subtopologyExcessSpread);
            totalSpread.add(metrics.totalSpreadPerMember);
            processLoadSpread.add(metrics.processLoadSpread);
            standbyLoadSpread.add(metrics.standbyLoadSpread);
            metrics.processStickiness.ifPresent(processStickiness::add);
            metrics.memberStickiness.ifPresent(memberStickiness::add);
            metrics.stateMoves.ifPresent(stateMoves::add);
            metrics.stateReuse.ifPresent(stateReuse::add);
            metrics.copiesLost.ifPresent(copiesLost::add);
            metrics.diversityPerTag.forEach((key, value) -> diversityPerTag.computeIfAbsent(key, k -> new Stat()).add(value));
        }

        /** Records how many assignor runs a rebalance needed until the assignment was stable. */
        void addConvergence(final int iterations) {
            convergenceIterations.add(iterations);
        }

        /** Records a rebalance whose assignment was still changing when the iteration limit was reached. */
        void addNotConverged() {
            notConverged++;
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder("Fuzz metrics: ").append(title).append('\n')
                .append(String.format("  %-44s %8s %8s %8s %8s%n", "metric", "avg", "min", "max", "n"));
            row(builder, "active tasks per member (max-min)", activeSpread);
            row(builder, "stateful active tasks per member (max-min)", statefulActiveSpread);
            row(builder, "subtopology spread above unavoidable", subtopologyExcessSpread);
            row(builder, "total tasks per member (max-min)", totalSpread);
            row(builder, "process load (max-min)", processLoadSpread);
            row(builder, "standby load per process (max-min)", standbyLoadSpread);
            row(builder, "process stickiness", processStickiness);
            row(builder, "member stickiness", memberStickiness);
            row(builder, "stateful copies moved per rebalance", stateMoves);
            row(builder, "restored state reused (1.0 = all)", stateReuse);
            row(builder, "all copies lost in multi-process failure", copiesLost);
            diversityPerTag.forEach((key, stat) -> row(builder, "rack diversity [" + key + "] (vs per-tag best)", stat));
            row(builder, "convergence iterations", convergenceIterations);
            builder.append(String.format("  %-44s %d of %d rebalances%n", "not converged within limit", notConverged, rebalances));
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
