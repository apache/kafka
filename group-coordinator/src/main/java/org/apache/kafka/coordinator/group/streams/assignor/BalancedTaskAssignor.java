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

import org.apache.kafka.coordinator.group.Utils;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignmentState;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.api.streams.assignor.TopologyDescriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A task assignor that computes a <em>balanced</em> assignment: tasks of the same subtopology are spread over as
 * many processes as possible, and the per-member task load is evened out across processes.
 * <p>
 * The assignment follows the placement half of the "classic" protocol's {@code HighAvailabilityTaskAssignor},
 * translated to the streams rebalance protocol:
 * <ol>
 *     <li>Stateful active tasks are dealt over the processes in sorted task order. Each task goes to the process
 *     with the smallest {@code (2n + 1) / c}, where {@code n} is the number of active tasks the process holds so
 *     far and {@code c} its number of members, ties going to the smaller process ID (the Sainte-Laguë method).
 *     With equal member counts this is a round-robin over the processes in ID order. The per-member loads of any
 *     two processes differ by at most one, and moving a single task from one process to another would not bring
 *     their loads closer.</li>
 *     <li>Standby tasks are placed on the least loaded process that does not hold the task yet, and then moved
 *     between processes as long as a move reduces the skew of the per-member task load.</li>
 *     <li>Stateless active tasks continue the deal of step 1 where the stateful tasks stopped, so that all active
 *     tasks together are dealt in proportion to the member counts.</li>
 *     <li>Within a process, the tasks are spread over its members in three rounds, as the classic client spreads a
 *     process's tasks over its stream threads: stateful active tasks, then standby tasks, then stateless active
 *     tasks. Each round levels the members' total task counts. A stateful task stays on the member that currently
 *     holds it, in whichever role, where that does not leave another member short; stateless tasks are dealt out
 *     without regard to their current member.</li>
 * </ol>
 * In contrast to the {@link StickyTaskAssignor}, the placement across processes does not depend on the previous
 * assignment, so the assignment stays orderly across many membership changes at the price of moving more tasks.
 * <p>
 * The parts of the classic assignor that this assignor deliberately does <em>not</em> implement are not assignor
 * concerns in the streams rebalance protocol: warm-up tasks are inserted by the group coordinator when it applies the
 * target assignment (so the member's {@link MemberAssignmentState#taskOffsets()} and
 * {@link MemberAssignmentState#taskEndOffsets()} are not read here, and {@link MemberAssignmentState#warmupTasks()}
 * only tells the fan-out which member of a process holds a task's state), and rack-aware placement is tracked
 * separately.
 */
public class BalancedTaskAssignor implements TaskAssignor {

    private static final String BALANCED_ASSIGNOR_NAME = "balanced";
    private static final Logger log = LoggerFactory.getLogger(BalancedTaskAssignor.class);

    private static final Comparator<ProcessTasks> BY_ASSIGNED_LOAD =
        Comparator.comparingDouble(ProcessTasks::assignedTaskLoad).thenComparing(ProcessTasks::processId);

    @Override
    public String name() {
        return BALANCED_ASSIGNOR_NAME;
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public GroupAssignment assign(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) throws TaskAssignorException {
        final TreeMap<String, ProcessTasks> processes = initializeProcesses(groupSpec);

        final SortedSet<TaskId> statefulTasks = new TreeSet<>();
        final SortedSet<TaskId> statelessTasks = new TreeSet<>();
        for (final String subtopology : topologyDescriber.subtopologies()) {
            final SortedSet<TaskId> tasks = topologyDescriber.isStateful(subtopology) ? statefulTasks : statelessTasks;
            final int numberOfPartitions = topologyDescriber.maxNumInputPartitions(subtopology);
            for (int partition = 0; partition < numberOfPartitions; partition++) {
                tasks.add(new TaskId(subtopology, partition));
            }
        }

        if (processes.isEmpty()) {
            if (!statefulTasks.isEmpty() || !statelessTasks.isEmpty()) {
                throw new TaskAssignorException("No process available to assign active tasks.");
            }
            return new GroupAssignment(Map.of());
        }

        // The deal refers to the processes by their index in sorted order of their ID.
        final ProcessTasks[] processesById = processes.values().toArray(new ProcessTasks[0]);
        final int[] period = dealPeriod(processesById);
        dealTasks(processesById, period, 0, statefulTasks, process -> process.statefulActiveTasks);

        final int numStandbyReplicas = groupSpec.configs().numStandbyReplicas();
        if (numStandbyReplicas > 0) {
            assignStandbyReplicaTasks(processes.values(), statefulTasks, numStandbyReplicas);
        }

        // The stateless tasks come after the standbys, so that the load by which the standbys are placed counts
        // stateful tasks only. Where they go does not depend on the standbys.
        dealTasks(processesById, period, statefulTasks.size(), statelessTasks, process -> process.statelessActiveTasks);

        return buildGroupAssignment(processes.values());
    }

    /**
     * Groups the members by process. The processes are kept in sorted order of their ID, which makes the
     * assignment deterministic for a given group.
     */
    private static TreeMap<String, ProcessTasks> initializeProcesses(final GroupSpec groupSpec) {
        final TreeMap<String, ProcessTasks> processes = new TreeMap<>();
        for (final String memberId : groupSpec.memberIds()) {
            final String processId = groupSpec.memberMetadata(memberId).processId();
            processes.computeIfAbsent(processId, ProcessTasks::new)
                .addMember(memberId, groupSpec.memberAssignmentState(memberId));
        }
        return processes;
    }

    /**
     * Computes one period of the deal of the active tasks: item {@code i} of the deal goes to the process at index
     * {@code period[i % period.length]}, for every {@code i >= 0}.
     * <p>
     * Each item goes to the process with the smallest {@code (2n + 1) / c}, where {@code n} is the number of items
     * the process already holds and {@code c} its member count, ties going to the smaller index. Dividing every
     * member count by their greatest common divisor {@code g} changes none of these comparisons. The deal then
     * repeats after every {@code C / g} items, {@code C} the total member count, and each process receives exactly
     * {@code c / g} of the items of a period. With equal member counts the period lists the processes once, in
     * order, which is the round-robin.
     * <p>
     * With {@code P} the number of processes, {@code c_p} the member count of process {@code p} and
     * {@code c_max = max_p c_p}, computing {@code g} takes {@code O(P + log(c_max))} with Euclid's algorithm folded
     * over the processes, and building the period takes {@code O((C / g) log P)}: one poll and at most one offer on
     * a heap of {@code P} processes per item.
     *
     * @param processes The processes in sorted order of their ID; not empty.
     * @return The index of the process that receives each item of one period.
     */
    private static int[] dealPeriod(final ProcessTasks[] processes) {
        int divisor = 0;

        for (final ProcessTasks process : processes) {
            divisor = greatestCommonDivisor(divisor, process.capacity());
        }
        final int[] weights = new int[processes.length];
        int periodLength = 0;
        for (int i = 0; i < processes.length; i++) {
            weights[i] = processes[i].capacity() / divisor;
            periodLength += weights[i];
        }

        final int[] dealt = new int[processes.length];
        // Orders the processes by the cost (2n + 1) / w of their next item, compared exactly by cross-multiplying,
        // ties to the smaller index. Only a polled process changes its count, and it is offered again afterwards, so
        // the order stays valid.
        final PriorityQueue<Integer> processesByNextCost = new PriorityQueue<>(processes.length, (a, b) -> {
            final int comparison = Long.compare((2L * dealt[a] + 1) * weights[b], (2L * dealt[b] + 1) * weights[a]);
            return comparison != 0 ? comparison : Integer.compare(a, b);
        });
        for (int i = 0; i < processes.length; i++) {
            processesByNextCost.add(i);
        }

        final int[] period = new int[periodLength];
        for (int item = 0; item < periodLength; item++) {
            final int process = processesByNextCost.poll();
            period[item] = process;
            // A process takes no more than its weight in one period, so it is not offered again once it has.
            if (++dealt[process] < weights[process]) {
                processesByNextCost.add(process);
            }
        }
        return period;
    }

    private static int greatestCommonDivisor(final int a, final int b) {
        int x = a;
        int y = b;
        while (y != 0) {
            final int remainder = x % y;
            x = y;
            y = remainder;
        }
        return x;
    }

    /**
     * Hands the tasks, in their sorted order, to the processes that receive items {@code firstItem},
     * {@code firstItem + 1}, ... of the deal.
     */
    private static void dealTasks(final ProcessTasks[] processes,
                                  final int[] period,
                                  final int firstItem,
                                  final SortedSet<TaskId> tasks,
                                  final Function<ProcessTasks, SortedSet<TaskId>> tasksToDeal) {
        int position = firstItem % period.length;
        for (final TaskId task : tasks) {
            tasksToDeal.apply(processes[period[position]]).add(task);
            if (++position == period.length) {
                position = 0;
            }
        }
    }

    private static void assignStandbyReplicaTasks(final Collection<ProcessTasks> processes,
                                                  final SortedSet<TaskId> statefulTasks,
                                                  final int numStandbyReplicas) {
        // The queue orders the processes by their load as of the time they were offered. A polled process is offered
        // again after its load changed, so the order stays valid.
        final PriorityQueue<ProcessTasks> processesByLoad = new PriorityQueue<>(BY_ASSIGNED_LOAD);
        processesByLoad.addAll(processes);

        int tasksShortOfReplicas = 0;
        int missingReplicas = 0;
        for (final TaskId task : statefulTasks) {
            int remainingReplicas = numStandbyReplicas;
            while (remainingReplicas > 0) {
                final ProcessTasks process = pollLeastLoadedProcessWithoutTask(processesByLoad, task);
                if (process == null) {
                    break;
                }
                process.standbyTasks.add(task);
                processesByLoad.add(process);
                remainingReplicas--;
            }

            if (remainingReplicas > 0) {
                tasksShortOfReplicas++;
                missingReplicas += remainingReplicas;
            }
        }

        if (tasksShortOfReplicas > 0) {
            // Expected whenever the group runs on fewer processes than copies are configured, and repeated on every
            // assignment of that group, so one line at INFO rather than a warning per task.
            log.info("{} of {} stateful tasks got fewer than the configured {} standby replicas ({} replicas missing in "
                    + "total): the copies of a task must be on different processes, and the group runs on {} process(es). "
                    + "Add application instances to get the configured number of standby replicas.",
                tasksShortOfReplicas, statefulTasks.size(), numStandbyReplicas, missingReplicas, processes.size());
        }

        balanceTasksOverProcesses(processes, process -> process.standbyTasks);
    }

    /**
     * Polls the least loaded process that does not hold the task yet, or {@code null} if every process holds it.
     * Processes that were skipped are offered back to the queue; the returned process is not, since the caller
     * changes its load.
     */
    private static ProcessTasks pollLeastLoadedProcessWithoutTask(final PriorityQueue<ProcessTasks> processesByLoad,
                                                                  final TaskId task) {
        final List<ProcessTasks> skipped = new ArrayList<>();
        ProcessTasks found = null;
        while (!processesByLoad.isEmpty()) {
            final ProcessTasks candidate = processesByLoad.poll();
            if (candidate.hasTask(task)) {
                skipped.add(candidate);
            } else {
                found = candidate;
                break;
            }
        }
        processesByLoad.addAll(skipped);
        return found;
    }

    /**
     * Moves tasks from more loaded to less loaded processes until no single move reduces the skew of the per-member
     * task load any further. A task is never moved to a process that already holds it as active or standby task.
     */
    private static void balanceTasksOverProcesses(final Collection<ProcessTasks> processes,
                                                  final Function<ProcessTasks, SortedSet<TaskId>> tasksToBalance) {
        boolean keepBalancing = true;
        while (keepBalancing) {
            keepBalancing = false;
            for (final ProcessTasks source : processes) {
                for (final ProcessTasks destination : processes) {
                    if (source == destination || !shouldMoveATask(source, destination)) {
                        continue;
                    }

                    final SortedSet<TaskId> destinationTasks = tasksToBalance.apply(destination);
                    // The moves change only the source's tasks, which the iterator removes in place.
                    final Iterator<TaskId> sourceIterator = tasksToBalance.apply(source).iterator();
                    while (shouldMoveATask(source, destination) && sourceIterator.hasNext()) {
                        final TaskId taskToMove = sourceIterator.next();
                        if (!destination.hasTask(taskToMove)) {
                            sourceIterator.remove();
                            destinationTasks.add(taskToMove);
                            keepBalancing = true;
                        }
                    }
                }
            }
        }
    }

    /**
     * A task should move from the source to the destination only if the destination is less loaded per member, and
     * the move would reduce that skew without tipping the imbalance over to the other side.
     */
    private static boolean shouldMoveATask(final ProcessTasks source, final ProcessTasks destination) {
        final double skew = source.assignedTaskLoad() - destination.assignedTaskLoad();

        if (skew <= 0) {
            return false;
        }

        final double proposedAssignedTasksPerMemberAtDestination =
            (destination.assignedTaskCount() + 1.0) / destination.capacity();
        final double proposedAssignedTasksPerMemberAtSource =
            (source.assignedTaskCount() - 1.0) / source.capacity();
        final double proposedSkew = proposedAssignedTasksPerMemberAtSource - proposedAssignedTasksPerMemberAtDestination;

        if (proposedSkew < 0) {
            // then the move would only create an imbalance in the other direction.
            return false;
        }
        // we should only move a task if doing so would actually improve the skew.
        return proposedSkew < skew;
    }

    /**
     * Every member belongs to exactly one process and each process reports an assignment for each of its members,
     * so the per-process assignments cover the whole group and never overlap.
     */
    private static GroupAssignment buildGroupAssignment(final Collection<ProcessTasks> processes) {
        return new GroupAssignment(processes.stream()
            .flatMap(process -> process.distributeTasksOverMembers().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private static Map<String, Set<Integer>> toCompactedTaskIds(final Set<TaskId> taskIds) {
        final Map<String, Set<Integer>> ret = new HashMap<>();
        for (final TaskId taskId : taskIds) {
            ret.computeIfAbsent(taskId.subtopologyId(), subtopologyId -> new HashSet<>())
                .add(taskId.partition());
        }
        return ret;
    }

    /**
     * The tasks placed on one process, and the members the process contributes. The assignment across processes
     * works on this level; the tasks are spread over the members only once the placement is final.
     */
    private static final class ProcessTasks {
        private final String processId;
        private final TreeSet<String> memberIds = new TreeSet<>();
        private final SortedSet<TaskId> statefulActiveTasks = new TreeSet<>();
        private final SortedSet<TaskId> statelessActiveTasks = new TreeSet<>();
        private final SortedSet<TaskId> standbyTasks = new TreeSet<>();
        // The member of this process that currently holds a task in any role, so that a stateful task can stay on
        // the member that has its state, as the classic client does through the offset sums each thread reports.
        private final Map<TaskId, String> currentOwner = new HashMap<>();

        private ProcessTasks(final String processId) {
            this.processId = processId;
        }

        private String processId() {
            return processId;
        }

        private void addMember(final String memberId, final MemberAssignmentState currentAssignment) {
            memberIds.add(memberId);
            recordCurrentOwner(memberId, currentAssignment.activeTasks());
            recordCurrentOwner(memberId, currentAssignment.standbyTasks());
            recordCurrentOwner(memberId, currentAssignment.warmupTasks());
        }

        private void recordCurrentOwner(final String memberId, final Map<String, Set<Integer>> tasks) {
            for (final Map.Entry<String, Set<Integer>> entry : tasks.entrySet()) {
                for (final int partition : entry.getValue()) {
                    // Two members of one process cannot hold the same task; the smaller member ID wins if they do.
                    currentOwner.merge(new TaskId(entry.getKey(), partition), memberId,
                        (existing, candidate) -> existing.compareTo(candidate) <= 0 ? existing : candidate);
                }
            }
        }

        private int capacity() {
            return memberIds.size();
        }

        private int activeTaskCount() {
            return statefulActiveTasks.size() + statelessActiveTasks.size();
        }

        private int assignedTaskCount() {
            return activeTaskCount() + standbyTasks.size();
        }

        private double assignedTaskLoad() {
            return ((double) assignedTaskCount()) / capacity();
        }

        private boolean hasTask(final TaskId task) {
            return statefulActiveTasks.contains(task) || statelessActiveTasks.contains(task) || standbyTasks.contains(task);
        }

        /**
         * Spreads the process's tasks over its members in the three rounds of the classic client's
         * {@code assignTasksToThreads}: stateful active tasks first, then standby tasks, then stateless active
         * tasks. Each round levels the members' total task counts on top of what the previous rounds placed, so the
         * stateful active tasks are even on their own and the standbys fill up the members with fewer of them. The
         * stateless tasks come last and are dealt out without regard to their current member, since they have no
         * state to keep close. As in the classic client, a member may therefore end up with more active tasks than a
         * sibling that holds standbys instead.
         *
         * @return The assignment of every member of this process, including members that received no task.
         */
        private Map<String, MemberAssignment> distributeTasksOverMembers() {
            final Map<String, Integer> taskCountByMember = Utils.newHashMap(capacity());
            final Map<String, Set<TaskId>> activeTasksByMember = Utils.newHashMap(capacity());
            final Map<String, Set<TaskId>> standbyTasksByMember = Utils.newHashMap(capacity());
            for (final String memberId : memberIds) {
                taskCountByMember.put(memberId, 0);
                activeTasksByMember.put(memberId, new HashSet<>());
                standbyTasksByMember.put(memberId, new HashSet<>());
            }

            distributeTasksOverMembers(statefulActiveTasks, currentOwner, taskCountByMember, activeTasksByMember);
            distributeTasksOverMembers(standbyTasks, currentOwner, taskCountByMember, standbyTasksByMember);
            distributeTasksOverMembers(statelessActiveTasks, Map.of(), taskCountByMember, activeTasksByMember);

            return memberIds.stream().collect(Collectors.toMap(
                Function.identity(),
                memberId -> new MemberAssignment(
                    toCompactedTaskIds(activeTasksByMember.get(memberId)),
                    toCompactedTaskIds(standbyTasksByMember.get(memberId))
                )
            ));
        }

        /**
         * Distributes the tasks of one round over the members so that every member ends the round at the same
         * level, {@code (tasks placed so far + tasks of this round) / members}, or one above it:
         * <ol>
         *     <li>A task stays on the member that currently owns it as long as that member is below the level.</li>
         *     <li>The members still below the level take the remaining tasks in turn, in member ID order, until
         *     they reach it.</li>
         *     <li>Fewer tasks than members are left. Each goes to a member at the level: its current owner if that
         *     member is one of them, otherwise the next such member in ID order.</li>
         * </ol>
         * Keeping the sticky pass below the level, rather than one above it, is what keeps a member that joins from
         * being left with nothing while the others keep everything.
         *
         * @param tasksToDistribute        The tasks of this round placed on the process.
         * @param currentOwner             The member of this process that currently holds a task, if any; empty for
         *                                 a round without stickiness.
         * @param taskCountByMember        The number of tasks each member holds so far, over all rounds; updated as
         *                                 tasks are distributed.
         * @param distributedTasksByMember The tasks of this round's role each member receives; the output of this
         *                                 method.
         */
        private void distributeTasksOverMembers(final SortedSet<TaskId> tasksToDistribute,
                                                final Map<TaskId, String> currentOwner,
                                                final Map<String, Integer> taskCountByMember,
                                                final Map<String, Set<TaskId>> distributedTasksByMember) {
            if (tasksToDistribute.isEmpty()) {
                return;
            }
            int tasksPlacedSoFar = 0;
            for (final int count : taskCountByMember.values()) {
                tasksPlacedSoFar += count;
            }
            final int level = (tasksPlacedSoFar + tasksToDistribute.size()) / capacity();

            // Step 1. Tasks whose owner is at the level already are remembered, so the owner can still keep them in
            // step 3.
            final List<TaskId> unassignedTasks = new ArrayList<>();
            final Map<TaskId, String> ownersOfSkippedTasks = new LinkedHashMap<>();
            for (final TaskId task : tasksToDistribute) {
                final String owner = currentOwner.get(task);
                if (owner != null && taskCountByMember.get(owner) < level) {
                    place(task, owner, taskCountByMember, distributedTasksByMember);
                } else {
                    unassignedTasks.add(task);
                    if (owner != null) {
                        ownersOfSkippedTasks.put(task, owner);
                    }
                }
            }

            final int firstLeftoverTask = fillMembersBelowLevel(unassignedTasks, level, taskCountByMember, distributedTasksByMember);
            placeLeftoverTasks(unassignedTasks.subList(firstLeftoverTask, unassignedTasks.size()), ownersOfSkippedTasks, level,
                taskCountByMember, distributedTasksByMember);
        }

        /**
         * Step 2 of a round: the members below the level take the unassigned tasks in turn, in member ID order,
         * until every member has reached the level.
         *
         * @return The index of the first unassigned task that was not placed.
         */
        private int fillMembersBelowLevel(final List<TaskId> unassignedTasks,
                                          final int level,
                                          final Map<String, Integer> taskCountByMember,
                                          final Map<String, Set<TaskId>> distributedTasksByMember) {
            final Deque<String> membersToFill = new ArrayDeque<>();
            for (final String memberId : memberIds) {
                if (taskCountByMember.get(memberId) < level) {
                    membersToFill.add(memberId);
                }
            }
            int nextTask = 0;
            while (!membersToFill.isEmpty() && nextTask < unassignedTasks.size()) {
                final String member = membersToFill.poll();
                place(unassignedTasks.get(nextTask++), member, taskCountByMember, distributedTasksByMember);
                if (taskCountByMember.get(member) < level) {
                    membersToFill.add(member);
                }
            }
            return nextTask;
        }

        /**
         * Step 3 of a round: every member is at the level or one above it, and fewer tasks are left than there are
         * members at the level, since every round before this one levelled its tasks the same way. Each leftover
         * task goes to its current owner if that member is at the level, otherwise to the next member at the level
         * in ID order.
         */
        private void placeLeftoverTasks(final List<TaskId> leftoverTasks,
                                        final Map<TaskId, String> ownersOfSkippedTasks,
                                        final int level,
                                        final Map<String, Integer> taskCountByMember,
                                        final Map<String, Set<TaskId>> distributedTasksByMember) {
            final Set<String> membersAtLevel = new LinkedHashSet<>();
            for (final String memberId : memberIds) {
                if (taskCountByMember.get(memberId) == level) {
                    membersAtLevel.add(memberId);
                }
            }
            final List<TaskId> tasksWithoutOwnerAtLevel = new ArrayList<>();
            for (final TaskId task : leftoverTasks) {
                final String owner = ownersOfSkippedTasks.get(task);
                if (owner != null && membersAtLevel.remove(owner)) {
                    place(task, owner, taskCountByMember, distributedTasksByMember);
                } else {
                    tasksWithoutOwnerAtLevel.add(task);
                }
            }
            for (final TaskId task : tasksWithoutOwnerAtLevel) {
                final Iterator<String> nextMemberAtLevel = membersAtLevel.iterator();
                final String member = nextMemberAtLevel.next();
                nextMemberAtLevel.remove();
                place(task, member, taskCountByMember, distributedTasksByMember);
            }
        }

        private static void place(final TaskId task,
                                  final String member,
                                  final Map<String, Integer> taskCountByMember,
                                  final Map<String, Set<TaskId>> distributedTasksByMember) {
            distributedTasksByMember.get(member).add(task);
            taskCountByMember.merge(member, 1, Integer::sum);
        }
    }
}
