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
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignmentState;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.api.streams.assignor.TopologyDescriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
 * The assignment is the placement half of the "classic" protocol's {@code HighAvailabilityTaskAssignor}, translated
 * to the streams rebalance protocol:
 * <ol>
 *     <li>Stateful active tasks are dealt round-robin over the processes, in sorted order of task and process ID,
 *     and then moved between processes as long as a move reduces the skew of the per-member task load.</li>
 *     <li>Standby tasks are placed on the least loaded process that does not hold the task yet, and evened out the
 *     same way.</li>
 *     <li>Stateless active tasks fill in the gaps, going to the process with the lowest active task load.</li>
 *     <li>Within a process, the tasks are spread evenly over its members, keeping a task on the member that currently
 *     owns it where the quota allows.</li>
 * </ol>
 * In contrast to the {@link StickyTaskAssignor}, the placement across processes does not depend on the previous
 * assignment, so the assignment stays orderly across many membership changes at the price of moving more tasks.
 * <p>
 * The parts of the classic assignor that this assignor deliberately does <em>not</em> implement are not assignor
 * concerns in the streams rebalance protocol: warm-up tasks are inserted by the group coordinator when it applies the
 * target assignment (so the member's {@link MemberAssignmentState#warmupTasks()}, {@link MemberAssignmentState#taskOffsets()}
 * and {@link MemberAssignmentState#taskEndOffsets()} are not read here), and rack-aware placement is tracked
 * separately.
 */
public class BalancedTaskAssignor implements TaskAssignor {

    private static final String BALANCED_ASSIGNOR_NAME = "balanced";
    private static final Logger log = LoggerFactory.getLogger(BalancedTaskAssignor.class);

    private static final Comparator<ProcessTasks> BY_ASSIGNED_LOAD =
        Comparator.comparingDouble(ProcessTasks::assignedTaskLoad).thenComparing(ProcessTasks::processId);
    private static final Comparator<ProcessTasks> BY_ACTIVE_LOAD =
        Comparator.comparingDouble(ProcessTasks::activeTaskLoad).thenComparing(ProcessTasks::processId);

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

        assignActiveStatefulTasks(processes.values(), statefulTasks);

        final int numStandbyReplicas = groupSpec.configs().numStandbyReplicas();
        if (numStandbyReplicas > 0) {
            assignStandbyReplicaTasks(processes.values(), statefulTasks, numStandbyReplicas);
        }

        assignStatelessActiveTasks(processes.values(), statelessTasks);

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
     * Deals the stateful tasks round-robin over the processes and then evens out the load. Iterating the tasks in
     * subtopology order spreads the tasks of each subtopology over the processes, which is what makes the assignment
     * balanced rather than merely even.
     */
    private static void assignActiveStatefulTasks(final Collection<ProcessTasks> processes,
                                                  final SortedSet<TaskId> statefulTasks) {
        Iterator<ProcessTasks> processIterator = null;
        for (final TaskId task : statefulTasks) {
            if (processIterator == null || !processIterator.hasNext()) {
                processIterator = processes.iterator();
            }
            processIterator.next().activeTasks.add(task);
        }

        balanceTasksOverProcesses(processes, process -> process.activeTasks);
    }

    private static void assignStandbyReplicaTasks(final Collection<ProcessTasks> processes,
                                                  final SortedSet<TaskId> statefulTasks,
                                                  final int numStandbyReplicas) {
        // The queue orders the processes by their load as of the time they were offered. A polled process is offered
        // again after its load changed, so the order stays valid.
        final PriorityQueue<ProcessTasks> processesByLoad = new PriorityQueue<>(BY_ASSIGNED_LOAD);
        processesByLoad.addAll(processes);

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
                log.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                        "There is not enough available capacity. You should increase the number of " +
                        "application instances to maintain the requested number of standby replicas.",
                    remainingReplicas, numStandbyReplicas, task);
            }
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
     * Stateless tasks carry no state to restore, so they are simply placed on the process with the lowest active task
     * load, which fills in any imbalance the stateful placement left behind.
     */
    private static void assignStatelessActiveTasks(final Collection<ProcessTasks> processes,
                                                   final SortedSet<TaskId> statelessTasks) {
        final PriorityQueue<ProcessTasks> processesByActiveLoad = new PriorityQueue<>(BY_ACTIVE_LOAD);
        processesByActiveLoad.addAll(processes);

        for (final TaskId task : statelessTasks) {
            final ProcessTasks process = processesByActiveLoad.poll();
            process.activeTasks.add(task);
            processesByActiveLoad.add(process);
        }
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

                    final SortedSet<TaskId> sourceTasks = tasksToBalance.apply(source);
                    final SortedSet<TaskId> destinationTasks = tasksToBalance.apply(destination);
                    // Iterate over a copy, since the moves below modify the source's tasks.
                    final Iterator<TaskId> sourceIterator = new ArrayList<>(sourceTasks).iterator();
                    while (shouldMoveATask(source, destination) && sourceIterator.hasNext()) {
                        final TaskId taskToMove = sourceIterator.next();
                        if (!destination.hasTask(taskToMove)) {
                            sourceTasks.remove(taskToMove);
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

    private static int computeTasksPerMember(final int numberOfTasks, final int numberOfMembers) {
        int tasksPerMember = numberOfTasks / numberOfMembers;
        if (numberOfTasks % numberOfMembers > 0) {
            tasksPerMember++;
        }
        return tasksPerMember;
    }

    /**
     * The tasks placed on one process, and the members the process contributes. The assignment across processes
     * works on this level; the tasks are spread over the members only once the placement is final.
     */
    private static final class ProcessTasks {
        private final String processId;
        private final TreeSet<String> memberIds = new TreeSet<>();
        private final SortedSet<TaskId> activeTasks = new TreeSet<>();
        private final SortedSet<TaskId> standbyTasks = new TreeSet<>();
        // The member of this process that currently owns a task, used to keep the task on that member if possible.
        private final Map<TaskId, String> currentActiveOwner = new HashMap<>();
        private final Map<TaskId, String> currentStandbyOwner = new HashMap<>();

        private ProcessTasks(final String processId) {
            this.processId = processId;
        }

        private String processId() {
            return processId;
        }

        private void addMember(final String memberId, final MemberAssignmentState currentAssignment) {
            memberIds.add(memberId);
            recordCurrentOwner(currentActiveOwner, memberId, currentAssignment.activeTasks());
            recordCurrentOwner(currentStandbyOwner, memberId, currentAssignment.standbyTasks());
        }

        private static void recordCurrentOwner(final Map<TaskId, String> currentOwner,
                                               final String memberId,
                                               final Map<String, Set<Integer>> tasks) {
            for (final Map.Entry<String, Set<Integer>> entry : tasks.entrySet()) {
                for (final int partition : entry.getValue()) {
                    // Two members of one process cannot own the same task; the smaller member ID wins if they do.
                    currentOwner.merge(new TaskId(entry.getKey(), partition), memberId,
                        (existing, candidate) -> existing.compareTo(candidate) <= 0 ? existing : candidate);
                }
            }
        }

        private int capacity() {
            return memberIds.size();
        }

        private int assignedTaskCount() {
            return activeTasks.size() + standbyTasks.size();
        }

        private double assignedTaskLoad() {
            return ((double) assignedTaskCount()) / capacity();
        }

        private double activeTaskLoad() {
            return ((double) activeTasks.size()) / capacity();
        }

        private boolean hasTask(final TaskId task) {
            return activeTasks.contains(task) || standbyTasks.contains(task);
        }

        /**
         * Spreads the process's tasks evenly over its members: first the active tasks, then the standby tasks on
         * top of them. A task stays on the member that currently owns it as long as that member is below its quota;
         * the remaining tasks go to the least loaded member.
         *
         * @return The assignment of every member of this process, including members that received no task.
         */
        private Map<String, MemberAssignment> distributeTasksOverMembers() {
            final Map<String, Integer> totalTaskCountByMember = new HashMap<>(capacity());
            final Map<String, Set<TaskId>> activeTasksByMember = new HashMap<>(capacity());
            final Map<String, Set<TaskId>> standbyTasksByMember = new HashMap<>(capacity());
            for (final String memberId : memberIds) {
                totalTaskCountByMember.put(memberId, 0);
                activeTasksByMember.put(memberId, new HashSet<>());
                standbyTasksByMember.put(memberId, new HashSet<>());
            }

            distributeTasksOverMembers(activeTasks, currentActiveOwner, totalTaskCountByMember,
                computeTasksPerMember(activeTasks.size(), capacity()), activeTasksByMember);
            distributeTasksOverMembers(standbyTasks, currentStandbyOwner, totalTaskCountByMember,
                computeTasksPerMember(assignedTaskCount(), capacity()), standbyTasksByMember);

            return memberIds.stream().collect(Collectors.toMap(
                Function.identity(),
                memberId -> new MemberAssignment(
                    toCompactedTaskIds(activeTasksByMember.get(memberId)),
                    toCompactedTaskIds(standbyTasksByMember.get(memberId))
                )
            ));
        }

        /**
         * Distributes the tasks of one role (active or standby) over the members.
         *
         * @param tasksToDistribute        The tasks of this role placed on the process.
         * @param currentOwner             The member of this process that currently owns a task, if any.
         * @param totalTaskCountByMember   The number of tasks of <em>both</em> roles each member holds so far; the
         *                                 quota and the least-loaded choice are based on this total, and it is
         *                                 updated as tasks are distributed.
         * @param quota                    The number of tasks a member may hold in total before it stops keeping
         *                                 its current tasks.
         * @param distributedTasksByMember The tasks of this role each member receives; the output of this method.
         */
        private void distributeTasksOverMembers(final SortedSet<TaskId> tasksToDistribute,
                                                final Map<TaskId, String> currentOwner,
                                                final Map<String, Integer> totalTaskCountByMember,
                                                final int quota,
                                                final Map<String, Set<TaskId>> distributedTasksByMember) {
            final List<TaskId> unassignedTasks = new ArrayList<>();
            for (final TaskId task : tasksToDistribute) {
                final String owner = currentOwner.get(task);
                if (owner != null && totalTaskCountByMember.get(owner) < quota) {
                    distributedTasksByMember.get(owner).add(task);
                    totalTaskCountByMember.merge(owner, 1, Integer::sum);
                } else {
                    unassignedTasks.add(task);
                }
            }

            final PriorityQueue<String> membersByLoad = new PriorityQueue<>(
                Comparator.<String>comparingInt(totalTaskCountByMember::get).thenComparing(Comparator.naturalOrder())
            );
            membersByLoad.addAll(memberIds);
            for (final TaskId task : unassignedTasks) {
                final String member = membersByLoad.poll();
                distributedTasksByMember.get(member).add(task);
                totalTaskCountByMember.merge(member, 1, Integer::sum);
                membersByLoad.add(member);
            }
        }
    }
}
