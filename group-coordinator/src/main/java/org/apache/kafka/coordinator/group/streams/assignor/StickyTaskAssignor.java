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
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignmentMetadata;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StickyTaskAssignor implements TaskAssignor {

    private static final String STICKY_ASSIGNOR_NAME = "sticky";
    private static final Logger log = LoggerFactory.getLogger(StickyTaskAssignor.class);

    // Ranks of a process among the previous holders of a task, used to break ties between equally rack-diverse processes.
    private static final int PREV_ACTIVE_HOLDER = 0;
    private static final int PREV_STANDBY_HOLDER = 1;
    private static final int NOT_PREV_HOLDER = 2;

    /**
     * Members that currently hold the task as a standby or warm-up rank ahead of members only known to hold state
     * through their reported offsets; within each group, the most caught-up state comes first.
     */
    private static final Comparator<StandbyCandidate> STANDBY_CANDIDATE_ORDER =
        Comparator.comparingInt((StandbyCandidate candidate) -> candidate.isPrevStandby() ? 0 : 1)
            .thenComparing(Comparator.comparingLong(StandbyCandidate::offsetSum).reversed());

    /**
     * Least loaded process first; among equally loaded processes, the one with the fewest stateless active tasks, so
     * that a stateless task fills up a process heavy on stateful ones. Written as one lambda to keep heap operations cheap.
     */
    private static final Comparator<ProcessState> PROCESS_BY_LOAD = (process1, process2) -> {
        final int byLoad = Double.compare(process1.load(), process2.load());
        return byLoad != 0 ? byLoad : Double.compare(process1.statelessActiveLoad(), process2.statelessActiveLoad());
    };

    @Override
    public String name() {
        return STICKY_ASSIGNOR_NAME;
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public GroupAssignment assign(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) throws TaskAssignorException {
        return doAssign(
            initialize(groupSpec, topologyDescriber),
            groupSpec
        );
    }

    private static GroupAssignment doAssign(
        final LocalState localState,
        final GroupSpec groupSpec
    ) {
        // Stateful and stateless active tasks are balanced independently: the stateful ones are placed first, then
        // the stateless ones fill up the remaining active capacity. The stateful pass must run before anything else
        // is assigned, because the stateful quota check reads a member's task count as its stateful active task count.
        // assignActive consumes its list, so the stateful tasks are copied for it and the original goes to assignStandby.
        assignActive(localState, new LinkedList<>(localState.statefulActiveTaskIds), true);
        assignActive(localState, localState.statelessActiveTaskIds, false);

        if (localState.numStandbyReplicas > 0) {
            assignStandby(localState, localState.statefulActiveTaskIds);
        }

        return buildGroupAssignment(localState, groupSpec.memberIds());
    }

    private static LocalState initialize(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) {
        final LocalState localState = new LocalState();
        localState.numStandbyReplicas = groupSpec.configs().numStandbyReplicas();
        localState.rackAwareAssignmentTags = groupSpec.configs().rackAwareAssignmentTags();

        // Helpers for computing stateful active tasks per member, active tasks per member, and tasks per member
        localState.totalStatefulActiveTasks = 0;
        localState.totalActiveTasks = 0;
        localState.totalTasks = 0;
        localState.statefulActiveTaskIds = new LinkedList<>();
        localState.statelessActiveTaskIds = new LinkedList<>();
        for (final String subtopology : topologyDescriber.subtopologies()) {
            final int numberOfPartitions = topologyDescriber.maxNumInputPartitions(subtopology);
            final boolean stateful = topologyDescriber.isStateful(subtopology);
            final LinkedList<TaskId> taskIds = stateful ? localState.statefulActiveTaskIds : localState.statelessActiveTaskIds;
            for (int i = 0; i < numberOfPartitions; i++) {
                taskIds.add(new TaskId(subtopology, i));
            }
            localState.totalTasks += numberOfPartitions;
            localState.totalActiveTasks += numberOfPartitions;
            if (stateful) {
                localState.totalStatefulActiveTasks += numberOfPartitions;
                localState.totalTasks += numberOfPartitions * localState.numStandbyReplicas;
            }
        }
        localState.totalMembersWithStatefulActiveTaskCapacity = groupSpec.memberIds().size();
        localState.totalMembersWithActiveTaskCapacity = groupSpec.memberIds().size();
        localState.totalMembersWithTaskCapacity = groupSpec.memberIds().size();
        localState.statefulActiveTasksPerMember = computeTasksPerMember(localState.totalStatefulActiveTasks, localState.totalMembersWithStatefulActiveTaskCapacity);
        localState.activeTasksPerMember = computeTasksPerMember(localState.totalActiveTasks, localState.totalMembersWithActiveTaskCapacity);
        localState.totalTasksPerMember = computeTasksPerMember(localState.totalTasks, localState.totalMembersWithTaskCapacity);

        localState.processIdToState = new HashMap<>(localState.totalMembersWithActiveTaskCapacity);
        localState.processIdToClientTags = new HashMap<>(localState.totalMembersWithActiveTaskCapacity);
        localState.activeTaskToPrevMember = new HashMap<>(localState.totalActiveTasks);

        // Standby-strength candidates per task, gathered in a single pass over the members and ranked below.
        final Map<TaskId, ArrayList<StandbyCandidate>> standbyCandidates = new HashMap<>();
        for (final String memberId : groupSpec.memberIds()) {
            final MemberAssignmentState memberAssignmentState = groupSpec.memberAssignmentState(memberId);
            final MemberAssignmentMetadata memberMetadata = groupSpec.memberMetadata(memberId);
            final String processId = memberMetadata.processId();
            final Member member = new Member(processId, memberId);

            localState.processIdToState.computeIfAbsent(processId, ProcessState::new)
                .addMember(memberId);
            // Client tags belong to the process, so every member of it reports the same ones.
            localState.processIdToClientTags.putIfAbsent(processId, memberMetadata.clientTags());

            // prev active tasks
            for (final Map.Entry<String, Set<Integer>> entry : memberAssignmentState.activeTasks().entrySet()) {
                final Set<Integer> partitionNoSet = entry.getValue();
                for (final int partitionNo : partitionNoSet) {
                    localState.activeTaskToPrevMember.put(new TaskId(entry.getKey(), partitionNo), member);
                }
            }

            collectStandbyCandidates(standbyCandidates, memberAssignmentState, member);
        }

        localState.standbyTaskToPrevMember = rankStandbyCandidates(standbyCandidates);
        return localState;
    }

    private static void collectStandbyCandidates(final Map<TaskId, ArrayList<StandbyCandidate>> standbyCandidates,
                                                 final MemberAssignmentState memberAssignmentState,
                                                 final Member member) {
        // prev standby tasks, carrying any reported offset sum so the most caught-up standby ranks first
        for (final Map.Entry<String, Set<Integer>> entry : memberAssignmentState.standbyTasks().entrySet()) {
            final String subtopologyId = entry.getKey();
            final Set<Integer> partitionNoSet = entry.getValue();
            for (final int partitionNo : partitionNoSet) {
                standbyCandidates
                    .computeIfAbsent(new TaskId(subtopologyId, partitionNo), task -> new ArrayList<>())
                    .add(new StandbyCandidate(member, true, reportedOffsetSum(memberAssignmentState, subtopologyId, partitionNo)));
            }
        }

        // prev warm-up tasks: a warm-up task is a member restoring the state of a task whose active task is being
        // migrated to it, so for stickiness it counts as a prev standby -- the member that has already restored the
        // state gets the task, instead of the restore work being thrown away. Its reported offset sum ranks it among
        // the standbys, most caught-up first.
        for (final Map.Entry<String, Set<Integer>> entry : memberAssignmentState.warmupTasks().entrySet()) {
            final String subtopologyId = entry.getKey();
            final Set<Integer> partitionNoSet = entry.getValue();
            for (final int partitionNo : partitionNoSet) {
                standbyCandidates
                    .computeIfAbsent(new TaskId(subtopologyId, partitionNo), task -> new ArrayList<>())
                    .add(new StandbyCandidate(member, true, reportedOffsetSum(memberAssignmentState, subtopologyId, partitionNo)));
            }
        }

        // A member that rejoins after a restart gets a fresh member ID and an empty target assignment, so the maps
        // above cannot capture what it owned before. Offsets reported for tasks with state on local disk make it a
        // weaker standby candidate, so stickiness can still hand those tasks back to the local state.
        for (final Map.Entry<String, Map<Integer, Long>> entry : memberAssignmentState.taskOffsets().entrySet()) {
            final String subtopologyId = entry.getKey();
            for (final Map.Entry<Integer, Long> partitionOffset : entry.getValue().entrySet()) {
                final int partitionNo = partitionOffset.getKey();
                if (isCurrentlyAssignedStandbyOrWarmupTask(memberAssignmentState, subtopologyId, partitionNo)) {
                    continue;
                }
                standbyCandidates
                    .computeIfAbsent(new TaskId(subtopologyId, partitionNo), task -> new ArrayList<>())
                    .add(new StandbyCandidate(member, false, partitionOffset.getValue()));
            }
        }
    }

    private static Map<TaskId, ArrayList<Member>> rankStandbyCandidates(final Map<TaskId, ArrayList<StandbyCandidate>> standbyCandidates) {
        final Map<TaskId, ArrayList<Member>> standbyTaskToPrevMember = new HashMap<>(standbyCandidates.size());
        standbyCandidates.forEach((taskId, candidates) -> {
            candidates.sort(STANDBY_CANDIDATE_ORDER);
            final ArrayList<Member> prevMembers = new ArrayList<>(candidates.size());
            for (final StandbyCandidate candidate : candidates) {
                prevMembers.add(candidate.member());
            }
            standbyTaskToPrevMember.put(taskId, prevMembers);
        });
        return standbyTaskToPrevMember;
    }

    /** Falls back to {@code 0} when no offset is reported, the conservative bound implying maximum lag. */
    private static long reportedOffsetSum(final MemberAssignmentState memberAssignmentState,
                                          final String subtopologyId,
                                          final int partitionNo) {
        return memberAssignmentState.taskOffsets()
            .getOrDefault(subtopologyId, Map.of())
            .getOrDefault(partitionNo, 0L);
    }

    private static boolean isCurrentlyAssignedStandbyOrWarmupTask(
        final MemberAssignmentState memberAssignmentState,
        final String subtopologyId,
        final int partitionNo
    ) {
        final Set<Integer> standbyPartitionNoSet = memberAssignmentState.standbyTasks().get(subtopologyId);
        if (standbyPartitionNoSet != null && standbyPartitionNoSet.contains(partitionNo)) {
            return true;
        }
        final Set<Integer> warmupPartitionNoSet = memberAssignmentState.warmupTasks().get(subtopologyId);
        return warmupPartitionNoSet != null && warmupPartitionNoSet.contains(partitionNo);
    }

    private static GroupAssignment buildGroupAssignment(final LocalState localState, final Collection<String> members) {
        final Map<String, MemberAssignment> memberAssignments = new HashMap<>();

        final Map<String, Set<TaskId>> activeTasksAssignments = localState.processIdToState.entrySet().stream()
            .flatMap(entry -> entry.getValue().assignedActiveTasksByMember().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (set1, set2) -> {
                set1.addAll(set2);
                return set1;
            }));

        final Map<String, Set<TaskId>> standbyTasksAssignments = localState.processIdToState.entrySet().stream()
            .flatMap(entry -> entry.getValue().assignedStandbyTasksByMember().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (set1, set2) -> {
                set1.addAll(set2);
                return set1;
            }));

        for (final String memberId : members) {
            final Map<String, Set<Integer>> activeTasks = new HashMap<>();
            if (activeTasksAssignments.containsKey(memberId)) {
                activeTasks.putAll(toCompactedTaskIds(activeTasksAssignments.get(memberId)));
            }
            final Map<String, Set<Integer>> standByTasks = new HashMap<>();

            if (standbyTasksAssignments.containsKey(memberId)) {
                standByTasks.putAll(toCompactedTaskIds(standbyTasksAssignments.get(memberId)));
            }
            memberAssignments.put(memberId, new MemberAssignment(activeTasks, standByTasks));
        }

        return new GroupAssignment(memberAssignments);
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
     * Assigns active tasks that are either all stateful or all stateless, as told by {@code stateful}; a previous
     * owner stays sticky only while it is below the quota of that flavor, so that each flavor spreads evenly on its own.
     * The stateful pass must run before anything else is assigned: it reads a member's task count as its stateful
     * active task count.
     */
    private static void assignActive(final LocalState localState, final LinkedList<TaskId> activeTasks, final boolean stateful) {

        // Assuming our current assignment pairs same partitions (range-based), we want to sort by partition first
        activeTasks.sort(Comparator.comparing(TaskId::partition).thenComparing(TaskId::subtopologyId));

        // 1. re-assigning existing active tasks to clients that previously had the same active tasks
        for (final Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final Member prevMember = localState.activeTaskToPrevMember.get(task);
            if (prevMember != null) {
                final ProcessState processState = localState.processIdToState.get(prevMember.processId);
                if (hasUnfulfilledActiveTaskQuota(localState, processState, prevMember, stateful)) {
                    addActiveTask(localState, processState, prevMember, task, stateful);
                    it.remove();
                }
            }
        }

        // 2. re-assigning tasks to clients that previously have seen the same task (as standby task)
        for (final Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final ArrayList<Member> prevMembers = localState.standbyTaskToPrevMember.get(task);
            final Member prevMember = findPrevMemberWithLeastLoad(localState, prevMembers, Optional.empty());
            if (prevMember != null) {
                final ProcessState processState = localState.processIdToState.get(prevMember.processId);
                if (hasUnfulfilledActiveTaskQuota(localState, processState, prevMember, stateful)) {
                    addActiveTask(localState, processState, prevMember, task, stateful);
                    it.remove();
                }
            }
        }

        // To achieve an initially range-based assignment, sort by subtopology
        activeTasks.sort(Comparator.comparing(TaskId::subtopologyId).thenComparing(TaskId::partition));

        // 3. assign any remaining unassigned tasks
        final PriorityQueue<ProcessState> processByLoad = new PriorityQueue<>(PROCESS_BY_LOAD);
        processByLoad.addAll(localState.processIdToState.values());
        for (final TaskId task: activeTasks) {
            final ProcessState processWithLeastLoad = processByLoad.poll();
            if (processWithLeastLoad == null) {
                throw new TaskAssignorException(String.format("No process available to assign active task %s.", task));
            }
            final int newTaskCount = processWithLeastLoad.addTaskToLeastLoadedMember(task, true, stateful);
            if (newTaskCount != -1) {
                // The stateful active task quota is only checked in steps 1 and 2, so it needs no update here.
                maybeUpdateActiveTasksPerMember(localState, newTaskCount);
                maybeUpdateTotalTasksPerMember(localState, newTaskCount);
            } else {
                throw new TaskAssignorException(String.format("No member available to assign active task %s.", task));
            }
            processByLoad.add(processWithLeastLoad); // Add it back to the queue after updating its state
        }
    }

    /** Assigns an active task to the given member and updates every quota the task counts towards. */
    private static void addActiveTask(
        final LocalState localState,
        final ProcessState processState,
        final Member member,
        final TaskId task,
        final boolean stateful
    ) {
        final int newTaskCount = processState.addTask(member.memberId, task, true, stateful);
        if (stateful) {
            // Nothing else is assigned yet, so the member's task count is its stateful active task count.
            maybeUpdateStatefulActiveTasksPerMember(localState, newTaskCount);
        }
        maybeUpdateActiveTasksPerMember(localState, newTaskCount);
        maybeUpdateTotalTasksPerMember(localState, newTaskCount);
    }

    private static void maybeUpdateStatefulActiveTasksPerMember(final LocalState localState, final int statefulActiveTasksNo) {
        if (statefulActiveTasksNo == localState.statefulActiveTasksPerMember) {
            localState.totalMembersWithStatefulActiveTaskCapacity--;
            localState.totalStatefulActiveTasks -= statefulActiveTasksNo;
            localState.statefulActiveTasksPerMember = computeTasksPerMember(localState.totalStatefulActiveTasks, localState.totalMembersWithStatefulActiveTaskCapacity);
        }
    }

    private static void maybeUpdateActiveTasksPerMember(final LocalState localState, final int activeTasksNo) {
        if (activeTasksNo == localState.activeTasksPerMember) {
            localState.totalMembersWithActiveTaskCapacity--;
            localState.totalActiveTasks -= activeTasksNo;
            localState.activeTasksPerMember = computeTasksPerMember(localState.totalActiveTasks, localState.totalMembersWithActiveTaskCapacity);
        }
    }

    private static void maybeUpdateTotalTasksPerMember(final LocalState localState, final int taskNo) {
        if (taskNo == localState.totalTasksPerMember) {
            localState.totalMembersWithTaskCapacity--;
            localState.totalTasks -= taskNo;
            localState.totalTasksPerMember = computeTasksPerMember(localState.totalTasks, localState.totalMembersWithTaskCapacity);
        }
    }

    private static boolean assignStandbyToMemberWithLeastLoad(
        final LocalState localState,
        final PriorityQueue<ProcessState> queue,
        final TaskId taskId
    ) {
        final ProcessState processWithLeastLoad = queue.poll();
        if (processWithLeastLoad == null) {
            return false;
        }
        boolean found = false;
        if (!processWithLeastLoad.hasTask(taskId)) {
            final int newTaskCount = processWithLeastLoad.addTaskToLeastLoadedMember(taskId, false, true);
            if (newTaskCount != -1) {
                found = true;
                maybeUpdateTotalTasksPerMember(localState, newTaskCount);
            }
        } else if (!queue.isEmpty()) {
            found = assignStandbyToMemberWithLeastLoad(localState, queue, taskId);
        }
        queue.add(processWithLeastLoad); // Add it back to the queue after updating its state
        return found;
    }

    /**
     * Finds the previous member with the least load for a given task.
     *
     * @param localState
     *        The state of the assignment in progress.
     * @param members
     *        The list of previous members owning the task.
     * @param standbyTaskId
     *        The taskId, to check if the previous member already has the task.
     *
     * @return Previous member with the least load that does not have the task, or null if no such member exists.
     */
    private static Member findPrevMemberWithLeastLoad(
        final LocalState localState,
        final ArrayList<Member> members,
        final Optional<TaskId> standbyTaskId
    ) {
        if (members == null || members.isEmpty()) {
            return null;
        }

        Member candidate = null;
        double candidateProcessLoad = Double.MAX_VALUE;
        double candidateMemberLoad = Double.MAX_VALUE;
        for (final Member member : members) {
            final ProcessState processState = localState.processIdToState.get(member.processId);
            // A process that already owns a standby task (either as active or standby) cannot take it again
            if (standbyTaskId.isPresent() && processState.hasTask(standbyTaskId.get())) {
                continue;
            }

            final double newProcessLoad = processState.load();
            final double newMemberLoad = processState.memberToTaskCounts().get(member.memberId);
            if (candidate == null || (newProcessLoad < candidateProcessLoad && newMemberLoad < candidateMemberLoad)) {
                candidateProcessLoad = newProcessLoad;
                candidateMemberLoad = newMemberLoad;
                candidate = member;
            }
        }

        return candidate;
    }

    private static boolean hasUnfulfilledActiveTaskQuota(
        final LocalState localState,
        final ProcessState process,
        final Member member,
        final boolean stateful
    ) {
        // During the stateful pass nothing else is assigned yet, so the member's task count is its stateful active task count.
        final int quota = stateful ? localState.statefulActiveTasksPerMember : localState.activeTasksPerMember;
        return process.memberToTaskCounts().get(member.memberId) < quota;
    }

    private static boolean hasUnfulfilledTaskQuota(
        final LocalState localState,
        final ProcessState process,
        final Member member
    ) {
        return process.memberToTaskCounts().get(member.memberId) < localState.totalTasksPerMember;
    }

    private static void assignStandby(final LocalState localState, final LinkedList<TaskId> standbyTasks) {
        final ArrayList<StandbyToAssign> toLeastLoaded = new ArrayList<>(standbyTasks.size() * localState.numStandbyReplicas);
        final RackAwareStandbyPicker<ProcessState> rackAwarePicker = localState.rackAwareAssignmentTags.isEmpty()
            ? null
            : new RackAwareStandbyPicker<>(
                localState.rackAwareAssignmentTags,
                localState.processIdToState.values(),
                process -> localState.processIdToClientTags.get(process.processId())
            );

        // Assuming our current assignment is range-based, we want to sort by partition first.
        standbyTasks.sort(Comparator.comparing(TaskId::partition).thenComparing(TaskId::subtopologyId).reversed());

        for (TaskId task : standbyTasks) {
            // Rack diversity ranks above stickiness for standbys: the rack-aware pick places every standby that can
            // still make the task more diverse, using stickiness only to break its ties. The rest go to the sticky pass.
            final int rackAwareStandbys = rackAwarePicker == null ? 0 : assignRackAwareStandbys(localState, rackAwarePicker, task);
            assignStickyStandbys(localState, task, rackAwareStandbys, toLeastLoaded);
        }

        // To achieve a range-based assignment, sort by subtopology
        toLeastLoaded.sort(Comparator.<StandbyToAssign, String>comparing(x -> x.taskId.subtopologyId())
            .thenComparing(x -> x.taskId.partition()).reversed());

        final PriorityQueue<ProcessState> processByLoad = new PriorityQueue<>(Comparator.comparingDouble(ProcessState::load));
        processByLoad.addAll(localState.processIdToState.values());
        for (final StandbyToAssign toAssign : toLeastLoaded) {
            for (int i = 0; i < toAssign.remainingReplicas; i++) {
                if (!assignStandbyToMemberWithLeastLoad(localState, processByLoad, toAssign.taskId)) {
                    log.warn("{} There is not enough available capacity. " +
                            "You should increase the number of threads and/or application instances to maintain the requested number of standby replicas.",
                        errorMessage(localState.numStandbyReplicas, i, toAssign.taskId));
                    break;
                }
            }
        }
    }

    /**
     * Assigns the standbys of {@code task} that still make it more rack-diverse to processes with room, each to the
     * least-loaded member, and returns how many were placed. Equally diverse processes are ordered by holding the
     * task before as active, then as standby, then by load.
     */
    private static int assignRackAwareStandbys(
        final LocalState localState,
        final RackAwareStandbyPicker<ProcessState> picker,
        final TaskId task
    ) {
        picker.startTask();
        // The active owner is the only holder of the task so far.
        for (final ProcessState process : localState.processIdToState.values()) {
            if (process.hasTask(task)) {
                picker.markUsed(process);
            }
        }

        final Map<String, Integer> prevHolderRanks = prevHolderRanks(localState, task);
        final Predicate<ProcessState> eligible = process -> !process.hasTask(task) && hasRoom(localState, process);
        final Comparator<ProcessState> tieBreak = (process1, process2) -> {
            final int byPrevHolder = Integer.compare(
                prevHolderRanks.getOrDefault(process1.processId(), NOT_PREV_HOLDER),
                prevHolderRanks.getOrDefault(process2.processId(), NOT_PREV_HOLDER)
            );
            return byPrevHolder != 0 ? byPrevHolder : Double.compare(process1.load(), process2.load());
        };

        int placed = 0;
        while (placed < localState.numStandbyReplicas) {
            final ProcessState winner = picker.pickNext(eligible, tieBreak);
            if (winner == null) {
                break;
            }
            final int newTaskCount = winner.addTaskToLeastLoadedMember(task, false, true);
            maybeUpdateTotalTasksPerMember(localState, newTaskCount);
            picker.markUsed(winner);
            placed++;
        }
        return placed;
    }

    /**
     * Ranks the processes whose members held {@code task} before this assignment: the previous active member's process
     * as {@link #PREV_ACTIVE_HOLDER}, the previous standby members' processes as {@link #PREV_STANDBY_HOLDER}.
     */
    private static Map<String, Integer> prevHolderRanks(final LocalState localState, final TaskId task) {
        final Map<String, Integer> prevHolderRanks = new HashMap<>();
        final ArrayList<Member> prevStandbyMembers = localState.standbyTaskToPrevMember.get(task);
        if (prevStandbyMembers != null) {
            for (final Member prevStandbyMember : prevStandbyMembers) {
                prevHolderRanks.put(prevStandbyMember.processId, PREV_STANDBY_HOLDER);
            }
        }
        // Put last: a process that held the active task outranks one that held a standby.
        final Member prevActiveMember = localState.activeTaskToPrevMember.get(task);
        if (prevActiveMember != null) {
            prevHolderRanks.put(prevActiveMember.processId, PREV_ACTIVE_HOLDER);
        }
        return prevHolderRanks;
    }

    /**
     * Assigns the standbys of {@code task} from replica {@code firstReplica} on to the previous active member, then to
     * the least-loaded previous standby member, each while it is below the quota; the rest are recorded in
     * {@code toLeastLoaded}.
     */
    private static void assignStickyStandbys(
        final LocalState localState,
        final TaskId task,
        final int firstReplica,
        final ArrayList<StandbyToAssign> toLeastLoaded
    ) {
        for (int i = firstReplica; i < localState.numStandbyReplicas; i++) {

            // prev active task
            final Member prevActiveMember = localState.activeTaskToPrevMember.get(task);
            if (prevActiveMember != null) {
                final ProcessState prevActiveMemberProcessState = localState.processIdToState.get(prevActiveMember.processId);
                if (!prevActiveMemberProcessState.hasTask(task) && hasUnfulfilledTaskQuota(localState, prevActiveMemberProcessState, prevActiveMember)) {
                    int newTaskCount = prevActiveMemberProcessState.addTask(prevActiveMember.memberId, task, false, true);
                    maybeUpdateTotalTasksPerMember(localState, newTaskCount);
                    continue;
                }
            }

            // prev standby tasks
            final ArrayList<Member> prevStandbyMembers = localState.standbyTaskToPrevMember.get(task);
            if (prevStandbyMembers != null && !prevStandbyMembers.isEmpty()) {
                final Member prevStandbyMember = findPrevMemberWithLeastLoad(localState, prevStandbyMembers, Optional.of(task));
                if (prevStandbyMember != null) {
                    final ProcessState prevStandbyMemberProcessState = localState.processIdToState.get(prevStandbyMember.processId);
                    if (hasUnfulfilledTaskQuota(localState, prevStandbyMemberProcessState, prevStandbyMember)) {
                        int newTaskCount = prevStandbyMemberProcessState.addTask(prevStandbyMember.memberId, task, false, true);
                        maybeUpdateTotalTasksPerMember(localState, newTaskCount);
                        continue;
                    }
                }
            }

            toLeastLoaded.add(new StandbyToAssign(task, localState.numStandbyReplicas - i));
            return;
        }
    }

    /** A process has room while one of its members is below the per-member quota on active plus standby tasks. */
    private static boolean hasRoom(final LocalState localState, final ProcessState process) {
        for (final int taskCount : process.memberToTaskCounts().values()) {
            if (taskCount < localState.totalTasksPerMember) {
                return true;
            }
        }
        return false;
    }

    private static String errorMessage(final int numStandbyReplicas, final int i, final TaskId task) {
        return "Unable to assign " + (numStandbyReplicas - i) +
            " of " + numStandbyReplicas + " standby tasks for task [" + task + "].";
    }

    private static int computeTasksPerMember(final int numberOfTasks, final int numberOfMembers) {
        if (numberOfMembers == 0) {
            return 0;
        }
        int tasksPerMember = numberOfTasks / numberOfMembers;
        if (numberOfTasks % numberOfMembers > 0) {
            tasksPerMember++;
        }
        return tasksPerMember;
    }

    static class StandbyToAssign {
        private final TaskId taskId;
        private final int remainingReplicas;

        public StandbyToAssign(final TaskId taskId, final int remainingReplicas) {
            this.taskId = taskId;
            this.remainingReplicas = remainingReplicas;
        }
    }

    static class Member {
        private final String processId;
        private final String memberId;

        public Member(final String processId, final String memberId) {
            this.processId = processId;
            this.memberId = memberId;
        }
    }

    private record StandbyCandidate(Member member, boolean isPrevStandby, long offsetSum) {
    }

    private static class LocalState {
        // helper data structures:
        Map<TaskId, Member> activeTaskToPrevMember;
        Map<TaskId, ArrayList<Member>> standbyTaskToPrevMember;
        Map<String, ProcessState> processIdToState;
        Map<String, Map<String, String>> processIdToClientTags;
        LinkedList<TaskId> statefulActiveTaskIds;
        LinkedList<TaskId> statelessActiveTaskIds;

        int numStandbyReplicas;
        List<String> rackAwareAssignmentTags;
        int totalStatefulActiveTasks;
        int totalActiveTasks;
        int totalTasks;
        int totalMembersWithStatefulActiveTaskCapacity;
        int totalMembersWithActiveTaskCapacity;
        int totalMembersWithTaskCapacity;
        int statefulActiveTasksPerMember;
        int activeTasksPerMember;
        int totalTasksPerMember;
    }
}
