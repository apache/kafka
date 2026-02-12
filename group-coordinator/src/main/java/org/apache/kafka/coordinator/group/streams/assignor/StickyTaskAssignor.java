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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

public class StickyTaskAssignor implements TaskAssignor {

    private static final String STICKY_ASSIGNOR_NAME = "sticky";
    private static final Logger log = LoggerFactory.getLogger(StickyTaskAssignor.class);

    private LocalState localState;


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
        initialize(groupSpec, topologyDescriber);
        final GroupAssignment assignments =  doAssign(groupSpec, topologyDescriber);
        localState = null;
        return assignments;
    }

    private GroupAssignment doAssign(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) {
        final LinkedList<TaskId> activeTasks = taskIds(topologyDescriber, true);
        assignActive(activeTasks);

        if (localState.numStandbyReplicas > 0) {
            final LinkedList<TaskId> statefulTasks = taskIds(topologyDescriber, false);
            assignStandby(statefulTasks);
        }

        return buildGroupAssignment(groupSpec.members().keySet());
    }

    private LinkedList<TaskId> taskIds(final TopologyDescriber topologyDescriber, final boolean isActive) {
        final LinkedList<TaskId> ret = new LinkedList<>();
        for (final String subtopology : topologyDescriber.subtopologies()) {
            if (isActive || topologyDescriber.isStateful(subtopology)) {
                final int numberOfPartitions = topologyDescriber.maxNumInputPartitions(subtopology);
                for (int i = 0; i < numberOfPartitions; i++) {
                    ret.add(new TaskId(subtopology, i));
                }
            }
        }
        return ret;
    }

    private void initialize(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) {
        localState = new LocalState();
        localState.numStandbyReplicas =
            groupSpec.assignmentConfigs().isEmpty() ? 0
                : Integer.parseInt(groupSpec.assignmentConfigs().get("num.standby.replicas"));

        // Helpers for computing active tasks per member, and tasks per member
        localState.totalActiveTasks = 0;
        localState.totalTasks = 0;
        for (final String subtopology : topologyDescriber.subtopologies()) {
            final int numberOfPartitions = topologyDescriber.maxNumInputPartitions(subtopology);
            localState.totalTasks += numberOfPartitions;
            localState.totalActiveTasks += numberOfPartitions;
            if (topologyDescriber.isStateful(subtopology))
                localState.totalTasks += numberOfPartitions * localState.numStandbyReplicas;
        }
        localState.totalMembersWithActiveTaskCapacity = groupSpec.members().size();
        localState.totalMembersWithTaskCapacity = groupSpec.members().size();
        localState.activeTasksPerMember = computeTasksPerMember(localState.totalActiveTasks, localState.totalMembersWithActiveTaskCapacity);
        localState.totalTasksPerMember = computeTasksPerMember(localState.totalTasks, localState.totalMembersWithTaskCapacity);

        localState.processIdToState = new HashMap<>(localState.totalMembersWithActiveTaskCapacity);
        localState.activeTaskToPrevMember = new HashMap<>(localState.totalActiveTasks);
        localState.standbyTaskToPrevMember = new HashMap<>(localState.numStandbyReplicas > 0 ? (localState.totalTasks - localState.totalActiveTasks) / localState.numStandbyReplicas : 0);
        for (final Map.Entry<String, AssignmentMemberSpec> memberEntry : groupSpec.members().entrySet()) {
            final String memberId = memberEntry.getKey();
            final String processId = memberEntry.getValue().processId();
            final Member member = new Member(processId, memberId);
            final AssignmentMemberSpec memberSpec = memberEntry.getValue();

            localState.processIdToState.putIfAbsent(processId, new ProcessState(processId));
            localState.processIdToState.get(processId).addMember(memberId);

            // prev active tasks
            for (final Map.Entry<String, Set<Integer>> entry : memberSpec.activeTasks().entrySet()) {
                final Set<Integer> partitionNoSet = entry.getValue();
                for (final int partitionNo : partitionNoSet) {
                    localState.activeTaskToPrevMember.put(new TaskId(entry.getKey(), partitionNo), member);
                }
            }

            // prev standby tasks
            for (final Map.Entry<String, Set<Integer>> entry : memberSpec.standbyTasks().entrySet()) {
                final Set<Integer> partitionNoSet = entry.getValue();
                for (final int partitionNo : partitionNoSet) {
                    final TaskId taskId = new TaskId(entry.getKey(), partitionNo);
                    localState.standbyTaskToPrevMember.putIfAbsent(taskId, new ArrayList<>(localState.numStandbyReplicas));
                    localState.standbyTaskToPrevMember.get(taskId).add(member);
                }
            }
        }
    }

    private GroupAssignment buildGroupAssignment(final Set<String> members) {
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
            memberAssignments.put(memberId, new MemberAssignment(activeTasks, standByTasks, new HashMap<>()));
        }

        return new GroupAssignment(memberAssignments);
    }

    private Map<String, Set<Integer>> toCompactedTaskIds(final Set<TaskId> taskIds) {
        final Map<String, Set<Integer>> ret = new HashMap<>();
        for (final TaskId taskId : taskIds) {
            ret.putIfAbsent(taskId.subtopologyId(), new HashSet<>());
            ret.get(taskId.subtopologyId()).add(taskId.partition());
        }
        return ret;
    }

    private void assignActive(final LinkedList<TaskId> activeTasks) {

        // Assuming our current assignment pairs same partitions (range-based), we want to sort by partition first
        activeTasks.sort(Comparator.comparing(TaskId::partition).thenComparing(TaskId::subtopologyId));

        // 1. re-assigning existing active tasks to clients that previously had the same active tasks
        for (final Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final Member prevMember = localState.activeTaskToPrevMember.get(task);
            if (prevMember != null) {
                final ProcessState processState = localState.processIdToState.get(prevMember.processId);
                if (hasUnfulfilledActiveTaskQuota(processState, prevMember)) {
                    int newActiveTasks = processState.addTask(prevMember.memberId, task, true);
                    maybeUpdateActiveTasksPerMember(newActiveTasks);
                    maybeUpdateTotalTasksPerMember(newActiveTasks);
                    it.remove();
                }
            }
        }

        // 2. re-assigning tasks to clients that previously have seen the same task (as standby task)
        for (final Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final ArrayList<Member> prevMembers = localState.standbyTaskToPrevMember.get(task);
            final Member prevMember = findPrevMemberWithLeastLoad(prevMembers, null);
            if (prevMember != null) {
                final ProcessState processState = localState.processIdToState.get(prevMember.processId);
                if (hasUnfulfilledActiveTaskQuota(processState, prevMember)) {
                    int newActiveTasks = processState.addTask(prevMember.memberId, task, true);
                    maybeUpdateActiveTasksPerMember(newActiveTasks);
                    maybeUpdateTotalTasksPerMember(newActiveTasks);
                    it.remove();
                }
            }
        }

        // To achieve an initially range-based assignment, sort by subtopology
        activeTasks.sort(Comparator.comparing(TaskId::subtopologyId).thenComparing(TaskId::partition));

        // 3. assign any remaining unassigned tasks
        final PriorityQueue<ProcessState> processByLoad = new PriorityQueue<>(Comparator.comparingDouble(ProcessState::load));
        processByLoad.addAll(localState.processIdToState.values());
        for (final TaskId task: activeTasks) {
            final ProcessState processWithLeastLoad = processByLoad.poll();
            if (processWithLeastLoad == null) {
                throw new TaskAssignorException(String.format("No process available to assign active task %s.", task));
            }
            final int newTaskCount = processWithLeastLoad.addTaskToLeastLoadedMember(task, true);
            if (newTaskCount != -1) {
                maybeUpdateActiveTasksPerMember(newTaskCount);
                maybeUpdateTotalTasksPerMember(newTaskCount);
            } else {
                throw new TaskAssignorException(String.format("No member available to assign active task %s.", task));
            }
            processByLoad.add(processWithLeastLoad); // Add it back to the queue after updating its state
        }
    }

    private void maybeUpdateActiveTasksPerMember(final int activeTasksNo) {
        if (activeTasksNo == localState.activeTasksPerMember) {
            localState.totalMembersWithActiveTaskCapacity--;
            localState.totalActiveTasks -= activeTasksNo;
            localState.activeTasksPerMember = computeTasksPerMember(localState.totalActiveTasks, localState.totalMembersWithActiveTaskCapacity);
        }
    }

    private void maybeUpdateTotalTasksPerMember(final int taskNo) {
        if (taskNo == localState.totalTasksPerMember) {
            localState.totalMembersWithTaskCapacity--;
            localState.totalTasks -= taskNo;
            localState.totalTasksPerMember = computeTasksPerMember(localState.totalTasks, localState.totalMembersWithTaskCapacity);
        }
    }

    private boolean assignStandbyToMemberWithLeastLoad(PriorityQueue<ProcessState> queue, TaskId taskId) {
        final ProcessState processWithLeastLoad = queue.poll();
        if (processWithLeastLoad == null) {
            return false;
        }
        boolean found = false;
        if (!processWithLeastLoad.hasTask(taskId)) {
            final int newTaskCount = processWithLeastLoad.addTaskToLeastLoadedMember(taskId, false);
            if (newTaskCount != -1) {
                found = true;
                maybeUpdateTotalTasksPerMember(newTaskCount);
            }
        } else if (!queue.isEmpty()) {
            found = assignStandbyToMemberWithLeastLoad(queue, taskId);
        }
        queue.add(processWithLeastLoad); // Add it back to the queue after updating its state
        return found;
    }

    /**
     * Finds the previous member with the least load for a given task.
     *
     * @param members The list of previous members owning the task.
     * @param taskId  The taskId, to check if the previous member already has the task. Can be null, if we assign it
     *                for the first time (e.g., during active task assignment).
     *
     * @return Previous member with the least load that does not have the task, or null if no such member exists.
     */
    private Member findPrevMemberWithLeastLoad(final ArrayList<Member> members, final TaskId taskId) {
        if (members == null || members.isEmpty()) {
            return null;
        }

        Member candidate = members.get(0);
        final ProcessState candidateProcessState = localState.processIdToState.get(candidate.processId);
        double candidateProcessLoad = candidateProcessState.load();
        double candidateMemberLoad = candidateProcessState.memberToTaskCounts().get(candidate.memberId);
        for (int i = 1; i < members.size(); i++) {
            final Member member = members.get(i);
            final ProcessState processState = localState.processIdToState.get(member.processId);
            final double newProcessLoad = processState.load();
            if (newProcessLoad < candidateProcessLoad && (taskId == null || !processState.hasTask(taskId))) {
                final double newMemberLoad = processState.memberToTaskCounts().get(member.memberId);
                if (newMemberLoad < candidateMemberLoad) {
                    candidateProcessLoad = newProcessLoad;
                    candidateMemberLoad = newMemberLoad;
                    candidate = member;
                }
            }
        }

        if (taskId == null || !candidateProcessState.hasTask(taskId)) {
            return candidate;
        }
        return null;
    }

    private boolean hasUnfulfilledActiveTaskQuota(final ProcessState process, final Member member) {
        return process.memberToTaskCounts().get(member.memberId) < localState.activeTasksPerMember;
    }

    private boolean hasUnfulfilledTaskQuota(final ProcessState process, final Member member) {
        return process.memberToTaskCounts().get(member.memberId) < localState.totalTasksPerMember;
    }

    private void assignStandby(final LinkedList<TaskId> standbyTasks) {
        final ArrayList<StandbyToAssign> toLeastLoaded = new ArrayList<>(standbyTasks.size() * localState.numStandbyReplicas);
        
        // Assuming our current assignment is range-based, we want to sort by partition first.
        standbyTasks.sort(Comparator.comparing(TaskId::partition).thenComparing(TaskId::subtopologyId).reversed());

        for (TaskId task : standbyTasks) {
            for (int i = 0; i < localState.numStandbyReplicas; i++) {

                // prev active task
                final Member prevActiveMember = localState.activeTaskToPrevMember.get(task);
                if (prevActiveMember != null) {
                    final ProcessState prevActiveMemberProcessState = localState.processIdToState.get(prevActiveMember.processId);
                    if (!prevActiveMemberProcessState.hasTask(task) && hasUnfulfilledTaskQuota(prevActiveMemberProcessState, prevActiveMember)) {
                        int newTaskCount = prevActiveMemberProcessState.addTask(prevActiveMember.memberId, task, false);
                        maybeUpdateTotalTasksPerMember(newTaskCount);
                        continue;
                    }
                }

                // prev standby tasks
                final ArrayList<Member> prevStandbyMembers = localState.standbyTaskToPrevMember.get(task);
                if (prevStandbyMembers != null && !prevStandbyMembers.isEmpty()) {
                    final Member prevStandbyMember = findPrevMemberWithLeastLoad(prevStandbyMembers, task);
                    if (prevStandbyMember != null) {
                        final ProcessState prevStandbyMemberProcessState = localState.processIdToState.get(prevStandbyMember.processId);
                        if (hasUnfulfilledTaskQuota(prevStandbyMemberProcessState, prevStandbyMember)) {
                            int newTaskCount = prevStandbyMemberProcessState.addTask(prevStandbyMember.memberId, task, false);
                            maybeUpdateTotalTasksPerMember(newTaskCount);
                            continue;
                        }
                    }
                }

                toLeastLoaded.add(new StandbyToAssign(task, localState.numStandbyReplicas - i));
                break;
            }
        }

        // To achieve a range-based assignment, sort by subtopology
        toLeastLoaded.sort(Comparator.<StandbyToAssign, String>comparing(x -> x.taskId.subtopologyId())
            .thenComparing(x -> x.taskId.partition()).reversed());

        final PriorityQueue<ProcessState> processByLoad = new PriorityQueue<>(Comparator.comparingDouble(ProcessState::load));
        processByLoad.addAll(localState.processIdToState.values());
        for (final StandbyToAssign toAssign : toLeastLoaded) {
            for (int i = 0; i < toAssign.remainingReplicas; i++) {
                if (!assignStandbyToMemberWithLeastLoad(processByLoad, toAssign.taskId)) {
                    log.warn("{} There is not enough available capacity. " +
                            "You should increase the number of threads and/or application instances to maintain the requested number of standby replicas.",
                        errorMessage(localState.numStandbyReplicas, i, toAssign.taskId));
                    break;
                }
            }
        }
    }

    private String errorMessage(final int numStandbyReplicas, final int i, final TaskId task) {
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

    private static class LocalState {
        // helper data structures:
        Map<TaskId, Member> activeTaskToPrevMember;
        Map<TaskId, ArrayList<Member>> standbyTaskToPrevMember;
        Map<String, ProcessState> processIdToState;

        int numStandbyReplicas;
        int totalActiveTasks;
        int totalTasks;
        int totalMembersWithActiveTaskCapacity;
        int totalMembersWithTaskCapacity;
        int activeTasksPerMember;
        int totalTasksPerMember;
    }
}
