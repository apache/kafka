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
    public GroupAssignment assign(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) throws TaskAssignorException {
        initialize(groupSpec, topologyDescriber);
        GroupAssignment assignments =  doAssign(groupSpec, topologyDescriber);
        localState = null;
        return assignments;
    }

    private GroupAssignment doAssign(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) {
        //active
        Set<TaskId> activeTasks = taskIds(topologyDescriber, true);
        assignActive(activeTasks);

        //standby
        if (localState.numStandbyReplicas > 0) {
            Set<TaskId> statefulTasks = taskIds(topologyDescriber, false);
            assignStandby(statefulTasks);
        }

        return buildGroupAssignment(groupSpec.members().keySet());
    }

    private Set<TaskId> taskIds(final TopologyDescriber topologyDescriber, final boolean isActive) {
        Set<TaskId> ret = new HashSet<>();
        for (String subtopology : topologyDescriber.subtopologies()) {
            if (isActive || topologyDescriber.isStateful(subtopology)) {
                int numberOfPartitions = topologyDescriber.maxNumInputPartitions(subtopology);
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
        for (String subtopology : topologyDescriber.subtopologies()) {
            int numberOfPartitions = topologyDescriber.maxNumInputPartitions(subtopology);
            localState.totalTasks += numberOfPartitions;
            localState.totalActiveTasks += numberOfPartitions;
            if (topologyDescriber.isStateful(subtopology))
                localState.totalTasks += numberOfPartitions * localState.numStandbyReplicas;
        }
        localState.totalMembersWithActiveTaskCapacity = groupSpec.members().size();
        localState.totalMembersWithTaskCapacity = groupSpec.members().size();
        localState.activeTasksPerMember = computeTasksPerMember(localState.totalActiveTasks, localState.totalMembersWithActiveTaskCapacity);
        localState.tasksPerMember = computeTasksPerMember(localState.totalTasks, localState.totalMembersWithActiveTaskCapacity);

        localState.processIdToState = new HashMap<>(localState.totalMembersWithActiveTaskCapacity);
        localState.activeTaskToPrevMember = new HashMap<>(localState.totalActiveTasks);
        localState.standbyTaskToPrevMember = new HashMap<>(localState.numStandbyReplicas > 0 ? (localState.totalTasks - localState.totalActiveTasks) / localState.numStandbyReplicas : 0);
        for (Map.Entry<String, AssignmentMemberSpec> memberEntry : groupSpec.members().entrySet()) {
            final String memberId = memberEntry.getKey();
            final String processId = memberEntry.getValue().processId();
            final Member member = new Member(processId, memberId);
            final AssignmentMemberSpec memberSpec = memberEntry.getValue();

            localState.processIdToState.putIfAbsent(processId, new ProcessState(processId));
            localState.processIdToState.get(processId).addMember(memberId);

            // prev active tasks
            for (Map.Entry<String, Set<Integer>> entry : memberSpec.activeTasks().entrySet()) {
                Set<Integer> partitionNoSet = entry.getValue();
                for (int partitionNo : partitionNoSet) {
                    localState.activeTaskToPrevMember.put(new TaskId(entry.getKey(), partitionNo), member);
                }
            }

            // prev standby tasks
            for (Map.Entry<String, Set<Integer>> entry : memberSpec.standbyTasks().entrySet()) {
                Set<Integer> partitionNoSet = entry.getValue();
                for (int partitionNo : partitionNoSet) {
                    TaskId taskId = new TaskId(entry.getKey(), partitionNo);
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

        for (String memberId : members) {
            Map<String, Set<Integer>> activeTasks = new HashMap<>();
            if (activeTasksAssignments.containsKey(memberId)) {
                activeTasks = toCompactedTaskIds(activeTasksAssignments.get(memberId));
            }
            Map<String, Set<Integer>> standByTasks = new HashMap<>();

            if (standbyTasksAssignments.containsKey(memberId)) {
                standByTasks = toCompactedTaskIds(standbyTasksAssignments.get(memberId));
            }
            memberAssignments.put(memberId, new MemberAssignment(activeTasks, standByTasks, new HashMap<>()));
        }

        return new GroupAssignment(memberAssignments);
    }

    private Map<String, Set<Integer>> toCompactedTaskIds(final Set<TaskId> taskIds) {
        Map<String, Set<Integer>> ret = new HashMap<>();
        for (TaskId taskId : taskIds) {
            ret.putIfAbsent(taskId.subtopologyId(), new HashSet<>());
            ret.get(taskId.subtopologyId()).add(taskId.partition());
        }
        return ret;
    }

    private void assignActive(final Set<TaskId> activeTasks) {

        // 1. re-assigning existing active tasks to clients that previously had the same active tasks
        for (Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final Member prevMember = localState.activeTaskToPrevMember.get(task);
            if (prevMember != null) {
                ProcessState processState = localState.processIdToState.get(prevMember.processId);
                if (hasUnfulfilledActiveTaskQuota(processState, prevMember)) {
                    processState.addTask(prevMember.memberId, task, true);
                    maybeUpdateActiveTasksPerMember(processState.memberToTaskCounts().get(prevMember.memberId));
                    it.remove();
                }
            }
        }

        // 2. re-assigning tasks to clients that previously have seen the same task (as standby task)
        for (Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final ArrayList<Member> prevMembers = localState.standbyTaskToPrevMember.get(task);
            final Member prevMember = findPrevMemberWithLeastLoad(prevMembers, null);
            if (prevMember != null) {
                ProcessState processState = localState.processIdToState.get(prevMember.processId);
                if (hasUnfulfilledActiveTaskQuota(processState, prevMember)) {
                    processState.addTask(prevMember.memberId, task, true);
                    maybeUpdateActiveTasksPerMember(processState.memberToTaskCounts().get(prevMember.memberId));
                    it.remove();
                }
            }
        }

        // 3. assign any remaining unassigned tasks
        PriorityQueue<ProcessState> processByLoad = new PriorityQueue<>(Comparator.comparingDouble(ProcessState::load));
        processByLoad.addAll(localState.processIdToState.values());
        for (TaskId task: activeTasks) {
            ProcessState processWithLeastLoad = processByLoad.poll();
            if (processWithLeastLoad == null) {
                throw new TaskAssignorException("No process available to assign active task {}." + task);
            }
            int newTaskCount = processWithLeastLoad.addTaskToLeastLoadedMember(task, true);
            if (newTaskCount != -1) {
                maybeUpdateActiveTasksPerMember(newTaskCount);
            } else {
                throw new TaskAssignorException("No member available to assign active task {}." + task);
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

    private void maybeUpdateTasksPerMember(final int taskNo) {
        if (taskNo == localState.tasksPerMember) {
            localState.totalMembersWithTaskCapacity--;
            localState.totalTasks -= taskNo;
            localState.activeTasksPerMember = computeTasksPerMember(localState.totalActiveTasks, localState.totalMembersWithActiveTaskCapacity);
        }
    }

    private boolean assignStandbyToMemberWithLeastLoad(PriorityQueue<ProcessState> queue, TaskId taskId) {
        ProcessState processWithLeastLoad = queue.poll();
        if (processWithLeastLoad == null) {
            return false;
        }
        boolean found = false;
        if (!processWithLeastLoad.hasTask(taskId)) {
            int newTaskCount = processWithLeastLoad.addTaskToLeastLoadedMember(taskId, false);
            if (newTaskCount != -1) {
                found = true;
                maybeUpdateTasksPerMember(newTaskCount);
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
     * @return Previous member with the least load that deoes not have the task, or null if no such member exists.
     */
    private Member findPrevMemberWithLeastLoad(final ArrayList<Member> members, final TaskId taskId) {
        if (members == null || members.isEmpty()) {
            return null;
        }

        Member candidate = members.get(0);
        ProcessState candidateProcessState = localState.processIdToState.get(candidate.processId);
        double candidateProcessLoad = candidateProcessState.load();
        double candidateMemberLoad = candidateProcessState.memberToTaskCounts().get(candidate.memberId);
        for (int i = 1; i < members.size(); i++) {
            Member member = members.get(i);
            ProcessState processState = localState.processIdToState.get(member.processId);
            double newProcessLoad = processState.load();
            if (newProcessLoad < candidateProcessLoad && (taskId == null || !processState.hasTask(taskId))) {
                double newMemberLoad = processState.memberToTaskCounts().get(member.memberId);
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
        return process.memberToTaskCounts().get(member.memberId) < localState.tasksPerMember;
    }

    private void assignStandby(final Set<TaskId> standbyTasks) {
        ArrayList<StandbyToAssign> toLeastLoaded = new ArrayList<>(standbyTasks.size() * localState.numStandbyReplicas);
        for (TaskId task : standbyTasks) {
            for (int i = 0; i < localState.numStandbyReplicas; i++) {

                // prev active task
                Member prevMember = localState.activeTaskToPrevMember.get(task);
                if (prevMember != null) {
                    ProcessState prevMemberProcessState = localState.processIdToState.get(prevMember.processId);
                    if (!prevMemberProcessState.hasTask(task) && hasUnfulfilledTaskQuota(prevMemberProcessState, prevMember)) {
                        prevMemberProcessState.addTask(prevMember.memberId, task, false);
                        maybeUpdateTasksPerMember(prevMemberProcessState.memberToTaskCounts().get(prevMember.memberId));
                        continue;
                    }
                }

                // prev standby tasks
                final ArrayList<Member> prevMembers = localState.standbyTaskToPrevMember.get(task);
                if (prevMembers != null && !prevMembers.isEmpty()) {
                    prevMember = findPrevMemberWithLeastLoad(prevMembers, task);
                    if (prevMember != null) {
                        ProcessState prevMemberProcessState = localState.processIdToState.get(prevMember.processId);
                        if (hasUnfulfilledTaskQuota(prevMemberProcessState, prevMember)) {
                            prevMemberProcessState.addTask(prevMember.memberId, task, false);
                            maybeUpdateTasksPerMember(prevMemberProcessState.memberToTaskCounts().get(prevMember.memberId));
                            continue;
                        }
                    }
                }

                toLeastLoaded.add(new StandbyToAssign(task, localState.numStandbyReplicas - i));
                break;
            }
        }

        PriorityQueue<ProcessState> processByLoad = new PriorityQueue<>(Comparator.comparingDouble(ProcessState::load));
        processByLoad.addAll(localState.processIdToState.values());
        for (StandbyToAssign toAssign : toLeastLoaded) {
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
        int tasksPerMember;
    }
}
