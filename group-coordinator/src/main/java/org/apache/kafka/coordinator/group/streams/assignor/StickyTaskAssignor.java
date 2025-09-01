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
import java.util.Optional;
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
        final GroupAssignment assignments =  doAssign(groupSpec, topologyDescriber);
        localState = null;
        return assignments;
    }

    private GroupAssignment doAssign(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) {
        //active
        final Set<TaskId> activeTasks = taskIds(topologyDescriber, true);
        assignActive(activeTasks);

        //standby
        final int numStandbyReplicas =
            groupSpec.assignmentConfigs().isEmpty() ? 0
                : Integer.parseInt(groupSpec.assignmentConfigs().get("num.standby.replicas"));
        if (numStandbyReplicas > 0) {
            final Set<TaskId> statefulTasks = taskIds(topologyDescriber, false);
            assignStandby(statefulTasks, numStandbyReplicas);
        }

        return buildGroupAssignment(groupSpec.members().keySet());
    }

    private Set<TaskId> taskIds(final TopologyDescriber topologyDescriber, final boolean isActive) {
        final Set<TaskId> ret = new HashSet<>();
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
        localState.allTasks = 0;
        for (final String subtopology : topologyDescriber.subtopologies()) {
            final int numberOfPartitions = topologyDescriber.maxNumInputPartitions(subtopology);
            localState.allTasks += numberOfPartitions;
        }
        localState.totalCapacity = groupSpec.members().size();
        localState.tasksPerMember = computeTasksPerMember(localState.allTasks, localState.totalCapacity);

        localState.processIdToState = new HashMap<>();
        localState.activeTaskToPrevMember = new HashMap<>();
        localState.standbyTaskToPrevMember = new HashMap<>();
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
                    localState.standbyTaskToPrevMember.putIfAbsent(taskId, new ArrayList<>());
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

    private void assignActive(final Set<TaskId> activeTasks) {

        // 1. re-assigning existing active tasks to clients that previously had the same active tasks
        for (final Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final Member prevMember = localState.activeTaskToPrevMember.get(task);
            if (prevMember != null && hasUnfulfilledQuota(prevMember)) {
                final ProcessState processState = localState.processIdToState.get(prevMember.processId);
                processState.addTask(prevMember.memberId, task, true);
                maybeUpdateTasksPerMember(processState.activeTaskCount());
                it.remove();
            }
        }

        // 2. re-assigning tasks to clients that previously have seen the same task (as standby task)
        for (final Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final ArrayList<Member> prevMembers = localState.standbyTaskToPrevMember.get(task);
            final Member prevMember = findPrevMemberWithLeastLoad(prevMembers, null);
            if (prevMember != null && hasUnfulfilledQuota(prevMember)) {
                final ProcessState processState = localState.processIdToState.get(prevMember.processId);
                processState.addTask(prevMember.memberId, task, true);
                maybeUpdateTasksPerMember(processState.activeTaskCount());
                it.remove();
            }
        }

        // 3. assign any remaining unassigned tasks
        final PriorityQueue<ProcessState> processByLoad = new PriorityQueue<>(Comparator.comparingDouble(ProcessState::load));
        processByLoad.addAll(localState.processIdToState.values());
        for (final Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final ProcessState processWithLeastLoad = processByLoad.poll();
            if (processWithLeastLoad == null) {
                throw new TaskAssignorException("No process available to assign active task {}." + task);
            }
            final String member = memberWithLeastLoad(processWithLeastLoad);
            if (member == null) {
                throw new TaskAssignorException("No member available to assign active task {}." + task);
            }
            processWithLeastLoad.addTask(member, task, true);
            it.remove();
            maybeUpdateTasksPerMember(processWithLeastLoad.activeTaskCount());
            processByLoad.add(processWithLeastLoad); // Add it back to the queue after updating its state
        }
    }

    private void maybeUpdateTasksPerMember(final int activeTasksNo) {
        if (activeTasksNo == localState.tasksPerMember) {
            localState.totalCapacity--;
            localState.allTasks -= activeTasksNo;
            localState.tasksPerMember = computeTasksPerMember(localState.allTasks, localState.totalCapacity);
        }
    }

    private boolean assignStandbyToMemberWithLeastLoad(PriorityQueue<ProcessState> queue, TaskId taskId) {
        final ProcessState processWithLeastLoad = queue.poll();
        if (processWithLeastLoad == null) {
            return false;
        }
        boolean found = false;
        if (!processWithLeastLoad.hasTask(taskId)) {
            final String memberId = memberWithLeastLoad(processWithLeastLoad);
            if (memberId != null) {
                processWithLeastLoad.addTask(memberId, taskId, false);
                found = true;
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

    private String memberWithLeastLoad(final ProcessState processWithLeastLoad) {
        final Map<String, Integer> members = processWithLeastLoad.memberToTaskCounts();
        if (members.isEmpty()) {
            return null;
        }
        if (members.size() == 1) {
            return members.keySet().iterator().next();
        }
        final Optional<String> memberWithLeastLoad = processWithLeastLoad.memberToTaskCounts().entrySet().stream()
            .min(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
        return memberWithLeastLoad.orElse(null);
    }

    private boolean hasUnfulfilledQuota(final Member member) {
        return localState.processIdToState.get(member.processId).memberToTaskCounts().get(member.memberId) < localState.tasksPerMember;
    }

    private void assignStandby(final Set<TaskId> standbyTasks, final int numStandbyReplicas) {
        final ArrayList<StandbyToAssign> toLeastLoaded = new ArrayList<>(standbyTasks.size() * numStandbyReplicas);
        for (final TaskId task : standbyTasks) {
            for (int i = 0; i < numStandbyReplicas; i++) {

                // prev active task
                final Member prevMember = localState.activeTaskToPrevMember.get(task);
                if (prevMember != null) {
                    final ProcessState prevMemberProcessState = localState.processIdToState.get(prevMember.processId);
                    if (!prevMemberProcessState.hasTask(task) && isLoadBalanced(prevMemberProcessState)) {
                        prevMemberProcessState.addTask(prevMember.memberId, task, false);
                        continue;
                    }
                }

                // prev standby tasks
                final ArrayList<Member> prevMembers = localState.standbyTaskToPrevMember.get(task);
                if (prevMembers != null && !prevMembers.isEmpty()) {
                    final Member prevMember2 = findPrevMemberWithLeastLoad(prevMembers, task);
                    if (prevMember2 != null) {
                        final ProcessState prevMemberProcessState = localState.processIdToState.get(prevMember2.processId);
                        if (isLoadBalanced(prevMemberProcessState)) {
                            prevMemberProcessState.addTask(prevMember2.memberId, task, false);
                            continue;
                        }
                    }
                }

                toLeastLoaded.add(new StandbyToAssign(task, numStandbyReplicas - i));
                break;
            }
        }

        final PriorityQueue<ProcessState> processByLoad = new PriorityQueue<>(Comparator.comparingDouble(ProcessState::load));
        processByLoad.addAll(localState.processIdToState.values());
        for (final StandbyToAssign toAssign : toLeastLoaded) {
            for (int i = 0; i < toAssign.remainingReplicas; i++) {
                if (!assignStandbyToMemberWithLeastLoad(processByLoad, toAssign.taskId)) {
                    log.warn("{} There is not enough available capacity. " +
                            "You should increase the number of threads and/or application instances to maintain the requested number of standby replicas.",
                        errorMessage(numStandbyReplicas, i, toAssign.taskId));
                    break;
                }
            }
        }
    }

    private String errorMessage(final int numStandbyReplicas, final int i, final TaskId task) {
        return "Unable to assign " + (numStandbyReplicas - i) +
            " of " + numStandbyReplicas + " standby tasks for task [" + task + "].";
    }

    private boolean isLoadBalanced(final ProcessState process) {
        final double load = process.load();
        final boolean isLeastLoadedProcess = localState.processIdToState.values().stream()
            .allMatch(p -> p.load() >= load);
        return process.hasCapacity() || isLeastLoadedProcess;
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

        int allTasks;
        int totalCapacity;
        int tasksPerMember;
    }
}
