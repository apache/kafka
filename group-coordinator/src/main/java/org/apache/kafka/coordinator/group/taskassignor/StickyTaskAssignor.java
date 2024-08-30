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

package org.apache.kafka.coordinator.group.taskassignor;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class StickyTaskAssignor implements TaskAssignor {

    public static final String STICKY_ASSIGNOR_NAME = "sticky";
    private static final Logger log = LoggerFactory.getLogger(StickyTaskAssignor.class);

    // helper data structures:
    private TaskPairs taskPairs;
    Map<TaskId, Member> activeTaskToPrevMember;
    Map<TaskId, Set<Member>> standbyTaskToPrevMember;
    Map<String, ProcessState> processIdToState;

    int allTasks;
    int totalCapacity;
    int tasksPerMember;

    @Override
    public String name() {
        return STICKY_ASSIGNOR_NAME;
    }

    @Override
    public GroupAssignment assign(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) throws TaskAssignorException {

        initialize(groupSpec, topologyDescriber);

        //active
        Set<TaskId> activeTasks = toTaskIds(groupSpec, topologyDescriber, true);
        assignActive(activeTasks);

        //standby
        final int numStandbyReplicas =
                groupSpec.assignmentConfigs().isEmpty() ? 0
                        : Integer.parseInt(groupSpec.assignmentConfigs().get("numStandbyReplicas"));
        if (numStandbyReplicas > 0) {
            Set<TaskId> statefulTasks = toTaskIds(groupSpec, topologyDescriber, false);
            assignStandby(statefulTasks, numStandbyReplicas);
        }

        return buildGroupAssignment(groupSpec.members().keySet());
    }

    private Set<TaskId> toTaskIds(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber, final boolean isActive) {
        Set<TaskId> ret = new HashSet<>();
        for (String subtopology : groupSpec.subtopologies()) {
            if (isActive || topologyDescriber.isStateful(subtopology)) {
                int numberOfPartitions = topologyDescriber.numPartitions(subtopology);
                for (int i = 0; i < numberOfPartitions; i++) {
                    ret.add(new TaskId(subtopology, i));
                }
            }
        }
        return ret;
    }

    private void initialize(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) {

        allTasks = 0;
        for (String subtopology : groupSpec.subtopologies()) {
            int numberOfPartitions = topologyDescriber.numPartitions(subtopology);
            allTasks += numberOfPartitions;
        }
        totalCapacity = groupSpec.members().size();
        tasksPerMember = computeTasksPerMember(allTasks, totalCapacity);

        taskPairs = new TaskPairs(allTasks * (allTasks - 1) / 2);

        processIdToState = new HashMap<>();
        activeTaskToPrevMember = new HashMap<>();
        standbyTaskToPrevMember = new HashMap<>();
        for (Map.Entry<String, AssignmentMemberSpec> memberEntry : groupSpec.members().entrySet()) {
            final String memberId = memberEntry.getKey();
            final String processId = memberEntry.getValue().processId();
            final Member member = new Member(processId, memberId);
            final AssignmentMemberSpec memberSpec = memberEntry.getValue();

            processIdToState.putIfAbsent(processId, new ProcessState(processId));
            processIdToState.get(processId).addMember(memberId);

            // prev active tasks
            for (Map.Entry<String, Set<Integer>> entry : memberSpec.activeTasks().entrySet()) {
                Set<Integer> partitionNoSet = entry.getValue();
                for (int partitionNo : partitionNoSet) {
                    activeTaskToPrevMember.put(new TaskId(entry.getKey(), partitionNo), member);
                }
            }

            // prev standby tasks
            for (Map.Entry<String, Set<Integer>> entry : memberSpec.standbyTasks().entrySet()) {
                Set<Integer> partitionNoSet = entry.getValue();
                for (int partitionNo : partitionNoSet) {
                    TaskId taskId = new TaskId(entry.getKey(), partitionNo);
                    standbyTaskToPrevMember.putIfAbsent(taskId, new HashSet<>());
                    standbyTaskToPrevMember.get(taskId).add(member);
                }
            }
        }
    }

    private GroupAssignment buildGroupAssignment(final Set<String> members) {
        final Map<String, MemberAssignment> memberAssignments = new HashMap<>();
        final Map<String, Set<TaskId>> activeTasksAssignments = activeTasksAssignments();
        final Map<String, Set<TaskId>> standbyTasksAssignments = standbyTasksAssignments();

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

    private Map<String, Set<TaskId>> standbyTasksAssignments() {
        return processIdToState.entrySet().stream()
                .flatMap(entry -> entry.getValue().assignedStandbyTasksByMember().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (set1, set2) -> {
                    set1.addAll(set2);
                    return set1;
                }));
    }

    private Map<String, Set<TaskId>> activeTasksAssignments() {
        return processIdToState.entrySet().stream()
                .flatMap(entry -> entry.getValue().assignedActiveTasksByMember().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (set1, set2) -> {
                    set1.addAll(set2);
                    return set1;
                }));
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
            final Member prevMember = activeTaskToPrevMember.get(task);
            if (prevMember != null && (hasUnfulfilledQuota(prevMember))) {
                processIdToState.get(prevMember.processId).addTask(prevMember.memberId, task, true);
                updateHelpers(prevMember, task, true);
                it.remove();
            }
        }

        // 2. re-assigning tasks to clients that previously have seen the same task (as standby task)
        for (Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final Set<Member> prevMembers = standbyTaskToPrevMember.get(task);
            if (prevMembers != null && !prevMembers.isEmpty()) {
                final Member prevMember = findMemberWithLeastLoad(prevMembers, task, true);
                if (prevMember != null && hasUnfulfilledQuota(prevMember)) {
                    processIdToState.get(prevMember.processId).addTask(prevMember.memberId, task, true);
                    updateHelpers(prevMember, task, true);
                    it.remove();
                }
            }
        }

        // 3. assign any remaining unassigned tasks
        for (Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final Member member = findMemberWithLeastLoad(task);
            if (member != null) {
                processIdToState.get(member.processId).addTask(member.memberId, task, true);
                it.remove();
                updateHelpers(member, task, true);
            }
        }
    }

    private void maybeUpdateTasksPerMember(final int activeTasksNo) {
        if (activeTasksNo == tasksPerMember) {
            totalCapacity--;
            allTasks -= activeTasksNo;
            tasksPerMember = computeTasksPerMember(allTasks, totalCapacity);
        }
    }

    private Member findMemberWithLeastLoad(final Set<Member> members, TaskId taskId, final boolean returnSameMember) {
        Set<Member> rightPairs = members.stream()
                .filter(member  -> taskPairs.hasNewPair(taskId, processIdToState.get(member.processId).assignedTasks()))
                .collect(Collectors.toSet());
        if (rightPairs.isEmpty()) {
            rightPairs = members;
        }
        Optional<ProcessState> processWithLeastLoad = rightPairs.stream()
                .map(member  -> processIdToState.get(member.processId))
                .min(Comparator.comparingDouble(ProcessState::load));

        assert processWithLeastLoad.isPresent();
        // if the same exact former member is needed
        if (returnSameMember) {
            return standbyTaskToPrevMember.get(taskId).stream()
                    .filter(standby -> standby.processId.equals(processWithLeastLoad.get().processId()))
                    .findFirst()
                    .orElseGet(() -> memberWithLeastLoad(processWithLeastLoad.get()));
        }
        return memberWithLeastLoad(processWithLeastLoad.get());
    }

    private Member findMemberWithLeastLoad(final TaskId taskId) {
        Set<Member> allMembers = processIdToState.entrySet().stream()
                .flatMap(entry -> entry.getValue().memberToTaskCounts().keySet().stream()
                        .map(memberId -> new Member(entry.getKey(), memberId)))
                .collect(Collectors.toSet());
        return findMemberWithLeastLoad(allMembers, taskId, false);
    }

    private Member findMemberWithLeastLoad(final TaskId taskId, final Set<String> processes) {
        Set<Member> allMembers = processes.stream()
                .flatMap(processId -> processIdToState.get(processId).memberToTaskCounts().keySet().stream()
                        .map(memberId -> new Member(processId, memberId)))
                .collect(Collectors.toSet());
        return findMemberWithLeastLoad(allMembers, taskId, false);
    }

    private Member memberWithLeastLoad(final ProcessState processWithLeastLoad) {
        Optional<String> memberWithLeastLoad = processWithLeastLoad.memberToTaskCounts().entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
        return memberWithLeastLoad.map(memberId -> new Member(processWithLeastLoad.processId(), memberId)).orElse(null);
    }

    private boolean hasUnfulfilledQuota(final Member member) {
        return processIdToState.get(member.processId).memberToTaskCounts().get(member.memberId) < tasksPerMember;
    }

    private void assignStandby(final Set<TaskId> standbyTasks, final int numStandbyReplicas) {
        for (TaskId task : standbyTasks) {
            for (int i = 0; i < numStandbyReplicas; i++) {
                final Set<String> availableProcesses = findAllowedProcesses(task);
                if (availableProcesses.isEmpty()) {
                    log.warn("Unable to assign " + (numStandbyReplicas - i) +
                            " of " + numStandbyReplicas + " standby tasks for task [" + task + "]. " +
                            "There is not enough available capacity. You should " +
                            "increase the number of threads and/or application instances " +
                            "to maintain the requested number of standby replicas.");
                    break;
                }
                Member standby = null;

                // prev active task
                Member prevMember = activeTaskToPrevMember.get(task);
                if (prevMember != null && availableProcesses.contains(prevMember.processId) && isLoadBalanced(prevMember.processId)
                        && taskPairs.hasNewPair(task, processIdToState.get(prevMember.processId).assignedTasks())) {
                    standby = prevMember;
                }

                // prev standby tasks
                if (standby == null) {
                    final Set<Member> prevMembers = standbyTaskToPrevMember.get(task);
                    if (prevMembers != null && !prevMembers.isEmpty()) {
                        prevMembers.removeIf(member  -> !availableProcesses.contains(member.processId));
                        prevMember = findMemberWithLeastLoad(prevMembers, task, true);
                        if (prevMember != null && isLoadBalanced(prevMember.processId)) {
                            standby = prevMember;
                        }
                    }
                }

                // others
                if (standby == null) {
                    standby = findMemberWithLeastLoad(task, availableProcesses);
                }
                processIdToState.get(standby.processId).addTask(standby.memberId, task, false);
                updateHelpers(standby, task, false);
            }

        }
    }

    private boolean isLoadBalanced(final String processId) {
        final ProcessState process = processIdToState.get(processId);
        return process.hasCapacity() || isLeastLoadedProcess(process.load());
    }

    private boolean isLeastLoadedProcess(final double load) {
        return processIdToState.values().stream()
                .allMatch(process -> process.load() >= load);
    }

    private Set<String> findAllowedProcesses(final TaskId taskId) {
        return processIdToState.values().stream()
                .filter(process -> !process.hasTask(taskId))
                .map(ProcessState::processId)
                .collect(Collectors.toSet());
    }

    private void updateHelpers(final Member member, final TaskId taskId, final boolean isActive) {
        // add all pair combinations: update taskPairs
        taskPairs.addPairs(taskId, processIdToState.get(member.processId).assignedTasks());

        if (isActive) {
            // update task per process
            maybeUpdateTasksPerMember(processIdToState.get(member.processId).assignedActiveTasks().size());
        }
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

    private static class TaskPairs {
        private final Set<Pair> pairs;
        private final int maxPairs;

        TaskPairs(final int maxPairs) {
            this.maxPairs = maxPairs;
            this.pairs = new HashSet<>(maxPairs);
        }

        boolean hasNewPair(final TaskId task1,
                           final Set<TaskId> taskIds) {
            if (pairs.size() == maxPairs) {
                return false;
            }
            if (taskIds.size() == 0) {
                return true;
            }
            for (final TaskId taskId : taskIds) {
                if (!pairs.contains(pair(task1, taskId))) {
                    return true;
                }
            }
            return false;
        }

        void addPairs(final TaskId taskId, final Set<TaskId> assigned) {
            for (final TaskId id : assigned) {
                if (!id.equals(taskId))
                    pairs.add(pair(id, taskId));
            }
        }

        Pair pair(final TaskId task1, final TaskId task2) {
            if (task1.compareTo(task2) < 0) {
                return new Pair(task1, task2);
            }
            return new Pair(task2, task1);
        }


        private static class Pair {
            private final TaskId task1;
            private final TaskId task2;

            Pair(final TaskId task1, final TaskId task2) {
                this.task1 = task1;
                this.task2 = task2;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                final Pair pair = (Pair) o;
                return Objects.equals(task1, pair.task1) &&
                        Objects.equals(task2, pair.task2);
            }

            @Override
            public int hashCode() {
                return Objects.hash(task1, task2);
            }
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
}
