package org.apache.kafka.coordinator.group.taskassignor;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class StickyAssignor implements TaskAssignor{

    public static final String STICKY_ASSIGNOR_NAME = "sticky";
    private final boolean mustPreserveActiveTaskAssignment;
    private static final Logger log = LoggerFactory.getLogger(StickyAssignor.class);

    // helper data structures:
    Map<String, String[]> subtopologyToActiveMember = new HashMap<>();
    private TaskPairs taskPairs;
    Map<String, ProcessSpec> processIdToProcessSpec = new HashMap<>();
    Map<String, String> memberIdToProcessId = new HashMap<>();
    Map<String, String[]> subtopologyToPrevActiveMember = new HashMap<>();
    Map<String, Set<String>[]> subtopologyToPrevStandbyMember = new HashMap<>();

    int allTasks = 0;
    int totalCapacity = 0;
    int tasksPerThread = 0;

    // results/outputs:

    /**
     * The standby assignments keyed by member id
     */
    Map<String, Map<String, Set<Integer>>> standbyTasksAssignments = new HashMap<>();

    /**
     * The active assignments keyed by member id
     */
    Map<String, Map<String, Set<Integer>>> activeTasksAssignments = new HashMap<>();


    public StickyAssignor(boolean mustPreserveActiveTaskAssignment) {
        this.mustPreserveActiveTaskAssignment = mustPreserveActiveTaskAssignment;
    }

    @Override
    public String name() {
        return STICKY_ASSIGNOR_NAME;
    }

    @Override
    public GroupAssignment assign(GroupSpec groupSpec, TopologyDescriber topologyDescriber) throws TaskAssignorException {

        initialize(groupSpec, topologyDescriber);
        //active
        assignActive(groupSpec);
        //standby
        assignStandby(groupSpec, topologyDescriber);
        //warm-up
        // ...
        return buildGroupAssignment();
    }

    @SuppressWarnings({"unchecked"})
    private void initialize(GroupSpec groupSpec, TopologyDescriber topologyDescriber) {

        for (String subtopology : groupSpec.subtopologies()) {
            int numberOfPartitions = topologyDescriber.numPartitions(subtopology);
            allTasks += numberOfPartitions;

            subtopologyToActiveMember.put(subtopology, new String[numberOfPartitions]);
            subtopologyToPrevActiveMember.put(subtopology, new String[numberOfPartitions]);

            HashSet<String>[] prevstandbyMembers = new HashSet[numberOfPartitions];
            subtopologyToPrevStandbyMember.put(subtopology, prevstandbyMembers);
        }

        totalCapacity = groupSpec.members().size();
        tasksPerThread = computeTasksPerThread(allTasks, totalCapacity);
        taskPairs = new TaskPairs(allTasks * (allTasks - 1) / 2);

        for (Map.Entry<String, AssignmentMemberSpec> memberEntry : groupSpec.members().entrySet()) {
            final String memberId = memberEntry.getKey();
            final AssignmentMemberSpec memberSpec = memberEntry.getValue();

            memberIdToProcessId.put(memberId, memberSpec.processId());
            processIdToProcessSpec.putIfAbsent(memberSpec.processId(), new ProcessSpec(memberSpec.processId()));
            processIdToProcessSpec.get(memberSpec.processId()).addMember(memberId);


            // prev active tasks
            final Map<String, Set<Integer>> prevActiveTasks = new HashMap<>(memberSpec.activeTasks());
            for (Map.Entry<String, Set<Integer>> entry : prevActiveTasks.entrySet()) {
                final String subtopologyId = entry.getKey();
                final Set<Integer> taskIds = entry.getValue();
                final String[] activeMembers = subtopologyToPrevActiveMember.get(subtopologyId);
                for (int taskId : taskIds) {
                    if (taskId < activeMembers.length) {
                        activeMembers[taskId] = memberId;
                    }
                }
            }

            // prev standby tasks
            final Map<String, Set<Integer>> prevStandByTasks = new HashMap<>(memberSpec.standbyTasks());
            for (Map.Entry<String, Set<Integer>> entry : prevStandByTasks.entrySet()) {
                final String subtopologyId = entry.getKey();
                final Set<Integer> taskIds = entry.getValue();
                final Set<String>[] standbyMembers = subtopologyToPrevStandbyMember.get(subtopologyId);
                for (int taskId : taskIds) {
                    if (standbyMembers[taskId] == null) {
                        standbyMembers[taskId] = new HashSet<>();
                    }
                    standbyMembers[taskId].add(memberId);
                }
            }
        }
    }

    private GroupAssignment buildGroupAssignment() {
        final Map<String, MemberAssignment> memberAssignments = new HashMap<>();

        for (String memberId : activeTasksAssignments.keySet()) {
            memberAssignments.put(memberId,
                    new MemberAssignment(activeTasksAssignments.get(memberId),
                            standbyTasksAssignments.getOrDefault(memberId, new HashMap<>()),
                            new HashMap<>()));
            standbyTasksAssignments.remove(memberId);
        }

        for (String memberId : standbyTasksAssignments.keySet()) {
            memberAssignments.put(memberId,
                    new MemberAssignment(new HashMap<>(),
                            standbyTasksAssignments.get(memberId),
                            new HashMap<>()));
        }
        return new GroupAssignment(memberAssignments);
    }

    private void assignActive(GroupSpec groupSpec) {

        // 1. re-assigning existing active tasks to clients that previously had the same active tasks
        for (Map.Entry<String, AssignmentMemberSpec> memberEntry : groupSpec.members().entrySet()) {
            final String memberId = memberEntry.getKey();
            final AssignmentMemberSpec memberSpec = memberEntry.getValue();

            Map<String, Set<Integer>> activeTasks = new HashMap<>(memberSpec.activeTasks());
            maybeRemoveExtraTasks(activeTasks);
            activeTasksAssignments.put(memberId, activeTasks);
            updateHelpers(memberId, activeTasks);
            maybeUpdateTasksPerThread(activeTasks.values().stream().mapToInt(Set::size).sum());
        }

        // 2. re-assigning tasks to clients that previously have seen the same task (as standby task)
        for (Map.Entry<String, String[]> entry : subtopologyToActiveMember.entrySet()) {
            final String subtopologyId = entry.getKey();
            final String[] memberIds = entry.getValue();
            for (int taskId =0; taskId < memberIds.length; taskId ++) {
                if (memberIds[taskId] == null) {
                    Set<String> standbyMembers = subtopologyToPrevStandbyMember.get(subtopologyId)[taskId];
                    if (standbyMembers != null) {
                        final String standbyMemberWithLeastLoad = findMemberWithLeastLoad(standbyMembers, subtopologyId, taskId, true);
                        if (standbyMemberWithLeastLoad != null) {
                            memberIds[taskId] = standbyMemberWithLeastLoad;
                            updateActiveTasksAssignments(standbyMemberWithLeastLoad, subtopologyId, taskId);
                            updateHelpers(standbyMemberWithLeastLoad, subtopologyId, taskId);
                        }
                    }
                }
            }
        }

        // 3. assign any remaining unassigned tasks
        for (Map.Entry<String, String[]> entry : subtopologyToActiveMember.entrySet()) {
            final String subtopologyId = entry.getKey();
            final String[] activeMembers = entry.getValue();
            for (int i = 0; i < activeMembers.length; i++) {
                if (activeMembers[i] == null) {
                    String member = findMember(subtopologyId, i, memberIdToProcessId.keySet(), true);
                    if (member == null) {
                        throw new TaskAssignorException("No member available to assign task " + i + " of subtopology " + subtopologyId);
                    }
                    activeMembers[i] = member;
                    updateActiveTasksAssignments(member, subtopologyId, i);
                    updateHelpers(member, subtopologyId, i);
                }
            }
        }
    }

    private void assignStandby(GroupSpec groupSpec, TopologyDescriber topologyDescriber) {
        final int numStandbyReplicas =
                groupSpec.assignmentConfigs().isEmpty() ? 0
                        : Integer.parseInt(groupSpec.assignmentConfigs().get("numStandbyReplicas"));

        Map<String, Set<Integer>> stateFulTasks = new HashMap<>();
        for (String subtopology : groupSpec.subtopologies()) {
            stateFulTasks.put(subtopology, topologyDescriber.statefulTaskIds(subtopology));
        }

        for (Map.Entry<String, Set<Integer>> task : stateFulTasks.entrySet()) {
            final String subtopologyId = task.getKey();
            for (int taskId : task.getValue()) {
                for (int i = 0; i < numStandbyReplicas; i++) {
                    final Set<String> availableMembers = findMembersWithoutAssignedTask(subtopologyId, taskId);
                    if (availableMembers.isEmpty()) {
                        log.warn("Unable to assign " + (numStandbyReplicas - i) +
                                " of " + numStandbyReplicas+" standby tasks for task [" + taskId + "]. " +
                                "There is not enough available capacity. You should " +
                                "increase the number of threads and/or application instances " +
                                "to maintain the requested number of standby replicas.");
                        break;
                    }
                    final String memberId = findMember(subtopologyId, taskId, availableMembers, false);
                    if (memberId != null) {
                        addStandbyTask(memberId, subtopologyId, taskId);
                    }
                }
            }
        }
    }

    private String findMember(String subtopologyId, int taskId, Set<String> availableMembers, boolean includePrevs) {
        String memberId = null;
        // if one option
        if (availableMembers.size() == 1) {
            memberId =  availableMembers.iterator().next();
        } else {
            // find prev active or if no relevant active task existing, find prev standby task
            if (!includePrevs)
                memberId = findFamiliarMember(subtopologyId, taskId, availableMembers);

            if (memberId != null) {
                if (shouldBalanceLoad(memberId)) {
                    final String standby = findPrevStandbyMemberWithLeastLoad(subtopologyId, taskId, availableMembers);
                    if (standby == null || shouldBalanceLoad(standby)) {
                        memberId = findMemberWithLeastLoad(availableMembers, subtopologyId, taskId, false);
                    } else {
                        memberId = standby;
                    }
                }
            } else {
                memberId = findMemberWithLeastLoad(availableMembers, subtopologyId, taskId, false);
            }
        }

        return memberId;
    }

    private boolean shouldBalanceLoad(String memberId) {
        final ProcessSpec processSpec = processIdToProcessSpec.get(memberIdToProcessId.get(memberId));
        return processSpec.reachedCapacity() && hasNodesWithMoreAvailableCapacity(processSpec.processId());
    }

    private boolean hasNodesWithMoreAvailableCapacity(String processId) {
        final ProcessSpec processSpec = processIdToProcessSpec.get(processId);
        final ProcessSpec minProcessSpec = findProcessWithLeastLoad(processIdToProcessSpec.keySet());
        return !(Objects.equals(processSpec.processId(), minProcessSpec.processId()));
    }

    private String findFamiliarMember(String subtopologyId, int taskId, Set<String> availableMembers) {
        String memberId;
        final String prevActiveMember = subtopologyToPrevActiveMember.get(subtopologyId)[taskId];

        if (prevActiveMember != null && availableMembers.contains(prevActiveMember)) {
            memberId = prevActiveMember;
            // prev standby with least load
        } else {
            memberId = findPrevStandbyMemberWithLeastLoad(subtopologyId, taskId, availableMembers);
        }
        return memberId;
    }

    private void addStandbyTask(String memberId, String subtopologyId, int taskId) {
        // add to standbyTasksAssignments
        standbyTasksAssignments.putIfAbsent(memberId, new HashMap<>());
        standbyTasksAssignments.get(memberId).putIfAbsent(subtopologyId, new HashSet<>());
        standbyTasksAssignments.get(memberId).get(subtopologyId).add(taskId);
        updateHelpers(memberId, subtopologyId, taskId);
    }

    private Set<String> findMembersWithoutAssignedTask(String subtopologyId, int taskId) {
        Set<String> availableMembers = new HashSet<>();
        for (ProcessSpec processSpec : processIdToProcessSpec.values()) {
            if (!processSpec.hasTask(subtopologyId, taskId))
                availableMembers.addAll(processSpec.memberToTaskCounts().keySet());
        }
        return availableMembers;
    }

    private void updateActiveTasksAssignments(String memberId, String subtopologyId, int taskId) {
        Set<Integer> newSet = new HashSet<>(activeTasksAssignments.get(memberId).getOrDefault(subtopologyId, new HashSet<>()));
        newSet.add(taskId);
        activeTasksAssignments.get(memberId).put(subtopologyId, newSet);
    }

    private void maybeUpdateTasksPerThread(int activeTasksCount) {
        // update tasksPerThread: explanation
        if (activeTasksCount == tasksPerThread) {
            totalCapacity --;
            allTasks -= activeTasksCount;
            tasksPerThread = computeTasksPerThread(allTasks, totalCapacity);
        }
    }

    private void updateHelpers(String memberId, Map<String, Set<Integer>> tasks) {
        final String processId = memberIdToProcessId.get(memberId);

        // add the tasks to the corresponding ProcessSpec
        processIdToProcessSpec.get(processId).addTasks(memberId, tasks);

        // add all pair combinations: update taskPairs
        addToTaskPairs(tasks);

        // update subtopologyToActiveMember
        for (Map.Entry<String, Set<Integer>> entry : tasks.entrySet()) {
            final String subtopologyId = entry.getKey();
            final Set<Integer> taskIds = entry.getValue();
            final String[] activeMembers = subtopologyToActiveMember.get(subtopologyId);//new...maybe empty
            if (activeMembers != null) {
                for (int taskId : taskIds) {
                    if (taskId < activeMembers.length) {
                        if (activeMembers[taskId] != null) {
                            throw new TaskAssignorException(
                                    "Task " + taskId + " of subtopology " + subtopologyId + " is assigned to multiple members.");
                        }
                        activeMembers[taskId] = memberId;
                    }
                }
            }
        }
    }

    private void updateHelpers(String memberId, String subtopologyId, int taskId) {
        final String processId = memberIdToProcessId.get(memberId);

        // add the tasks to the corresponding ProcessSpec
        processIdToProcessSpec.get(processId).addTask(memberId, subtopologyId, taskId);
        // add all pair combinations: update taskPairs
        addToTaskPairs(memberId, subtopologyId, taskId);
    }

    private void maybeRemoveExtraTasks(Map<String, Set<Integer>> tasks) {
        int activeTasksCount = tasks.values().stream().mapToInt(Set::size).sum();
        if (activeTasksCount > tasksPerThread) {
            int curActiveTasksCount = 0;
            for (Map.Entry<String, Set<Integer>> entry : tasks.entrySet()) {
                int remaining = tasksPerThread - curActiveTasksCount;
                if (curActiveTasksCount < tasksPerThread) {
                    entry.setValue(entry.getValue().stream()
                            .skip(0) // start offset
                            .limit(Math.min(remaining, entry.getValue().size()))
                            .collect(Collectors.toSet()));
                    curActiveTasksCount += entry.getValue().size();
                } else { // remove the extra tasks
                    entry.setValue(new HashSet<>());
                }
            }
        }
    }

    private void addToTaskPairs(String memberId, String subtopologyId, int taskId) {
        final String processId = memberIdToProcessId.get(memberId);
        taskPairs.addPairs(new TaskId(subtopologyId, taskId), processIdToProcessSpec.get(processId).assignedTasks());
    }

    private void addToTaskPairs(Map<String, Set<Integer>> curActiveTasks) {
        List<TaskId> taskList = new ArrayList<>();
        for (Map.Entry<String, Set<Integer>> entry : curActiveTasks.entrySet()) {
            String subtopologyId = entry.getKey();
            for (Integer id : entry.getValue()) {
                taskList.add(new TaskId(subtopologyId, id));
            }
        }

        for (int i = 1; i < taskList.size(); i++) {
            taskPairs.addPairs(taskList.get(i - 1), new HashSet<>(taskList.subList(i, taskList.size())));
        }
    }

    private int computeTasksPerThread(int allTasks, int totalCapacity) {
        if (totalCapacity == 0) {
            return 0;
        }
        int tasksPerThread = allTasks / totalCapacity;
        if (allTasks - (tasksPerThread * totalCapacity) > 0) {
            tasksPerThread++;
        }
        return tasksPerThread;
    }

    private String findMemberWithLeastLoad(Set<String> members, String subtopologyId, int taskId, boolean isLimited) {
        Set<String> processes = new HashSet<>();
        for (String member: members) {
            processes.add(memberIdToProcessId.get(member));
        }
        // find the set of right pairs
        Set<String> rightPairs = findRightPairs (processes, new TaskId(subtopologyId, taskId));
        if (rightPairs.isEmpty()) {
            rightPairs = processes;
        }
        ProcessSpec minProcessSpec = findProcessWithLeastLoad (rightPairs);

        if (minProcessSpec.processId() != null) {
            Set<String> processMembers = minProcessSpec.memberToTaskCounts().keySet();
            Optional<String> minMember = processMembers.stream()
                    .min(Comparator.comparingInt(minProcessSpec.memberToTaskCounts()::get));
            if (isLimited) {
                return minMember.filter(member -> minProcessSpec.memberToTaskCounts().get(member) + 1 <= tasksPerThread).orElse(null);
            }
            return minMember.orElse(null);
        }
        return null;
    }

    private Set<String> findRightPairs(Set<String> processes, TaskId task) {
        Set<String> rightPairs = new HashSet<>();
        for (String processId : processes) {
            final ProcessSpec processSpec = processIdToProcessSpec.get(processId);
            if (taskPairs.hasNewPair(task, processSpec.assignedTasks())) {
                rightPairs.add(processId);
            }
        }
        return  rightPairs;
    }


    private ProcessSpec findProcessWithLeastLoad(Set<String> processes) {
        ProcessSpec minProcessSpec = new ProcessSpec(null);
        for (String processId : processes) {
            final ProcessSpec processSpec = processIdToProcessSpec.get(processId);
            if (minProcessSpec.compareTo(processSpec) >= 0)
                minProcessSpec = processSpec;
        }
        return minProcessSpec;
    }


    private String findPrevStandbyMemberWithLeastLoad(String subtopologyId, int taskId, Set<String> availableMembers) {
        Set<String> prevStandbyMembers = subtopologyToPrevStandbyMember.get(subtopologyId)[taskId];
        if (prevStandbyMembers != null) {
            final HashSet<String> constrainTo = new HashSet<>(prevStandbyMembers);
            constrainTo.retainAll(availableMembers);
            return findMemberWithLeastLoad(constrainTo, subtopologyId, taskId, false);
        }
        return null;
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
}
