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

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents the state of a process in the group coordinator.
 * This includes the capacity of the process, the load on the process, and the tasks assigned to the process.
 */
public class ProcessState {
    private final String processId;
    // number of members
    private int capacity;
    private double load;
    private int taskCount;
    private final Map<String, Integer> memberToTaskCounts;
    private final Map<String, Set<TaskId>> assignedActiveTasks;
    private final Map<String, Set<TaskId>> assignedStandbyTasks;
    private final Set<TaskId> assignedTasks;
    private PriorityQueue<Map.Entry<String, Integer>> membersByLoad;

    ProcessState(final String processId) {
        this.processId = processId;
        this.capacity = 0;
        this.load = Double.MAX_VALUE;
        this.assignedTasks = new HashSet<>();
        this.assignedActiveTasks = new HashMap<>();
        this.assignedStandbyTasks = new HashMap<>();
        this.memberToTaskCounts = new HashMap<>();
        this.membersByLoad = null;
    }

    public String processId() {
        return processId;
    }

    public int capacity() {
        return capacity;
    }

    public double load() {
        return load;
    }

    public Map<String, Integer> memberToTaskCounts() {
        return memberToTaskCounts;
    }

    public Set<TaskId> assignedActiveTasks() {
        return assignedActiveTasks.values().stream()
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
    }

    public Map<String, Set<TaskId>> assignedActiveTasksByMember() {
        return assignedActiveTasks;
    }

    public Set<TaskId> assignedStandbyTasks() {
        return assignedStandbyTasks.values().stream()
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
    }

    public Map<String, Set<TaskId>> assignedStandbyTasksByMember() {
        return assignedStandbyTasks;
    }

    /**
     * Assigns a task to a member of this process.
     *
     * @param memberId The member to assign to.
     * @param taskId   The task to assign.
     * @param isActive Whether the task is an active task (true) or a standby task (false).
     * @return the number of tasks that `memberId` has assigned after adding the new task.
     */
    public int addTask(final String memberId, final TaskId taskId, final boolean isActive) {
        int newTaskCount = addTaskInternal(memberId, taskId, isActive);
        // We cannot efficiently add a task to a specific member and keep the memberByLoad ordered correctly.
        // So we just drop the heap here.
        //
        // The order in which addTask and addTaskToLeastLoadedMember is called ensures that the heaps are built at most
        // twice (once for active, once for standby)
        membersByLoad = null;
        return newTaskCount;
    }

    private int addTaskInternal(final String memberId, final TaskId taskId, final boolean isActive) {
        taskCount += 1;
        assignedTasks.add(taskId);
        if (isActive) {
            assignedActiveTasks.putIfAbsent(memberId, new HashSet<>());
            assignedActiveTasks.get(memberId).add(taskId);
        } else {
            assignedStandbyTasks.putIfAbsent(memberId, new HashSet<>());
            assignedStandbyTasks.get(memberId).add(taskId);
        }
        int newTaskCount = memberToTaskCounts.get(memberId) + 1;
        memberToTaskCounts.put(memberId, newTaskCount);
        computeLoad();
        return newTaskCount;
    }

    /**
     * Assigns a task to the least loaded member of this process
     *
     * @param taskId   The task to assign.
     * @param isActive Whether the task is an active task (true) or a standby task (false).
     * @return the number of tasks that `memberId` has assigned after adding the new task, or -1 if the
     *         task was not assigned to any member.
     */
    public int addTaskToLeastLoadedMember(final TaskId taskId, final boolean isActive) {
        if (memberToTaskCounts.isEmpty()) {
            return -1;
        }
        if (memberToTaskCounts.size() == 1) {
            return addTaskInternal(memberToTaskCounts.keySet().iterator().next(), taskId, isActive);
        }
        if (membersByLoad == null) {
            membersByLoad = new PriorityQueue<>(
                memberToTaskCounts.size(),
                Map.Entry.comparingByValue()
            );
            for (Map.Entry<String, Integer> entry : memberToTaskCounts.entrySet()) {
                // Copy here, since map entry objects are allowed to be reused by the underlying map implementation.
                membersByLoad.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
            }
        }
        Map.Entry<String, Integer> member = membersByLoad.poll();
        if (member != null) {
            int newTaskCount = addTaskInternal(member.getKey(), taskId, isActive);
            member.setValue(newTaskCount);
            membersByLoad.add(member); // Reinsert the updated member back into the priority queue
            return newTaskCount;
        } else {
            throw new TaskAssignorException("No members available to assign task " + taskId);
        }
    }

    private void incrementCapacity() {
        capacity++;
        computeLoad();
    }

    public void computeLoad() {
        if (capacity <= 0) {
            this.load = -1;
        } else {
            this.load = (double) taskCount / capacity;
        }
    }

    public void addMember(final String member) {
        this.memberToTaskCounts.put(member, 0);
        incrementCapacity();
    }

    public boolean hasCapacity() {
        return this.load < 1.0;
    }

    public int compareTo(final ProcessState other) {
        int loadCompare = Double.compare(this.load, other.load());
        if (loadCompare == 0) {
            return Integer.compare(other.capacity, this.capacity);
        }
        return loadCompare;
    }

    public boolean hasTask(final TaskId taskId) {
        return assignedTasks.contains(taskId);
    }

    Set<TaskId> assignedTasks() {
        return assignedTasks;
    }
}