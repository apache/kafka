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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;
import static org.apache.kafka.common.utils.Utils.union;

/**
 * Represents the state of a process in the group coordinator.
 * This includes the capacity of the process, the load on the process, and the tasks assigned to the process.
 */
public class ProcessState {
    private final String processId;
    // number of members
    private int capacity;
    private double load;
    private final Map<String, Integer> memberToTaskCounts;
    private final Map<String, Set<TaskId>> assignedActiveTasks;
    private final Map<String, Set<TaskId>> assignedStandbyTasks;


    ProcessState(final String processId) {
        this.processId = processId;
        this.capacity = 0;
        this.load = Double.MAX_VALUE;
        this.assignedActiveTasks = new HashMap<>();
        this.assignedStandbyTasks = new HashMap<>();
        this.memberToTaskCounts = new HashMap<>();
    }


    public String processId() {
        return processId;
    }

    public int capacity() {
        return capacity;
    }

    public int totalTaskCount() {
        return assignedStandbyTasks().size() + assignedActiveTasks().size();
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

    public void addTask(final String memberId, final TaskId taskId, final boolean isActive) {
        if (isActive) {
            assignedActiveTasks.putIfAbsent(memberId, new HashSet<>());
            assignedActiveTasks.get(memberId).add(taskId);
        } else {
            assignedStandbyTasks.putIfAbsent(memberId, new HashSet<>());
            assignedStandbyTasks.get(memberId).add(taskId);
        }
        memberToTaskCounts.put(memberId, memberToTaskCounts.get(memberId) + 1);
        computeLoad();
    }

    private void incrementCapacity() {
        capacity++;
        computeLoad();
    }

    public void computeLoad() {
        if (capacity <= 0) {
            this.load = -1;
        } else {
            this.load = (double) totalTaskCount() / capacity;
        }
    }

    public void addMember(final String member) {
        this.memberToTaskCounts.put(member, 0);
        incrementCapacity();
    }

    public boolean hasCapacity() {
        return totalTaskCount() < capacity;
    }

    public int compareTo(final ProcessState other) {
        int loadCompare = Double.compare(this.load, other.load());
        if (loadCompare == 0) {
            return Integer.compare(other.capacity, this.capacity);
        }
        return loadCompare;
    }

    public boolean hasTask(final TaskId taskId) {
        return assignedActiveTasks().contains(taskId) || assignedStandbyTasks().contains(taskId);    }


    Set<TaskId> assignedTasks() {
        final Set<TaskId> assignedActiveTaskIds = assignedActiveTasks();
        final Set<TaskId> assignedStandbyTaskIds = assignedStandbyTasks();
        return unmodifiableSet(
            union(
                () -> new HashSet<>(assignedActiveTaskIds.size() + assignedStandbyTaskIds.size()),
                assignedActiveTaskIds,
                assignedStandbyTaskIds
            )
        );
    }
}