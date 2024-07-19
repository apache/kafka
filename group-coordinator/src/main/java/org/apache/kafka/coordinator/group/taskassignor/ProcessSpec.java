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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ProcessSpec {
    private final String processId;
    private int capacity;
    private double load;
    private final Map<String, Integer> memberToTaskCounts;

    private final Set<TaskId> assignedTasks;

    ProcessSpec(String processId) {
        this.processId = processId;
        this.capacity = 0;
        this.load = Double.MAX_VALUE;
        this.assignedTasks = new HashSet<>();
        this.memberToTaskCounts = new HashMap<>();
    }


    public String processId() {
        return processId;
    }

    public int capacity() {
        return capacity;
    }

    public int totalTaskCount() {
        return assignedTasks.size();
    }

    public double load() {
        return load;
    }

    public Map<String, Integer> memberToTaskCounts() {
        return memberToTaskCounts;
    }

    public Set<TaskId> assignedTasks() {
        return assignedTasks;
    }

    public void addTasks (String memberId, Map<String, Set<Integer>> newTasks) {
        int taskCount = 0 ;
        for (Map.Entry<String, Set<Integer>> entry : newTasks.entrySet()) {
            String subtopologyId = entry.getKey();
            for (Integer id : entry.getValue()) {
                //todo maybe double check if one task is assigned two times?!
                assignedTasks.add(new TaskId(subtopologyId, id));
                taskCount ++;
            }
        }
        memberToTaskCounts.put(memberId, memberToTaskCounts.get(memberId) + taskCount);
        computeLoad();
    }

    public void addTask (String memberId, String subtopology, int taskId) {
        assignedTasks.add(new TaskId(subtopology, taskId));
        memberToTaskCounts.put(memberId, memberToTaskCounts.get(memberId) + 1);
        computeLoad();
    }

    private void incrementCapacity () {
        capacity ++;
        computeLoad();
    }
    public void computeLoad () {
        if (capacity <= 0 ) {
            this.load = -1;
        } else {
            this.load = (double) assignedTasks.size() / capacity;
        }
    }

    public void addMember(String member) {
        this.memberToTaskCounts.put(member, 0);
        incrementCapacity();
    }

    public boolean reachedCapacity() {
        return assignedTasks.size() >= capacity;
    }

    public int compareTo (ProcessSpec other) {
        int loadCompare = Double.compare(this.load, other.load());
        if (loadCompare == 0) {
            return Integer.compare(other.capacity, this.capacity);
        }
        return loadCompare;
    }

    public boolean hasTask (String subtopologyId, int partition) {
        return assignedTasks.contains(new TaskId(subtopologyId, partition));
    }



}
