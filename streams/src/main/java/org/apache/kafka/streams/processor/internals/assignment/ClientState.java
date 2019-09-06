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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.util.HashSet;
import java.util.Set;

public class ClientState {
    private final Set<TaskId> activeTasks;
    private final Set<TaskId> standbyTasks;
    private final Set<TaskId> assignedTasks;
    private final Set<TaskId> prevActiveTasks;
    private final Set<TaskId> prevStandbyTasks;
    private final Set<TaskId> prevAssignedTasks;

    private int capacity;


    public ClientState() {
        this(0);
    }

    ClientState(final int capacity) {
        this(new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>(), capacity);
    }

    private ClientState(final Set<TaskId> activeTasks,
                        final Set<TaskId> standbyTasks,
                        final Set<TaskId> assignedTasks,
                        final Set<TaskId> prevActiveTasks,
                        final Set<TaskId> prevStandbyTasks,
                        final Set<TaskId> prevAssignedTasks,
                        final int capacity) {
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
        this.assignedTasks = assignedTasks;
        this.prevActiveTasks = prevActiveTasks;
        this.prevStandbyTasks = prevStandbyTasks;
        this.prevAssignedTasks = prevAssignedTasks;
        this.capacity = capacity;
    }

    public ClientState copy() {
        return new ClientState(
            new HashSet<>(activeTasks),
            new HashSet<>(standbyTasks),
            new HashSet<>(assignedTasks),
            new HashSet<>(prevActiveTasks),
            new HashSet<>(prevStandbyTasks),
            new HashSet<>(prevAssignedTasks),
            capacity);
    }

    public void assign(final TaskId taskId, final boolean active) {
        if (active) {
            activeTasks.add(taskId);
        } else {
            standbyTasks.add(taskId);
        }

        assignedTasks.add(taskId);
    }

    public Set<TaskId> activeTasks() {
        return activeTasks;
    }

    public Set<TaskId> standbyTasks() {
        return standbyTasks;
    }

    public Set<TaskId> prevActiveTasks() {
        return prevActiveTasks;
    }

    public Set<TaskId> prevStandbyTasks() {
        return prevStandbyTasks;
    }

    @SuppressWarnings("WeakerAccess")
    public int assignedTaskCount() {
        return assignedTasks.size();
    }

    public void incrementCapacity() {
        capacity++;
    }

    @SuppressWarnings("WeakerAccess")
    public int activeTaskCount() {
        return activeTasks.size();
    }

    public void addPreviousActiveTasks(final Set<TaskId> prevTasks) {
        prevActiveTasks.addAll(prevTasks);
        prevAssignedTasks.addAll(prevTasks);
    }

    public void addPreviousStandbyTasks(final Set<TaskId> standbyTasks) {
        prevStandbyTasks.addAll(standbyTasks);
        prevAssignedTasks.addAll(standbyTasks);
    }

    @Override
    public String toString() {
        return "[activeTasks: (" + activeTasks +
                ") standbyTasks: (" + standbyTasks +
                ") assignedTasks: (" + assignedTasks +
                ") prevActiveTasks: (" + prevActiveTasks +
                ") prevStandbyTasks: (" + prevStandbyTasks +
                ") prevAssignedTasks: (" + prevAssignedTasks +
                ") capacity: " + capacity +
                "]";
    }

    boolean reachedCapacity() {
        return assignedTasks.size() >= capacity;
    }

    boolean hasMoreAvailableCapacityThan(final ClientState other) {
        if (this.capacity <= 0) {
            throw new IllegalStateException("Capacity of this ClientState must be greater than 0.");
        }

        if (other.capacity <= 0) {
            throw new IllegalStateException("Capacity of other ClientState must be greater than 0");
        }

        final double otherLoad = (double) other.assignedTaskCount() / other.capacity;
        final double thisLoad = (double) assignedTaskCount() / capacity;

        if (thisLoad < otherLoad) {
            return true;
        } else if (thisLoad > otherLoad) {
            return false;
        } else {
            return capacity > other.capacity;
        }
    }

    Set<TaskId> previousStandbyTasks() {
        final Set<TaskId> standby = new HashSet<>(prevAssignedTasks);
        standby.removeAll(prevActiveTasks);
        return standby;
    }

    Set<TaskId> previousActiveTasks() {
        return prevActiveTasks;
    }

    boolean hasAssignedTask(final TaskId taskId) {
        return assignedTasks.contains(taskId);
    }

    // Visible for testing
    Set<TaskId> assignedTasks() {
        return assignedTasks;
    }

    Set<TaskId> previousAssignedTasks() {
        return prevAssignedTasks;
    }

    int capacity() {
        return capacity;
    }

    boolean hasUnfulfilledQuota(final int tasksPerThread) {
        return activeTasks.size() < capacity * tasksPerThread;
    }
}