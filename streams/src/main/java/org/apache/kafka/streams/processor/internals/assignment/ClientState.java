/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import java.util.HashSet;
import java.util.Set;

public class ClientState<T> {
    private final Set<T> activeTasks;
    private final Set<T> standbyTasks;
    private final Set<T> assignedTasks;
    private final Set<T> prevActiveTasks;
    private final Set<T> prevAssignedTasks;

    private int capacity;

    public ClientState() {
        this(0);
    }

    ClientState(final int capacity) {
        this(new HashSet<T>(), new HashSet<T>(), new HashSet<T>(), new HashSet<T>(), new HashSet<T>(), capacity);
    }

    private ClientState(Set<T> activeTasks, Set<T> standbyTasks, Set<T> assignedTasks, Set<T> prevActiveTasks, Set<T> prevAssignedTasks, int capacity) {
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
        this.assignedTasks = assignedTasks;
        this.prevActiveTasks = prevActiveTasks;
        this.prevAssignedTasks = prevAssignedTasks;
        this.capacity = capacity;
    }

    public ClientState<T> copy() {
        return new ClientState<>(new HashSet<>(activeTasks), new HashSet<>(standbyTasks), new HashSet<>(assignedTasks),
                new HashSet<>(prevActiveTasks), new HashSet<>(prevAssignedTasks), capacity);
    }

    public void assign(final T taskId, final boolean active) {
        if (active) {
            activeTasks.add(taskId);
        } else {
            standbyTasks.add(taskId);
        }

        assignedTasks.add(taskId);
    }

    public Set<T> activeTasks() {
        return activeTasks;
    }

    public Set<T> standbyTasks() {
        return standbyTasks;
    }

    public int assignedTaskCount() {
        return assignedTasks.size();
    }

    public void incrementCapacity() {
        capacity++;
    }

    public int activeTaskCount() {
        return activeTasks.size();
    }

    public void addPreviousActiveTasks(final Set<T> prevTasks) {
        prevActiveTasks.addAll(prevTasks);
        prevAssignedTasks.addAll(prevTasks);
    }

    public void addPreviousStandbyTasks(final Set<T> standbyTasks) {
        prevAssignedTasks.addAll(standbyTasks);
    }

    @Override
    public String toString() {
        return "[activeTasks: (" + activeTasks +
            ") standbyTasks: (" + standbyTasks +
            ") assignedTasks: (" + assignedTasks +
            ") prevActiveTasks: (" + prevActiveTasks +
            ") prevAssignedTasks: (" + prevAssignedTasks +
            ") capacity: " + capacity +
            "]";
    }

    boolean reachedCapacity() {
        return assignedTasks.size() >= capacity;
    }

    boolean hasMoreAvailableCapacityThan(final ClientState<T> other) {
        if (this.capacity <= 0) {
            throw new IllegalStateException("Capacity of this ClientState must be greater than 0.");
        }

        if (other.capacity <= 0) {
            throw new IllegalStateException("Capacity of other ClientState must be greater than 0");
        }

        final double otherLoad = (double) other.assignedTaskCount() / other.capacity;
        final double thisLoad = (double) assignedTaskCount() / capacity;

        if (thisLoad == otherLoad) {
            return capacity > other.capacity;
        }

        return thisLoad < otherLoad;
    }

    Set<T> previousStandbyTasks() {
        final Set<T> standby = new HashSet<>(prevAssignedTasks);
        standby.removeAll(prevActiveTasks);
        return standby;
    }

    Set<T> previousActiveTasks() {
        return prevActiveTasks;
    }

    boolean hasAssignedTask(final T taskId) {
        return assignedTasks.contains(taskId);
    }

    // Visible for testing
    Set<T> assignedTasks() {
        return assignedTasks;
    }

    Set<T> previousAssignedTasks() {
        return prevAssignedTasks;
    }

    int capacity() {
        return capacity;
    }
}
