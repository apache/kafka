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

import org.apache.kafka.streams.processor.internals.TaskMetadata;

import java.util.HashSet;
import java.util.Set;

public class ClientState {
    private final Set<TaskMetadata> activeTasks;
    private final Set<TaskMetadata> standbyTasks;
    private final Set<TaskMetadata> assignedTasks;
    private final Set<TaskMetadata> prevActiveTasks;
    private final Set<TaskMetadata> prevStandbyTasks;
    private final Set<TaskMetadata> prevAssignedTasks;

    private int capacity;
    private int numberOfActiveStateStores;
    private int numberOfStandbyStateStores;
    private int numberOfActivePartitions;


    public ClientState() {
        this(0);
    }

    ClientState(final int capacity) {
        this(new HashSet<TaskMetadata>(), new HashSet<TaskMetadata>(), new HashSet<TaskMetadata>(), new HashSet<TaskMetadata>(), new HashSet<TaskMetadata>(), new HashSet<TaskMetadata>(), capacity);
        this.numberOfActiveStateStores = 0;
        this.numberOfStandbyStateStores = 0;
        this.numberOfActivePartitions = 0;
    }

    private ClientState(final Set<TaskMetadata> activeTasks,
                        final Set<TaskMetadata> standbyTasks,
                        final Set<TaskMetadata> assignedTasks,
                        final Set<TaskMetadata> prevActiveTasks,
                        final Set<TaskMetadata> prevStandbyTasks,
                        final Set<TaskMetadata> prevAssignedTasks,
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

    public void assign(final TaskMetadata taskId, final boolean active) {
        if (active) {
            activeTasks.add(taskId);
            numberOfActiveStateStores += taskId.numberOfStateStores();
            numberOfActivePartitions += taskId.numberOfPartitions();

        } else {
            standbyTasks.add(taskId);
            numberOfStandbyStateStores += taskId.numberOfStateStores();
        }

        assignedTasks.add(taskId);
    }

    public Set<TaskMetadata> activeTasks() {
        return activeTasks;
    }

    public Set<TaskMetadata> standbyTasks() {
        return standbyTasks;
    }

    public Set<TaskMetadata> prevActiveTasks() {
        return prevActiveTasks;
    }

    public Set<TaskMetadata> prevStandbyTasks() {
        return prevStandbyTasks;
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

    public void addPreviousActiveTasks(final Set<TaskMetadata> prevTasks) {
        prevActiveTasks.addAll(prevTasks);
        prevAssignedTasks.addAll(prevTasks);
    }

    public void addPreviousStandbyTasks(final Set<TaskMetadata> standbyTasks) {
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

    void checkCapacity(final ClientState other) {
        if (this.capacity <= 0) {
            throw new IllegalStateException("Capacity of this ClientState must be greater than 0.");
        }
        if (other.capacity <= 0) {
            throw new IllegalStateException("Capacity of other ClientState must be greater than 0");
        }
    }

    boolean leastLoaded(final double thisLoad, final double otherLoad, final ClientState other) {
        if (thisLoad < otherLoad)
            return true;
        else if (thisLoad > otherLoad)
            return false;
        else
            return capacity > other.capacity;
    }

    boolean hasMoreAvailableCapacityThan(final ClientState other) {
        checkCapacity(other);

        final double otherLoad = (double) other.assignedTaskCount() / other.capacity();
        final double thisLoad = (double) assignedTaskCount() / capacity();

        return leastLoaded(thisLoad, otherLoad, other);
    }

    boolean hasMoreAvailableActiveTaskCapacityThan(final ClientState other) {
        checkCapacity(other);

        final double otherLoad = (double) ((other.numberOfActiveStateStores + 1) * other.numberOfActivePartitions
                + other.activeTasks().size()) / other.capacity;
        final double thisLoad = (double) ((numberOfActiveStateStores + 1) * numberOfActivePartitions
                + activeTasks.size()) / capacity;

        return leastLoaded(thisLoad, otherLoad, other);
    }

    boolean hasMoreAvailableStandbyTaskCapacityThan(final ClientState other) {
        checkCapacity(other);

        final double thisLoad = (double) (numberOfStandbyStateStores + standbyTasks.size()) / capacity;
        final double otherLoad = (double) (other.numberOfStandbyStateStores + other.standbyTasks().size()) / capacity;

        return leastLoaded(thisLoad, otherLoad, other);
    }

    Set<TaskMetadata> previousStandbyTasks() {
        final Set<TaskMetadata> standby = new HashSet<>(prevAssignedTasks);
        standby.removeAll(prevActiveTasks);
        return standby;
    }

    Set<TaskMetadata> previousActiveTasks() {
        return prevActiveTasks;
    }

    boolean hasAssignedTask(final TaskMetadata taskId) {
        return assignedTasks.contains(taskId);
    }

    // Visible for testing
    Set<TaskMetadata> assignedTasks() {
        return assignedTasks;
    }

    Set<TaskMetadata> previousAssignedTasks() {
        return prevAssignedTasks;
    }

    int capacity() {
        return capacity;
    }

    int getNumberOfActiveStateStores() {
        return numberOfActiveStateStores;
    }

    int getNumberOfActivePartitions() {
        return numberOfActivePartitions;
    }

    int getNumberOfStandbyStateStores() {
        return numberOfStandbyStateStores;
    }

    boolean hasUnfulfilledQuota(final int tasksPerThread) {
        return activeTasks.size() < capacity * tasksPerThread;
    }
}