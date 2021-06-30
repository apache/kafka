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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Comparator.comparing;
import static org.apache.kafka.common.utils.Utils.union;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;

public class ClientState {
    private static final Logger LOG = LoggerFactory.getLogger(ClientState.class);
    public static final Comparator<TopicPartition> TOPIC_PARTITION_COMPARATOR = comparing(TopicPartition::topic).thenComparing(TopicPartition::partition);

    private final Map<TaskId, Long> taskOffsetSums; // contains only stateful tasks we previously owned
    private final Map<TaskId, Long> taskLagTotals;  // contains lag for all stateful tasks in the app topology
    private final Map<TopicPartition, String> ownedPartitions = new TreeMap<>(TOPIC_PARTITION_COMPARATOR);
    private final Map<String, Set<TaskId>> consumerToPreviousStatefulTaskIds = new TreeMap<>();

    private final ClientStateTask assignedActiveTasks = new ClientStateTask(new TreeSet<>(), new TreeMap<>());
    private final ClientStateTask assignedStandbyTasks = new ClientStateTask(new TreeSet<>(), new TreeMap<>());
    private final ClientStateTask previousActiveTasks = new ClientStateTask(null, new TreeMap<>());
    private final ClientStateTask previousStandbyTasks = new ClientStateTask(null, null);
    private final ClientStateTask revokingActiveTasks = new ClientStateTask(null, new TreeMap<>());

    private int capacity;

    public ClientState() {
        this(0);
    }

    ClientState(final int capacity) {
        previousStandbyTasks.taskIds(new TreeSet<>());
        previousActiveTasks.taskIds(new TreeSet<>());

        taskOffsetSums = new TreeMap<>();
        taskLagTotals = new TreeMap<>();
        this.capacity = capacity;
    }

    // For testing only
    public ClientState(final Set<TaskId> previousActiveTasks,
                       final Set<TaskId> previousStandbyTasks,
                       final Map<TaskId, Long> taskLagTotals,
                       final int capacity) {
        this.previousStandbyTasks.taskIds(unmodifiableSet(new TreeSet<>(previousStandbyTasks)));
        this.previousActiveTasks.taskIds(unmodifiableSet(new TreeSet<>(previousActiveTasks)));
        taskOffsetSums = emptyMap();
        this.taskLagTotals = unmodifiableMap(taskLagTotals);
        this.capacity = capacity;
    }

    int capacity() {
        return capacity;
    }

    public void incrementCapacity() {
        capacity++;
    }

    boolean reachedCapacity() {
        return assignedTaskCount() >= capacity;
    }

    public Set<TaskId> activeTasks() {
        return unmodifiableSet(assignedActiveTasks.taskIds());
    }

    public int activeTaskCount() {
        return assignedActiveTasks.taskIds().size();
    }

    double activeTaskLoad() {
        return ((double) activeTaskCount()) / capacity;
    }

    public void assignActiveTasks(final Collection<TaskId> tasks) {
        assignedActiveTasks.taskIds().addAll(tasks);
    }

    public void assignActiveToConsumer(final TaskId task, final String consumer) {
        if (!assignedActiveTasks.taskIds().contains(task)) {
            throw new IllegalStateException("added not assign active task " + task + " to this client state.");
        }
        assignedActiveTasks.consumerToTaskIds()
                           .computeIfAbsent(consumer, k -> new HashSet<>()).add(task);
    }

    public void assignStandbyToConsumer(final TaskId task, final String consumer) {
        assignedStandbyTasks.consumerToTaskIds().computeIfAbsent(consumer, k -> new HashSet<>()).add(task);
    }

    public void revokeActiveFromConsumer(final TaskId task, final String consumer) {
        revokingActiveTasks.consumerToTaskIds().computeIfAbsent(consumer, k -> new HashSet<>()).add(task);
    }

    public Map<String, Set<TaskId>> prevOwnedActiveTasksByConsumer() {
        return previousActiveTasks.consumerToTaskIds();
    }

    public Map<String, Set<TaskId>> prevOwnedStandbyByConsumer() {
        // standbys are just those stateful tasks minus active tasks
        final Map<String, Set<TaskId>> consumerToPreviousStandbyTaskIds = new TreeMap<>();
        final Map<String, Set<TaskId>> consumerToPreviousActiveTaskIds = previousActiveTasks.consumerToTaskIds();

        for (final Map.Entry<String, Set<TaskId>> entry: consumerToPreviousStatefulTaskIds.entrySet()) {
            final Set<TaskId> standbyTaskIds = new HashSet<>(entry.getValue());
            if (consumerToPreviousActiveTaskIds.containsKey(entry.getKey()))
                standbyTaskIds.removeAll(consumerToPreviousActiveTaskIds.get(entry.getKey()));
            consumerToPreviousStandbyTaskIds.put(entry.getKey(), standbyTaskIds);
        }

        return consumerToPreviousStandbyTaskIds;
    }

    // including both active and standby tasks
    public Set<TaskId> prevOwnedStatefulTasksByConsumer(final String memberId) {
        return consumerToPreviousStatefulTaskIds.get(memberId);
    }

    public Map<String, Set<TaskId>> assignedActiveTasksByConsumer() {
        return assignedActiveTasks.consumerToTaskIds();
    }

    public Map<String, Set<TaskId>> revokingActiveTasksByConsumer() {
        return revokingActiveTasks.consumerToTaskIds();
    }

    public Map<String, Set<TaskId>> assignedStandbyTasksByConsumer() {
        return assignedStandbyTasks.consumerToTaskIds();
    }

    public void assignActive(final TaskId task) {
        assertNotAssigned(task);
        assignedActiveTasks.taskIds().add(task);
    }

    public void unassignActive(final TaskId task) {
        final Set<TaskId> taskIds = assignedActiveTasks.taskIds();
        if (!taskIds.contains(task)) {
            throw new IllegalArgumentException("Tried to unassign active task " + task + ", but it is not currently assigned: " + this);
        }
        taskIds.remove(task);
    }

    public Set<TaskId> standbyTasks() {
        return unmodifiableSet(assignedStandbyTasks.taskIds());
    }

    boolean hasStandbyTask(final TaskId taskId) {
        return assignedStandbyTasks.taskIds().contains(taskId);
    }

    int standbyTaskCount() {
        return assignedStandbyTasks.taskIds().size();
    }

    public void assignStandby(final TaskId task) {
        assertNotAssigned(task);
        assignedStandbyTasks.taskIds().add(task);
    }

    void unassignStandby(final TaskId task) {
        final Set<TaskId> taskIds = assignedStandbyTasks.taskIds();
        if (!taskIds.contains(task)) {
            throw new IllegalArgumentException("Tried to unassign standby task " + task + ", but it is not currently assigned: " + this);
        }
        taskIds.remove(task);
    }

    Set<TaskId> assignedTasks() {
        final Set<TaskId> assignedActiveTaskIds = assignedActiveTasks.taskIds();
        final Set<TaskId> assignedStandbyTaskIds = assignedStandbyTasks.taskIds();
        // Since we're copying it, it's not strictly necessary to make it unmodifiable also.
        // I'm just trying to prevent subtle bugs if we write code that thinks it can update
        // the assignment by updating the returned set.
        return unmodifiableSet(
            union(
                () -> new HashSet<>(assignedActiveTaskIds.size() + assignedStandbyTaskIds.size()),
                assignedActiveTaskIds,
                assignedStandbyTaskIds
            )
        );
    }

    public int assignedTaskCount() {
        return activeTaskCount() + standbyTaskCount();
    }

    double assignedTaskLoad() {
        return ((double) assignedTaskCount()) / capacity;
    }

    boolean hasAssignedTask(final TaskId taskId) {
        return assignedActiveTasks.taskIds().contains(taskId) || assignedStandbyTasks.taskIds().contains(taskId);
    }

    Set<TaskId> prevActiveTasks() {
        return unmodifiableSet(previousActiveTasks.taskIds());
    }

    private void addPreviousActiveTask(final TaskId task) {
        previousActiveTasks.taskIds().add(task);
    }

    void addPreviousActiveTasks(final Set<TaskId> prevTasks) {
        previousActiveTasks.taskIds().addAll(prevTasks);
    }

    Set<TaskId> prevStandbyTasks() {
        return unmodifiableSet(previousStandbyTasks.taskIds());
    }

    private void addPreviousStandbyTask(final TaskId task) {
        previousStandbyTasks.taskIds().add(task);
    }

    void addPreviousStandbyTasks(final Set<TaskId> standbyTasks) {
        previousStandbyTasks.taskIds().addAll(standbyTasks);
    }

    Set<TaskId> previousAssignedTasks() {
        final Set<TaskId> previousActiveTaskIds = previousActiveTasks.taskIds();
        final Set<TaskId> previousStandbyTaskIds = previousStandbyTasks.taskIds();
        return union(() -> new HashSet<>(previousActiveTaskIds.size() + previousStandbyTaskIds.size()),
                     previousActiveTaskIds,
                     previousStandbyTaskIds);
    }

    // May return null
    public String previousOwnerForPartition(final TopicPartition partition) {
        return ownedPartitions.get(partition);
    }

    public void addOwnedPartitions(final Collection<TopicPartition> ownedPartitions, final String consumer) {
        for (final TopicPartition tp : ownedPartitions) {
            this.ownedPartitions.put(tp, consumer);
        }
    }

    public void addPreviousTasksAndOffsetSums(final String consumerId, final Map<TaskId, Long> taskOffsetSums) {
        this.taskOffsetSums.putAll(taskOffsetSums);
        consumerToPreviousStatefulTaskIds.put(consumerId, taskOffsetSums.keySet());
    }

    public void initializePrevTasks(final Map<TopicPartition, TaskId> taskForPartitionMap) {
        if (!previousActiveTasks.taskIds().isEmpty() || !previousStandbyTasks.taskIds().isEmpty()) {
            throw new IllegalStateException("Already added previous tasks to this client state.");
        }
        initializePrevActiveTasksFromOwnedPartitions(taskForPartitionMap);
        initializeRemainingPrevTasksFromTaskOffsetSums();
    }

    /**
     * Compute the lag for each stateful task, including tasks this client did not previously have.
     */
    public void computeTaskLags(final UUID uuid, final Map<TaskId, Long> allTaskEndOffsetSums) {
        if (!taskLagTotals.isEmpty()) {
            throw new IllegalStateException("Already computed task lags for this client.");
        }

        for (final Map.Entry<TaskId, Long> taskEntry : allTaskEndOffsetSums.entrySet()) {
            final TaskId task = taskEntry.getKey();
            final Long endOffsetSum = taskEntry.getValue();
            final Long offsetSum = taskOffsetSums.getOrDefault(task, 0L);

            if (offsetSum == Task.LATEST_OFFSET) {
                taskLagTotals.put(task, Task.LATEST_OFFSET);
            } else if (offsetSum == UNKNOWN_OFFSET_SUM) {
                taskLagTotals.put(task, UNKNOWN_OFFSET_SUM);
            } else if (endOffsetSum < offsetSum) {
                LOG.warn("Task " + task + " had endOffsetSum=" + endOffsetSum + " smaller than offsetSum=" +
                             offsetSum + " on member " + uuid + ". This probably means the task is corrupted," +
                             " which in turn indicates that it will need to restore from scratch if it gets assigned." +
                             " The assignor will de-prioritize returning this task to this member in the hopes that" +
                             " some other member may be able to re-use its state.");
                taskLagTotals.put(task, endOffsetSum);
            } else {
                taskLagTotals.put(task, endOffsetSum - offsetSum);
            }
        }
    }

    /**
     * Returns the total lag across all logged stores in the task. Equal to the end offset sum if this client
     * did not have any state for this task on disk.
     *
     * @return end offset sum - offset sum
     *          Task.LATEST_OFFSET if this was previously an active running task on this client
     */
    public long lagFor(final TaskId task) {
        final Long totalLag = taskLagTotals.get(task);
        if (totalLag == null) {
            throw new IllegalStateException("Tried to lookup lag for unknown task " + task);
        }
        return totalLag;
    }

    public Set<TaskId> statefulActiveTasks() {
        return assignedActiveTasks.taskIds().stream().filter(this::isStateful).collect(Collectors.toSet());
    }

    public Set<TaskId> statelessActiveTasks() {
        return assignedActiveTasks.taskIds().stream().filter(task -> !isStateful(task)).collect(Collectors.toSet());
    }

    boolean hasUnfulfilledQuota(final int tasksPerThread) {
        return assignedActiveTasks.taskIds().size() < capacity * tasksPerThread;
    }

    boolean hasMoreAvailableCapacityThan(final ClientState other) {
        if (capacity <= 0) {
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

    public String currentAssignment() {
        return "[activeTasks: (" + assignedActiveTasks.taskIds() +
               ") standbyTasks: (" + assignedStandbyTasks.taskIds() + ")]";
    }

    @Override
    public String toString() {
        return "[activeTasks: (" + assignedActiveTasks.taskIds() +
               ") standbyTasks: (" + assignedStandbyTasks.taskIds() +
               ") prevActiveTasks: (" + previousActiveTasks.taskIds() +
               ") prevStandbyTasks: (" + previousStandbyTasks.taskIds() +
               ") changelogOffsetTotalsByTask: (" + taskOffsetSums.entrySet() +
               ") taskLagTotals: (" + taskLagTotals.entrySet() +
               ") capacity: " + capacity +
               " assigned: " + assignedTaskCount() +
               "]";
    }

    private boolean isStateful(final TaskId task) {
        return taskLagTotals.containsKey(task);
    }

    private void initializePrevActiveTasksFromOwnedPartitions(final Map<TopicPartition, TaskId> taskForPartitionMap) {
        // there are three cases where we need to construct some or all of the prevTasks from the ownedPartitions:
        // 1) COOPERATIVE clients on version 2.4-2.5 do not encode active tasks at all and rely on ownedPartitions
        // 2) future client during version probing, when we can't decode the future subscription info's prev tasks
        // 3) stateless tasks are not encoded in the task lags, and must be figured out from the ownedPartitions
        for (final Map.Entry<TopicPartition, String> partitionEntry : ownedPartitions.entrySet()) {
            final TopicPartition tp = partitionEntry.getKey();
            final TaskId task = taskForPartitionMap.get(tp);
            if (task != null) {
                addPreviousActiveTask(task);
                previousActiveTasks.consumerToTaskIds().computeIfAbsent(partitionEntry.getValue(), k -> new HashSet<>()).add(task);
            } else {
                LOG.error("No task found for topic partition {}", tp);
            }
        }
    }

    private void initializeRemainingPrevTasksFromTaskOffsetSums() {
        final Set<TaskId> previousActiveTaskIds = previousActiveTasks.taskIds();
        if (previousActiveTaskIds.isEmpty() && !ownedPartitions.isEmpty()) {
            LOG.error("Tried to process tasks in offset sum map before processing tasks from ownedPartitions = {}", ownedPartitions);
            throw new IllegalStateException("Must initialize prevActiveTasks from ownedPartitions before initializing remaining tasks.");
        }
        for (final Map.Entry<TaskId, Long> taskEntry : taskOffsetSums.entrySet()) {
            final TaskId task = taskEntry.getKey();
            if (!previousActiveTaskIds.contains(task)) {
                final long offsetSum = taskEntry.getValue();
                if (offsetSum == Task.LATEST_OFFSET) {
                    addPreviousActiveTask(task);
                } else {
                    addPreviousStandbyTask(task);
                }
            }
        }
    }

    private void assertNotAssigned(final TaskId task) {
        if (assignedStandbyTasks.taskIds().contains(task) || assignedActiveTasks.taskIds().contains(task)) {
            throw new IllegalArgumentException("Tried to assign task " + task + ", but it is already assigned: " + this);
        }
    }
}