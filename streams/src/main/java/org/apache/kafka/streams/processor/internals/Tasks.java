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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.PendingUpdateAction.Action;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.filterMap;
import static org.apache.kafka.common.utils.Utils.union;

/**
 * All tasks contained by the Streams instance.
 *
 * Note that these tasks are shared between the TaskManager (stream thread) and the StateUpdater (restore thread),
 * i.e. all running active tasks are processed by the former and all restoring active tasks and standby tasks are
 * processed by the latter.
 */
class Tasks implements TasksRegistry {
    private final Logger log;

    // TODO: convert to Stream/StandbyTask when we remove TaskManager#StateMachineTask with mocks
    // note that these two maps may be accessed by concurrent threads and hence
    // should be synchronized when accessed
    private final Map<TaskId, Task> activeTasksPerId = new TreeMap<>();
    private final Map<TaskId, Task> standbyTasksPerId = new TreeMap<>();

    // Tasks may have been assigned for a NamedTopology that is not yet known by this host. When that occurs we stash
    // these unknown tasks until either the corresponding NamedTopology is added and we can create them at last, or
    // we receive a new assignment and they are revoked from the thread.
    private final Map<TaskId, Set<TopicPartition>> pendingActiveTasksToCreate = new HashMap<>();
    private final Map<TaskId, Set<TopicPartition>> pendingStandbyTasksToCreate = new HashMap<>();
    private final Set<Task> pendingTasksToInit = new HashSet<>();
    private final Map<TaskId, PendingUpdateAction> pendingUpdateActions = new HashMap<>();

    // TODO: convert to Stream/StandbyTask when we remove TaskManager#StateMachineTask with mocks
    private final Map<TopicPartition, Task> activeTasksPerPartition = new HashMap<>();

    Tasks(final LogContext logContext) {
        this.log = logContext.logger(getClass());
    }

    @Override
    public void clearPendingTasksToCreate() {
        pendingActiveTasksToCreate.clear();
        pendingStandbyTasksToCreate.clear();
    }

    @Override
    public Map<TaskId, Set<TopicPartition>> drainPendingActiveTasksForTopologies(final Set<String> currentTopologies) {
        final Map<TaskId, Set<TopicPartition>> pendingActiveTasksForTopologies =
            filterMap(pendingActiveTasksToCreate, t -> currentTopologies.contains(t.getKey().topologyName()));

        pendingActiveTasksToCreate.keySet().removeAll(pendingActiveTasksForTopologies.keySet());

        return pendingActiveTasksForTopologies;
    }

    @Override
    public Map<TaskId, Set<TopicPartition>> drainPendingStandbyTasksForTopologies(final Set<String> currentTopologies) {
        final Map<TaskId, Set<TopicPartition>> pendingActiveTasksForTopologies =
            filterMap(pendingStandbyTasksToCreate, t -> currentTopologies.contains(t.getKey().topologyName()));

        pendingStandbyTasksToCreate.keySet().removeAll(pendingActiveTasksForTopologies.keySet());

        return pendingActiveTasksForTopologies;
    }

    @Override
    public void addPendingActiveTasksToCreate(final Map<TaskId, Set<TopicPartition>> pendingTasks) {
        pendingActiveTasksToCreate.putAll(pendingTasks);
    }

    @Override
    public void addPendingStandbyTasksToCreate(final Map<TaskId, Set<TopicPartition>> pendingTasks) {
        pendingStandbyTasksToCreate.putAll(pendingTasks);
    }

    @Override
    public Set<TopicPartition> removePendingTaskToRecycle(final TaskId taskId) {
        return removePendingUpdateActionWithInputPartitions(taskId, Action.RECYCLE);
    }

    @Override
    public void addPendingTaskToRecycle(final TaskId taskId, final Set<TopicPartition> inputPartitions) {
        updatePendingUpdateAction(taskId, PendingUpdateAction.createRecycleTask(inputPartitions));
    }

    @Override
    public boolean hasPendingTasksToRecycle() {
        return pendingUpdateActions.values().stream().anyMatch(action -> action.getAction() == Action.RECYCLE);
    }

    @Override
    public Set<TopicPartition> removePendingTaskToCloseReviveAndUpdateInputPartitions(final TaskId taskId) {
        return removePendingUpdateActionWithInputPartitions(taskId, Action.CLOSE_REVIVE_AND_UPDATE_INPUT_PARTITIONS);
    }

    @Override
    public void addPendingTaskToCloseReviveAndUpdateInputPartitions(final TaskId taskId, final Set<TopicPartition> inputPartitions) {
        updatePendingUpdateAction(taskId, PendingUpdateAction.createCloseReviveAndUpdateInputPartition(inputPartitions));
    }

    @Override
    public Set<TopicPartition> removePendingTaskToUpdateInputPartitions(final TaskId taskId) {
        return removePendingUpdateActionWithInputPartitions(taskId, Action.UPDATE_INPUT_PARTITIONS);
    }

    @Override
    public void addPendingTaskToUpdateInputPartitions(final TaskId taskId, final Set<TopicPartition> inputPartitions) {
        updatePendingUpdateAction(taskId, PendingUpdateAction.createUpdateInputPartition(inputPartitions));
    }

    @Override
    public boolean removePendingTaskToAddBack(final TaskId taskId) {
        return removePendingUpdateAction(taskId, Action.ADD_BACK);
    }

    @Override
    public void addPendingTaskToAddBack(final TaskId taskId) {
        updatePendingUpdateAction(taskId, PendingUpdateAction.createAddBack());
    }

    @Override
    public boolean removePendingTaskToCloseClean(final TaskId taskId) {
        return removePendingUpdateAction(taskId, Action.CLOSE_CLEAN);
    }

    @Override
    public void addPendingTaskToCloseClean(final TaskId taskId) {
        updatePendingUpdateAction(taskId, PendingUpdateAction.createCloseClean());
    }

    @Override
    public boolean removePendingActiveTaskToSuspend(final TaskId taskId) {
        return removePendingUpdateAction(taskId, Action.SUSPEND);
    }

    @Override
    public void addPendingActiveTaskToSuspend(final TaskId taskId) {
        updatePendingUpdateAction(taskId, PendingUpdateAction.createSuspend());
    }

    private Set<TopicPartition> removePendingUpdateActionWithInputPartitions(final TaskId taskId, final Action action) {
        if (containsTaskIdWithAction(taskId, action)) {
            final PendingUpdateAction pendingUpdateAction = pendingUpdateActions.remove(taskId);
            log.info("Removing pending update action {} for task {}", taskId, pendingUpdateAction);
            return pendingUpdateAction.getInputPartitions();
        }
        return null;
    }

    private boolean removePendingUpdateAction(final TaskId taskId, final Action action) {
        if (containsTaskIdWithAction(taskId, action)) {
            log.info("Removing pending update action {} for task {}", taskId, pendingUpdateActions.remove(taskId));
            return true;
        }
        return false;
    }

    private void updatePendingUpdateAction(final TaskId taskId, final PendingUpdateAction newAction) {
        if (pendingUpdateActions.containsKey(taskId)) {
            log.info("Adding pending update action {} for task {}, previous action was {}",
                newAction, taskId, pendingUpdateActions.get(taskId));
        } else {
            log.info("Adding pending update action {} for task {}, no previous action", newAction, taskId);
        }
        pendingUpdateActions.put(taskId, newAction);
    }

    private boolean containsTaskIdWithAction(final TaskId taskId, final Action action) {
        final PendingUpdateAction pendingUpdateAction = pendingUpdateActions.get(taskId);
        return pendingUpdateAction != null && pendingUpdateAction.getAction() == action;
    }

    @Override
    public Set<Task> drainPendingTasksToInit() {
        final Set<Task> result = new HashSet<>(pendingTasksToInit);
        pendingTasksToInit.clear();
        return result;
    }

    @Override
    public void addPendingTasksToInit(final Collection<Task> tasks) {
        pendingTasksToInit.addAll(tasks);
    }

    @Override
    public boolean hasPendingTasksToInit() {
        return !pendingTasksToInit.isEmpty();
    }

    @Override
    public void addActiveTasks(final Collection<Task> newTasks) {
        if (!newTasks.isEmpty()) {
            for (final Task activeTask : newTasks) {
                addTask(activeTask);
            }
        }
    }

    @Override
    public void addStandbyTasks(final Collection<Task> newTasks) {
        if (!newTasks.isEmpty()) {
            for (final Task standbyTask : newTasks) {
                addTask(standbyTask);
            }
        }
    }

    @Override
    public synchronized void addTask(final Task task) {
        final TaskId taskId = task.id();
        if (activeTasksPerId.containsKey(taskId)) {
            throw new IllegalStateException("Attempted to create an active task that we already own: " + taskId);
        }

        if (standbyTasksPerId.containsKey(taskId)) {
            throw new IllegalStateException("Attempted to create an active task while we already own its standby: " + taskId);
        }

        if (task.isActive()) {
            activeTasksPerId.put(task.id(), task);
            pendingActiveTasksToCreate.remove(task.id());
            for (final TopicPartition topicPartition : task.inputPartitions()) {
                activeTasksPerPartition.put(topicPartition, task);
            }
        } else {
            standbyTasksPerId.put(task.id(), task);
        }
    }

    @Override
    public synchronized void removeTask(final Task taskToRemove) {
        final TaskId taskId = taskToRemove.id();

        if (taskToRemove.state() != Task.State.CLOSED && taskToRemove.state() != State.SUSPENDED) {
            throw new IllegalStateException("Attempted to remove a task that is not closed or suspended: " + taskId);
        }

        if (taskToRemove.isActive()) {
            if (activeTasksPerId.remove(taskId) == null) {
                throw new IllegalArgumentException("Attempted to remove an active task that is not owned: " + taskId);
            }
            removePartitionsForActiveTask(taskId);
        } else {
            if (standbyTasksPerId.remove(taskId) == null) {
                throw new IllegalArgumentException("Attempted to remove a standby task that is not owned: " + taskId);
            }
        }
    }

    @Override
    public synchronized void replaceActiveWithStandby(final StandbyTask standbyTask) {
        final TaskId taskId = standbyTask.id();
        if (activeTasksPerId.remove(taskId) == null) {
            throw new IllegalStateException("Attempted to replace unknown active task with standby task: " + taskId);
        }
        removePartitionsForActiveTask(taskId);

        standbyTasksPerId.put(standbyTask.id(), standbyTask);
    }

    @Override
    public synchronized void replaceStandbyWithActive(final StreamTask activeTask) {
        final TaskId taskId = activeTask.id();
        if (standbyTasksPerId.remove(taskId) == null) {
            throw new IllegalStateException("Attempted to convert unknown standby task to stream task: " + taskId);
        }

        activeTasksPerId.put(activeTask.id(), activeTask);
        for (final TopicPartition topicPartition : activeTask.inputPartitions()) {
            activeTasksPerPartition.put(topicPartition, activeTask);
        }
    }

    @Override
    public boolean updateActiveTaskInputPartitions(final Task task, final Set<TopicPartition> topicPartitions) {
        final boolean requiresUpdate = !task.inputPartitions().equals(topicPartitions);
        if (requiresUpdate) {
            log.debug("Update task {} inputPartitions: current {}, new {}", task, task.inputPartitions(), topicPartitions);
            if (task.isActive()) {
                for (final TopicPartition inputPartition : task.inputPartitions()) {
                    activeTasksPerPartition.remove(inputPartition);
                }
                for (final TopicPartition topicPartition : topicPartitions) {
                    activeTasksPerPartition.put(topicPartition, task);
                }
            }
        }

        return requiresUpdate;
    }

    private void removePartitionsForActiveTask(final TaskId taskId) {
        final Set<TopicPartition> toBeRemoved = activeTasksPerPartition.entrySet().stream()
            .filter(e -> e.getValue().id().equals(taskId))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        toBeRemoved.forEach(activeTasksPerPartition::remove);
    }

    @Override
    public synchronized void clear() {
        activeTasksPerId.clear();
        standbyTasksPerId.clear();
        activeTasksPerPartition.clear();
    }

    // TODO: change return type to `StreamTask`
    @Override
    public Task activeTasksForInputPartition(final TopicPartition partition) {
        return activeTasksPerPartition.get(partition);
    }

    private synchronized Task getTask(final TaskId taskId) {
        if (activeTasksPerId.containsKey(taskId)) {
            return activeTasksPerId.get(taskId);
        }
        if (standbyTasksPerId.containsKey(taskId)) {
            return standbyTasksPerId.get(taskId);
        }
        return null;
    }

    @Override
    public Task task(final TaskId taskId) {
        final Task task = getTask(taskId);

        if (task != null)
            return task;
        else
            throw new IllegalStateException("Task unknown: " + taskId);
    }

    @Override
    public Collection<Task> tasks(final Collection<TaskId> taskIds) {
        final Set<Task> tasks = new HashSet<>();
        for (final TaskId taskId : taskIds) {
            tasks.add(task(taskId));
        }
        return tasks;
    }

    @Override
    public synchronized Collection<TaskId> activeTaskIds() {
        return Collections.unmodifiableCollection(activeTasksPerId.keySet());
    }

    @Override
    public synchronized Collection<Task> activeTasks() {
        return Collections.unmodifiableCollection(activeTasksPerId.values());
    }

    /**
     * All tasks returned by any of the getters are read-only and should NOT be modified;
     * and the returned task could be modified by other threads concurrently
     */
    @Override
    public synchronized Set<Task> allTasks() {
        return union(HashSet::new, new HashSet<>(activeTasksPerId.values()), new HashSet<>(standbyTasksPerId.values()));
    }

    @Override
    public synchronized Set<TaskId> allTaskIds() {
        return union(HashSet::new, activeTasksPerId.keySet(), standbyTasksPerId.keySet());
    }

    @Override
    public synchronized Map<TaskId, Task> allTasksPerId() {
        final Map<TaskId, Task> ret = new HashMap<>();
        ret.putAll(activeTasksPerId);
        ret.putAll(standbyTasksPerId);
        return ret;
    }

    @Override
    public boolean contains(final TaskId taskId) {
        return getTask(taskId) != null;
    }
}
