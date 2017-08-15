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

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singleton;

class TaskManager {
    // initialize the task list
    // activeTasks needs to be concurrent as it can be accessed
    // by QueryableState
    private static final Logger log = LoggerFactory.getLogger(TaskManager.class);
    private final Map<TaskId, Task> activeTasks = new ConcurrentHashMap<>();
    private final Map<TaskId, Task> standbyTasks = new HashMap<>();
    private final Map<TopicPartition, Task> activeTasksByPartition = new HashMap<>();
    private final Map<TopicPartition, Task> standbyTasksByPartition = new HashMap<>();
    private final Set<TaskId> prevActiveTasks = new TreeSet<>();
    private final Map<TaskId, Task> suspendedTasks = new HashMap<>();
    private final Map<TaskId, Task> suspendedStandbyTasks = new HashMap<>();
    private final ChangelogReader changelogReader;
    private final Time time;
    private final String logPrefix;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final StreamThread.AbstractTaskCreator taskCreator;
    private final StreamThread.AbstractTaskCreator standbyTaskCreator;
    private ThreadMetadataProvider threadMetadataProvider;
    private Consumer<byte[], byte[]> consumer;

    TaskManager(final ChangelogReader changelogReader,
                final Time time,
                final String logPrefix,
                final Consumer<byte[], byte[]> restoreConsumer,
                final StreamThread.AbstractTaskCreator taskCreator,
                final StreamThread.AbstractTaskCreator standbyTaskCreator) {
        this.changelogReader = changelogReader;
        this.time = time;
        this.logPrefix = logPrefix;
        this.restoreConsumer = restoreConsumer;
        this.taskCreator = taskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
    }

    void createTasks(final Collection<TopicPartition> assignment) {
        if (threadMetadataProvider == null) {
            throw new IllegalStateException(logPrefix + " taskIdProvider has not been initialized while adding stream tasks. This should not happen.");
        }
        if (consumer == null) {
            throw new IllegalStateException(logPrefix + " consumer has not been initialized while adding stream tasks. This should not happen.");
        }

        final long start = time.milliseconds();
        changelogReader.clear();
        // do this first as we may have suspended standby tasks that
        // will become active or vice versa
        closeNonAssignedSuspendedStandbyTasks();
        Map<TaskId, Set<TopicPartition>> assignedActiveTasks = threadMetadataProvider.activeTasks();
        closeNonAssignedSuspendedTasks(assignedActiveTasks);
        addStreamTasks(assignment, assignedActiveTasks, start);
        changelogReader.restore();
        addStandbyTasks(start);
    }

    void setThreadMetadataProvider(final ThreadMetadataProvider threadMetadataProvider) {
        this.threadMetadataProvider = threadMetadataProvider;
    }

    private void closeNonAssignedSuspendedStandbyTasks() {
        final Set<TaskId> currentSuspendedTaskIds = threadMetadataProvider.standbyTasks().keySet();
        final Iterator<Map.Entry<TaskId, Task>> standByTaskIterator = suspendedStandbyTasks.entrySet().iterator();
        while (standByTaskIterator.hasNext()) {
            final Map.Entry<TaskId, Task> suspendedTask = standByTaskIterator.next();
            if (!currentSuspendedTaskIds.contains(suspendedTask.getKey())) {
                final Task task = suspendedTask.getValue();
                log.debug("{} Closing suspended and not re-assigned standby task {}", logPrefix, task.id());
                try {
                    task.close(true);
                } catch (final Exception e) {
                    log.error("{} Failed to remove suspended standby task {} due to the following error:", logPrefix, task.id(), e);
                } finally {
                    standByTaskIterator.remove();
                }
            }
        }
    }

    private void closeNonAssignedSuspendedTasks(final Map<TaskId, Set<TopicPartition>> newTaskAssignment) {
        final Iterator<Map.Entry<TaskId, Task>> suspendedTaskIterator = suspendedTasks.entrySet().iterator();
        while (suspendedTaskIterator.hasNext()) {
            final Map.Entry<TaskId, Task> next = suspendedTaskIterator.next();
            final Task task = next.getValue();
            final Set<TopicPartition> assignedPartitionsForTask = newTaskAssignment.get(next.getKey());
            if (!task.partitions().equals(assignedPartitionsForTask)) {
                log.debug("{} Closing suspended and not re-assigned task {}", logPrefix, task.id());
                try {
                    task.closeSuspended(true, null);
                } catch (final Exception e) {
                    log.error("{} Failed to close suspended task {} due to the following error:", logPrefix, next.getKey(), e);
                } finally {
                    suspendedTaskIterator.remove();
                }
            }
        }
    }

    private void addStreamTasks(final Collection<TopicPartition> assignment, final Map<TaskId, Set<TopicPartition>> assignedTasks, final long start) {
        final Map<TaskId, Set<TopicPartition>> newTasks = new HashMap<>();

        // collect newly assigned tasks and reopen re-assigned tasks
        log.debug("{} Adding assigned tasks as active: {}", logPrefix, assignedTasks);
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : assignedTasks.entrySet()) {
            final TaskId taskId = entry.getKey();
            final Set<TopicPartition> partitions = entry.getValue();

            if (assignment.containsAll(partitions)) {
                try {
                    final Task task = findMatchingSuspendedTask(taskId, partitions);
                    if (task != null) {
                        suspendedTasks.remove(taskId);
                        task.resume();

                        activeTasks.put(taskId, task);

                        for (final TopicPartition partition : partitions) {
                            activeTasksByPartition.put(partition, task);
                        }
                    } else {
                        newTasks.put(taskId, partitions);
                    }
                } catch (final StreamsException e) {
                    log.error("{} Failed to create an active task {} due to the following error:", logPrefix, taskId, e);
                    throw e;
                }
            } else {
                log.warn("{} Task {} owned partitions {} are not contained in the assignment {}", logPrefix, taskId, partitions, assignment);
            }
        }

        // create all newly assigned tasks (guard against race condition with other thread via backoff and retry)
        // -> other thread will call removeSuspendedTasks(); eventually
        log.trace("{} New active tasks to be created: {}", logPrefix, newTasks);

        if (!newTasks.isEmpty()) {
            final Map<Task, Set<TopicPartition>> createdTasks = taskCreator.retryWithBackoff(consumer, newTasks, start);
            for (final Map.Entry<Task, Set<TopicPartition>> entry : createdTasks.entrySet()) {
                final Task task = entry.getKey();
                activeTasks.put(task.id(), task);
                for (final TopicPartition partition : entry.getValue()) {
                    activeTasksByPartition.put(partition, task);
                }
            }
        }
    }

    private void addStandbyTasks(final long start) {
        final Map<TopicPartition, Long> checkpointedOffsets = new HashMap<>();

        final Map<TaskId, Set<TopicPartition>> newStandbyTasks = new HashMap<>();

        Map<TaskId, Set<TopicPartition>> assignedStandbyTasks = threadMetadataProvider.standbyTasks();
        log.debug("{} Adding assigned standby tasks {}", logPrefix, assignedStandbyTasks);
        // collect newly assigned standby tasks and reopen re-assigned standby tasks
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : assignedStandbyTasks.entrySet()) {
            final TaskId taskId = entry.getKey();
            final Set<TopicPartition> partitions = entry.getValue();
            final Task task = findMatchingSuspendedStandbyTask(taskId, partitions);

            if (task != null) {
                suspendedStandbyTasks.remove(taskId);
                task.resume();
            } else {
                newStandbyTasks.put(taskId, partitions);
            }

            updateStandByTasks(checkpointedOffsets, taskId, partitions, task);
        }

        // create all newly assigned standby tasks (guard against race condition with other thread via backoff and retry)
        // -> other thread will call removeSuspendedStandbyTasks(); eventually
        log.trace("{} New standby tasks to be created: {}", logPrefix, newStandbyTasks);
        if (!newStandbyTasks.isEmpty()) {
            final Map<Task, Set<TopicPartition>> createdStandbyTasks = standbyTaskCreator.retryWithBackoff(consumer, newStandbyTasks, start);
            for (Map.Entry<Task, Set<TopicPartition>> entry : createdStandbyTasks.entrySet()) {
                final Task task = entry.getKey();
                updateStandByTasks(checkpointedOffsets, task.id(), entry.getValue(), task);
            }
        }

        restoreConsumer.assign(checkpointedOffsets.keySet());

        for (final Map.Entry<TopicPartition, Long> entry : checkpointedOffsets.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final long offset = entry.getValue();
            if (offset >= 0) {
                restoreConsumer.seek(partition, offset);
            } else {
                restoreConsumer.seekToBeginning(singleton(partition));
            }
        }
    }

    private void updateStandByTasks(final Map<TopicPartition, Long> checkpointedOffsets,
                                    final TaskId taskId,
                                    final Set<TopicPartition> partitions,
                                    final Task task) {
        if (task != null) {
            standbyTasks.put(taskId, task);
            for (final TopicPartition partition : partitions) {
                standbyTasksByPartition.put(partition, task);
            }
            // collect checked pointed offsets to position the restore consumer
            // this include all partitions from which we restore states
            for (final TopicPartition partition : task.checkpointedOffsets().keySet()) {
                standbyTasksByPartition.put(partition, task);
            }
            checkpointedOffsets.putAll(task.checkpointedOffsets());
        }
    }

    List<Task> allTasks() {
        final List<Task> tasks = activeAndStandbytasks();
        tasks.addAll(suspendedAndSuspendedStandbytasks());
        return tasks;
    }

    private List<Task> activeAndStandbytasks() {
        final List<Task> tasks = new ArrayList<>(activeTasks.values());
        tasks.addAll(standbyTasks.values());
        return tasks;
    }

    private List<Task> suspendedAndSuspendedStandbytasks() {
        final List<Task> tasks = new ArrayList<>(suspendedTasks.values());
        tasks.addAll(suspendedStandbyTasks.values());
        return tasks;
    }

    private Task findMatchingSuspendedTask(final TaskId taskId, final Set<TopicPartition> partitions) {
        if (suspendedTasks.containsKey(taskId)) {
            final Task task = suspendedTasks.get(taskId);
            if (task.partitions().equals(partitions)) {
                return task;
            }
        }
        return null;
    }

    private Task findMatchingSuspendedStandbyTask(final TaskId taskId, final Set<TopicPartition> partitions) {
        if (suspendedStandbyTasks.containsKey(taskId)) {
            final Task task = suspendedStandbyTasks.get(taskId);
            if (task.partitions().equals(partitions)) {
                return task;
            }
        }
        return null;
    }

    Set<TaskId> activeTaskIds() {
        return Collections.unmodifiableSet(activeTasks.keySet());
    }

    Set<TaskId> standbyTaskIds() {
        return Collections.unmodifiableSet(standbyTasks.keySet());
    }

    Set<TaskId> prevActiveTaskIds() {
        return Collections.unmodifiableSet(prevActiveTasks);
    }

    /**
     * Similar to shutdownTasksAndState, however does not close the task managers, in the hope that
     * soon the tasks will be assigned again
     */
    void suspendTasksAndState()  {
        log.debug("{} Suspending all active tasks {} and standby tasks {}",
                  logPrefix, activeTasks.keySet(), standbyTasks.keySet());

        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        firstException.compareAndSet(null, performOnActiveTasks(new TaskAction() {
            @Override
            public String name() {
                return "suspend";
            }

            @Override
            public void apply(final Task task) {
                try {
                    task.suspend();
                } catch (final CommitFailedException e) {
                    // commit failed during suspension. Just log it.
                    log.warn("{} Failed to commit task {} state when suspending due to CommitFailedException", logPrefix, task.id());
                } catch (final Exception e) {
                    log.error("{} Suspending task {} failed due to the following error:", logPrefix, task.id(), e);
                    try {
                        task.close(false);
                    } catch (final Exception f) {
                        log.error("{} After suspending failed, closing the same task {} failed again due to the following error:", logPrefix, task.id(), f);
                    }
                    throw e;
                }
            }
        }));

        for (final Task task : standbyTasks.values()) {
            try {
                try {
                    task.suspend();
                } catch (final Exception e) {
                    log.error("{} Suspending standby task {} failed due to the following error:", logPrefix, task.id(), e);
                    try {
                        task.close(false);
                    } catch (final Exception f) {
                        log.error("{} After suspending failed, closing the same standby task {} failed again due to the following error:", logPrefix, task.id(), f);
                    }
                    throw e;
                }
            } catch (final RuntimeException e) {
                firstException.compareAndSet(null, e);
            }
        }

        // remove the changelog partitions from restore consumer
        firstException.compareAndSet(null, unAssignChangeLogPartitions());

        updateSuspendedTasks();

        if (firstException.get() != null) {
            throw new StreamsException(logPrefix + " failed to suspend stream tasks", firstException.get());
        }
    }

    private RuntimeException unAssignChangeLogPartitions() {
        try {
            // un-assign the change log partitions
            restoreConsumer.assign(Collections.<TopicPartition>emptyList());
        } catch (final RuntimeException e) {
            log.error("{} Failed to un-assign change log partitions due to the following error:", logPrefix, e);
            return e;
        }
        return null;
    }

    private void updateSuspendedTasks() {
        suspendedTasks.clear();
        suspendedTasks.putAll(activeTasks);
        suspendedStandbyTasks.putAll(standbyTasks);
    }

    private void removeStreamTasks() {
        log.debug("{} Removing all active tasks {}", logPrefix, activeTasks.keySet());

        try {
            prevActiveTasks.clear();
            prevActiveTasks.addAll(activeTasks.keySet());

            activeTasks.clear();
            activeTasksByPartition.clear();
        } catch (final Exception e) {
            log.error("{} Failed to remove stream tasks due to the following error:", logPrefix, e);
        }
    }

    void closeZombieTask(final Task task) {
        log.warn("{} Producer of task {} fenced; closing zombie task", logPrefix, task.id());
        try {
            task.close(false);
        } catch (final Exception e) {
            log.warn("{} Failed to close zombie task due to {}, ignore and proceed", logPrefix, e);
        }
        activeTasks.remove(task.id());
    }


    RuntimeException performOnActiveTasks(final TaskAction action) {
        return performOnTasks(action, activeTasks, "stream task");
    }

    RuntimeException performOnStandbyTasks(final TaskAction action) {
        return performOnTasks(action, standbyTasks, "standby task");
    }

    private RuntimeException performOnTasks(final TaskAction action, final Map<TaskId, Task> tasks, final String taskType) {
        RuntimeException firstException = null;
        final Iterator<Map.Entry<TaskId, Task>> it = tasks.entrySet().iterator();
        while (it.hasNext()) {
            final Task task = it.next().getValue();
            try {
                action.apply(task);
            } catch (final ProducerFencedException e) {
                closeZombieTask(task);
                it.remove();
            } catch (final RuntimeException t) {
                log.error("{} Failed to {} " + taskType + " {} due to the following error:",
                          logPrefix,
                          action.name(),
                          task.id(),
                          t);
                if (firstException == null) {
                    firstException = t;
                }
            }
        }

        return firstException;
    }



    void shutdown(final boolean clean) {
        log.debug("{} Shutting down all active tasks {}, standby tasks {}, suspended tasks {}, and suspended standby tasks {}",
                  logPrefix, activeTasks.keySet(), standbyTasks.keySet(),
                  suspendedTasks.keySet(), suspendedStandbyTasks.keySet());

        for (final Task task : allTasks()) {
            try {
                task.close(clean);
            } catch (final RuntimeException e) {
                log.error("{} Failed while closing {} {} due to the following error:",
                          logPrefix,
                          task.getClass().getSimpleName(),
                          task.id(),
                          e);
            }
        }
        try {
            threadMetadataProvider.close();
        } catch (final Throwable e) {
            log.error("{} Failed to close KafkaStreamClient due to the following error:", logPrefix, e);
        }
        // remove the changelog partitions from restore consumer
        unAssignChangeLogPartitions();

    }

    Set<TaskId> suspendedActiveTaskIds() {
        return Collections.unmodifiableSet(suspendedTasks.keySet());
    }

    Set<TaskId> suspendedStandbyTaskIds() {
        return Collections.unmodifiableSet(suspendedStandbyTasks.keySet());
    }

    void removeTasks() {
        removeStreamTasks();
        removeStandbyTasks();
    }

    private void removeStandbyTasks() {
        log.debug("{} Removing all standby tasks {}", logPrefix, standbyTasks.keySet());
        standbyTasks.clear();
        standbyTasksByPartition.clear();
    }

    Task activeTask(final TopicPartition partition) {
        return activeTasksByPartition.get(partition);
    }

    boolean hasStandbyTasks() {
        return !standbyTasks.isEmpty();
    }

    Task standbyTask(final TopicPartition partition) {
        return standbyTasksByPartition.get(partition);
    }

    public Map<TaskId, Task> activeTasks() {
        return activeTasks;
    }

    boolean hasActiveTasks() {
        return !activeTasks.isEmpty();
    }

    void setConsumer(final Consumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    public void closeProducer() {
        taskCreator.close();
    }




    interface TaskAction {
        String name();
        void apply(final Task task);
    }
}
