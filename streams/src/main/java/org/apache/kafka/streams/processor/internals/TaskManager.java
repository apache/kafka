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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singleton;

class TaskManager {
    // initialize the task list
    // activeTasks needs to be concurrent as it can be accessed
    // by QueryableState
    private static final Logger log = LoggerFactory.getLogger(TaskManager.class);
    private final AssignedTasks active;
    private final AssignedTasks standby;
    private final ChangelogReader changelogReader;
    private final String logPrefix;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final StreamThread.AbstractTaskCreator taskCreator;
    private final StreamThread.AbstractTaskCreator standbyTaskCreator;
    private ThreadMetadataProvider threadMetadataProvider;
    private Consumer<byte[], byte[]> consumer;

    TaskManager(final ChangelogReader changelogReader,
                final String logPrefix,
                final Consumer<byte[], byte[]> restoreConsumer,
                final StreamThread.AbstractTaskCreator taskCreator,
                final StreamThread.AbstractTaskCreator standbyTaskCreator,
                final AssignedTasks active,
                final AssignedTasks standby) {
        this.changelogReader = changelogReader;
        this.logPrefix = logPrefix;
        this.restoreConsumer = restoreConsumer;
        this.taskCreator = taskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
        this.active = active;
        this.standby = standby;
    }

    void createTasks(final Collection<TopicPartition> assignment) {
        if (threadMetadataProvider == null) {
            throw new IllegalStateException(logPrefix + " taskIdProvider has not been initialized while adding stream tasks. This should not happen.");
        }
        if (consumer == null) {
            throw new IllegalStateException(logPrefix + " consumer has not been initialized while adding stream tasks. This should not happen.");
        }

        changelogReader.reset();
        // do this first as we may have suspended standby tasks that
        // will become active or vice versa
        standby.closeNonAssignedSuspendedTasks(threadMetadataProvider.standbyTasks());
        Map<TaskId, Set<TopicPartition>> assignedActiveTasks = threadMetadataProvider.activeTasks();
        active.closeNonAssignedSuspendedTasks(assignedActiveTasks);
        addStreamTasks(assignment);
        addStandbyTasks();
        final Set<TopicPartition> partitions = active.uninitializedPartitions();
        log.trace("{} pausing partitions: {}", logPrefix, partitions);
        consumer.pause(partitions);
    }

    void setThreadMetadataProvider(final ThreadMetadataProvider threadMetadataProvider) {
        this.threadMetadataProvider = threadMetadataProvider;
    }

    private void addStreamTasks(final Collection<TopicPartition> assignment) {
        Map<TaskId, Set<TopicPartition>> assignedTasks = threadMetadataProvider.activeTasks();
        if (assignedTasks.isEmpty()) {
            return;
        }
        final Map<TaskId, Set<TopicPartition>> newTasks = new HashMap<>();
        // collect newly assigned tasks and reopen re-assigned tasks
        log.debug("{} Adding assigned tasks as active: {}", logPrefix, assignedTasks);
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : assignedTasks.entrySet()) {
            final TaskId taskId = entry.getKey();
            final Set<TopicPartition> partitions = entry.getValue();

            if (assignment.containsAll(partitions)) {
                try {
                    if (!active.maybeResumeSuspendedTask(taskId, partitions)) {
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

        if (newTasks.isEmpty()) {
            return;
        }

        // create all newly assigned tasks (guard against race condition with other thread via backoff and retry)
        // -> other thread will call removeSuspendedTasks(); eventually
        log.trace("{} New active tasks to be created: {}", logPrefix, newTasks);

        for (final Task task : taskCreator.createTasks(consumer, newTasks)) {
            active.addNewTask(task);
        }
    }

    private void addStandbyTasks() {
        final Map<TaskId, Set<TopicPartition>> assignedStandbyTasks = threadMetadataProvider.standbyTasks();
        if (assignedStandbyTasks.isEmpty()) {
            return;
        }
        log.debug("{} Adding assigned standby tasks {}", logPrefix, assignedStandbyTasks);
        final Map<TaskId, Set<TopicPartition>> newStandbyTasks = new HashMap<>();
        // collect newly assigned standby tasks and reopen re-assigned standby tasks
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : assignedStandbyTasks.entrySet()) {
            final TaskId taskId = entry.getKey();
            final Set<TopicPartition> partitions = entry.getValue();
            if (!standby.maybeResumeSuspendedTask(taskId, partitions)) {
                newStandbyTasks.put(taskId, partitions);
            }

        }

        if (newStandbyTasks.isEmpty()) {
            return;
        }

        // create all newly assigned standby tasks (guard against race condition with other thread via backoff and retry)
        // -> other thread will call removeSuspendedStandbyTasks(); eventually
        log.trace("{} New standby tasks to be created: {}", logPrefix, newStandbyTasks);

        for (final Task task : standbyTaskCreator.createTasks(consumer, newStandbyTasks)) {
            standby.addNewTask(task);
        }
    }

    Set<TaskId> activeTaskIds() {
        return active.allAssignedTaskIds();
    }

    Set<TaskId> standbyTaskIds() {
        return standby.allAssignedTaskIds();
    }

    Set<TaskId> prevActiveTaskIds() {
        return active.previousTaskIds();
    }

    /**
     * Similar to shutdownTasksAndState, however does not close the task managers, in the hope that
     * soon the tasks will be assigned again
     */
    void suspendTasksAndState()  {
        log.debug("{} Suspending all active tasks {} and standby tasks {}",
                  logPrefix, active.runningTaskIds(), standby.runningTaskIds());

        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        firstException.compareAndSet(null, active.suspend());
        firstException.compareAndSet(null, standby.suspend());
        // remove the changelog partitions from restore consumer
        firstException.compareAndSet(null, unAssignChangeLogPartitions());

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

    void shutdown(final boolean clean) {
        log.debug("{} Shutting down all active tasks {}, standby tasks {}, suspended tasks {}, and suspended standby tasks {}",
                  logPrefix, active.runningTaskIds(), standby.runningTaskIds(),
                  active.previousTaskIds(), standby.previousTaskIds());

        active.close(clean);
        standby.close(clean);
        try {
            threadMetadataProvider.close();
        } catch (final Throwable e) {
            log.error("{} Failed to close KafkaStreamClient due to the following error:", logPrefix, e);
        }
        // remove the changelog partitions from restore consumer
        unAssignChangeLogPartitions();
        taskCreator.close();

    }

    Set<TaskId> suspendedActiveTaskIds() {
        return active.previousTaskIds();
    }

    Set<TaskId> suspendedStandbyTaskIds() {
        return standby.previousTaskIds();
    }

    Task activeTask(final TopicPartition partition) {
        return active.runningTaskFor(partition);
    }


    Task standbyTask(final TopicPartition partition) {
        return standby.runningTaskFor(partition);
    }

    Map<TaskId, Task> activeTasks() {
        return active.runningTaskMap();
    }

    Map<TaskId, Task> standbyTasks() {
        return standby.runningTaskMap();
    }

    void setConsumer(final Consumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }


    boolean updateNewAndRestoringTasks() {
        active.initializeNewTasks();
        standby.initializeNewTasks();

        final Collection<TopicPartition> restored = changelogReader.restore();
        final Set<TopicPartition> resumed = active.updateRestored(restored);

        if (!resumed.isEmpty()) {
            log.trace("{} resuming partitions {}", logPrefix, resumed);
            consumer.resume(resumed);
        }
        if (active.allTasksRunning()) {
            assignStandbyPartitions();
            return true;
        }
        return false;
    }

    boolean hasActiveRunningTasks() {
        return active.hasRunningTasks();
    }

    boolean hasStandbyRunningTasks() {
        return standby.hasRunningTasks();
    }

    private void assignStandbyPartitions() {
        final Collection<Task> running = standby.running();
        final Map<TopicPartition, Long> checkpointedOffsets = new HashMap<>();
        for (final Task standbyTask : running) {
            checkpointedOffsets.putAll(standbyTask.checkpointedOffsets());
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

    int commitAll() {
        int committed = active.commit();
        return committed + standby.commit();
    }

    int process() {
        return active.process();
    }

    int punctuate() {
        return active.punctuate();
    }

    int maybeCommitActiveTasks() {
        return active.maybeCommit();
    }

    public String toString(final String indent) {
        final StringBuilder builder = new StringBuilder();
        builder.append(indent).append("\tActive tasks:\n");
        builder.append(active.toString(indent + "\t\t"));
        builder.append(indent).append("\tStandby tasks:\n");
        builder.append(standby.toString(indent + "\t\t"));
        return builder.toString();
    }
}
