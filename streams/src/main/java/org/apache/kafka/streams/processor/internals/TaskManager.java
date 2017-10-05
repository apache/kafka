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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

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
    private final Logger log;
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

        final LogContext logContext = new LogContext(logPrefix);

        this.log = logContext.logger(getClass());
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void createTasks(final Collection<TopicPartition> assignment) {
        if (threadMetadataProvider == null) {
            throw new IllegalStateException(logPrefix + "taskIdProvider has not been initialized while adding stream tasks. This should not happen.");
        }
        if (consumer == null) {
            throw new IllegalStateException(logPrefix + "consumer has not been initialized while adding stream tasks. This should not happen.");
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
        log.trace("pausing partitions: {}", partitions);
        consumer.pause(partitions);
    }

    void setThreadMetadataProvider(final ThreadMetadataProvider threadMetadataProvider) {
        this.threadMetadataProvider = threadMetadataProvider;
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    private void addStreamTasks(final Collection<TopicPartition> assignment) {
        Map<TaskId, Set<TopicPartition>> assignedTasks = threadMetadataProvider.activeTasks();
        if (assignedTasks.isEmpty()) {
            return;
        }
        final Map<TaskId, Set<TopicPartition>> newTasks = new HashMap<>();
        // collect newly assigned tasks and reopen re-assigned tasks
        log.debug("Adding assigned tasks as active: {}", assignedTasks);
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : assignedTasks.entrySet()) {
            final TaskId taskId = entry.getKey();
            final Set<TopicPartition> partitions = entry.getValue();

            if (assignment.containsAll(partitions)) {
                try {
                    if (!active.maybeResumeSuspendedTask(taskId, partitions)) {
                        newTasks.put(taskId, partitions);
                    }
                } catch (final StreamsException e) {
                    log.error("Failed to resume an active task {} due to the following error:", taskId, e);
                    throw e;
                }
            } else {
                log.warn("Task {} owned partitions {} are not contained in the assignment {}", taskId, partitions, assignment);
            }
        }

        if (newTasks.isEmpty()) {
            return;
        }

        // CANNOT FIND RETRY AND BACKOFF LOGIC
        // create all newly assigned tasks (guard against race condition with other thread via backoff and retry)
        // -> other thread will call removeSuspendedTasks(); eventually
        log.trace("New active tasks to be created: {}", newTasks);

        for (final Task task : taskCreator.createTasks(consumer, newTasks)) {
            active.addNewTask(task);
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    private void addStandbyTasks() {
        final Map<TaskId, Set<TopicPartition>> assignedStandbyTasks = threadMetadataProvider.standbyTasks();
        if (assignedStandbyTasks.isEmpty()) {
            return;
        }
        log.debug("Adding assigned standby tasks {}", assignedStandbyTasks);
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
        log.trace("New standby tasks to be created: {}", newStandbyTasks);

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
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void suspendTasksAndState()  {
        log.debug("Suspending all active tasks {} and standby tasks {}", active.runningTaskIds(), standby.runningTaskIds());

        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        firstException.compareAndSet(null, active.suspend());
        firstException.compareAndSet(null, standby.suspend());
        // remove the changelog partitions from restore consumer
        restoreConsumer.assign(Collections.<TopicPartition>emptyList());

        final Exception exception = firstException.get();
        if (exception != null) {
            throw new StreamsException(logPrefix + "failed to suspend stream tasks", exception);
        }
    }

    void shutdown(final boolean clean) {
        log.debug("Shutting down all active tasks {}, standby tasks {}, suspended tasks {}, and suspended standby tasks {}", active.runningTaskIds(), standby.runningTaskIds(),
                  active.previousTaskIds(), standby.previousTaskIds());

        active.close(clean);
        standby.close(clean);
        try {
            threadMetadataProvider.close();
        } catch (final Throwable e) {
            log.error("Failed to close KafkaStreamClient due to the following error:", e);
        }
        // remove the changelog partitions from restore consumer
        restoreConsumer.assign(Collections.<TopicPartition>emptyList());
        taskCreator.close();
        standbyTaskCreator.close();
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

    /**
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     * @throws TaskMigratedException if another thread wrote to the changelog topic that is currently restored
     */
    boolean updateNewAndRestoringTasks() {
        final Set<TopicPartition> resumed = active.initializeNewTasks();
        standby.initializeNewTasks();

        final Collection<TopicPartition> restored = changelogReader.restore(active);
        resumed.addAll(active.updateRestored(restored));

        if (!resumed.isEmpty()) {
            log.trace("resuming partitions {}", resumed);
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

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int commitAll() {
        int committed = active.commit();
        return committed + standby.commit();
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int process() {
        return active.process();
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int punctuate() {
        return active.punctuate();
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
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
