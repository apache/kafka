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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskIdFormatException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singleton;

class TaskManager {
    // initialize the task list
    // activeTasks needs to be concurrent as it can be accessed
    // by QueryableState
    private final Logger log;
    private final UUID processId;
    private final AssignedStreamsTasks active;
    private final AssignedStandbyTasks standby;
    private final ChangelogReader changelogReader;
    private final String logPrefix;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final StreamThread.AbstractTaskCreator<StreamTask> taskCreator;
    private final StreamThread.AbstractTaskCreator<StandbyTask> standbyTaskCreator;
    private final StreamsMetadataState streamsMetadataState;

    final AdminClient adminClient;
    private DeleteRecordsResult deleteRecordsResult;

    // following information is updated during rebalance phase by the partition assignor
    private Cluster cluster;
    private Map<TaskId, Set<TopicPartition>> assignedActiveTasks;
    private Map<TaskId, Set<TopicPartition>> assignedStandbyTasks;

    private Consumer<byte[], byte[]> consumer;

    TaskManager(final ChangelogReader changelogReader,
                final UUID processId,
                final String logPrefix,
                final Consumer<byte[], byte[]> restoreConsumer,
                final StreamsMetadataState streamsMetadataState,
                final StreamThread.AbstractTaskCreator<StreamTask> taskCreator,
                final StreamThread.AbstractTaskCreator<StandbyTask> standbyTaskCreator,
                final AdminClient adminClient,
                final AssignedStreamsTasks active,
                final AssignedStandbyTasks standby) {
        this.changelogReader = changelogReader;
        this.processId = processId;
        this.logPrefix = logPrefix;
        this.streamsMetadataState = streamsMetadataState;
        this.restoreConsumer = restoreConsumer;
        this.taskCreator = taskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
        this.active = active;
        this.standby = standby;

        final LogContext logContext = new LogContext(logPrefix);

        this.log = logContext.logger(getClass());

        this.adminClient = adminClient;
    }

    void createTasks(final Collection<TopicPartition> assignment) {
        if (consumer == null) {
            throw new IllegalStateException(logPrefix + "consumer has not been initialized while adding stream tasks. This should not happen.");
        }

        changelogReader.reset();
        // do this first as we may have suspended standby tasks that
        // will become active or vice versa
        standby.closeNonAssignedSuspendedTasks(assignedStandbyTasks);
        active.closeNonAssignedSuspendedTasks(assignedActiveTasks);
        addStreamTasks(assignment);
        addStandbyTasks();
        // Pause all the partitions until the underlying state store is ready for all the active tasks.
        log.trace("Pausing partitions: {}", assignment);
        consumer.pause(assignment);
    }

    private void addStreamTasks(final Collection<TopicPartition> assignment) {
        if (assignedActiveTasks.isEmpty()) {
            return;
        }
        final Map<TaskId, Set<TopicPartition>> newTasks = new HashMap<>();
        // collect newly assigned tasks and reopen re-assigned tasks
        log.debug("Adding assigned tasks as active: {}", assignedActiveTasks);
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : assignedActiveTasks.entrySet()) {
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

        for (final StreamTask task : taskCreator.createTasks(consumer, newTasks)) {
            active.addNewTask(task);
        }
    }

    private void addStandbyTasks() {
        final Map<TaskId, Set<TopicPartition>> assignedStandbyTasks = this.assignedStandbyTasks;
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

        for (final StandbyTask task : standbyTaskCreator.createTasks(consumer, newStandbyTasks)) {
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
     * Returns ids of tasks whose states are kept on the local storage.
     */
    Set<TaskId> cachedTasksIds() {
        // A client could contain some inactive tasks whose states are still kept on the local storage in the following scenarios:
        // 1) the client is actively maintaining standby tasks by maintaining their states from the change log.
        // 2) the client has just got some tasks migrated out of itself to other clients while these task states
        //    have not been cleaned up yet (this can happen in a rolling bounce upgrade, for example).

        final HashSet<TaskId> tasks = new HashSet<>();

        final File[] stateDirs = taskCreator.stateDirectory().listTaskDirectories();
        if (stateDirs != null) {
            for (final File dir : stateDirs) {
                try {
                    final TaskId id = TaskId.parse(dir.getName());
                    // if the checkpoint file exists, the state is valid.
                    if (new File(dir, ProcessorStateManager.CHECKPOINT_FILE_NAME).exists()) {
                        tasks.add(id);
                    }
                } catch (final TaskIdFormatException e) {
                    // there may be some unknown files that sits in the same directory,
                    // we should ignore these files instead trying to delete them as well
                }
            }
        }

        return tasks;
    }

    UUID processId() {
        return processId;
    }

    InternalTopologyBuilder builder() {
        return taskCreator.builder();
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
        restoreConsumer.unsubscribe();

        final Exception exception = firstException.get();
        if (exception != null) {
            throw new StreamsException(logPrefix + "failed to suspend stream tasks", exception);
        }
    }

    void shutdown(final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        log.debug("Shutting down all active tasks {}, standby tasks {}, suspended tasks {}, and suspended standby tasks {}", active.runningTaskIds(), standby.runningTaskIds(),
                  active.previousTaskIds(), standby.previousTaskIds());

        try {
            active.close(clean);
        } catch (final RuntimeException fatalException) {
            firstException.compareAndSet(null, fatalException);
        }
        standby.close(clean);

        // remove the changelog partitions from restore consumer
        try {
            restoreConsumer.unsubscribe();
        } catch (final RuntimeException fatalException) {
            firstException.compareAndSet(null, fatalException);
        }
        taskCreator.close();
        standbyTaskCreator.close();

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw fatalException;
        }
    }

    Set<TaskId> suspendedActiveTaskIds() {
        return active.previousTaskIds();
    }

    Set<TaskId> suspendedStandbyTaskIds() {
        return standby.previousTaskIds();
    }

    StreamTask activeTask(final TopicPartition partition) {
        return active.runningTaskFor(partition);
    }

    StandbyTask standbyTask(final TopicPartition partition) {
        return standby.runningTaskFor(partition);
    }

    Map<TaskId, StreamTask> activeTasks() {
        return active.runningTaskMap();
    }

    Map<TaskId, StandbyTask> standbyTasks() {
        return standby.runningTaskMap();
    }

    void setConsumer(final Consumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    /**
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     * @throws TaskMigratedException if the task producer got fenced or consumer discovered changelog offset changes (EOS only)
     */
    boolean updateNewAndRestoringTasks() {
        active.initializeNewTasks();
        standby.initializeNewTasks();

        final Collection<TopicPartition> restored = changelogReader.restore(active);

        active.updateRestored(restored);

        if (active.allTasksRunning()) {
            Set<TopicPartition> assignment = consumer.assignment();
            log.trace("Resuming partitions {}", assignment);
            consumer.resume(assignment);
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
        final Collection<StandbyTask> running = standby.running();
        final Map<TopicPartition, Long> checkpointedOffsets = new HashMap<>();
        for (final StandbyTask standbyTask : running) {
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

    void setClusterMetadata(final Cluster cluster) {
        this.cluster = cluster;
    }

    void setPartitionsByHostState(final Map<HostInfo, Set<TopicPartition>> partitionsByHostState) {
        this.streamsMetadataState.onChange(partitionsByHostState, cluster);
    }

    void setAssignmentMetadata(final Map<TaskId, Set<TopicPartition>> activeTasks,
                               final Map<TaskId, Set<TopicPartition>> standbyTasks) {
        this.assignedActiveTasks = activeTasks;
        this.assignedStandbyTasks = standbyTasks;
    }

    void updateSubscriptionsFromAssignment(List<TopicPartition> partitions) {
        if (builder().sourceTopicPattern() != null) {
            final Set<String> assignedTopics = new HashSet<>();
            for (final TopicPartition topicPartition : partitions) {
                assignedTopics.add(topicPartition.topic());
            }

            final Collection<String> existingTopics = builder().subscriptionUpdates().getUpdates();
            if (!existingTopics.containsAll(assignedTopics)) {
                assignedTopics.addAll(existingTopics);
                builder().updateSubscribedTopics(assignedTopics, logPrefix);
            }
        }
    }

    void updateSubscriptionsFromMetadata(Set<String> topics) {
        if (builder().sourceTopicPattern() != null) {
            final Collection<String> existingTopics = builder().subscriptionUpdates().getUpdates();
            if (!existingTopics.equals(topics)) {
                builder().updateSubscribedTopics(topics, logPrefix);
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

    void maybePurgeCommitedRecords() {
        // we do not check any possible exceptions since none of them are fatal
        // that should cause the application to fail, and we will try delete with
        // newer offsets anyways.
        if (deleteRecordsResult == null || deleteRecordsResult.all().isDone()) {

            if (deleteRecordsResult != null && deleteRecordsResult.all().isCompletedExceptionally()) {
                log.debug("Previous delete-records request has failed: {}. Try sending the new request now", deleteRecordsResult.lowWatermarks());
            }

            final Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            for (final Map.Entry<TopicPartition, Long> entry : active.recordsToDelete().entrySet()) {
                recordsToDelete.put(entry.getKey(), RecordsToDelete.beforeOffset(entry.getValue()));
            }
            deleteRecordsResult = adminClient.deleteRecords(recordsToDelete);

            log.trace("Sent delete-records request: {}", recordsToDelete);
        }
    }

    /**
     * Produces a string representation containing useful information about the TaskManager.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the TaskManager instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    public String toString(final String indent) {
        final StringBuilder builder = new StringBuilder();
        builder.append("TaskManager\n");
        builder.append(indent).append("\tMetadataState:\n");
        builder.append(streamsMetadataState.toString(indent + "\t\t"));
        builder.append(indent).append("\tActive tasks:\n");
        builder.append(active.toString(indent + "\t\t"));
        builder.append(indent).append("\tStandby tasks:\n");
        builder.append(standby.toString(indent + "\t\t"));
        return builder.toString();
    }

    // the following functions are for testing only
    Map<TaskId, Set<TopicPartition>> assignedActiveTasks() {
        return assignedActiveTasks;
    }

    Map<TaskId, Set<TopicPartition>> assignedStandbyTasks() {
        return assignedStandbyTasks;
    }
}
