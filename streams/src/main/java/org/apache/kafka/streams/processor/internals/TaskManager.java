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

import java.util.ArrayList;
import org.apache.kafka.clients.admin.Admin;
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

public class TaskManager {
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

    private final Admin adminClient;
    private DeleteRecordsResult deleteRecordsResult;

    // the restore consumer is only ever assigned changelogs from restoring tasks or standbys (but not both)
    private boolean restoreConsumerAssignedStandbys = false;

    // following information is updated during rebalance phase by the partition assignor
    private Cluster cluster;
    private Map<TopicPartition, TaskId> partitionsToTaskId = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> assignedActiveTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> assignedStandbyTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> addedActiveTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> addedStandbyTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> revokedActiveTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> revokedStandbyTasks = new HashMap<>();

    private Consumer<byte[], byte[]> consumer;

    TaskManager(final ChangelogReader changelogReader,
                final UUID processId,
                final String logPrefix,
                final Consumer<byte[], byte[]> restoreConsumer,
                final StreamsMetadataState streamsMetadataState,
                final StreamThread.AbstractTaskCreator<StreamTask> taskCreator,
                final StreamThread.AbstractTaskCreator<StandbyTask> standbyTaskCreator,
                final Admin adminClient,
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

    public Admin adminClient() {
        return adminClient;
    }

    void createTasks(final Collection<TopicPartition> assignment) {
        if (consumer == null) {
            throw new IllegalStateException(logPrefix + "consumer has not been initialized while adding stream tasks. This should not happen.");
        }

        if (!assignment.isEmpty() && !assignedActiveTasks.isEmpty()) {
            resumeSuspended(assignment);
        }
        if (!addedActiveTasks.isEmpty()) {
            addNewActiveTasks(addedActiveTasks);
        }
        if (!addedStandbyTasks.isEmpty()) {
            addNewStandbyTasks(addedStandbyTasks);
        }

        // need to clear restore consumer if it was reading standbys but we have active tasks that may need restoring
        if (!addedActiveTasks.isEmpty() && restoreConsumerAssignedStandbys) {
            restoreConsumer.unsubscribe();
            restoreConsumerAssignedStandbys = false;
        }

        // Pause all the new partitions until the underlying state store is ready for all the active tasks.
        log.trace("Pausing partitions: {}", assignment);
        consumer.pause(assignment);
    }

    private void resumeSuspended(final Collection<TopicPartition> assignment) {
        final Set<TaskId> suspendedTasks = partitionsToTaskSet(assignment);
        suspendedTasks.removeAll(addedActiveTasks.keySet());

        for (final TaskId taskId : suspendedTasks) {
            final Set<TopicPartition> partitions = assignedActiveTasks.get(taskId);
            try {
                if (!active.maybeResumeSuspendedTask(taskId, partitions)) {
                    // recreate if resuming the suspended task failed because the associated partitions changed
                    addedActiveTasks.put(taskId, partitions);
                }
            } catch (final StreamsException e) {
                log.error("Failed to resume an active task {} due to the following error:", taskId, e);
                throw e;
            }
        }
    }

    private void addNewActiveTasks(final Map<TaskId, Set<TopicPartition>> newActiveTasks) {
        log.debug("New active tasks to be created: {}", newActiveTasks);

        for (final StreamTask task : taskCreator.createTasks(consumer, newActiveTasks)) {
            active.addNewTask(task);
        }
    }

    private void addNewStandbyTasks(final Map<TaskId, Set<TopicPartition>> newStandbyTasks) {
        log.trace("New standby tasks to be created: {}", newStandbyTasks);

        for (final StandbyTask task : standbyTaskCreator.createTasks(consumer, newStandbyTasks)) {
            standby.addNewTask(task);
        }
    }

    /**
     * Returns ids of tasks whose states are kept on the local storage.
     */
    public Set<TaskId> cachedTasksIds() {
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
                    if (new File(dir, StateManagerUtil.CHECKPOINT_FILE_NAME).exists()) {
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

    /**
     * Closes standby tasks that were not reassigned at the end of a rebalance.
     *
     * @return list of changelog topic partitions from revoked tasks
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    List<TopicPartition> closeRevokedStandbyTasks() {
        final List<TopicPartition> revokedChangelogs = standby.closeRevokedStandbyTasks(revokedStandbyTasks);

        // If the restore consumer is assigned any standby partitions they must be removed
        removeChangelogsFromRestoreConsumer(revokedChangelogs, true);

        return revokedChangelogs;
    }

    /**
     * Closes suspended active tasks that were not reassigned at the end of a rebalance.
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void closeRevokedSuspendedTasks() {
        // changelogs should have already been removed during suspend
        final RuntimeException exception = active.closeNotAssignedSuspendedTasks(revokedActiveTasks.keySet());

        // At this point all revoked tasks should have been closed, we can just throw the exception
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Similar to shutdownTasksAndState, however does not close the task managers, in the hope that
     * soon the tasks will be assigned again.
     * @return list of suspended tasks
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    Set<TaskId> suspendActiveTasksAndState(final Collection<TopicPartition> revokedPartitions)  {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        final List<TopicPartition> revokedChangelogs = new ArrayList<>();

        final Set<TaskId> revokedTasks = partitionsToTaskSet(revokedPartitions);

        firstException.compareAndSet(null, active.suspendOrCloseTasks(revokedTasks, revokedChangelogs));

        changelogReader.remove(revokedChangelogs);
        removeChangelogsFromRestoreConsumer(revokedChangelogs, false);

        final Exception exception = firstException.get();
        if (exception != null) {
            throw new StreamsException(logPrefix + "failed to suspend stream tasks", exception);
        }
        return active.suspendedTaskIds();
    }

    /**
     * Closes active tasks as zombies, as these partitions have been lost and are no longer owned.
     * @return list of lost tasks
     */
    Set<TaskId> closeLostTasks(final Collection<TopicPartition> lostPartitions) {
        final Set<TaskId> zombieTasks = partitionsToTaskSet(lostPartitions);
        log.debug("Closing lost tasks as zombies: {}", zombieTasks);

        final List<TopicPartition> lostTaskChangelogs = new ArrayList<>();

        final RuntimeException exception = active.closeZombieTasks(zombieTasks, lostTaskChangelogs);

        assignedActiveTasks.keySet().removeAll(zombieTasks);
        changelogReader.remove(lostTaskChangelogs);
        removeChangelogsFromRestoreConsumer(lostTaskChangelogs, false);

        if (exception != null) {
            throw exception;
        } else if (!assignedActiveTasks.isEmpty()) {
            throw new IllegalStateException("TaskManager had leftover tasks after removing all zombies");
        }

        return zombieTasks;
    }

    void shutdown(final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        log.debug("Shutting down all active tasks {}, standby tasks {}, and suspended tasks {}", active.runningTaskIds(), standby.runningTaskIds(),
                  active.suspendedTaskIds());

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

    Set<TaskId> activeTaskIds() {
        return active.allAssignedTaskIds();
    }

    Set<TaskId> standbyTaskIds() {
        return standby.allAssignedTaskIds();
    }

    Set<TaskId> revokedActiveTaskIds() {
        return revokedActiveTasks.keySet();
    }

    Set<TaskId> revokedStandbyTaskIds() {
        return revokedStandbyTasks.keySet();
    }

    public Set<TaskId> previousRunningTaskIds() {
        return active.previousRunningTaskIds();
    }

    Set<TaskId> previousActiveTaskIds() {
        final HashSet<TaskId> previousActiveTasks = new HashSet<>(assignedActiveTasks.keySet());
        previousActiveTasks.addAll(revokedActiveTasks.keySet());
        previousActiveTasks.removeAll(addedActiveTasks.keySet());
        return previousActiveTasks;
    }

    Set<TaskId> previousStandbyTaskIds() {
        final HashSet<TaskId> previousStandbyTasks = new HashSet<>(assignedStandbyTasks.keySet());
        previousStandbyTasks.addAll(revokedStandbyTasks.keySet());
        previousStandbyTasks.removeAll(addedStandbyTasks.keySet());
        return previousStandbyTasks;
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

    public UUID processId() {
        return processId;
    }

    InternalTopologyBuilder builder() {
        return taskCreator.builder();
    }

    /**
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    boolean updateNewAndRestoringTasks() {
        active.initializeNewTasks();
        standby.initializeNewTasks();

        final Collection<TopicPartition> restored = changelogReader.restore(active);
        active.updateRestored(restored);
        removeChangelogsFromRestoreConsumer(restored, false);

        if (active.allTasksRunning()) {
            final Set<TopicPartition> assignment = consumer.assignment();
            log.trace("Resuming partitions {}", assignment);
            consumer.resume(assignment);
            assignStandbyPartitions();
            return standby.allTasksRunning();
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

        restoreConsumerAssignedStandbys = true;
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

    public void setClusterMetadata(final Cluster cluster) {
        this.cluster = cluster;
    }

    public void setPartitionsByHostState(final Map<HostInfo, Set<TopicPartition>> partitionsByHostState) {
        this.streamsMetadataState.onChange(partitionsByHostState, cluster);
    }

    public void setPartitionsToTaskId(final Map<TopicPartition, TaskId> partitionsToTaskId) {
        this.partitionsToTaskId = partitionsToTaskId;
    }

    public void setAssignmentMetadata(final Map<TaskId, Set<TopicPartition>> activeTasks,
                                      final Map<TaskId, Set<TopicPartition>> standbyTasks) {
        addedActiveTasks.clear();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : activeTasks.entrySet()) {
            if (!assignedActiveTasks.containsKey(entry.getKey())) {
                addedActiveTasks.put(entry.getKey(), entry.getValue());
            }
        }

        addedStandbyTasks.clear();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : standbyTasks.entrySet()) {
            if (!assignedStandbyTasks.containsKey(entry.getKey())) {
                addedStandbyTasks.put(entry.getKey(), entry.getValue());
            }
        }

        revokedActiveTasks.clear();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : assignedActiveTasks.entrySet()) {
            if (!activeTasks.containsKey(entry.getKey())) {
                revokedActiveTasks.put(entry.getKey(), entry.getValue());
            }
        }

        revokedStandbyTasks.clear();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : assignedStandbyTasks.entrySet()) {
            if (!standbyTasks.containsKey(entry.getKey())) {
                revokedStandbyTasks.put(entry.getKey(), entry.getValue());
            }
        }

        this.assignedActiveTasks = activeTasks;
        this.assignedStandbyTasks = standbyTasks;
    }

    public void updateSubscriptionsFromAssignment(final List<TopicPartition> partitions) {
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

    public void updateSubscriptionsFromMetadata(final Set<String> topics) {
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
        final int committed = active.commit();
        return committed + standby.commit();
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int process(final long now) {
        return active.process(now);
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
    int maybeCommitActiveTasksPerUserRequested() {
        return active.maybeCommitPerUserRequested();
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
            if (!recordsToDelete.isEmpty()) {
                deleteRecordsResult = adminClient.deleteRecords(recordsToDelete);
                log.trace("Sent delete-records request: {}", recordsToDelete);
            }
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

    // this should be safe to call whether the restore consumer is assigned standby or active restoring partitions
    // as the removal will be a no-op
    private void removeChangelogsFromRestoreConsumer(final Collection<TopicPartition> changelogs, final boolean areStandbyPartitions) {
        if (!changelogs.isEmpty() && areStandbyPartitions == restoreConsumerAssignedStandbys) {
            final Set<TopicPartition> updatedAssignment = new HashSet<>(restoreConsumer.assignment());
            updatedAssignment.removeAll(changelogs);
            restoreConsumer.assign(updatedAssignment);
        }
    }

    private Set<TaskId> partitionsToTaskSet(final Collection<TopicPartition> partitions) {
        final Set<TaskId> taskIds = new HashSet<>();
        for (final TopicPartition tp : partitions) {
            final TaskId id = partitionsToTaskId.get(tp);
            if (id != null) {
                taskIds.add(id);
            } else {
                log.error("Failed to lookup taskId for partition {}", tp);
                throw new StreamsException("Found partition in assignment with no corresponding task");
            }
        }
        return taskIds;
    }

    // the following functions are for testing only
    Map<TaskId, Set<TopicPartition>> assignedActiveTasks() {
        return assignedActiveTasks;
    }

    Map<TaskId, Set<TopicPartition>> assignedStandbyTasks() {
        return assignedStandbyTasks;
    }
}
