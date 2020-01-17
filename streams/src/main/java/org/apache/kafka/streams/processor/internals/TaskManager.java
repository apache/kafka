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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskIdFormatException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.apache.kafka.streams.processor.internals.Task.State.RESTORING;
import static org.apache.kafka.streams.processor.internals.Task.State.RUNNING;

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
    private final StreamThread.TaskCreator taskCreator;
    private final StreamThread.AbstractTaskCreator<StandbyTask> standbyTaskCreator;

    private final Admin adminClient;
    private DeleteRecordsResult deleteRecordsResult;
    private boolean rebalanceInProgress = false;  // if we are in the middle of a rebalance, it is not safe to commit

    // the restore consumer is only ever assigned changelogs from restoring tasks or standbys (but not both)
    private boolean restoreConsumerAssignedStandbys = false;

    // following information is updated during rebalance phase by the partition assignor
    private Map<TopicPartition, TaskId> partitionsToTaskId = new HashMap<>();
    private Map<TopicPartition, Task> partitionToTask = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> activeTasksToCreate = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> standbyTasksToCreate = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> addedActiveTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> addedStandbyTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> revokedActiveTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> revokedStandbyTasks = new HashMap<>();
    private Map<TaskId, Task> tasks = new TreeMap<>();

    private Consumer<byte[], byte[]> consumer;

    TaskManager(final ChangelogReader changelogReader,
                final UUID processId,
                final String logPrefix,
                final Consumer<byte[], byte[]> restoreConsumer,
                final StreamsMetadataState streamsMetadataState,
                final StreamThread.TaskCreator taskCreator,
                final StreamThread.StandbyTaskCreator standbyTaskCreator,
                final Admin adminClient,
                final AssignedStreamsTasks active,
                final AssignedStandbyTasks standby) {
        this(changelogReader, processId, logPrefix, restoreConsumer, taskCreator, standbyTaskCreator, adminClient, active, standby);
    }

    TaskManager(final ChangelogReader changelogReader,
                final UUID processId,
                final String logPrefix,
                final Consumer<byte[], byte[]> restoreConsumer,
                final StreamThread.TaskCreator taskCreator,
                final StreamThread.StandbyTaskCreator standbyTaskCreator,
                final Admin adminClient,
                final AssignedStreamsTasks active,
                final AssignedStandbyTasks standby) {
        this.changelogReader = changelogReader;
        this.processId = processId;
        this.logPrefix = logPrefix;
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

    public void handleAssignment(final Map<TaskId, Set<TopicPartition>> activeTasks,
                                 final Map<TaskId, Set<TopicPartition>> standbyTasks) {
        final Map<TaskId, Set<TopicPartition>> activeTasksToCreate = new TreeMap<>(activeTasks);

        final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate = new TreeMap<>(standbyTasks);

        // first rectify all existing tasks
        final LinkedHashMap<TaskId, RuntimeException> taskCloseExceptions = new LinkedHashMap<>();
        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task task = iterator.next();
            if (activeTasks.containsKey(task.id()) && task instanceof StreamTask) {
                task.resume();
                activeTasksToCreate.remove(task.id());
            } else if (standbyTasks.containsKey(task.id()) && task instanceof StandbyTask) {
                task.resume();
                standbyTasksToCreate.remove(task.id());
            } else /* we previously task, and we don't have it anymore, or it has changed active/standby state */ {
                try {
                    task.closeClean();
                } catch (final RuntimeException e) {
                    log.error(
                        "Failed to close {} cleanly. Attempting to close remaining tasks before re-throwing.",
                        task
                    );
                    taskCloseExceptions.put(task.id(), e);
                }
                iterator.remove();
            }
        }

        if (!taskCloseExceptions.isEmpty()) {
            final Map.Entry<TaskId, RuntimeException> first = taskCloseExceptions.entrySet().iterator().next();
            throw new RuntimeException(
                "Unexpected failure to close " + taskCloseExceptions.size() +
                    " task(s) [" + taskCloseExceptions.keySet() + "]. " +
                    "First exception (for task "+first.getKey()+") follows.", first.getValue()
            );
        }

        for (final StreamTask task : taskCreator.createTasks(consumer, activeTasksToCreate)) {
            tasks.put(task.id(), task);
        }

        for (final StandbyTask task : standbyTaskCreator.createTasks(consumer, standbyTasksToCreate)) {
            tasks.put(task.id(), task);
        }
    }

    /**
     * Returns ids of tasks whose states are kept on the local storage. This includes active, standby, and previously
     * assigned but not yet cleaned up tasks
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
     * Similar to shutdownTasksAndState, however does not close the task managers, in the hope that
     * soon the tasks will be assigned again.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void handleRevocation(final Collection<TopicPartition> revokedPartitions) {
        final Set<TaskId> revokedTasks = partitionsToTaskSet(revokedPartitions);
        for (final TaskId taskId : revokedTasks) {
            final Task task = tasks.get(taskId);
            task.suspend();
        }
    }

    /**
     * Closes active tasks as zombies, as these partitions have been lost and are no longer owned.
     * NOTE this method assumes that when it is called, EVERY task/partition has been lost and must
     * be closed as a zombie.
     */
    void closeLostTasks() {
        log.debug("Closing lost active tasks as zombies.");

        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task task = iterator.next();
            // Even though we've apparently dropped out of the group, we can continue safely to maintain our
            // standby tasks while we rejoin.
            if (task instanceof StreamTask) {
                task.closeDirty();
            }
            iterator.remove();
        }
    }

    void shutdown(final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        try {
            active.shutdown(clean);
        } catch (final RuntimeException fatalException) {
            firstException.compareAndSet(null, fatalException);
        }
        standby.shutdown(clean);
        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task next = iterator.next();
            next.transitionTo(Task.State.SUSPENDED);
            next.transitionTo(Task.State.CLOSED);
            iterator.remove();
        }

        // remove the changelog partitions from restore consumer
        try {
            restoreConsumer.unsubscribe();
        } catch (final RuntimeException fatalException) {
            firstException.compareAndSet(null, fatalException);
        }
        taskCreator.close();

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw fatalException;
        }
    }

    public Set<TaskId> previousRunningTaskIds() {
        return tasks.values()
                    .stream()
                    .filter(t -> t instanceof StreamTask && t.state() == Task.State.SUSPENDED)
                    .map(Task::id)
                    .collect(Collectors.toSet());
    }

    public Set<TaskId> activeTaskIds() {
        return tasks.values()
                    .stream()
                    .filter(t -> t instanceof StreamTask)
                    .map(Task::id)
                    .collect(Collectors.toSet());
    }

    Set<TaskId> standbyTaskIds() {
        return tasks.values()
                    .stream()
                    .filter(t -> t instanceof StandbyTask)
                    .map(Task::id)
                    .collect(Collectors.toSet());
    }

    // the following functions are for testing only
    Map<TaskId, Set<TopicPartition>> assignedActiveTasks() {
        return activeTasksToCreate;
    }

    Map<TaskId, Set<TopicPartition>> assignedStandbyTasks() {
        return standbyTasksToCreate;
    }

    StreamTask activeTask(final TopicPartition partition) {
        for (final Task task : tasks.values()) {
            if (task instanceof StreamTask && task.partitions().contains(partition)) {
                return (StreamTask) task;
            }
        }
        return null;
    }

    StandbyTask standbyTask(final TopicPartition partition) {
        for (final Task task : tasks.values()) {
            if (task instanceof StandbyTask && task.partitions().contains(partition)) {
                return (StandbyTask) task;
            }
        }
        return null;
    }

    Map<TaskId, StreamTask> activeTasks() {
        return tasks.values().stream().filter(t -> t instanceof StreamTask).map(t -> (StreamTask) t).collect(Collectors.toMap(Task::id, t -> t));
    }

    Map<TaskId, StandbyTask> standbyTasks() {
        return tasks.values().stream().filter(t -> t instanceof StandbyTask).map(t -> (StandbyTask) t).collect(Collectors.toMap(Task::id, t -> t));
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
        final List<Task> restoringTasks = new LinkedList<>();
        for (final Task task : tasks.values()) {
            task.initializeIfNeeded();
            if (task.state() == RESTORING) {
                restoringTasks.add(task);
            }
        }

        if (!restoringTasks.isEmpty()) {
            changelogReader.restore();
            final Set<TopicPartition> restored = changelogReader.completedChangelogs();
            for (final Task task : restoringTasks) {
                if (restored.containsAll(task.changelogPartitions())) {
                    task.startRunning();
                }
            }
        }

        for (final Task task : tasks.values()) {
            // TODO, can we make StandbyTasks partitions always empty (since they don't process any inputs)?
            // If so, we can simplify this logic here, as the resume would be a no-op.
            if (task instanceof StreamTask && task.state() == RUNNING) {
                consumer.resume(task.partitions());
            }
        }

        // NOTE: left off here...
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
        for (final Task task : tasks.values()) {
            if (task instanceof StreamTask && task.state() == Task.State.RUNNING) {
                return true;
            }
        }
        return false;
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

        log.debug("Assigning and seeking restoreConsumer to {}", checkpointedOffsets);
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

    public void setRebalanceInProgress(final boolean rebalanceInProgress) {
        this.rebalanceInProgress = rebalanceInProgress;
    }

    public void setPartitionsToTaskId(final Map<TopicPartition, TaskId> partitionsToTaskId) {
        this.partitionsToTaskId = partitionsToTaskId;
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
     * @return number of committed offsets, or -1 if we are in the middle of a rebalance and cannot commit
     */
    int commitAll() {
        return rebalanceInProgress ? -1 : active.commit() + standby.commit();
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
        return rebalanceInProgress ? -1 : active.maybeCommitPerUserRequested();
    }

    void maybePurgeCommitedRecords() {
        // we do not check any possible exceptions since none of them are fatal
        // that should cause the application to fail, and we will try delete with
        // newer offsets anyways.
        if (deleteRecordsResult == null || deleteRecordsResult.all().isDone()) {

            if (deleteRecordsResult != null && deleteRecordsResult.all().isCompletedExceptionally()) {
                log.debug("Previous delete-records request has failed: {}. Try sending the new request now",
                          deleteRecordsResult.lowWatermarks());
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
        builder.append(indent).append("\tActive tasks:\n");
        builder.append(active.toString(indent + "\t\t"));
        builder.append(indent).append("\tStandby tasks:\n");
        builder.append(standby.toString(indent + "\t\t"));
        return builder.toString();
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
}
