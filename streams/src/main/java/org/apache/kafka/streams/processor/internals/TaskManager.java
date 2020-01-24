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
import org.apache.kafka.common.KafkaException;
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
import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.Task.State.RESTORING;

public class TaskManager {
    // initialize the task list
    // activeTasks needs to be concurrent as it can be accessed
    // by QueryableState
    private final Logger log;
    private final UUID processId;
    private final ChangelogReader changelogReader;
    private final String logPrefix;
    private final StreamThread.AbstractTaskCreator<? extends Task> taskCreator;
    private final StreamThread.AbstractTaskCreator<? extends Task> standbyTaskCreator;

    private final Admin adminClient;
    private DeleteRecordsResult deleteRecordsResult;
    private boolean rebalanceInProgress = false;  // if we are in the middle of a rebalance, it is not safe to commit

    private final Map<TaskId, Task> tasks = new TreeMap<>();

    private Consumer<byte[], byte[]> consumer;
    private final InternalTopologyBuilder builder;

    TaskManager(final ChangelogReader changelogReader,
                final UUID processId,
                final String logPrefix,
                final StreamThread.AbstractTaskCreator<? extends Task> taskCreator,
                final StreamThread.AbstractTaskCreator<? extends Task> standbyTaskCreator,
                final InternalTopologyBuilder builder,
                final Admin adminClient) {
        this.changelogReader = changelogReader;
        this.processId = processId;
        this.logPrefix = logPrefix;
        this.taskCreator = taskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
        this.builder = builder;
        final LogContext logContext = new LogContext(logPrefix);

        log = logContext.logger(getClass());

        this.adminClient = adminClient;
    }

    void setConsumer(final Consumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    public UUID processId() {
        return processId;
    }

    InternalTopologyBuilder builder() {
        return builder;
    }

    public Admin adminClient() {
        return adminClient;
    }

    public void handleRebalanceStart() {
        rebalanceInProgress = true;
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
            if (activeTasks.containsKey(task.id()) && task.isActive()) {
                task.resume();
                activeTasksToCreate.remove(task.id());
            } else if (standbyTasks.containsKey(task.id()) && !task.isActive()) {
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
                    "First exception (for task " + first.getKey() + ") follows.", first.getValue()
            );
        }

        if (!activeTasksToCreate.isEmpty()) {
            taskCreator.createTasks(consumer, activeTasksToCreate).forEach(this::addNewTask);
        }

        if (!standbyTasksToCreate.isEmpty()) {
            standbyTaskCreator.createTasks(consumer, standbyTasksToCreate).forEach(this::addNewTask);
        }

        builder.addSubscribedTopics(
            activeTasks.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
            logPrefix
        );

        rebalanceInProgress = false;
    }

    private void addNewTask(final Task task) {
        final Task previous = tasks.put(task.id(), task);
        if (previous != null) {
            throw new IllegalStateException("Attempted to create a task that we already owned: " + task.id());
        }
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

        boolean allRunning = true;
        if (!restoringTasks.isEmpty()) {
            changelogReader.restore();
            final Set<TopicPartition> restored = changelogReader.completedChangelogs();
            for (final Task task : restoringTasks) {
                if (restored.containsAll(task.changelogPartitions())) {
                    task.completeInitializationAfterRestore();
                } else {
                    // we found a restoring task that isn't done restoring, which is evidence that
                    // not all tasks are running
                    allRunning = false;
                }
            }
        }

        // Note: if we wanted to process ready tasks while others are still restoring, all we have to do
        // is delete this conditional and run the loop always.
        if (allRunning) {
            for (final Task task : tasks.values()) {
                // TODO, can we make StandbyTasks partitions always empty (since they don't process any inputs)?
                // If so, we can simplify this logic here, as the resume would be a no-op.
                if (task.isActive()) {
                    consumer.resume(task.inputPartitions());
                }
            }
        }
        return allRunning;
    }

    /**
     * Similar to shutdownTasksAndState, however does not close the task managers, in the hope that
     * soon the tasks will be assigned again.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void handleRevocation(final Collection<TopicPartition> revokedPartitions) {
        final Set<TaskId> revokedTasks = new HashSet<>();

        for (final Task task : tasks.values()) {
            for (final TopicPartition partition : revokedPartitions) {
                if (task.inputPartitions().contains(partition)) {
                    revokedTasks.add(task.id());
                    break;
                }
            }
        }

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
    void handleTaskLoss() {
        log.debug("Closing lost active tasks as zombies.");

        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task task = iterator.next();
            // Even though we've apparently dropped out of the group, we can continue safely to maintain our
            // standby tasks while we rejoin.
            if (task.isActive()) {
                task.closeDirty();
            }
            iterator.remove();
        }
    }

    /**
     * Returns ids of tasks whose states are kept on the local storage. This includes active, standby, and previously
     * assigned but not yet cleaned up tasks
     */
    public Set<TaskId> tasksOnLocalStorage() {
        // A client could contain some inactive tasks whose states are still kept on the local storage in the following scenarios:
        // 1) the client is actively maintaining standby tasks by maintaining their states from the change log.
        // 2) the client has just got some tasks migrated out of itself to other clients while these task states
        //    have not been cleaned up yet (this can happen in a rolling bounce upgrade, for example).

        final Set<TaskId> locallyStoredTasks = new HashSet<>();

        final File[] stateDirs = taskCreator.stateDirectory().listTaskDirectories();
        if (stateDirs != null) {
            for (final File dir : stateDirs) {
                try {
                    final TaskId id = TaskId.parse(dir.getName());
                    // if the checkpoint file exists, the state is valid.
                    if (new File(dir, StateManagerUtil.CHECKPOINT_FILE_NAME).exists()) {
                        locallyStoredTasks.add(id);
                    }
                } catch (final TaskIdFormatException e) {
                    // there may be some unknown files that sits in the same directory,
                    // we should ignore these files instead trying to delete them as well
                }
            }
        }

        return locallyStoredTasks;
    }

    void shutdown(final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task task = iterator.next();
            if (clean) {
                try {
                    task.closeClean();
                } catch (final TaskMigratedException e) {
                    // just ignore the exception as it doesn't matter during shutdown
                    task.closeDirty();
                } catch (final RuntimeException e) {
                    firstException.compareAndSet(null, e);
                    task.closeDirty();
                }
            } else {
                task.closeDirty();
            }
            iterator.remove();
        }

        taskCreator.close();

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw fatalException;
        }
    }

    public Set<TaskId> activeTaskIds() {
        return activeTaskStream()
            .map(Task::id)
            .collect(Collectors.toSet());
    }

    Set<TaskId> standbyTaskIds() {
        return standbyTaskStream()
            .map(Task::id)
            .collect(Collectors.toSet());
    }

    StreamTask streamTask(final TopicPartition partition) {
        for (final Task task : activeTaskIterable()) {
            if (task.inputPartitions().contains(partition)) {
                return (StreamTask) task;
            }
        }
        return null;
    }

    StandbyTask standbyTask(final TopicPartition partition) {
        for (final Task task : (Iterable<Task>) standbyTaskStream()::iterator) {
            if (task.inputPartitions().contains(partition)) {
                return (StandbyTask) task;
            }
        }
        return null;
    }

    Map<TaskId, Task> tasks() {
        // not bothering with an unmodifiable map, since the tasks themselves are mutable, but
        // if any outside code modifies the map or the tasks, it would be a severe transgression.
        return tasks;
    }

    // TODO K9113: this is used from StreamThread only for a hack to collect metrics from the record collectors inside of StreamTasks
    // Instead, we should register and record the metrics properly inside of the record collector.
    Map<TaskId, StreamTask> fixmeStreamTasks() {
        return tasks.values().stream().filter(t -> t instanceof StreamTask).map(t -> (StreamTask) t).collect(Collectors.toMap(Task::id, t -> t));
    }

    Map<TaskId, Task> activeTaskMap() {
        return activeTaskStream().collect(Collectors.toMap(Task::id, t -> t));
    }

    Iterable<Task> activeTaskIterable() {
        return activeTaskStream()::iterator;
    }

    private Stream<Task> activeTaskStream() {
        return tasks.values().stream().filter(Task::isActive);
    }

    Map<TaskId, Task> standbyTaskMap() {
        return standbyTaskStream().collect(Collectors.toMap(Task::id, t -> t));
    }

    private Stream<Task> standbyTaskStream() {
        return tasks.values().stream().filter(t -> !t.isActive());
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     * @return number of committed offsets, or -1 if we are in the middle of a rebalance and cannot commit
     */
    int commitAll() {
        if (rebalanceInProgress) {
            return -1;
        } else {
            int commits = 0;
            for (final Task task : tasks.values()) {
                if (task.commitNeeded()) {
                    task.commit();
                    commits++;
                }
            }
            return commits;
        }
    }


    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int maybeCommitActiveTasksPerUserRequested() {
        if (rebalanceInProgress) {
            return -1;
        } else {
            int commits = 0;
            for (final Task task : activeTaskIterable()) {
                if (task.commitRequested() && task.commitNeeded()) {
                    task.commit();
                    commits++;
                }
            }
            return commits;
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int process(final long now) {
        int processed = 0;

        for (final Task task : activeTaskIterable()) {
            try {
                if (task.process(now)) {
                    processed++;
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to process stream task {} since it got migrated to another thread already. " +
                             "Will trigger a new rebalance and close all tasks as zombies together.", task.id());
                throw e;
            } catch (final RuntimeException e) {
                log.error("Failed to process stream task {} due to the following error:", task.id(), e);
                throw e;
            }
        }

        return processed;
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int punctuate() {
        int punctuated = 0;

        for (final Task task : activeTaskIterable()) {
            try {
                if (task.maybePunctuateStreamTime()) {
                    punctuated++;
                }
                if (task.maybePunctuateSystemTime()) {
                    punctuated++;
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to punctuate stream task {} since it got migrated to another thread already. " +
                             "Will trigger a new rebalance and close all tasks as zombies together.", task.id());
                throw e;
            } catch (final KafkaException e) {
                log.error("Failed to punctuate stream task {} due to the following error:", task.id(), e);
                throw e;
            }
        }
        return punctuated;
    }

    void maybePurgeCommittedRecords() {
        // we do not check any possible exceptions since none of them are fatal
        // that should cause the application to fail, and we will try delete with
        // newer offsets anyways.
        if (deleteRecordsResult == null || deleteRecordsResult.all().isDone()) {

            if (deleteRecordsResult != null && deleteRecordsResult.all().isCompletedExceptionally()) {
                log.debug("Previous delete-records request has failed: {}. Try sending the new request now",
                          deleteRecordsResult.lowWatermarks());
            }

            final Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            for (final Task task : activeTaskIterable()) {
                for (final Map.Entry<TopicPartition, Long> entry : task.purgableOffsets().entrySet()) {
                    recordsToDelete.put(entry.getKey(), RecordsToDelete.beforeOffset(entry.getValue()));
                }
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
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("TaskManager\n");
        stringBuilder.append(indent).append("\tMetadataState:\n");
        stringBuilder.append(indent).append("\tTasks:\n");
        for (final Task task : tasks.values()) {
            stringBuilder.append(indent)
                         .append("\t\t")
                         .append(task.id())
                         .append(" ")
                         .append(task.state())
                         .append(" ")
                         .append(task.getClass().getSimpleName())
                         .append('(').append(task.isActive() ? "active" : "standby").append(')');
        }
        return stringBuilder.toString();
    }

    // FIXME: inappropriately used from StreamsUpgradeTest
    public void fixmeUpdateSubscriptionsFromAssignment(final List<TopicPartition> partitions) {
        builder.addSubscribedTopics(partitions, logPrefix);
    }
}
