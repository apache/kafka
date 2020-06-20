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
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskIdFormatException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
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

import static org.apache.kafka.common.utils.Utils.union;
import static org.apache.kafka.streams.processor.internals.StreamThread.ProcessingMode.EXACTLY_ONCE_ALPHA;
import static org.apache.kafka.streams.processor.internals.StreamThread.ProcessingMode.EXACTLY_ONCE_BETA;

public class TaskManager {
    // initialize the task list
    // activeTasks needs to be concurrent as it can be accessed
    // by QueryableState
    private final Logger log;
    private final ChangelogReader changelogReader;
    private final UUID processId;
    private final String logPrefix;
    private final StreamsMetricsImpl streamsMetrics;
    private final ActiveTaskCreator activeTaskCreator;
    private final StandbyTaskCreator standbyTaskCreator;
    private final InternalTopologyBuilder builder;
    private final Admin adminClient;
    private final StateDirectory stateDirectory;
    private final StreamThread.ProcessingMode processingMode;

    private final Map<TaskId, Task> tasks = new TreeMap<>();
    // materializing this relationship because the lookup is on the hot path
    private final Map<TopicPartition, Task> partitionToTask = new HashMap<>();

    private Consumer<byte[], byte[]> mainConsumer;

    private DeleteRecordsResult deleteRecordsResult;

    private boolean rebalanceInProgress = false;  // if we are in the middle of a rebalance, it is not safe to commit

    // includes assigned & initialized tasks and unassigned tasks we locked temporarily during rebalance
    private final Set<TaskId> lockedTaskDirectories = new HashSet<>();

    TaskManager(final ChangelogReader changelogReader,
                final UUID processId,
                final String logPrefix,
                final StreamsMetricsImpl streamsMetrics,
                final ActiveTaskCreator activeTaskCreator,
                final StandbyTaskCreator standbyTaskCreator,
                final InternalTopologyBuilder builder,
                final Admin adminClient,
                final StateDirectory stateDirectory,
                final StreamThread.ProcessingMode processingMode) {
        this.changelogReader = changelogReader;
        this.processId = processId;
        this.logPrefix = logPrefix;
        this.streamsMetrics = streamsMetrics;
        this.activeTaskCreator = activeTaskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
        this.builder = builder;
        this.adminClient = adminClient;
        this.stateDirectory = stateDirectory;
        this.processingMode = processingMode;

        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());
    }

    void setMainConsumer(final Consumer<byte[], byte[]> mainConsumer) {
        this.mainConsumer = mainConsumer;
    }

    Consumer<byte[], byte[]> mainConsumer() {
        return mainConsumer;
    }

    public UUID processId() {
        return processId;
    }

    InternalTopologyBuilder builder() {
        return builder;
    }

    boolean isRebalanceInProgress() {
        return rebalanceInProgress;
    }

    void handleRebalanceStart(final Set<String> subscribedTopics) {
        builder.addSubscribedTopicsFromMetadata(subscribedTopics, logPrefix);

        tryToLockAllNonEmptyTaskDirectories();

        rebalanceInProgress = true;
    }

    void handleRebalanceComplete() {
        // we should pause consumer only within the listener since
        // before then the assignment has not been updated yet.
        mainConsumer.pause(mainConsumer.assignment());

        releaseLockedUnassignedTaskDirectories();

        rebalanceInProgress = false;
    }

    void handleCorruption(final Map<TaskId, Collection<TopicPartition>> tasksWithChangelogs) throws TaskMigratedException {
        final Map<Task, Collection<TopicPartition>> corruptedStandbyTasks = new HashMap<>();
        final Map<Task, Collection<TopicPartition>> corruptedActiveTasks = new HashMap<>();

        for (final Map.Entry<TaskId, Collection<TopicPartition>> taskEntry : tasksWithChangelogs.entrySet()) {
            final TaskId taskId = taskEntry.getKey();
            final Task task = tasks.get(taskId);
            if (task.isActive()) {
                corruptedActiveTasks.put(task, taskEntry.getValue());
            } else {
                corruptedStandbyTasks.put(task, taskEntry.getValue());
            }
        }

        // Make sure to clean up any corrupted standby tasks in their entirety before committing
        // since TaskMigrated can be thrown and the resulting handleLostAll will only clean up active tasks
        closeAndRevive(corruptedStandbyTasks);

        commit(tasks()
                   .values()
                   .stream()
                   .filter(t -> t.state() == Task.State.RUNNING || t.state() == Task.State.RESTORING)
                   .filter(t -> !tasksWithChangelogs.containsKey(t.id()))
                   .collect(Collectors.toSet())
        );

        closeAndRevive(corruptedActiveTasks);
    }

    private void closeAndRevive(final Map<Task, Collection<TopicPartition>> taskWithChangelogs) {
        for (final Map.Entry<Task, Collection<TopicPartition>> entry : taskWithChangelogs.entrySet()) {
            final Task task = entry.getKey();

            // mark corrupted partitions to not be checkpointed, and then close the task as dirty
            final Collection<TopicPartition> corruptedPartitions = entry.getValue();
            task.markChangelogAsCorrupted(corruptedPartitions);

            try {
                task.suspend();
            } catch (final RuntimeException swallow) {
                log.error("Error suspending corrupted task {} ", task.id(), swallow);
            }
            task.closeDirty();
            task.revive();
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     * @throws StreamsException fatal error while creating / initializing the task
     *
     * public for upgrade testing only
     */
    public void handleAssignment(final Map<TaskId, Set<TopicPartition>> activeTasks,
                                 final Map<TaskId, Set<TopicPartition>> standbyTasks) {
        log.info("Handle new assignment with:\n" +
                     "\tNew active tasks: {}\n" +
                     "\tNew standby tasks: {}\n" +
                     "\tExisting active tasks: {}\n" +
                     "\tExisting standby tasks: {}",
                 activeTasks.keySet(), standbyTasks.keySet(), activeTaskIds(), standbyTaskIds());

        builder.addSubscribedTopicsFromAssignment(
            activeTasks.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
            logPrefix
        );

        final LinkedHashMap<TaskId, RuntimeException> taskCloseExceptions = new LinkedHashMap<>();

        final Map<TaskId, Set<TopicPartition>> activeTasksToCreate = new HashMap<>(activeTasks);
        final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate = new HashMap<>(standbyTasks);
        final List<Task> tasksToClose = new LinkedList<>();
        final Set<Task> tasksToRecycle = new HashSet<>();
        final Set<Task> dirtyTasks = new HashSet<>();

        // first rectify all existing tasks
        for (final Task task : tasks.values()) {
            if (activeTasks.containsKey(task.id()) && task.isActive()) {
                updateInputPartitionsAndResume(task, activeTasks.get(task.id()));
                activeTasksToCreate.remove(task.id());
            } else if (standbyTasks.containsKey(task.id()) && !task.isActive()) {
                updateInputPartitionsAndResume(task, standbyTasks.get(task.id()));
                standbyTasksToCreate.remove(task.id());
            } else if (activeTasks.containsKey(task.id()) || standbyTasks.containsKey(task.id())) {
                // check for tasks that were owned previously but have changed active/standby status
                tasksToRecycle.add(task);
            } else {
                tasksToClose.add(task);
            }
        }

        for (final Task task : tasksToClose) {
            try {
                task.suspend(); // Should be a no-op for active tasks since they're suspended in handleRevocation
                if (task.commitNeeded()) {
                    if (task.isActive()) {
                        log.error("Active task {} was revoked and should have already been committed", task.id());
                        throw new IllegalStateException("Revoked active task was not committed during handleRevocation");
                    } else {
                        task.prepareCommit();
                        task.postCommit();
                    }
                }
                completeTaskCloseClean(task);
                cleanUpTaskProducer(task, taskCloseExceptions);
                tasks.remove(task.id());
            } catch (final RuntimeException e) {
                final String uncleanMessage = String.format(
                    "Failed to close task %s cleanly. Attempting to close remaining tasks before re-throwing:",
                    task.id());
                log.error(uncleanMessage, e);
                taskCloseExceptions.put(task.id(), e);
                // We've already recorded the exception (which is the point of clean).
                // Now, we should go ahead and complete the close because a half-closed task is no good to anyone.
                dirtyTasks.add(task);
            }
        }

        for (final Task oldTask : tasksToRecycle) {
            final Task newTask;
            try {
                if (oldTask.isActive()) {
                    final Set<TopicPartition> partitions = standbyTasksToCreate.remove(oldTask.id());
                    newTask = standbyTaskCreator.createStandbyTaskFromActive((StreamTask) oldTask, partitions);
                } else {
                    oldTask.suspend(); // Only need to suspend transitioning standbys, actives should be suspended already
                    final Set<TopicPartition> partitions = activeTasksToCreate.remove(oldTask.id());
                    newTask = activeTaskCreator.createActiveTaskFromStandby((StandbyTask) oldTask, partitions, mainConsumer);
                }
                tasks.remove(oldTask.id());
                addNewTask(newTask);
            } catch (final RuntimeException e) {
                final String uncleanMessage = String.format("Failed to recycle task %s cleanly. Attempting to close remaining tasks before re-throwing:", oldTask.id());
                log.error(uncleanMessage, e);
                taskCloseExceptions.put(oldTask.id(), e);
                dirtyTasks.add(oldTask);
            }
        }

        for (final Task task : dirtyTasks) {
            closeTaskDirty(task);
            cleanUpTaskProducer(task, taskCloseExceptions);
            tasks.remove(task.id());
        }

        if (!taskCloseExceptions.isEmpty()) {
            for (final Map.Entry<TaskId, RuntimeException> entry : taskCloseExceptions.entrySet()) {
                if (!(entry.getValue() instanceof TaskMigratedException)) {
                    if (entry.getValue() instanceof KafkaException) {
                        log.error("Hit Kafka exception while closing for first task {}", entry.getKey());
                        throw entry.getValue();
                    } else {
                        throw new RuntimeException(
                            "Unexpected failure to close " + taskCloseExceptions.size() +
                                " task(s) [" + taskCloseExceptions.keySet() + "]. " +
                                "First unexpected exception (for task " + entry.getKey() + ") follows.", entry.getValue()
                        );
                    }
                }
            }

            final Map.Entry<TaskId, RuntimeException> first = taskCloseExceptions.entrySet().iterator().next();
            // If all exceptions are task-migrated, we would just throw the first one.
            throw first.getValue();
        }

        if (!activeTasksToCreate.isEmpty()) {
            for (final Task task : activeTaskCreator.createTasks(mainConsumer, activeTasksToCreate)) {
                addNewTask(task);
            }
        }

        if (!standbyTasksToCreate.isEmpty()) {
            for (final Task task : standbyTaskCreator.createTasks(standbyTasksToCreate)) {
                addNewTask(task);
            }
        }

    }

    private void cleanUpTaskProducer(final Task task,
                                     final Map<TaskId, RuntimeException> taskCloseExceptions) {
        if (task.isActive()) {
            try {
                activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
            } catch (final RuntimeException e) {
                final String uncleanMessage = String.format("Failed to close task %s cleanly. Attempting to close remaining tasks before re-throwing:", task.id());
                log.error(uncleanMessage, e);
                taskCloseExceptions.putIfAbsent(task.id(), e);
            }
        }
    }

    private void updateInputPartitionsAndResume(final Task task, final Set<TopicPartition> topicPartitions) {
        final boolean requiresUpdate = !task.inputPartitions().equals(topicPartitions);
        if (requiresUpdate) {
            log.trace("Update task {} inputPartitions: current {}, new {}", task, task.inputPartitions(), topicPartitions);
            for (final TopicPartition inputPartition : task.inputPartitions()) {
                partitionToTask.remove(inputPartition);
            }
            for (final TopicPartition topicPartition : topicPartitions) {
                partitionToTask.put(topicPartition, task);
            }
            task.update(topicPartitions, builder.nodeToSourceTopics());
        }
        task.resume();
    }

    private void addNewTask(final Task task) {
        final Task previous = tasks.put(task.id(), task);
        if (previous != null) {
            throw new IllegalStateException("Attempted to create a task that we already owned: " + task.id());
        }

        for (final TopicPartition topicPartition : task.inputPartitions()) {
            partitionToTask.put(topicPartition, task);
        }
    }

    /**
     * Tries to initialize any new or still-uninitialized tasks, then checks if they can/have completed restoration.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     * @return {@code true} if all tasks are fully restored
     */
    boolean tryToCompleteRestoration() {
        boolean allRunning = true;

        final List<Task> activeTasks = new LinkedList<>();
        for (final Task task : tasks.values()) {
            try {
                task.initializeIfNeeded();
            } catch (final LockException | TimeoutException e) {
                // it is possible that if there are multiple threads within the instance that one thread
                // trying to grab the task from the other, while the other has not released the lock since
                // it did not participate in the rebalance. In this case we can just retry in the next iteration
                log.debug("Could not initialize {} due to the following exception; will retry", task.id(), e);
                allRunning = false;
            }

            if (task.isActive()) {
                activeTasks.add(task);
            }
        }

        if (allRunning && !activeTasks.isEmpty()) {

            final Set<TopicPartition> restored = changelogReader.completedChangelogs();

            for (final Task task : activeTasks) {
                if (restored.containsAll(task.changelogPartitions())) {
                    try {
                        task.completeRestoration();
                    } catch (final TimeoutException e) {
                        log.debug("Could not complete restoration for {} due to {}; will retry", task.id(), e);

                        allRunning = false;
                    }
                } else {
                    // we found a restoring task that isn't done restoring, which is evidence that
                    // not all tasks are running
                    allRunning = false;
                }
            }
        }

        if (allRunning) {
            // we can call resume multiple times since it is idempotent.
            mainConsumer.resume(mainConsumer.assignment());
        }

        return allRunning;
    }

    /**
     * Handle the revoked partitions and prepare for closing the associated tasks in {@link #handleAssignment(Map, Map)}
     * We should commit the revoked tasks now as we will not officially own them anymore when {@link #handleAssignment(Map, Map)}
     * is called. Note that only active task partitions are passed in from the rebalance listener, so we only need to
     * consider/commit active tasks here
     *
     * If eos-beta is used, we must commit ALL tasks. Otherwise, we can just commit those (active) tasks which are revoked
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void handleRevocation(final Collection<TopicPartition> revokedPartitions) {
        final Set<TopicPartition> remainingRevokedPartitions = new HashSet<>(revokedPartitions);

        final Set<Task> tasksToCommit = new HashSet<>();
        final Set<Task> additionalTasksForCommitting = new HashSet<>();

        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        for (final Task task : activeTaskIterable()) {
            if (remainingRevokedPartitions.containsAll(task.inputPartitions())) {
                try {
                    task.suspend();
                    if (task.commitNeeded()) {
                        tasksToCommit.add(task);
                    }
                } catch (final RuntimeException e) {
                    log.error("Caught the following exception while trying to suspend revoked task " + task.id(), e);
                    firstException.compareAndSet(null, new StreamsException("Failed to suspend " + task.id(), e));
                }
            } else if (task.commitNeeded()) {
                additionalTasksForCommitting.add(task);
            }
            remainingRevokedPartitions.removeAll(task.inputPartitions());
        }

        if (!remainingRevokedPartitions.isEmpty()) {
            log.warn("The following partitions {} are missing from the task partitions. It could potentially " +
                         "due to race condition of consumer detecting the heartbeat failure, or the tasks " +
                         "have been cleaned up by the handleAssignment callback.", remainingRevokedPartitions);
        }

        final RuntimeException suspendException = firstException.get();
        if (suspendException != null) {
            throw suspendException;
        }

        // If using eos-beta, if we must commit any task then we must commit all of them
        // TODO: when KAFKA-9450 is done this will be less expensive, and we can simplify by always committing everything
        if (processingMode ==  EXACTLY_ONCE_BETA && !tasksToCommit.isEmpty()) {
            tasksToCommit.addAll(additionalTasksForCommitting);
        }

        final Map<TaskId, Map<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadataPerTask = new HashMap<>();
        for (final Task task : tasksToCommit) {
            final Map<TopicPartition, OffsetAndMetadata> committableOffsets = task.prepareCommit();
            consumedOffsetsAndMetadataPerTask.put(task.id(), committableOffsets);
        }

        commitOffsetsOrTransaction(consumedOffsetsAndMetadataPerTask);

        for (final Task task : tasksToCommit) {
            try {
                task.postCommit();
            } catch (final RuntimeException e) {
                log.error("Exception caught while post-committing task " + task.id(), e);
                firstException.compareAndSet(null, e);
            }
        }

        final RuntimeException commitException = firstException.get();
        if (commitException != null) {
            throw commitException;
        }
    }

    /**
     * Closes active tasks as zombies, as these partitions have been lost and are no longer owned.
     * NOTE this method assumes that when it is called, EVERY task/partition has been lost and must
     * be closed as a zombie.
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void handleLostAll() {
        log.debug("Closing lost active tasks as zombies.");

        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task task = iterator.next();
            // Even though we've apparently dropped out of the group, we can continue safely to maintain our
            // standby tasks while we rejoin.
            if (task.isActive()) {
                closeTaskDirty(task);
                iterator.remove();

                try {
                    activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
                } catch (final RuntimeException e) {
                    log.warn("Error closing task producer for " + task.id() + " while handling lostAll", e);
                }
            }
        }

        if (processingMode == EXACTLY_ONCE_BETA) {
            activeTaskCreator.reInitializeThreadProducer();
        }
    }

    /**
     * Compute the offset total summed across all stores in a task. Includes offset sum for any tasks we own the
     * lock for, which includes assigned and unassigned tasks we locked in {@link #tryToLockAllNonEmptyTaskDirectories()}.
     * Does not include stateless or non-logged tasks.
     */
    public Map<TaskId, Long> getTaskOffsetSums() {
        final Map<TaskId, Long> taskOffsetSums = new HashMap<>();

        // Not all tasks will create directories, and there may be directories for tasks we don't currently own,
        // so we consider all tasks that are either owned or on disk. This includes stateless tasks, which should
        // just have an empty changelogOffsets map.
        for (final TaskId id : union(HashSet::new, lockedTaskDirectories, tasks.keySet())) {
            final Task task = tasks.get(id);
            if (task != null) {
                final Map<TopicPartition, Long> changelogOffsets = task.changelogOffsets();
                if (changelogOffsets.isEmpty()) {
                    log.debug("Skipping to encode apparently stateless (or non-logged) offset sum for task {}", id);
                } else {
                    taskOffsetSums.put(id, sumOfChangelogOffsets(id, changelogOffsets));
                }
            } else {
                final File checkpointFile = stateDirectory.checkpointFileFor(id);
                try {
                    if (checkpointFile.exists()) {
                        taskOffsetSums.put(id, sumOfChangelogOffsets(id, new OffsetCheckpoint(checkpointFile).read()));
                    }
                } catch (final IOException e) {
                    log.warn(String.format("Exception caught while trying to read checkpoint for task %s:", id), e);
                }
            }
        }

        return taskOffsetSums;
    }

    /**
     * Makes a weak attempt to lock all non-empty task directories in the state dir. We are responsible for computing and
     * reporting the offset sum for any unassigned tasks we obtain the lock for in the upcoming rebalance. Tasks
     * that we locked but didn't own will be released at the end of the rebalance (unless of course we were
     * assigned the task as a result of the rebalance). This method should be idempotent.
     */
    private void tryToLockAllNonEmptyTaskDirectories() {
        // Always clear the set at the beginning as we're always dealing with the
        // current set of actually-locked tasks.
        lockedTaskDirectories.clear();

        for (final File dir : stateDirectory.listNonEmptyTaskDirectories()) {
            try {
                final TaskId id = TaskId.parse(dir.getName());
                try {
                    if (stateDirectory.lock(id)) {
                        lockedTaskDirectories.add(id);
                        if (!tasks.containsKey(id)) {
                            log.debug("Temporarily locked unassigned task {} for the upcoming rebalance", id);
                        }
                    }
                } catch (final IOException e) {
                    // if for any reason we can't lock this task dir, just move on
                    log.warn(String.format("Exception caught while attempting to lock task %s:", id), e);
                }
            } catch (final TaskIdFormatException e) {
                // ignore any unknown files that sit in the same directory
            }
        }
    }

    /**
     * We must release the lock for any unassigned tasks that we temporarily locked in preparation for a
     * rebalance in {@link #tryToLockAllNonEmptyTaskDirectories()}.
     */
    private void releaseLockedUnassignedTaskDirectories() {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        final Iterator<TaskId> taskIdIterator = lockedTaskDirectories.iterator();
        while (taskIdIterator.hasNext()) {
            final TaskId id = taskIdIterator.next();
            if (!tasks.containsKey(id)) {
                try {
                    stateDirectory.unlock(id);
                    taskIdIterator.remove();
                } catch (final IOException e) {
                    log.error(String.format("Caught the following exception while trying to unlock task %s", id), e);
                    firstException.compareAndSet(null,
                        new StreamsException(String.format("Failed to unlock task directory %s", id), e));
                }
            }
        }

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw fatalException;
        }
    }

    private long sumOfChangelogOffsets(final TaskId id, final Map<TopicPartition, Long> changelogOffsets) {
        long offsetSum = 0L;
        for (final Map.Entry<TopicPartition, Long> changelogEntry : changelogOffsets.entrySet()) {
            final long offset = changelogEntry.getValue();


            if (offset == Task.LATEST_OFFSET) {
                // this condition can only be true for active tasks; never for standby
                // for this case, the offset of all partitions is set to `LATEST_OFFSET`
                // and we "forward" the sentinel value directly
                return Task.LATEST_OFFSET;
            } else {
                if (offset < 0) {
                    throw new IllegalStateException("Expected not to get a sentinel offset, but got: " + changelogEntry);
                }
                offsetSum += offset;
                if (offsetSum < 0) {
                    log.warn("Sum of changelog offsets for task {} overflowed, pinning to Long.MAX_VALUE", id);
                    return Long.MAX_VALUE;
                }
            }
        }

        return offsetSum;
    }

    private void closeTaskDirty(final Task task) {
        try {
            task.suspend();
        } catch (final RuntimeException swallow) {
            log.error("Error suspending dirty task {} ", task.id(), swallow);
        }
        cleanupTask(task);
        task.closeDirty();
    }

    private void completeTaskCloseClean(final Task task) {
        cleanupTask(task);
        task.closeClean();
    }

    // Note: this MUST be called *before* actually closing the task
    private void cleanupTask(final Task task) {
        for (final TopicPartition inputPartition : task.inputPartitions()) {
            partitionToTask.remove(inputPartition);
        }
    }

    void shutdown(final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        final Set<Task> tasksToClose = new HashSet<>();
        final Set<Task> tasksToCommit = new HashSet<>();
        final Map<TaskId, Map<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadataPerTask = new HashMap<>();

        for (final Task task : tasks.values()) {
            if (clean) {
                try {
                    task.suspend();
                    if (task.commitNeeded()) {
                        tasksToCommit.add(task);
                        final Map<TopicPartition, OffsetAndMetadata> committableOffsets = task.prepareCommit();
                        if (task.isActive()) {
                            consumedOffsetsAndMetadataPerTask.put(task.id(), committableOffsets);
                        }
                    }
                    tasksToClose.add(task);
                } catch (final TaskMigratedException e) {
                    // just ignore the exception as it doesn't matter during shutdown
                    closeTaskDirty(task);
                } catch (final RuntimeException e) {
                    firstException.compareAndSet(null, e);
                    closeTaskDirty(task);
                }
            } else {
                closeTaskDirty(task);
            }
        }

        try {
            if (clean) {
                commitOffsetsOrTransaction(consumedOffsetsAndMetadataPerTask);
                for (final Task task : tasksToCommit) {
                    try {
                        task.postCommit();
                    } catch (final RuntimeException e) {
                        log.error("Exception caught while post-committing task " + task.id(), e);
                        firstException.compareAndSet(null, e);
                    }
                }
            }
        } catch (final RuntimeException e) {
            log.error("Exception caught while committing tasks during shutdown", e);
            firstException.compareAndSet(null, e);
        }

        for (final Task task : tasksToClose) {
            try {
                completeTaskCloseClean(task);
            } catch (final RuntimeException e) {
                firstException.compareAndSet(null, e);
                closeTaskDirty(task);
            }
        }

        for (final Task task : tasks.values()) {
            if (task.isActive()) {
                try {
                    activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
                } catch (final RuntimeException e) {
                    if (clean) {
                        firstException.compareAndSet(null, e);
                    } else {
                        log.warn("Ignoring an exception while closing task " + task.id() + " producer.", e);
                    }
                }
            }
        }

        tasks.clear();

        try {
            activeTaskCreator.closeThreadProducerIfNeeded();
        } catch (final RuntimeException e) {
            if (clean) {
                firstException.compareAndSet(null, e);
            } else {
                log.warn("Ignoring an exception while closing thread producer.", e);
            }
        }

        try {
            // this should be called after closing all tasks, to make sure we unlock the task dir for tasks that may
            // have still been in CREATED at the time of shutdown, since Task#close will not do so
            releaseLockedUnassignedTaskDirectories();
        } catch (final RuntimeException e) {
            firstException.compareAndSet(null, e);
        }

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw new RuntimeException("Unexpected exception while closing task", fatalException);
        }
    }

    Set<TaskId> activeTaskIds() {
        return activeTaskStream()
            .map(Task::id)
            .collect(Collectors.toSet());
    }

    Set<TaskId> standbyTaskIds() {
        return standbyTaskStream()
            .map(Task::id)
            .collect(Collectors.toSet());
    }

    Map<TaskId, Task> tasks() {
        // not bothering with an unmodifiable map, since the tasks themselves are mutable, but
        // if any outside code modifies the map or the tasks, it would be a severe transgression.
        return tasks;
    }

    Map<TaskId, Task> activeTaskMap() {
        return activeTaskStream().collect(Collectors.toMap(Task::id, t -> t));
    }

    List<Task> activeTaskIterable() {
        return activeTaskStream().collect(Collectors.toList());
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

    // For testing only.
    int commitAll() {
        return commit(tasks.values());
    }

    /**
     * Take records and add them to each respective task
     *
     * @param records Records, can be null
     */
    void addRecordsToTasks(final ConsumerRecords<byte[], byte[]> records) {
        for (final TopicPartition partition : records.partitions()) {
            final Task task = partitionToTask.get(partition);

            if (task == null) {
                log.error("Unable to locate active task for received-record partition {}. Current tasks: {}",
                    partition, toString(">"));
                throw new NullPointerException("Task was unexpectedly missing for partition " + partition);
            }

            task.addRecords(partition, records.records(partition));
        }
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     * @return number of committed offsets, or -1 if we are in the middle of a rebalance and cannot commit
     */
    int commit(final Collection<Task> tasksToCommit) {
        if (rebalanceInProgress) {
            return -1;
        } else {
            int committed = 0;
            final Map<TaskId, Map<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadataPerTask = new HashMap<>();
            for (final Task task : tasksToCommit) {
                if (task.commitNeeded()) {
                    final Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = task.prepareCommit();
                    if (task.isActive()) {
                        consumedOffsetsAndMetadataPerTask.put(task.id(), offsetAndMetadata);
                    }
                }
            }

            commitOffsetsOrTransaction(consumedOffsetsAndMetadataPerTask);

            for (final Task task : tasksToCommit) {
                if (task.commitNeeded()) {
                    ++committed;
                    task.postCommit();
                }
            }

            return committed;
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
            for (final Task task : activeTaskIterable()) {
                if (task.commitRequested() && task.commitNeeded()) {
                    return commit(activeTaskIterable());
                }
            }
            return 0;
        }
    }

    private void commitOffsetsOrTransaction(final Map<TaskId, Map<TopicPartition, OffsetAndMetadata>> offsetsPerTask) {
        if (!offsetsPerTask.isEmpty()) {
            if (processingMode == EXACTLY_ONCE_ALPHA) {
                for (final Map.Entry<TaskId, Map<TopicPartition, OffsetAndMetadata>> taskToCommit : offsetsPerTask.entrySet()) {
                    activeTaskCreator.streamsProducerForTask(taskToCommit.getKey())
                        .commitTransaction(taskToCommit.getValue(), mainConsumer.groupMetadata());
                }
            } else {
                final Map<TopicPartition, OffsetAndMetadata> allOffsets = offsetsPerTask.values().stream()
                    .flatMap(e -> e.entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                if (processingMode == EXACTLY_ONCE_BETA) {
                    activeTaskCreator.threadProducer().commitTransaction(allOffsets, mainConsumer.groupMetadata());
                } else {
                    try {
                        mainConsumer.commitSync(allOffsets);
                    } catch (final CommitFailedException error) {
                        throw new TaskMigratedException("Consumer committing offsets failed, " +
                                                            "indicating the corresponding thread is no longer part of the group", error);
                    } catch (final TimeoutException error) {
                        // TODO KIP-447: we can consider treating it as non-fatal and retry on the thread level
                        throw new StreamsException("Timed out while committing offsets via consumer", error);
                    } catch (final KafkaException error) {
                        throw new StreamsException("Error encountered committing offsets via consumer", error);
                    }
                }
            }
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int process(final int maxNumRecords, final Time time) {
        int totalProcessed = 0;

        long now = time.milliseconds();
        for (final Task task : activeTaskIterable()) {
            try {
                int processed = 0;
                final long then = now;
                while (processed < maxNumRecords && task.process(now)) {
                    processed++;
                }
                now = time.milliseconds();
                totalProcessed += processed;
                task.recordProcessBatchTime(now - then);
            } catch (final TaskMigratedException e) {
                log.info("Failed to process stream task {} since it got migrated to another thread already. " +
                             "Will trigger a new rebalance and close all tasks as zombies together.", task.id());
                throw e;
            } catch (final RuntimeException e) {
                log.error("Failed to process stream task {} due to the following error:", task.id(), e);
                throw e;
            }
        }

        return totalProcessed;
    }

    void recordTaskProcessRatio(final long totalProcessLatencyMs, final long now) {
        for (final Task task : activeTaskIterable()) {
            task.recordProcessTimeRatioAndBufferSize(totalProcessLatencyMs, now);
        }
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
                for (final Map.Entry<TopicPartition, Long> entry : task.purgeableOffsets().entrySet()) {
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

    Map<MetricName, Metric> producerMetrics() {
        return activeTaskCreator.producerMetrics();
    }

    Set<String> producerClientIds() {
        return activeTaskCreator.producerClientIds();
    }

    Set<TaskId> lockedTaskDirectories() {
        return Collections.unmodifiableSet(lockedTaskDirectories);
    }
}
