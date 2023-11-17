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

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
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
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskIdFormatException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateDirectory.TaskDirectory;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.apache.kafka.streams.processor.internals.tasks.DefaultTaskManager;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.intersection;
import static org.apache.kafka.common.utils.Utils.union;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.parseTaskDirectoryName;

public class TaskManager {
    // initialize the task list
    // activeTasks needs to be concurrent as it can be accessed
    // by QueryableState
    private final Logger log;
    private final Time time;
    private final TasksRegistry tasks;
    private final UUID processId;
    private final String logPrefix;
    private final Admin adminClient;
    private final StateDirectory stateDirectory;
    private final ProcessingMode processingMode;
    private final ChangelogReader changelogReader;
    private final TopologyMetadata topologyMetadata;

    private final TaskExecutor taskExecutor;

    private Consumer<byte[], byte[]> mainConsumer;

    private DeleteRecordsResult deleteRecordsResult;

    private boolean rebalanceInProgress = false;  // if we are in the middle of a rebalance, it is not safe to commit

    // includes assigned & initialized tasks and unassigned tasks we locked temporarily during rebalance
    private final Set<TaskId> lockedTaskDirectories = new HashSet<>();

    private final ActiveTaskCreator activeTaskCreator;
    private final StandbyTaskCreator standbyTaskCreator;
    private final StateUpdater stateUpdater;
    private final DefaultTaskManager schedulingTaskManager;

    TaskManager(final Time time,
                final ChangelogReader changelogReader,
                final UUID processId,
                final String logPrefix,
                final ActiveTaskCreator activeTaskCreator,
                final StandbyTaskCreator standbyTaskCreator,
                final TasksRegistry tasks,
                final TopologyMetadata topologyMetadata,
                final Admin adminClient,
                final StateDirectory stateDirectory,
                final StateUpdater stateUpdater,
                final DefaultTaskManager schedulingTaskManager
                ) {
        this.time = time;
        this.processId = processId;
        this.logPrefix = logPrefix;
        this.adminClient = adminClient;
        this.stateDirectory = stateDirectory;
        this.changelogReader = changelogReader;
        this.topologyMetadata = topologyMetadata;
        this.activeTaskCreator = activeTaskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
        this.processingMode = topologyMetadata.processingMode();

        final LogContext logContext = new LogContext(logPrefix);
        this.log = logContext.logger(getClass());

        this.stateUpdater = stateUpdater;
        this.schedulingTaskManager = schedulingTaskManager;
        this.tasks = tasks;
        this.taskExecutor = new TaskExecutor(
            this.tasks,
            this,
            topologyMetadata.taskExecutionMetadata(),
            logContext
        );
    }

    void setMainConsumer(final Consumer<byte[], byte[]> mainConsumer) {
        this.mainConsumer = mainConsumer;
    }

    public double totalProducerBlockedTime() {
        return activeTaskCreator.totalProducerBlockedTime();
    }

    public UUID processId() {
        return processId;
    }

    public TopologyMetadata topologyMetadata() {
        return topologyMetadata;
    }

    ConsumerGroupMetadata consumerGroupMetadata() {
        return mainConsumer.groupMetadata();
    }

    void consumerCommitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        mainConsumer.commitSync(offsets);
    }

    StreamsProducer streamsProducerForTask(final TaskId taskId) {
        return activeTaskCreator.streamsProducerForTask(taskId);
    }

    StreamsProducer threadProducer() {
        return activeTaskCreator.threadProducer();
    }

    boolean rebalanceInProgress() {
        return rebalanceInProgress;
    }

    void handleRebalanceStart(final Set<String> subscribedTopics) {
        topologyMetadata.addSubscribedTopicsFromMetadata(subscribedTopics, logPrefix);

        tryToLockAllNonEmptyTaskDirectories();

        rebalanceInProgress = true;
    }

    void handleRebalanceComplete() {
        // we should pause consumer only within the listener since
        // before then the assignment has not been updated yet.
        if (stateUpdater == null) {
            mainConsumer.pause(mainConsumer.assignment());
        } else {
            // All tasks that are owned by the task manager are ready and do not need to be paused
            final Set<TopicPartition> partitionsNotToPause = tasks.allTasks()
                .stream()
                .flatMap(task -> task.inputPartitions().stream())
                .collect(Collectors.toSet());
            final Set<TopicPartition> partitionsToPause = new HashSet<>(mainConsumer.assignment());
            partitionsToPause.removeAll(partitionsNotToPause);
            mainConsumer.pause(partitionsToPause);
        }

        releaseLockedUnassignedTaskDirectories();

        rebalanceInProgress = false;
    }

    /**
     * @throws TaskMigratedException
     */
    boolean handleCorruption(final Set<TaskId> corruptedTasks) {
        final Set<TaskId> activeTasks = new HashSet<>(tasks.activeTaskIds());

        // We need to stop all processing, since we need to commit non-corrupted tasks as well.
        maybeLockTasks(activeTasks);

        final Set<Task> corruptedActiveTasks = new HashSet<>();
        final Set<Task> corruptedStandbyTasks = new HashSet<>();

        for (final TaskId taskId : corruptedTasks) {
            final Task task = tasks.task(taskId);
            if (task.isActive()) {
                corruptedActiveTasks.add(task);
            } else {
                corruptedStandbyTasks.add(task);
            }
        }

        // Make sure to clean up any corrupted standby tasks in their entirety before committing
        // since TaskMigrated can be thrown and the resulting handleLostAll will only clean up active tasks
        closeDirtyAndRevive(corruptedStandbyTasks, true);

        // We need to commit before closing the corrupted active tasks since this will force the ongoing txn to abort
        try {
            final Collection<Task> tasksToCommit = tasks.allTasksPerId()
                .values()
                .stream()
                .filter(t -> t.state() == Task.State.RUNNING)
                .filter(t -> !corruptedTasks.contains(t.id()))
                .collect(Collectors.toSet());
            commitTasksAndMaybeUpdateCommittableOffsets(tasksToCommit, new HashMap<>());
        } catch (final TaskCorruptedException e) {
            log.info("Some additional tasks were found corrupted while trying to commit, these will be added to the " +
                         "tasks to clean and revive: {}", e.corruptedTasks());
            corruptedActiveTasks.addAll(tasks.tasks(e.corruptedTasks()));
        } catch (final TimeoutException e) {
            log.info("Hit TimeoutException when committing all non-corrupted tasks, these will be closed and revived");
            final Collection<Task> uncorruptedTasks = new HashSet<>(tasks.activeTasks());
            uncorruptedTasks.removeAll(corruptedActiveTasks);
            // Those tasks which just timed out can just be closed dirty without marking changelogs as corrupted
            closeDirtyAndRevive(uncorruptedTasks, false);
        }

        closeDirtyAndRevive(corruptedActiveTasks, true);

        maybeUnlockTasks(activeTasks);

        return !corruptedActiveTasks.isEmpty();
    }

    private void closeDirtyAndRevive(final Collection<Task> taskWithChangelogs, final boolean markAsCorrupted) {
        for (final Task task : taskWithChangelogs) {
            if (task.state() != State.CLOSED) {
                final Collection<TopicPartition> corruptedPartitions = task.changelogPartitions();

                // mark corrupted partitions to not be checkpointed, and then close the task as dirty
                // TODO: this step should be removed as we complete migrating to state updater
                if (markAsCorrupted && stateUpdater == null) {
                    task.markChangelogAsCorrupted(corruptedPartitions);
                }

                try {
                    // we do not need to take the returned offsets since we are not going to commit anyways;
                    // this call is only used for active tasks to flush the cache before suspending and
                    // closing the topology
                    task.prepareCommit();
                } catch (final RuntimeException swallow) {
                    log.error("Error flushing cache for corrupted task {} ", task.id(), swallow);
                }

                try {
                    task.suspend();

                    // we need to enforce a checkpoint that removes the corrupted partitions
                    if (markAsCorrupted) {
                        task.postCommit(true);
                    }
                } catch (final RuntimeException swallow) {
                    log.error("Error suspending corrupted task {} ", task.id(), swallow);
                }
                task.closeDirty();
            }
            // For active tasks pause their input partitions so we won't poll any more records
            // for this task until it has been re-initialized;
            // Note, closeDirty already clears the partition-group for the task.
            if (task.isActive()) {
                final Set<TopicPartition> currentAssignment = mainConsumer.assignment();
                final Set<TopicPartition> taskInputPartitions = task.inputPartitions();
                final Set<TopicPartition> assignedToPauseAndReset =
                    intersection(HashSet::new, currentAssignment, taskInputPartitions);
                if (!assignedToPauseAndReset.equals(taskInputPartitions)) {
                    log.warn(
                        "Expected the current consumer assignment {} to contain the input partitions {}. " +
                            "Will proceed to recover.",
                        currentAssignment,
                        taskInputPartitions
                    );
                }

                task.addPartitionsForOffsetReset(assignedToPauseAndReset);
            }
            if (stateUpdater != null) {
                tasks.removeTask(task);
            }
            task.revive();
            if (stateUpdater != null) {
                tasks.addPendingTasksToInit(Collections.singleton(task));
            }
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

        topologyMetadata.addSubscribedTopicsFromAssignment(
            activeTasks.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
            logPrefix
        );

        final Map<TaskId, Set<TopicPartition>> activeTasksToCreate = new HashMap<>(activeTasks);
        final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate = new HashMap<>(standbyTasks);
        final Map<Task, Set<TopicPartition>> tasksToRecycle = new HashMap<>();
        final Set<Task> tasksToCloseClean = new TreeSet<>(Comparator.comparing(Task::id));

        final Set<TaskId> tasksToLock =
            tasks.allTaskIds().stream()
                .filter(x -> activeTasksToCreate.containsKey(x) || standbyTasksToCreate.containsKey(x))
                .collect(Collectors.toSet());

        maybeLockTasks(tasksToLock);

        // first put aside those unrecognized tasks because of unknown named-topologies
        tasks.clearPendingTasksToCreate();
        tasks.addPendingActiveTasksToCreate(pendingTasksToCreate(activeTasksToCreate));
        tasks.addPendingStandbyTasksToCreate(pendingTasksToCreate(standbyTasksToCreate));
        
        // first rectify all existing tasks:
        // 1. for tasks that are already owned, just update input partitions / resume and skip re-creating them
        // 2. for tasks that have changed active/standby status, just recycle and skip re-creating them
        // 3. otherwise, close them since they are no longer owned
        if (stateUpdater == null) {
            handleTasksWithoutStateUpdater(activeTasksToCreate, standbyTasksToCreate, tasksToRecycle, tasksToCloseClean);
        } else {
            handleTasksWithStateUpdater(activeTasksToCreate, standbyTasksToCreate, tasksToRecycle, tasksToCloseClean);
        }

        final Map<TaskId, RuntimeException> taskCloseExceptions = closeAndRecycleTasks(tasksToRecycle, tasksToCloseClean);

        maybeUnlockTasks(tasksToLock);

        maybeThrowTaskExceptions(taskCloseExceptions);

        createNewTasks(activeTasksToCreate, standbyTasksToCreate);
    }

    // Wrap and throw the exception in the following order
    // if at least one of the exception is a non-streams exception, then wrap and throw since it should be handled by thread's handler
    // if at least one of the exception is a streams exception, then directly throw since it should be handled by thread's handler
    // if at least one of the exception is a task-migrated exception, then directly throw since it indicates all tasks are lost
    // otherwise, all the exceptions are task-corrupted, then merge their tasks and throw a single one
    // TODO: move task-corrupted and task-migrated out of the public errors package since they are internal errors and always be
    //       handled by Streams library itself
    private void maybeThrowTaskExceptions(final Map<TaskId, RuntimeException> taskExceptions) {
        if (!taskExceptions.isEmpty()) {
            log.error("Get exceptions for the following tasks: {}", taskExceptions);

            final Set<TaskId> aggregatedCorruptedTaskIds = new HashSet<>();
            StreamsException lastFatal = null;
            TaskMigratedException lastTaskMigrated = null;
            for (final Map.Entry<TaskId, RuntimeException> entry : taskExceptions.entrySet()) {
                final TaskId taskId = entry.getKey();
                final RuntimeException exception = entry.getValue();

                if (exception instanceof StreamsException) {
                    if (exception instanceof TaskMigratedException) {
                        lastTaskMigrated = (TaskMigratedException) exception;
                    } else if (exception instanceof TaskCorruptedException) {
                        log.warn("Encounter corrupted task" + taskId + ", will group it with other corrupted tasks " +
                            "and handle together", exception);
                        aggregatedCorruptedTaskIds.add(taskId);
                    } else {
                        ((StreamsException) exception).setTaskId(taskId);
                        lastFatal = (StreamsException) exception;
                    }
                } else if (exception instanceof KafkaException) {
                    lastFatal = new StreamsException(exception, taskId);
                } else {
                    lastFatal = new StreamsException("Encounter unexpected fatal error for task " + taskId, exception, taskId);
                }
            }

            if (lastFatal != null) {
                throw lastFatal;
            } else if (lastTaskMigrated != null) {
                throw lastTaskMigrated;
            } else {
                throw new TaskCorruptedException(aggregatedCorruptedTaskIds);
            }
        }
    }

    private void createNewTasks(final Map<TaskId, Set<TopicPartition>> activeTasksToCreate,
                                final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate) {
        final Collection<Task> newActiveTasks = activeTaskCreator.createTasks(mainConsumer, activeTasksToCreate);
        final Collection<Task> newStandbyTasks = standbyTaskCreator.createTasks(standbyTasksToCreate);

        if (stateUpdater == null) {
            tasks.addActiveTasks(newActiveTasks);
            tasks.addStandbyTasks(newStandbyTasks);
        } else {
            tasks.addPendingTasksToInit(newActiveTasks);
            tasks.addPendingTasksToInit(newStandbyTasks);
        }
    }

    private void handleTasksWithoutStateUpdater(final Map<TaskId, Set<TopicPartition>> activeTasksToCreate,
                                                final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate,
                                                final Map<Task, Set<TopicPartition>> tasksToRecycle,
                                                final Set<Task> tasksToCloseClean) {
        for (final Task task : tasks.allTasks()) {
            final TaskId taskId = task.id();
            if (activeTasksToCreate.containsKey(taskId)) {
                if (task.isActive()) {
                    final Set<TopicPartition> topicPartitions = activeTasksToCreate.get(taskId);
                    if (tasks.updateActiveTaskInputPartitions(task, topicPartitions)) {
                        task.updateInputPartitions(topicPartitions, topologyMetadata.nodeToSourceTopics(task.id()));
                    }
                    task.resume();
                } else {
                    tasksToRecycle.put(task, activeTasksToCreate.get(taskId));
                }
                activeTasksToCreate.remove(taskId);
            } else if (standbyTasksToCreate.containsKey(taskId)) {
                if (!task.isActive()) {
                    updateInputPartitionsOfStandbyTaskIfTheyChanged(task, standbyTasksToCreate.get(taskId));
                    task.resume();
                } else {
                    tasksToRecycle.put(task, standbyTasksToCreate.get(taskId));
                }
                standbyTasksToCreate.remove(taskId);
            } else {
                tasksToCloseClean.add(task);
            }
        }
    }

    private void updateInputPartitionsOfStandbyTaskIfTheyChanged(final Task task,
                                                                 final Set<TopicPartition> inputPartitions) {
        /*
        We should only update input partitions of a standby task if the input partitions really changed. Updating the
        input partitions of tasks also updates the mapping from source nodes to input topics in the processor topology
        within the task. The mapping is updated with the topics from the topology metadata. The topology metadata does
        not prefix intermediate internal topics with the application ID. Thus, if a standby task has input partitions
        from an intermediate internal topic the update of the mapping in the processor topology leads to an invalid
        topology exception during recycling of a standby task to an active task when the input queues are created. This
        is because the input topics in the processor topology and the input partitions of the task do not match because
        the former miss the application ID prefix.
        For standby task that have only input partitions from intermediate internal topics this check avoids the invalid
        topology exception. Unfortunately, a subtopology might have input partitions subscribed to with a regex
        additionally intermediate internal topics which might still lead to an invalid topology exception during recycling
        irrespectively of this check here. Thus, there is still a bug to fix here.
         */
        if (!task.inputPartitions().equals(inputPartitions)) {
            task.updateInputPartitions(inputPartitions, topologyMetadata.nodeToSourceTopics(task.id()));
        }
    }

    private void handleTasksWithStateUpdater(final Map<TaskId, Set<TopicPartition>> activeTasksToCreate,
                                             final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate,
                                             final Map<Task, Set<TopicPartition>> tasksToRecycle,
                                             final Set<Task> tasksToCloseClean) {
        handleTasksPendingInitialization();
        handleRunningAndSuspendedTasks(activeTasksToCreate, standbyTasksToCreate, tasksToRecycle, tasksToCloseClean);
        handleTasksInStateUpdater(activeTasksToCreate, standbyTasksToCreate);
    }

    private void handleTasksPendingInitialization() {
        // All tasks pending initialization are not part of the usual bookkeeping
        for (final Task task : tasks.drainPendingTasksToInit()) {
            task.suspend();
            task.closeClean();
        }
    }

    private void handleRunningAndSuspendedTasks(final Map<TaskId, Set<TopicPartition>> activeTasksToCreate,
                                                final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate,
                                                final Map<Task, Set<TopicPartition>> tasksToRecycle,
                                                final Set<Task> tasksToCloseClean) {
        for (final Task task : tasks.allTasks()) {
            if (!task.isActive()) {
                throw new IllegalStateException("Standby tasks should only be managed by the state updater, " +
                    "but standby task " + task.id() + " is managed by the stream thread");
            }
            final TaskId taskId = task.id();
            if (activeTasksToCreate.containsKey(taskId)) {
                handleReassignedActiveTask(task, activeTasksToCreate.get(taskId));
                activeTasksToCreate.remove(taskId);
            } else if (standbyTasksToCreate.containsKey(taskId)) {
                tasksToRecycle.put(task, standbyTasksToCreate.get(taskId));
                standbyTasksToCreate.remove(taskId);
            } else {
                tasksToCloseClean.add(task);
            }
        }
    }

    private void handleReassignedActiveTask(final Task task,
                                            final Set<TopicPartition> inputPartitions) {
        if (tasks.updateActiveTaskInputPartitions(task, inputPartitions)) {
            task.updateInputPartitions(inputPartitions, topologyMetadata.nodeToSourceTopics(task.id()));
        }
        if (task.state() == State.SUSPENDED) {
            tasks.removeTask(task);
            task.resume();
            stateUpdater.add(task);
        }
    }

    private void handleTasksInStateUpdater(final Map<TaskId, Set<TopicPartition>> activeTasksToCreate,
                                           final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate) {
        for (final Task task : stateUpdater.getTasks()) {
            final TaskId taskId = task.id();
            if (activeTasksToCreate.containsKey(taskId)) {
                final Set<TopicPartition> inputPartitions = activeTasksToCreate.get(taskId);
                if (task.isActive()) {
                    updateInputPartitionsOrRemoveTaskFromTasksToSuspend(task, inputPartitions);
                } else {
                    removeTaskToRecycleFromStateUpdater(taskId, inputPartitions);
                }
                activeTasksToCreate.remove(taskId);
            } else if (standbyTasksToCreate.containsKey(taskId)) {
                if (task.isActive()) {
                    removeTaskToRecycleFromStateUpdater(taskId, standbyTasksToCreate.get(taskId));
                }
                standbyTasksToCreate.remove(taskId);
            } else {
                removeUnusedTaskFromStateUpdater(taskId);
            }
        }
    }

    private void updateInputPartitionsOrRemoveTaskFromTasksToSuspend(final Task task,
                                                                     final Set<TopicPartition> inputPartitions) {
        final TaskId taskId = task.id();
        if (!task.inputPartitions().equals(inputPartitions)) {
            stateUpdater.remove(taskId);
            tasks.addPendingTaskToUpdateInputPartitions(taskId, inputPartitions);
        } else {
            tasks.removePendingActiveTaskToSuspend(taskId);
        }
    }

    private void removeTaskToRecycleFromStateUpdater(final TaskId taskId,
                                                     final Set<TopicPartition> inputPartitions) {
        stateUpdater.remove(taskId);
        tasks.addPendingTaskToRecycle(taskId, inputPartitions);
    }

    private void removeUnusedTaskFromStateUpdater(final TaskId taskId) {
        stateUpdater.remove(taskId);
        tasks.addPendingTaskToCloseClean(taskId);
    }

    private Map<TaskId, Set<TopicPartition>> pendingTasksToCreate(final Map<TaskId, Set<TopicPartition>> tasksToCreate) {
        final Map<TaskId, Set<TopicPartition>> pendingTasks = new HashMap<>();
        final Iterator<Map.Entry<TaskId, Set<TopicPartition>>> iter = tasksToCreate.entrySet().iterator();
        while (iter.hasNext()) {
            final Map.Entry<TaskId, Set<TopicPartition>> entry = iter.next();
            final TaskId taskId = entry.getKey();
            final boolean taskIsOwned = tasks.allTaskIds().contains(taskId)
                || (stateUpdater != null && stateUpdater.getTasks().stream().anyMatch(task -> task.id() == taskId));
            if (taskId.topologyName() != null && !taskIsOwned && !topologyMetadata.namedTopologiesView().contains(taskId.topologyName())) {
                log.info("Cannot create the assigned task {} since it's topology name cannot be recognized, will put it " +
                        "aside as pending for now and create later when topology metadata gets refreshed", taskId);
                pendingTasks.put(taskId, entry.getValue());
                iter.remove();
            }
        }
        return pendingTasks;
    }

    private Map<TaskId, RuntimeException> closeAndRecycleTasks(final Map<Task, Set<TopicPartition>> tasksToRecycle,
                                                               final Set<Task> tasksToCloseClean) {
        final Map<TaskId, RuntimeException> taskCloseExceptions = new LinkedHashMap<>();
        final Set<Task> tasksToCloseDirty = new TreeSet<>(Comparator.comparing(Task::id));

        // for all tasks to close or recycle, we should first write a checkpoint as in post-commit
        final List<Task> tasksToCheckpoint = new ArrayList<>(tasksToCloseClean);
        tasksToCheckpoint.addAll(tasksToRecycle.keySet());
        for (final Task task : tasksToCheckpoint) {
            try {
                // Note that we are not actually committing here but just check if we need to write checkpoint file:
                // 1) for active tasks prepareCommit should return empty if it has committed during suspension successfully,
                //    and their changelog positions should not change at all postCommit would not write the checkpoint again.
                // 2) for standby tasks prepareCommit should always return empty, and then in postCommit we would probably
                //    write the checkpoint file.
                final Map<TopicPartition, OffsetAndMetadata> offsets = task.prepareCommit();
                if (!offsets.isEmpty()) {
                    log.error("Task {} should have been committed when it was suspended, but it reports non-empty " +
                                    "offsets {} to commit; this means it failed during last commit and hence should be closed dirty",
                            task.id(), offsets);

                    tasksToCloseDirty.add(task);
                } else if (!task.isActive()) {
                    // For standby tasks, always try to first suspend before committing (checkpointing) it;
                    // Since standby tasks do not actually need to commit offsets but only need to
                    // flush / checkpoint state stores, so we only need to call postCommit here.
                    task.suspend();

                    task.postCommit(true);
                }
            } catch (final RuntimeException e) {
                final String uncleanMessage = String.format(
                        "Failed to checkpoint task %s. Attempting to close remaining tasks before re-throwing:",
                        task.id());
                log.error(uncleanMessage, e);
                taskCloseExceptions.putIfAbsent(task.id(), e);
                // We've already recorded the exception (which is the point of clean).
                // Now, we should go ahead and complete the close because a half-closed task is no good to anyone.
                tasksToCloseDirty.add(task);
            }
        }

        tasksToCloseClean.removeAll(tasksToCloseDirty);
        for (final Task task : tasksToCloseClean) {
            try {
                closeTaskClean(task);
            } catch (final RuntimeException closeTaskException) {
                final String uncleanMessage = String.format(
                    "Failed to close task %s cleanly. Attempting to close remaining tasks before re-throwing:",
                    task.id());
                log.error(uncleanMessage, closeTaskException);

                if (task.state() != State.CLOSED) {
                    tasksToCloseDirty.add(task);
                }

                taskCloseExceptions.putIfAbsent(task.id(), closeTaskException);
            }
        }

        tasksToRecycle.keySet().removeAll(tasksToCloseDirty);
        for (final Map.Entry<Task, Set<TopicPartition>> entry : tasksToRecycle.entrySet()) {
            final Task oldTask = entry.getKey();
            final Set<TopicPartition> inputPartitions = entry.getValue();
            try {
                if (oldTask.isActive()) {
                    final StandbyTask standbyTask = convertActiveToStandby((StreamTask) oldTask, inputPartitions);
                    if (stateUpdater != null) {
                        tasks.removeTask(oldTask);
                        tasks.addPendingTasksToInit(Collections.singleton(standbyTask));
                    } else {
                        tasks.replaceActiveWithStandby(standbyTask);
                    }
                } else {
                    final StreamTask activeTask = convertStandbyToActive((StandbyTask) oldTask, inputPartitions);
                    tasks.replaceStandbyWithActive(activeTask);
                }
            } catch (final RuntimeException e) {
                final String uncleanMessage = String.format("Failed to recycle task %s cleanly. " +
                    "Attempting to close remaining tasks before re-throwing:", oldTask.id());
                log.error(uncleanMessage, e);
                taskCloseExceptions.putIfAbsent(oldTask.id(), e);
                tasksToCloseDirty.add(oldTask);
            }
        }

        // for tasks that cannot be cleanly closed or recycled, close them dirty
        for (final Task task : tasksToCloseDirty) {
            closeTaskDirty(task, true);
        }

        return taskCloseExceptions;
    }

    private StandbyTask convertActiveToStandby(final StreamTask activeTask, final Set<TopicPartition> partitions) {
        final StandbyTask standbyTask = standbyTaskCreator.createStandbyTaskFromActive(activeTask, partitions);
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(activeTask.id());
        return standbyTask;
    }

    private StreamTask convertStandbyToActive(final StandbyTask standbyTask, final Set<TopicPartition> partitions) {
        return activeTaskCreator.createActiveTaskFromStandby(standbyTask, partitions, mainConsumer);
    }

    /**
     * Tries to initialize any new or still-uninitialized tasks, then checks if they can/have completed restoration.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     * @return {@code true} if all tasks are fully restored
     */
    boolean tryToCompleteRestoration(final long now,
                                     final java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
        boolean allRunning = true;

        // transit to restore active is idempotent so we can call it multiple times
        changelogReader.enforceRestoreActive();

        final List<Task> activeTasks = new LinkedList<>();
        for (final Task task : tasks.allTasks()) {
            try {
                task.initializeIfNeeded();
                task.clearTaskTimeout();
            } catch (final LockException lockException) {
                // it is possible that if there are multiple threads within the instance that one thread
                // trying to grab the task from the other, while the other has not released the lock since
                // it did not participate in the rebalance. In this case we can just retry in the next iteration
                log.debug("Could not initialize task {} since: {}; will retry", task.id(), lockException.getMessage());
                allRunning = false;
            } catch (final TimeoutException timeoutException) {
                task.maybeInitTaskTimeoutOrThrow(now, timeoutException);
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
                        task.completeRestoration(offsetResetter);
                        task.clearTaskTimeout();
                    } catch (final TimeoutException timeoutException) {
                        task.maybeInitTaskTimeoutOrThrow(now, timeoutException);
                        log.debug(
                            String.format(
                                "Could not complete restoration for %s due to the following exception; will retry",
                                task.id()),
                            timeoutException
                        );

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
            changelogReader.transitToUpdateStandby();
        }

        return allRunning;
    }

    public boolean checkStateUpdater(final long now,
                                     final java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
        addTasksToStateUpdater();
        if (stateUpdater.hasExceptionsAndFailedTasks()) {
            handleExceptionsFromStateUpdater();
        }
        if (stateUpdater.hasRemovedTasks()) {
            handleRemovedTasksFromStateUpdater();
        }
        if (stateUpdater.restoresActiveTasks()) {
            handleRestoredTasksFromStateUpdater(now, offsetResetter);
        }
        return !stateUpdater.restoresActiveTasks()
            && !tasks.hasPendingTasksToRecycle()
            && !tasks.hasPendingTasksToInit();
    }

    private void recycleTaskFromStateUpdater(final Task task,
                                             final Set<TopicPartition> inputPartitions,
                                             final Set<Task> tasksToCloseDirty,
                                             final Map<TaskId, RuntimeException> taskExceptions) {
        Task newTask = null;
        try {
            task.suspend();
            newTask = task.isActive() ?
                convertActiveToStandby((StreamTask) task, inputPartitions) :
                convertStandbyToActive((StandbyTask) task, inputPartitions);
            addTaskToStateUpdater(newTask);
        } catch (final RuntimeException e) {
            final TaskId taskId = task.id();
            final String uncleanMessage = String.format("Failed to recycle task %s cleanly. " +
                "Attempting to close remaining tasks before re-throwing:", taskId);
            log.error(uncleanMessage, e);

            if (task.state() != State.CLOSED) {
                tasksToCloseDirty.add(task);
            }
            if (newTask != null && newTask.state() != State.CLOSED) {
                tasksToCloseDirty.add(newTask);
            }

            taskExceptions.putIfAbsent(taskId, e);
        }
    }

    private void closeTaskClean(final Task task,
                                final Set<Task> tasksToCloseDirty,
                                final Map<TaskId, RuntimeException> taskExceptions) {
        try {
            task.suspend();
            task.closeClean();
            if (task.isActive()) {
                activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
            }
        } catch (final RuntimeException e) {
            final String uncleanMessage = String.format("Failed to close task %s cleanly. " +
                "Attempting to close remaining tasks before re-throwing:", task.id());
            log.error(uncleanMessage, e);

            if (task.state() != State.CLOSED) {
                tasksToCloseDirty.add(task);
            }

            taskExceptions.putIfAbsent(task.id(), e);
        }
    }

    private void transitRestoredTaskToRunning(final Task task,
                                              final long now,
                                              final java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
        try {
            task.completeRestoration(offsetResetter);
            tasks.addTask(task);
            mainConsumer.resume(task.inputPartitions());
            task.clearTaskTimeout();
        } catch (final TimeoutException timeoutException) {
            task.maybeInitTaskTimeoutOrThrow(now, timeoutException);
            log.debug(
                String.format(
                    "Could not complete restoration for %s due to the following exception; will retry",
                    task.id()),
                timeoutException
            );
        }
    }

    private void addTasksToStateUpdater() {
        final Map<TaskId, RuntimeException> taskExceptions = new LinkedHashMap<>();
        for (final Task task : tasks.drainPendingTasksToInit()) {
            try {
                addTaskToStateUpdater(task);
            } catch (final RuntimeException e) {
                // need to add task back to the bookkeeping to be handled by the stream thread
                tasks.addTask(task);
                taskExceptions.put(task.id(), e);
            }
        }

        maybeThrowTaskExceptions(taskExceptions);
    }

    private void addTaskToStateUpdater(final Task task) {
        try {
            task.initializeIfNeeded();
            stateUpdater.add(task);
        } catch (final LockException lockException) {
            // The state directory may still be locked by another thread, when the rebalance just happened.
            // Retry in the next iteration.
            log.info("Encountered lock exception. Reattempting locking the state in the next iteration.", lockException);
            tasks.addPendingTasksToInit(Collections.singleton(task));
        }
    }

    public void handleExceptionsFromStateUpdater() {
        final Map<TaskId, RuntimeException> taskExceptions = new LinkedHashMap<>();

        for (final StateUpdater.ExceptionAndTasks exceptionAndTasks : stateUpdater.drainExceptionsAndFailedTasks()) {
            final RuntimeException exception = exceptionAndTasks.exception();
            final Set<Task> failedTasks = exceptionAndTasks.getTasks();

            for (final Task failedTask : failedTasks) {
                // need to add task back to the bookkeeping to be handled by the stream thread
                tasks.addTask(failedTask);
                taskExceptions.put(failedTask.id(), exception);
            }
        }

        maybeThrowTaskExceptions(taskExceptions);
    }

    private void handleRemovedTasksFromStateUpdater() {
        final Map<TaskId, RuntimeException> taskExceptions = new LinkedHashMap<>();
        final Set<Task> tasksToCloseDirty = new TreeSet<>(Comparator.comparing(Task::id));

        for (final Task task : stateUpdater.drainRemovedTasks()) {
            Set<TopicPartition> inputPartitions;
            if ((inputPartitions = tasks.removePendingTaskToRecycle(task.id())) != null) {
                recycleTaskFromStateUpdater(task, inputPartitions, tasksToCloseDirty, taskExceptions);
            } else if (tasks.removePendingTaskToCloseClean(task.id())) {
                closeTaskClean(task, tasksToCloseDirty, taskExceptions);
            } else if (tasks.removePendingTaskToCloseDirty(task.id())) {
                tasksToCloseDirty.add(task);
            } else if ((inputPartitions = tasks.removePendingTaskToUpdateInputPartitions(task.id())) != null) {
                task.updateInputPartitions(inputPartitions, topologyMetadata.nodeToSourceTopics(task.id()));
                stateUpdater.add(task);
            } else if (tasks.removePendingActiveTaskToSuspend(task.id())) {
                task.suspend();
                tasks.addTask(task);
            } else {
                throw new IllegalStateException("Got a removed task " + task.id() + " from the state updater " +
                    "that is not for recycle, closing, or updating input partitions; this should not happen");
            }
        }

        // for tasks that cannot be cleanly closed or recycled, close them dirty
        for (final Task task : tasksToCloseDirty) {
            closeTaskDirty(task, false);
        }

        maybeThrowTaskExceptions(taskExceptions);
    }

    private void handleRestoredTasksFromStateUpdater(final long now,
                                                     final java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
        final Map<TaskId, RuntimeException> taskExceptions = new LinkedHashMap<>();
        final Set<Task> tasksToCloseDirty = new TreeSet<>(Comparator.comparing(Task::id));

        final Duration timeout = Duration.ZERO;
        for (final Task task : stateUpdater.drainRestoredActiveTasks(timeout)) {
            Set<TopicPartition> inputPartitions;
            if ((inputPartitions = tasks.removePendingTaskToRecycle(task.id())) != null) {
                recycleTaskFromStateUpdater(task, inputPartitions, tasksToCloseDirty, taskExceptions);
            } else if (tasks.removePendingTaskToCloseClean(task.id())) {
                closeTaskClean(task, tasksToCloseDirty, taskExceptions);
            } else if (tasks.removePendingTaskToCloseDirty(task.id())) {
                tasksToCloseDirty.add(task);
            } else if ((inputPartitions = tasks.removePendingTaskToUpdateInputPartitions(task.id())) != null) {
                task.updateInputPartitions(inputPartitions, topologyMetadata.nodeToSourceTopics(task.id()));
                transitRestoredTaskToRunning(task, now, offsetResetter);
            } else if (tasks.removePendingActiveTaskToSuspend(task.id())) {
                task.suspend();
                tasks.addTask(task);
            } else {
                transitRestoredTaskToRunning(task, now, offsetResetter);
            }
        }

        for (final Task task : tasksToCloseDirty) {
            closeTaskDirty(task, false);
        }

        maybeThrowTaskExceptions(taskExceptions);
    }

    /**
     * Handle the revoked partitions and prepare for closing the associated tasks in {@link #handleAssignment(Map, Map)}
     * We should commit the revoking tasks first before suspending them as we will not officially own them anymore when
     * {@link #handleAssignment(Map, Map)} is called. Note that only active task partitions are passed in from the
     * rebalance listener, so we only need to consider/commit active tasks here
     *
     * If eos-v2 is used, we must commit ALL tasks. Otherwise, we can just commit those (active) tasks which are revoked
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void handleRevocation(final Collection<TopicPartition> revokedPartitions) {
        final Set<TopicPartition> remainingRevokedPartitions = new HashSet<>(revokedPartitions);

        final Set<Task> revokedActiveTasks = new HashSet<>();
        final Set<Task> commitNeededActiveTasks = new HashSet<>();
        final Map<Task, Map<TopicPartition, OffsetAndMetadata>> consumedOffsetsPerTask = new HashMap<>();
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        final Set<TaskId> lockedTaskIds = activeRunningTaskIterable().stream().map(Task::id).collect(Collectors.toSet());
        maybeLockTasks(lockedTaskIds);

        for (final Task task : activeRunningTaskIterable()) {
            if (remainingRevokedPartitions.containsAll(task.inputPartitions())) {
                // when the task input partitions are included in the revoked list,
                // this is an active task and should be revoked
                revokedActiveTasks.add(task);
                remainingRevokedPartitions.removeAll(task.inputPartitions());
            } else if (task.commitNeeded()) {
                commitNeededActiveTasks.add(task);
            }
        }

        addRevokedTasksInStateUpdaterToPendingTasksToSuspend(remainingRevokedPartitions);

        if (!remainingRevokedPartitions.isEmpty()) {
            log.debug("The following revoked partitions {} are missing from the current task partitions. It could "
                          + "potentially be due to race condition of consumer detecting the heartbeat failure, or the tasks " +
                         "have been cleaned up by the handleAssignment callback.", remainingRevokedPartitions);
        }

        prepareCommitAndAddOffsetsToMap(revokedActiveTasks, consumedOffsetsPerTask);

        // if we need to commit any revoking task then we just commit all of those needed committing together
        final boolean shouldCommitAdditionalTasks = !consumedOffsetsPerTask.isEmpty();
        if (shouldCommitAdditionalTasks) {
            prepareCommitAndAddOffsetsToMap(commitNeededActiveTasks, consumedOffsetsPerTask);
        }

        // even if commit failed, we should still continue and complete suspending those tasks, so we would capture
        // any exception and rethrow it at the end. some exceptions may be handled immediately and then swallowed,
        // as such we just need to skip those dirty tasks in the checkpoint
        final Set<Task> dirtyTasks = new HashSet<>();
        try {
            // in handleRevocation we must call commitOffsetsOrTransaction() directly rather than
            // commitAndFillInConsumedOffsetsAndMetadataPerTaskMap() to make sure we don't skip the
            // offset commit because we are in a rebalance
            taskExecutor.commitOffsetsOrTransaction(consumedOffsetsPerTask);
        } catch (final TaskCorruptedException e) {
            log.warn("Some tasks were corrupted when trying to commit offsets, these will be cleaned and revived: {}",
                     e.corruptedTasks());

            // If we hit a TaskCorruptedException it must be EOS, just handle the cleanup for those corrupted tasks right here
            dirtyTasks.addAll(tasks.tasks(e.corruptedTasks()));
            closeDirtyAndRevive(dirtyTasks, true);
        } catch (final TimeoutException e) {
            log.warn("Timed out while trying to commit all tasks during revocation, these will be cleaned and revived");

            // If we hit a TimeoutException it must be ALOS, just close dirty and revive without wiping the state
            dirtyTasks.addAll(consumedOffsetsPerTask.keySet());
            closeDirtyAndRevive(dirtyTasks, false);
        } catch (final RuntimeException e) {
            log.error("Exception caught while committing those revoked tasks " + revokedActiveTasks, e);
            firstException.compareAndSet(null, e);
            dirtyTasks.addAll(consumedOffsetsPerTask.keySet());
        }

        // we enforce checkpointing upon suspending a task: if it is resumed later we just proceed normally, if it is
        // going to be closed we would checkpoint by then
        for (final Task task : revokedActiveTasks) {
            if (!dirtyTasks.contains(task)) {
                try {
                    task.postCommit(true);
                } catch (final RuntimeException e) {
                    log.error("Exception caught while post-committing task " + task.id(), e);
                    maybeSetFirstException(false, maybeWrapTaskException(e, task.id()), firstException);
                }
            }
        }

        if (shouldCommitAdditionalTasks) {
            for (final Task task : commitNeededActiveTasks) {
                if (!dirtyTasks.contains(task)) {
                    try {
                        // for non-revoking active tasks, we should not enforce checkpoint
                        // since if it is EOS enabled, no checkpoint should be written while
                        // the task is in RUNNING tate
                        task.postCommit(false);
                    } catch (final RuntimeException e) {
                        log.error("Exception caught while post-committing task " + task.id(), e);
                        maybeSetFirstException(false, maybeWrapTaskException(e, task.id()), firstException);
                    }
                }
            }
        }

        for (final Task task : revokedActiveTasks) {
            try {
                task.suspend();
            } catch (final RuntimeException e) {
                log.error("Caught the following exception while trying to suspend revoked task " + task.id(), e);
                maybeSetFirstException(false, maybeWrapTaskException(e, task.id()), firstException);
            }
        }

        maybeUnlockTasks(lockedTaskIds);

        if (firstException.get() != null) {
            throw firstException.get();
        }
    }

    private void addRevokedTasksInStateUpdaterToPendingTasksToSuspend(final Set<TopicPartition> remainingRevokedPartitions) {
        if (stateUpdater != null) {
            for (final Task restoringTask : stateUpdater.getTasks()) {
                if (restoringTask.isActive()) {
                    if (remainingRevokedPartitions.containsAll(restoringTask.inputPartitions())) {
                        tasks.addPendingActiveTaskToSuspend(restoringTask.id());
                        remainingRevokedPartitions.removeAll(restoringTask.inputPartitions());
                    }
                }
            }
        }
    }

    private void prepareCommitAndAddOffsetsToMap(final Set<Task> tasksToPrepare,
                                                 final Map<Task, Map<TopicPartition, OffsetAndMetadata>> consumedOffsetsPerTask) {
        for (final Task task : tasksToPrepare) {
            try {
                final Map<TopicPartition, OffsetAndMetadata> committableOffsets = task.prepareCommit();
                if (!committableOffsets.isEmpty()) {
                    consumedOffsetsPerTask.put(task, committableOffsets);
                }
            } catch (final StreamsException e) {
                e.setTaskId(task.id());
                throw e;
            } catch (final Exception e) {
                throw new StreamsException(e, task.id());
            }
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

        closeRunningTasksDirty();
        removeLostActiveTasksFromStateUpdater();

        if (processingMode == EXACTLY_ONCE_V2) {
            activeTaskCreator.reInitializeThreadProducer();
        }
    }

    private void closeRunningTasksDirty() {
        final Set<Task> allTask = tasks.allTasks();
        final Set<TaskId> allTaskIds = tasks.allTaskIds();
        maybeLockTasks(allTaskIds);
        for (final Task task : allTask) {
            // Even though we've apparently dropped out of the group, we can continue safely to maintain our
            // standby tasks while we rejoin.
            if (task.isActive()) {
                closeTaskDirty(task, true);
            }
        }
        maybeUnlockTasks(allTaskIds);
    }

    private void removeLostActiveTasksFromStateUpdater() {
        if (stateUpdater != null) {
            for (final Task restoringTask : stateUpdater.getTasks()) {
                if (restoringTask.isActive()) {
                    tasks.addPendingTaskToCloseDirty(restoringTask.id());
                    stateUpdater.remove(restoringTask.id());
                }
            }
        }
    }

    public void signalResume() {
        if (stateUpdater != null) {
            stateUpdater.signalResume();
        }
        if (schedulingTaskManager != null) {
            schedulingTaskManager.signalTaskExecutors();
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
        final Map<TaskId, Task> tasks = allTasks();
        final Set<TaskId> lockedTaskDirectoriesOfNonOwnedTasksAndClosedAndCreatedTasks =
            union(HashSet::new, lockedTaskDirectories, tasks.keySet());
        for (final Task task : tasks.values()) {
            if (task.state() != State.CREATED && task.state() != State.CLOSED) {
                final Map<TopicPartition, Long> changelogOffsets = task.changelogOffsets();
                if (changelogOffsets.isEmpty()) {
                    log.debug("Skipping to encode apparently stateless (or non-logged) offset sum for task {}",
                        task.id());
                } else {
                    taskOffsetSums.put(task.id(), sumOfChangelogOffsets(task.id(), changelogOffsets));
                }
                lockedTaskDirectoriesOfNonOwnedTasksAndClosedAndCreatedTasks.remove(task.id());
            }
        }

        for (final TaskId id : lockedTaskDirectoriesOfNonOwnedTasksAndClosedAndCreatedTasks) {
            final File checkpointFile = stateDirectory.checkpointFileFor(id);
            try {
                if (checkpointFile.exists()) {
                    taskOffsetSums.put(id, sumOfChangelogOffsets(id, new OffsetCheckpoint(checkpointFile).read()));
                }
            } catch (final IOException e) {
                log.warn(String.format("Exception caught while trying to read checkpoint for task %s:", id), e);
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

        final Map<TaskId, Task> allTasks = allTasks();
        for (final TaskDirectory taskDir : stateDirectory.listNonEmptyTaskDirectories()) {
            final File dir = taskDir.file();
            final String namedTopology = taskDir.namedTopology();
            try {
                final TaskId id = parseTaskDirectoryName(dir.getName(), namedTopology);
                if (stateDirectory.lock(id)) {
                    lockedTaskDirectories.add(id);
                    if (!allTasks.containsKey(id)) {
                        log.debug("Temporarily locked unassigned task {} for the upcoming rebalance", id);
                    }
                }
            } catch (final TaskIdFormatException e) {
                // ignore any unknown files that sit in the same directory
            }
        }
    }

    /**
     * Clean up after closed or removed tasks by making sure to unlock any remaining locked directories for them, for
     * example unassigned tasks or those in the CREATED state when closed, since Task#close will not unlock them
     */
    private void releaseLockedDirectoriesForTasks(final Set<TaskId> tasksToUnlock) {
        final Iterator<TaskId> taskIdIterator = lockedTaskDirectories.iterator();
        while (taskIdIterator.hasNext()) {
            final TaskId id = taskIdIterator.next();
            if (tasksToUnlock.contains(id)) {
                stateDirectory.unlock(id);
                taskIdIterator.remove();
            }
        }
    }

    /**
     * We must release the lock for any unassigned tasks that we temporarily locked in preparation for a
     * rebalance in {@link #tryToLockAllNonEmptyTaskDirectories()}.
     */
    private void releaseLockedUnassignedTaskDirectories() {
        final Iterator<TaskId> taskIdIterator = lockedTaskDirectories.iterator();
        final Map<TaskId, Task> allTasks = allTasks();
        while (taskIdIterator.hasNext()) {
            final TaskId id = taskIdIterator.next();
            if (!allTasks.containsKey(id)) {
                stateDirectory.unlock(id);
                taskIdIterator.remove();
            }
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
            } else if (offset != OffsetCheckpoint.OFFSET_UNKNOWN) {
                if (offset < 0) {
                    throw new StreamsException(
                        new IllegalStateException("Expected not to get a sentinel offset, but got: " + changelogEntry),
                        id);
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

    private void closeTaskDirty(final Task task, final boolean removeFromTasksRegistry) {
        try {
            // we call this function only to flush the case if necessary
            // before suspending and closing the topology
            task.prepareCommit();
        } catch (final RuntimeException swallow) {
            log.error("Error flushing caches of dirty task {} ", task.id(), swallow);
        }

        try {
            task.suspend();
        } catch (final RuntimeException swallow) {
            log.error("Error suspending dirty task {}: {}", task.id(), swallow.getMessage());
        }

        task.closeDirty();

        try {
            if (removeFromTasksRegistry) {
                tasks.removeTask(task);
            }

            if (task.isActive()) {
                activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
            }
        } catch (final RuntimeException swallow) {
            log.error("Error removing dirty task {}: {}", task.id(), swallow.getMessage());
        }
    }

    private void closeTaskClean(final Task task) {
        task.closeClean();
        tasks.removeTask(task);
        if (task.isActive()) {
            activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
        }
    }

    void shutdown(final boolean clean) {
        shutdownStateUpdater();
        shutdownSchedulingTaskManager();

        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        // TODO: change type to `StreamTask`
        final Set<Task> activeTasks = new TreeSet<>(Comparator.comparing(Task::id));
        activeTasks.addAll(tasks.activeTasks());

        executeAndMaybeSwallow(
            clean,
            () -> closeAndCleanUpTasks(activeTasks, standbyTaskIterable(), clean),
            e -> firstException.compareAndSet(null, e),
            e -> log.warn("Ignoring an exception while unlocking remaining task directories.", e)
        );

        executeAndMaybeSwallow(
            clean,
            activeTaskCreator::closeThreadProducerIfNeeded,
            e -> firstException.compareAndSet(null, e),
            e -> log.warn("Ignoring an exception while closing thread producer.", e)
        );

        tasks.clear();

        // this should be called after closing all tasks and clearing them from `tasks` to make sure we unlock the dir
        // for any tasks that may have still been in CREATED at the time of shutdown, since Task#close will not do so
        executeAndMaybeSwallow(
            clean,
            this::releaseLockedUnassignedTaskDirectories,
            e -> firstException.compareAndSet(null, e),
            e -> log.warn("Ignoring an exception while unlocking remaining task directories.", e)
        );

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw fatalException;
        }

        log.info("Shutdown complete");
    }

    private void shutdownStateUpdater() {
        if (stateUpdater != null) {
            stateUpdater.shutdown(Duration.ofMillis(Long.MAX_VALUE));
            closeFailedTasksFromStateUpdater();
            addRestoredTasksToTaskRegistry();
            addRemovedTasksToTaskRegistry();
        }
    }

    private void shutdownSchedulingTaskManager() {
        if (schedulingTaskManager != null) {
            schedulingTaskManager.shutdown(Duration.ofMillis(Long.MAX_VALUE));
        }
    }

    private void closeFailedTasksFromStateUpdater() {
        final Set<Task> tasksToCloseDirty = stateUpdater.drainExceptionsAndFailedTasks().stream()
            .flatMap(exAndTasks -> exAndTasks.getTasks().stream()).collect(Collectors.toSet());

        for (final Task task : tasksToCloseDirty) {
            try {
                // we call this function only to flush the case if necessary
                // before suspending and closing the topology
                task.prepareCommit();
            } catch (final RuntimeException swallow) {
                log.error("Error flushing caches of dirty task {} ", task.id(), swallow);
            }

            try {
                task.suspend();
            } catch (final RuntimeException swallow) {
                log.error("Error suspending dirty task {}: {}", task.id(), swallow.getMessage());
            }

            task.closeDirty();

            try {
                if (task.isActive()) {
                    activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
                }
            } catch (final RuntimeException swallow) {
                log.error("Error closing dirty task {}: {}", task.id(), swallow.getMessage());
            }
        }
    }

    private void addRestoredTasksToTaskRegistry() {
        tasks.addActiveTasks(stateUpdater.drainRestoredActiveTasks(Duration.ZERO).stream()
            .map(t -> (Task) t)
            .collect(Collectors.toSet())
        );
    }

    private void addRemovedTasksToTaskRegistry() {
        final Set<Task> removedTasks = stateUpdater.drainRemovedTasks();
        final Set<Task> removedActiveTasks = new HashSet<>();
        final Iterator<Task> iterator = removedTasks.iterator();
        while (iterator.hasNext()) {
            final Task task = iterator.next();
            if (task.isActive()) {
                iterator.remove();
                removedActiveTasks.add(task);
            }
        }
        tasks.addActiveTasks(removedActiveTasks);
        tasks.addStandbyTasks(removedTasks);
    }

    /**
     * Closes and cleans up after the provided tasks, including closing their corresponding task producers
     */
    void closeAndCleanUpTasks(final Collection<Task> activeTasks, final Collection<Task> standbyTasks, final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        final Set<TaskId> ids =
            activeTasks.stream()
                .map(Task::id)
                .collect(Collectors.toSet());
        maybeLockTasks(ids);

        final Set<Task> tasksToCloseDirty = new HashSet<>();
        tasksToCloseDirty.addAll(tryCloseCleanActiveTasks(activeTasks, clean, firstException));
        tasksToCloseDirty.addAll(tryCloseCleanStandbyTasks(standbyTasks, clean, firstException));

        for (final Task task : tasksToCloseDirty) {
            closeTaskDirty(task, true);
        }

        maybeUnlockTasks(ids);

        final RuntimeException exception = firstException.get();
        if (exception != null) {
            throw exception;
        }
    }

    // Returns the set of active tasks that must be closed dirty
    private Collection<Task> tryCloseCleanActiveTasks(final Collection<Task> activeTasksToClose,
                                                      final boolean clean,
                                                      final AtomicReference<RuntimeException> firstException) {
        if (!clean) {
            return activeTaskIterable();
        }
        final Comparator<Task> byId = Comparator.comparing(Task::id);
        final Set<Task> tasksToCommit = new TreeSet<>(byId);
        final Set<Task> tasksToCloseDirty = new TreeSet<>(byId);
        final Set<Task> tasksToCloseClean = new TreeSet<>(byId);
        final Map<Task, Map<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadataPerTask = new HashMap<>();

        // first committing all tasks and then suspend and close them clean
        for (final Task task : activeTasksToClose) {
            try {
                final Map<TopicPartition, OffsetAndMetadata> committableOffsets = task.prepareCommit();
                tasksToCommit.add(task);
                if (!committableOffsets.isEmpty()) {
                    consumedOffsetsAndMetadataPerTask.put(task, committableOffsets);
                }
                tasksToCloseClean.add(task);
            } catch (final TaskMigratedException e) {
                // just ignore the exception as it doesn't matter during shutdown
                tasksToCloseDirty.add(task);
            } catch (final StreamsException e) {
                e.setTaskId(task.id());
                firstException.compareAndSet(null, e);
                tasksToCloseDirty.add(task);
            } catch (final RuntimeException e) {
                firstException.compareAndSet(null, new StreamsException(e, task.id()));
                tasksToCloseDirty.add(task);
            }
        }

        // If any active tasks can't be committed, none of them can be, and all that need a commit must be closed dirty
        if (processingMode == EXACTLY_ONCE_V2 && !tasksToCloseDirty.isEmpty()) {
            tasksToCloseClean.removeAll(tasksToCommit);
            tasksToCloseDirty.addAll(tasksToCommit);
        } else {
            try {
                taskExecutor.commitOffsetsOrTransaction(consumedOffsetsAndMetadataPerTask);
            } catch (final RuntimeException e) {
                log.error("Exception caught while committing tasks " + consumedOffsetsAndMetadataPerTask.keySet(), e);
                // TODO: should record the task ids when handling this exception
                maybeSetFirstException(false, e, firstException);

                if (e instanceof TaskCorruptedException) {
                    final TaskCorruptedException taskCorruptedException = (TaskCorruptedException) e;
                    final Set<TaskId> corruptedTaskIds = taskCorruptedException.corruptedTasks();
                    final Set<Task> corruptedTasks = tasksToCommit
                        .stream()
                        .filter(task -> corruptedTaskIds.contains(task.id()))
                        .collect(Collectors.toSet());
                    tasksToCloseClean.removeAll(corruptedTasks);
                    tasksToCloseDirty.addAll(corruptedTasks);
                } else {
                    // If the commit fails, everyone who participated in it must be closed dirty
                    tasksToCloseClean.removeAll(tasksToCommit);
                    tasksToCloseDirty.addAll(tasksToCommit);
                }
            }

            for (final Task task : activeTaskIterable()) {
                try {
                    task.postCommit(true);
                } catch (final RuntimeException e) {
                    log.error("Exception caught while post-committing task " + task.id(), e);
                    maybeSetFirstException(false, maybeWrapTaskException(e, task.id()), firstException);
                    tasksToCloseDirty.add(task);
                    tasksToCloseClean.remove(task);
                }
            }
        }

        for (final Task task : tasksToCloseClean) {
            try {
                task.suspend();
                closeTaskClean(task);
            } catch (final RuntimeException e) {
                log.error("Exception caught while clean-closing active task {}: {}", task.id(), e.getMessage());

                if (task.state() != State.CLOSED) {
                    tasksToCloseDirty.add(task);
                }
                // ignore task migrated exception as it doesn't matter during shutdown
                maybeSetFirstException(true, maybeWrapTaskException(e, task.id()), firstException);
            }
        }

        return tasksToCloseDirty;
    }

    // Returns the set of standby tasks that must be closed dirty
    private Collection<Task> tryCloseCleanStandbyTasks(final Collection<Task> standbyTasksToClose,
                                                       final boolean clean,
                                                       final AtomicReference<RuntimeException> firstException) {
        if (!clean) {
            return standbyTaskIterable();
        }
        final Set<Task> tasksToCloseDirty = new HashSet<>();

        // first committing and then suspend / close clean
        for (final Task task : standbyTasksToClose) {
            try {
                task.prepareCommit();
                task.postCommit(true);
                task.suspend();
                closeTaskClean(task);
            } catch (final RuntimeException e) {
                log.error("Exception caught while clean-closing standby task {}: {}", task.id(), e.getMessage());

                if (task.state() != State.CLOSED) {
                    tasksToCloseDirty.add(task);
                }
                // ignore task migrated exception as it doesn't matter during shutdown
                maybeSetFirstException(true, maybeWrapTaskException(e, task.id()), firstException);
            }
        }
        return tasksToCloseDirty;
    }

    Set<TaskId> activeTaskIds() {
        return activeTaskStream()
            .map(Task::id)
            .collect(Collectors.toSet());
    }

    Set<TaskId> activeRunningTaskIds() {
        return activeRunningTaskStream()
            .map(Task::id)
            .collect(Collectors.toSet());
    }

    Set<TaskId> standbyTaskIds() {
        return standbyTaskStream().map(Task::id).collect(Collectors.toSet());
    }

    Map<TaskId, Task> allTasks() {
        // not bothering with an unmodifiable map, since the tasks themselves are mutable, but
        // if any outside code modifies the map or the tasks, it would be a severe transgression.
        if (stateUpdater != null) {
            final Map<TaskId, Task> ret = stateUpdater.getTasks().stream().collect(Collectors.toMap(Task::id, x -> x));
            ret.putAll(tasks.allTasksPerId());
            return ret;
        } else {
            return tasks.allTasksPerId();
        }
    }

    /**
     * Returns tasks owned by the stream thread. With state updater disabled, these are all tasks. With
     * state updater enabled, this does not return any tasks currently owned by the state updater.
     *
     * TODO: after we complete switching to state updater, we could rename this function as allRunningTasks
     *       to be differentiated from allTasks including running and restoring tasks
     */
    Map<TaskId, Task> allOwnedTasks() {
        // not bothering with an unmodifiable map, since the tasks themselves are mutable, but
        // if any outside code modifies the map or the tasks, it would be a severe transgression.
        return tasks.allTasksPerId();
    }

    Set<Task> readOnlyAllTasks() {
        // not bothering with an unmodifiable map, since the tasks themselves are mutable, but
        // if any outside code modifies the map or the tasks, it would be a severe transgression.
        if (stateUpdater != null) {
            final HashSet<Task> ret = new HashSet<>(stateUpdater.getTasks());
            ret.addAll(tasks.allTasks());
            return Collections.unmodifiableSet(ret);
        } else {
            return Collections.unmodifiableSet(tasks.allTasks());
        }
    }

    Map<TaskId, Task> notPausedTasks() {
        return Collections.unmodifiableMap(tasks.allTasks()
            .stream()
            .filter(t -> !topologyMetadata.isPaused(t.id().topologyName()))
            .collect(Collectors.toMap(Task::id, v -> v)));
    }

    Map<TaskId, Task> activeTaskMap() {
        return activeTaskStream().collect(Collectors.toMap(Task::id, t -> t));
    }

    List<Task> activeTaskIterable() {
        return activeTaskStream().collect(Collectors.toList());
    }

    List<Task> activeRunningTaskIterable() {
        return activeRunningTaskStream().collect(Collectors.toList());
    }

    private Stream<Task> activeTaskStream() {
        if (stateUpdater != null) {
            return Stream.concat(
                activeRunningTaskStream(),
                stateUpdater.getTasks().stream().filter(Task::isActive)
            );
        }
        return activeRunningTaskStream();
    }

    private Stream<Task> activeRunningTaskStream() {
        return tasks.allTasks().stream().filter(Task::isActive);
    }

    Map<TaskId, Task> standbyTaskMap() {
        return standbyTaskStream().collect(Collectors.toMap(Task::id, t -> t));
    }

    private List<Task> standbyTaskIterable() {
        return standbyTaskStream().collect(Collectors.toList());
    }

    private Stream<Task> standbyTaskStream() {
        final Stream<Task> standbyTasksInTaskRegistry = tasks.allTasks().stream().filter(t -> !t.isActive());
        if (stateUpdater != null) {
            return Stream.concat(
                stateUpdater.getStandbyTasks().stream(),
                standbyTasksInTaskRegistry
            );
        } else {
            return standbyTasksInTaskRegistry;
        }
    }

    // For testing only.
    int commitAll() {
        return commit(tasks.allTasks());
    }

    /**
     * Resumes polling in the main consumer for all partitions for which
     * the corresponding record queues have capacity (again).
     */
    public void resumePollingForPartitionsWithAvailableSpace() {
        for (final Task t: tasks.activeTasks()) {
            t.resumePollingForPartitionsWithAvailableSpace();
        }
    }

    /**
     * Fetches up-to-date lag information from the consumer.
     */
    public void updateLags() {
        for (final Task t: tasks.activeTasks()) {
            t.updateLags();
        }
    }

    /**
     * Wake-up any sleeping processing threads.
     */
    public void signalTaskExecutors() {
        if (schedulingTaskManager != null) {
            // Wake up sleeping task executors after every poll, in case there is processing or punctuation to-do.
            schedulingTaskManager.signalTaskExecutors();
        }
    }

    /**
     * Take records and add them to each respective task
     *
     * @param records Records, can be null
     */
    void addRecordsToTasks(final ConsumerRecords<byte[], byte[]> records) {
        for (final TopicPartition partition : records.partitions()) {
            final Task activeTask = tasks.activeTasksForInputPartition(partition);

            if (activeTask == null) {
                log.error("Unable to locate active task for received-record partition {}. Current tasks: {}",
                    partition, toString(">"));
                throw new NullPointerException("Task was unexpectedly missing for partition " + partition);
            }

            activeTask.addRecords(partition, records.records(partition));
        }
    }

    private void maybeLockTasks(final Set<TaskId> ids) {
        if (schedulingTaskManager != null && !ids.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Locking tasks {}", ids.stream().map(TaskId::toString).collect(Collectors.joining(", ")));
            }
            boolean locked = false;
            while (!locked) {
                try {
                    schedulingTaskManager.lockTasks(ids).get();
                    locked = true;
                } catch (final InterruptedException e) {
                    log.warn("Interrupted while waiting for tasks {} to be locked",
                        ids.stream().map(TaskId::toString).collect(Collectors.joining(",")));
                } catch (final ExecutionException e) {
                    log.info("Failed to lock tasks");
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void maybeUnlockTasks(final Set<TaskId> ids) {
        if (schedulingTaskManager != null && !ids.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Unlocking tasks {}", ids.stream().map(TaskId::toString).collect(Collectors.joining(", ")));
            }
            schedulingTaskManager.unlockTasks(ids);
        }
    }

    public void maybeThrowTaskExceptionsFromProcessingThreads() {
        if (schedulingTaskManager != null) {
            maybeThrowTaskExceptions(schedulingTaskManager.drainUncaughtExceptions());
        }
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     * @throws TimeoutException if task.timeout.ms has been exceeded (non-EOS)
     * @throws TaskCorruptedException if committing offsets failed due to TimeoutException (EOS)
     * @return number of committed offsets, or -1 if we are in the middle of a rebalance and cannot commit
     */
    int commit(final Collection<Task> tasksToCommit) {
        int committed = 0;
        final Set<TaskId> ids =
            tasksToCommit.stream()
                .map(Task::id)
                .collect(Collectors.toSet());
        maybeLockTasks(ids);

        // We have to throw the first uncaught exception after locking the tasks, to not attempt to commit failure records.
        maybeThrowTaskExceptionsFromProcessingThreads();

        final Map<Task, Map<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadataPerTask = new HashMap<>();
        try {
            committed = commitTasksAndMaybeUpdateCommittableOffsets(tasksToCommit, consumedOffsetsAndMetadataPerTask);
        } catch (final TimeoutException timeoutException) {
            consumedOffsetsAndMetadataPerTask
                .keySet()
                .forEach(t -> t.maybeInitTaskTimeoutOrThrow(time.milliseconds(), timeoutException));
        }

        maybeUnlockTasks(ids);
        return committed;
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int maybeCommitActiveTasksPerUserRequested() {
        if (rebalanceInProgress) {
            return -1;
        } else {
            for (final Task task : activeRunningTaskIterable()) {
                if (task.commitRequested() && task.commitNeeded()) {
                    return commit(activeRunningTaskIterable());
                }
            }
            return 0;
        }
    }

    private int commitTasksAndMaybeUpdateCommittableOffsets(final Collection<Task> tasksToCommit,
                                                            final Map<Task, Map<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadata) {
        if (rebalanceInProgress) {
            return -1;
        } else {
            return taskExecutor.commitTasksAndMaybeUpdateCommittableOffsets(tasksToCommit, consumedOffsetsAndMetadata);
        }
    }

    public void updateTaskEndMetadata(final TopicPartition topicPartition, final Long offset) {
        for (final Task task : tasks.activeTasks()) {
            if (task instanceof StreamTask) {
                if (task.inputPartitions().contains(topicPartition)) {
                    ((StreamTask) task).updateEndOffsets(topicPartition, offset);
                }
            }
        }
    }

    /**
     * Handle any added or removed NamedTopologies. Check if any uncreated assigned tasks belong to a newly
     * added NamedTopology and create them if so, then close any tasks whose named topology no longer exists
     */
    void handleTopologyUpdates() {
        topologyMetadata.executeTopologyUpdatesAndBumpThreadVersion(
            this::createPendingTasks,
            this::maybeCloseTasksFromRemovedTopologies
        );

        if (topologyMetadata.isEmpty()) {
            log.info("Proactively unsubscribing from all topics due to empty topology");
            mainConsumer.unsubscribe();
        }

        topologyMetadata.maybeNotifyTopologyVersionListeners();
    }

    void maybeCloseTasksFromRemovedTopologies(final Set<String> currentNamedTopologies) {
        try {
            final Set<Task> activeTasksToRemove = new HashSet<>();
            final Set<Task> standbyTasksToRemove = new HashSet<>();
            for (final Task task : tasks.allTasks()) {
                if (!currentNamedTopologies.contains(task.id().topologyName())) {
                    if (task.isActive()) {
                        activeTasksToRemove.add(task);
                    } else {
                        standbyTasksToRemove.add(task);
                    }
                }
            }

            final Set<Task> allTasksToRemove = union(HashSet::new, activeTasksToRemove, standbyTasksToRemove);
            closeAndCleanUpTasks(activeTasksToRemove, standbyTasksToRemove, true);
            releaseLockedDirectoriesForTasks(allTasksToRemove.stream().map(Task::id).collect(Collectors.toSet()));
        } catch (final Exception e) {
            // TODO KAFKA-12648: for now just swallow the exception to avoid interfering with the other topologies
            //  that are running alongside, but eventually we should be able to rethrow up to the handler to inform
            //  the user of an error in this named topology without killing the thread and delaying the others
            log.error("Caught the following exception while closing tasks from a removed topology:", e);
        }
    }

    void createPendingTasks(final Set<String> currentNamedTopologies) {
        final Map<TaskId, Set<TopicPartition>> activeTasksToCreate = tasks.drainPendingActiveTasksForTopologies(currentNamedTopologies);
        final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate = tasks.drainPendingStandbyTasksForTopologies(currentNamedTopologies);

        createNewTasks(activeTasksToCreate, standbyTasksToCreate);
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     * @throws StreamsException      if any task threw an exception while processing
     */
    int process(final int maxNumRecords, final Time time) {
        return taskExecutor.process(maxNumRecords, time);
    }

    void recordTaskProcessRatio(final long totalProcessLatencyMs, final long now) {
        for (final Task task : activeRunningTaskIterable()) {
            task.recordProcessTimeRatioAndBufferSize(totalProcessLatencyMs, now);
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int punctuate() {
        return  taskExecutor.punctuate();
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
            for (final Task task : activeRunningTaskIterable()) {
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
        for (final Task task : tasks.allTasks()) {
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

    private void maybeSetFirstException(final boolean ignoreTaskMigrated,
                                        final RuntimeException exception,
                                        final AtomicReference<RuntimeException> firstException) {
        if (!ignoreTaskMigrated || !(exception instanceof TaskMigratedException)) {
            firstException.compareAndSet(null, exception);
        }
    }

    private StreamsException maybeWrapTaskException(final RuntimeException exception, final TaskId taskId) {
        if (exception instanceof StreamsException) {
            final StreamsException streamsException = (StreamsException) exception;
            streamsException.setTaskId(taskId);
            return streamsException;
        } else {
            return new StreamsException(exception, taskId);
        }
    }

    public static void executeAndMaybeSwallow(final boolean clean,
                                              final Runnable runnable,
                                              final java.util.function.Consumer<RuntimeException> actionIfClean,
                                              final java.util.function.Consumer<RuntimeException> actionIfNotClean) {
        try {
            runnable.run();
        } catch (final RuntimeException e) {
            if (clean) {
                actionIfClean.accept(e);
            } else {
                actionIfNotClean.accept(e);
            }
        }
    }

    public static void executeAndMaybeSwallow(final boolean clean,
                                              final Runnable runnable,
                                              final String name,
                                              final Logger log) {
        executeAndMaybeSwallow(
            clean,
            runnable,
            e -> {
                throw e;
            },
            e -> log.debug("Ignoring error in unclean {}", name));
    }

    boolean needsInitializationOrRestoration() {
        return activeTaskStream().anyMatch(Task::needsInitializationOrRestoration);
    }

    // for testing only
    void addTask(final Task task) {
        tasks.addTask(task);
    }
}
