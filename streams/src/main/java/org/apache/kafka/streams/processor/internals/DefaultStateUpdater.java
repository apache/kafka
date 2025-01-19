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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.apache.kafka.streams.processor.internals.TaskAndAction.Action;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATIO_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_ID_TAG;

public class DefaultStateUpdater implements StateUpdater {

    private static final String BUG_ERROR_MESSAGE = "This indicates a bug. " +
        "Please report at https://issues.apache.org/jira/projects/KAFKA/issues or to the dev-mailing list (https://kafka.apache.org/contact).";

    private class StateUpdaterThread extends Thread {

        private final ChangelogReader changelogReader;
        private final StateUpdaterMetrics updaterMetrics;
        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private final AtomicBoolean isIdle = new AtomicBoolean(false);
        private final Map<TaskId, Task> updatingTasks = new ConcurrentHashMap<>();
        private final Map<TaskId, Task> pausedTasks = new ConcurrentHashMap<>();

        private long totalCheckpointLatency = 0L;

        private volatile long fetchDeadlineClientInstanceId = -1L;
        private volatile KafkaFutureImpl<Uuid> clientInstanceIdFuture = new KafkaFutureImpl<>();

        public StateUpdaterThread(final String name,
                                  final Metrics metrics,
                                  final ChangelogReader changelogReader) {
            super(name);
            this.changelogReader = changelogReader;
            this.updaterMetrics = new StateUpdaterMetrics(metrics, name);
        }

        public Collection<Task> updatingTasks() {
            return updatingTasks.values();
        }

        public Collection<StandbyTask> updatingStandbyTasks() {
            return updatingTasks.values().stream()
                .filter(t -> !t.isActive())
                .map(t -> (StandbyTask) t)
                .collect(Collectors.toList());
        }

        private boolean onlyStandbyTasksUpdating() {
            return !updatingTasks.isEmpty() && updatingTasks.values().stream().noneMatch(Task::isActive);
        }

        public Collection<Task> pausedTasks() {
            return pausedTasks.values();
        }

        public long numUpdatingStandbyTasks() {
            return updatingTasks.values().stream()
                .filter(t -> !t.isActive())
                .count();
        }

        public long numRestoringActiveTasks() {
            return updatingTasks.values().stream()
                .filter(Task::isActive)
                .count();
        }

        public long numPausedStandbyTasks() {
            return pausedTasks.values().stream()
                .filter(t -> !t.isActive())
                .count();
        }

        public long numPausedActiveTasks() {
            return pausedTasks.values().stream()
                .filter(Task::isActive)
                .count();
        }

        @Override
        public void run() {
            log.info("State updater thread started");
            try {
                while (isRunning.get()) {
                    runOnce();
                }
            } catch (final RuntimeException anyOtherException) {
                handleRuntimeException(anyOtherException);
            } finally {
                clearInputQueue();
                clearUpdatingAndPausedTasks();
                updaterMetrics.clear();
                log.info("State updater thread stopped");
            }
        }

        private void clearInputQueue() {
            tasksAndActionsLock.lock();
            try {
                tasksAndActions.clear();
            } finally {
                tasksAndActionsLock.unlock();
            }
        }

        // In each iteration:
        //   1) check if updating tasks need to be paused
        //   2) check if paused tasks need to be resumed
        //   3) restore those updating tasks
        //   4) checkpoint those updating task states
        //   5) idle waiting if there is no more tasks to be restored
        //
        //   Note that, 1-3) are measured as restoring time, while 4) and 5) measured separately
        //   as checkpointing time and idle time
        private void runOnce() {
            final long totalStartTimeMs = time.milliseconds();
            performActionsOnTasks();

            resumeTasks();
            pauseTasks();
            restoreTasks(totalStartTimeMs);

            maybeGetClientInstanceIds();

            final long checkpointStartTimeMs = time.milliseconds();
            maybeCheckpointTasks(checkpointStartTimeMs);

            final long waitStartTimeMs = time.milliseconds();
            waitIfAllChangelogsCompletelyRead();

            final long endTimeMs = time.milliseconds();
            final long totalWaitTime = Math.max(0L, endTimeMs - waitStartTimeMs);
            final long totalTime = Math.max(0L, endTimeMs - totalStartTimeMs);

            recordMetrics(endTimeMs, totalTime, totalWaitTime);
        }

        private void performActionsOnTasks() {
            tasksAndActionsLock.lock();
            try {
                for (final TaskAndAction taskAndAction : tasksAndActions()) {
                    final Action action = taskAndAction.action();
                    switch (action) {
                        case ADD:
                            addTask(taskAndAction.task());
                            break;
                        case REMOVE:
                            if (taskAndAction.futureForRemove() == null) {
                                removeTask(taskAndAction.taskId());
                            } else {
                                removeTask(taskAndAction.taskId(), taskAndAction.futureForRemove());
                            }
                            break;
                        default:
                            throw new IllegalStateException("Unknown action type " + action);
                    }
                }
            } finally {
                tasksAndActionsLock.unlock();
            }
        }

        private void resumeTasks() {
            if (isTopologyResumed.compareAndSet(true, false)) {
                for (final Task task : pausedTasks.values()) {
                    if (!topologyMetadata.isPaused(task.id().topologyName())) {
                        resumeTask(task);
                    }
                }
            }
        }

        private void pauseTasks() {
            for (final Task task : updatingTasks.values()) {
                if (topologyMetadata.isPaused(task.id().topologyName())) {
                    pauseTask(task);
                }
            }
        }

        private void restoreTasks(final long now) {
            try {
                final long restored = changelogReader.restore(updatingTasks);
                updaterMetrics.restoreSensor.record(restored, now);
            } catch (final TaskCorruptedException taskCorruptedException) {
                handleTaskCorruptedException(taskCorruptedException);
            } catch (final StreamsException streamsException) {
                handleStreamsException(streamsException);
            }
            final Set<TopicPartition> completedChangelogs = changelogReader.completedChangelogs();
            final List<Task> activeTasks = updatingTasks.values().stream().filter(Task::isActive).collect(Collectors.toList());
            for (final Task task : activeTasks) {
                maybeCompleteRestoration((StreamTask) task, completedChangelogs);
            }
        }

        private void maybeGetClientInstanceIds() {
            if (fetchDeadlineClientInstanceId != -1) {
                if (!clientInstanceIdFuture.isDone()) {
                    if (fetchDeadlineClientInstanceId >= time.milliseconds()) {
                        try {
                            // if the state-updated thread has active work:
                            //    we pass in a timeout of zero into each `clientInstanceId()` call
                            //    to just trigger the "get instance id" background RPC;
                            //    we don't want to block the state updater thread that can do useful work in the meantime
                            // otherwise, we pass in 100ms to avoid busy waiting
                            clientInstanceIdFuture.complete(
                                restoreConsumer.clientInstanceId(
                                    allWorkDone() ? Duration.ofMillis(100L) : Duration.ZERO
                                )
                            );
                            fetchDeadlineClientInstanceId = -1L;
                        } catch (final IllegalStateException disabledError) {
                            // if telemetry is disabled on a client, we swallow the error,
                            // to allow returning a partial result for all other clients
                            clientInstanceIdFuture.complete(null);
                            fetchDeadlineClientInstanceId = -1L;
                        } catch (final TimeoutException swallow) {
                            // swallow
                        } catch (final Exception error) {
                            clientInstanceIdFuture.completeExceptionally(error);
                            fetchDeadlineClientInstanceId = -1L;
                        }
                    } else {
                        clientInstanceIdFuture.completeExceptionally(
                            new TimeoutException("Could not retrieve restore consumer client instance id.")
                        );
                        fetchDeadlineClientInstanceId = -1L;
                    }
                }
            }
        }

        private KafkaFutureImpl<Uuid> restoreConsumerInstanceId(final Duration timeout) {
            boolean setDeadline = false;

            if (clientInstanceIdFuture.isDone()) {
                if (clientInstanceIdFuture.isCompletedExceptionally()) {
                    clientInstanceIdFuture = new KafkaFutureImpl<>();
                    setDeadline = true;
                }
            } else {
                setDeadline = true;
            }

            if (setDeadline) {
                fetchDeadlineClientInstanceId = time.milliseconds() + timeout.toMillis();
                tasksAndActionsLock.lock();
                try {
                    tasksAndActionsCondition.signalAll();
                } finally {
                    tasksAndActionsLock.unlock();
                }
            }

            return clientInstanceIdFuture;
        }


        private void handleRuntimeException(final RuntimeException runtimeException) {
            log.error("An unexpected error occurred within the state updater thread: {}", String.valueOf(runtimeException));
            addToExceptionsAndFailedTasksThenClearUpdatingAndPausedTasks(runtimeException);
            isRunning.set(false);
        }

        private void handleTaskCorruptedException(final TaskCorruptedException taskCorruptedException) {
            log.info("Encountered task corrupted exception: ", taskCorruptedException);
            final Set<TaskId> corruptedTaskIds = taskCorruptedException.corruptedTasks();
            final Set<Task> corruptedTasks = new HashSet<>();
            final Set<TopicPartition> changelogsOfCorruptedTasks = new HashSet<>();
            for (final TaskId taskId : corruptedTaskIds) {
                final Task corruptedTask = updatingTasks.get(taskId);
                if (corruptedTask == null) {
                    throw new IllegalStateException("Task " + taskId + " is corrupted but is not updating. " + BUG_ERROR_MESSAGE);
                }
                corruptedTasks.add(corruptedTask);
                removeCheckpointForCorruptedTask(corruptedTask);
                changelogsOfCorruptedTasks.addAll(corruptedTask.changelogPartitions());
            }
            changelogReader.unregister(changelogsOfCorruptedTasks);
            corruptedTasks.forEach(
                task -> addToExceptionsAndFailedTasksThenRemoveFromUpdatingTasks(new ExceptionAndTask(taskCorruptedException, task))
            );
        }

        // TODO: we can let the exception encode the actual corrupted changelog partitions and only
        //       mark those instead of marking all changelogs
        private void removeCheckpointForCorruptedTask(final Task task) {
            task.markChangelogAsCorrupted(task.changelogPartitions());

            // we need to enforce a checkpoint that removes the corrupted partitions
            measureCheckpointLatency(() -> task.maybeCheckpoint(true));
        }

        private void handleStreamsException(final StreamsException streamsException) {
            log.info("Encountered streams exception: ", streamsException);
            if (streamsException.taskId().isPresent()) {
                handleStreamsExceptionWithTask(streamsException);
            } else {
                handleStreamsExceptionWithoutTask(streamsException);
            }
        }

        private void handleStreamsExceptionWithTask(final StreamsException streamsException) {
            final TaskId failedTaskId = streamsException.taskId().get();
            if (updatingTasks.containsKey(failedTaskId)) {
                addToExceptionsAndFailedTasksThenRemoveFromUpdatingTasks(
                    new ExceptionAndTask(streamsException, updatingTasks.get(failedTaskId))
                );
            } else if (pausedTasks.containsKey(failedTaskId)) {
                addToExceptionsAndFailedTasksThenRemoveFromPausedTasks(
                    new ExceptionAndTask(streamsException, pausedTasks.get(failedTaskId))
                );
            } else {
                throw new IllegalStateException("Task " + failedTaskId + " failed but is not updating or paused. " + BUG_ERROR_MESSAGE);
            }
        }

        private void handleStreamsExceptionWithoutTask(final StreamsException streamsException) {
            addToExceptionsAndFailedTasksThenClearUpdatingAndPausedTasks(streamsException);
        }

        // It is important to remove the corrupted tasks from the updating tasks after they were added to the
        // failed tasks.
        // This ensures that all tasks are found in DefaultStateUpdater#getTasks().
        private void addToExceptionsAndFailedTasksThenRemoveFromUpdatingTasks(final ExceptionAndTask exceptionAndTask) {
            exceptionsAndFailedTasksLock.lock();
            try {
                exceptionsAndFailedTasks.add(exceptionAndTask);
                updatingTasks.remove(exceptionAndTask.task().id());
                if (exceptionAndTask.task().isActive()) {
                    transitToUpdateStandbysIfOnlyStandbysLeft();
                }
            } finally {
                exceptionsAndFailedTasksLock.unlock();
            }
        }

        private void addToExceptionsAndFailedTasksThenRemoveFromPausedTasks(final ExceptionAndTask exceptionAndTask) {
            exceptionsAndFailedTasksLock.lock();
            try {
                exceptionsAndFailedTasks.add(exceptionAndTask);
                pausedTasks.remove(exceptionAndTask.task().id());
                if (exceptionAndTask.task().isActive()) {
                    transitToUpdateStandbysIfOnlyStandbysLeft();
                }
            } finally {
                exceptionsAndFailedTasksLock.unlock();
            }
        }

        private void addToExceptionsAndFailedTasksThenClearUpdatingAndPausedTasks(final RuntimeException runtimeException) {
            exceptionsAndFailedTasksLock.lock();
            try {
                updatingTasks.values().forEach(
                    task -> exceptionsAndFailedTasks.add(new ExceptionAndTask(runtimeException, task))
                );
                updatingTasks.clear();
                pausedTasks.values().forEach(
                    task -> exceptionsAndFailedTasks.add(new ExceptionAndTask(runtimeException, task))
                );
                pausedTasks.clear();
            } finally {
                exceptionsAndFailedTasksLock.unlock();
            }
        }

        private void waitIfAllChangelogsCompletelyRead() {
            tasksAndActionsLock.lock();
            try {
                while (allWorkDone() && fetchDeadlineClientInstanceId == -1L) {
                    isIdle.set(true);
                    tasksAndActionsCondition.await();
                }
            } catch (final InterruptedException ignored) {
                // we never interrupt the thread, but only signal the condition
                // and hence this exception should never be thrown
            } finally {
                tasksAndActionsLock.unlock();
                isIdle.set(false);
            }
        }

        private boolean allWorkDone() {
            final boolean noTasksToUpdate = changelogReader.allChangelogsCompleted() || updatingTasks.isEmpty();

            return isRunning.get() &&
                noTasksToUpdate &&
                tasksAndActions.isEmpty() &&
                !isTopologyResumed.get();
        }

        private void clearUpdatingAndPausedTasks() {
            updatingTasks.clear();
            pausedTasks.clear();
            changelogReader.clear();
        }

        private List<TaskAndAction> tasksAndActions() {
            final List<TaskAndAction> tasksAndActionsToProcess = new ArrayList<>(tasksAndActions);
            tasksAndActions.clear();
            return tasksAndActionsToProcess;
        }

        private void addTask(final Task task) {
            final TaskId taskId = task.id();

            Task existingTask = pausedTasks.get(taskId);
            if (existingTask != null) {
                throw new IllegalStateException(
                    (existingTask.isActive() ? "Active" : "Standby") + " task " + taskId + " already exist in paused tasks, " +
                        "should not try to add another " + (task.isActive() ? "active" : "standby") + " task with the same id. "
                        + BUG_ERROR_MESSAGE);
            }
            existingTask = updatingTasks.get(taskId);
            if (existingTask != null) {
                throw new IllegalStateException(
                    (existingTask.isActive() ? "Active" : "Standby") + " task " + taskId + " already exist in updating tasks, " +
                        "should not try to add another " + (task.isActive() ? "active" : "standby") + " task with the same id. "
                        + BUG_ERROR_MESSAGE);
            }

            if (isStateless(task)) {
                addToRestoredTasks((StreamTask) task);
                log.info("Stateless active task " + taskId + " was added to the restored tasks of the state updater");
            } else if (topologyMetadata.isPaused(taskId.topologyName())) {
                pausedTasks.put(taskId, task);
                changelogReader.register(task.changelogPartitions(), task.stateManager());
                log.debug((task.isActive() ? "Active" : "Standby")
                    + " task " + taskId + " was directly added to the paused tasks.");
            } else {
                updatingTasks.put(taskId, task);
                changelogReader.register(task.changelogPartitions(), task.stateManager());
                if (task.isActive()) {
                    log.info("Stateful active task " + taskId + " was added to the state updater");
                    changelogReader.enforceRestoreActive();
                } else {
                    log.info("Standby task " + taskId + " was added to the state updater");
                    if (updatingTasks.size() == 1) {
                        changelogReader.transitToUpdateStandby();
                    }
                }
            }
        }

        private void removeTask(final TaskId taskId, final CompletableFuture<RemovedTaskResult> future) {
            try {
                if (!removeUpdatingTask(taskId, future)
                    && !removePausedTask(taskId, future)
                    && !removeRestoredTask(taskId, future)
                    && !removeFailedTask(taskId, future)) {

                    future.complete(null);
                    log.warn("Task {} could not be removed from the state updater because the state updater does not"
                        + " own this task.", taskId);
                }
            } catch (final StreamsException streamsException) {
                handleStreamsException(streamsException);
                future.completeExceptionally(streamsException);
            } catch (final RuntimeException runtimeException) {
                handleRuntimeException(runtimeException);
                future.completeExceptionally(runtimeException);
            }
        }

        private boolean removeUpdatingTask(final TaskId taskId, final CompletableFuture<RemovedTaskResult> future) {
            if (!updatingTasks.containsKey(taskId)) {
                return false;
            }
            final Task task = updatingTasks.get(taskId);
            prepareUpdatingTaskForRemoval(task);
            updatingTasks.remove(taskId);
            if (task.isActive()) {
                transitToUpdateStandbysIfOnlyStandbysLeft();
            }
            log.info((task.isActive() ? "Active" : "Standby")
                + " task " + task.id() + " was removed from the updating tasks.");
            future.complete(new RemovedTaskResult(task));
            return true;
        }

        private void prepareUpdatingTaskForRemoval(final Task task) {
            measureCheckpointLatency(() -> task.maybeCheckpoint(true));
            final Collection<TopicPartition> changelogPartitions = task.changelogPartitions();
            changelogReader.unregister(changelogPartitions);
        }

        private boolean removePausedTask(final TaskId taskId, final CompletableFuture<RemovedTaskResult> future) {
            if (!pausedTasks.containsKey(taskId)) {
                return false;
            }
            final Task task = pausedTasks.get(taskId);
            preparePausedTaskForRemoval(task);
            pausedTasks.remove(taskId);
            log.info((task.isActive() ? "Active" : "Standby")
                + " task " + task.id() + " was removed from the paused tasks.");
            future.complete(new RemovedTaskResult(task));
            return true;
        }

        private void preparePausedTaskForRemoval(final Task task) {
            final Collection<TopicPartition> changelogPartitions = task.changelogPartitions();
            changelogReader.unregister(changelogPartitions);
        }

        private boolean removeRestoredTask(final TaskId taskId, final CompletableFuture<RemovedTaskResult> future) {
            restoredActiveTasksLock.lock();
            try {
                final Iterator<StreamTask> iterator = restoredActiveTasks.iterator();
                while (iterator.hasNext()) {
                    final StreamTask restoredTask = iterator.next();
                    if (restoredTask.id().equals(taskId)) {
                        iterator.remove();
                        log.info((restoredTask.isActive() ? "Active" : "Standby")
                            + " task " + restoredTask.id() + " was removed from the restored tasks.");
                        future.complete(new RemovedTaskResult(restoredTask));
                        return true;
                    }
                }
                return false;
            } finally {
                restoredActiveTasksLock.unlock();
            }
        }

        private boolean removeFailedTask(final TaskId taskId, final CompletableFuture<RemovedTaskResult> future) {
            exceptionsAndFailedTasksLock.lock();
            try {
                final Iterator<ExceptionAndTask> iterator = exceptionsAndFailedTasks.iterator();
                while (iterator.hasNext()) {
                    final ExceptionAndTask exceptionAndTask = iterator.next();
                    final Task failedTask = exceptionAndTask.task();
                    if (failedTask.id().equals(taskId)) {
                        iterator.remove();
                        log.info((failedTask.isActive() ? "Active" : "Standby")
                            + " task " + failedTask.id() + " was removed from the failed tasks.");
                        future.complete(new RemovedTaskResult(failedTask, exceptionAndTask.exception()));
                        return true;
                    }
                }
                return false;
            } finally {
                exceptionsAndFailedTasksLock.unlock();
            }
        }

        private void removeTask(final TaskId taskId) {
            final Task task;
            if (updatingTasks.containsKey(taskId)) {
                task = updatingTasks.get(taskId);
                measureCheckpointLatency(() -> task.maybeCheckpoint(true));
                final Collection<TopicPartition> changelogPartitions = task.changelogPartitions();
                changelogReader.unregister(changelogPartitions);
                removedTasks.add(task);
                updatingTasks.remove(taskId);
                if (task.isActive()) {
                    transitToUpdateStandbysIfOnlyStandbysLeft();
                }
                log.info((task.isActive() ? "Active" : "Standby")
                    + " task " + task.id() + " was removed from the updating tasks and added to the removed tasks.");
            } else if (pausedTasks.containsKey(taskId)) {
                task = pausedTasks.get(taskId);
                final Collection<TopicPartition> changelogPartitions = task.changelogPartitions();
                changelogReader.unregister(changelogPartitions);
                removedTasks.add(task);
                pausedTasks.remove(taskId);
                log.info((task.isActive() ? "Active" : "Standby")
                    + " task " + task.id() + " was removed from the paused tasks and added to the removed tasks.");
            } else {
                log.info("Task " + taskId + " was not removed since it is not updating or paused.");
            }
        }

        private void pauseTask(final Task task) {
            final TaskId taskId = task.id();
            // do not need to unregister changelog partitions for paused tasks
            measureCheckpointLatency(() -> task.maybeCheckpoint(true));
            pausedTasks.put(taskId, task);
            updatingTasks.remove(taskId);
            if (task.isActive()) {
                transitToUpdateStandbysIfOnlyStandbysLeft();
            }
            log.info((task.isActive() ? "Active" : "Standby")
                + " task " + task.id() + " was paused from the updating tasks and added to the paused tasks.");
        }

        private void resumeTask(final Task task) {
            final TaskId taskId = task.id();
            updatingTasks.put(taskId, task);
            pausedTasks.remove(taskId);

            if (task.isActive()) {
                log.info("Stateful active task " + task.id() + " was resumed to the updating tasks of the state updater");
                changelogReader.enforceRestoreActive();
            } else {
                log.info("Standby task " + task.id() + " was resumed to the updating tasks of the state updater");
                if (updatingTasks.size() == 1) {
                    changelogReader.transitToUpdateStandby();
                }
            }
        }

        private boolean isStateless(final Task task) {
            return task.changelogPartitions().isEmpty() && task.isActive();
        }

        private void maybeCompleteRestoration(final StreamTask task,
                                              final Set<TopicPartition> restoredChangelogs) {
            final Collection<TopicPartition> changelogPartitions = task.changelogPartitions();
            if (restoredChangelogs.containsAll(changelogPartitions)) {
                measureCheckpointLatency(() -> task.maybeCheckpoint(true));
                changelogReader.unregister(changelogPartitions);
                addToRestoredTasks(task);
                updatingTasks.remove(task.id());
                log.info("Stateful active task " + task.id() + " completed restoration");
                transitToUpdateStandbysIfOnlyStandbysLeft();
            }
        }

        private void transitToUpdateStandbysIfOnlyStandbysLeft() {
            if (onlyStandbyTasksUpdating()) {
                changelogReader.transitToUpdateStandby();
            }
        }

        private void addToRestoredTasks(final StreamTask task) {
            restoredActiveTasksLock.lock();
            try {
                restoredActiveTasks.add(task);
                log.debug("Active task " + task.id() + " was added to the restored tasks");
                restoredActiveTasksCondition.signalAll();
            } finally {
                restoredActiveTasksLock.unlock();
            }
        }

        private void maybeCheckpointTasks(final long now) {
            final long elapsedMsSinceLastCommit = now - lastCommitMs;
            if (elapsedMsSinceLastCommit > commitIntervalMs) {
                if (log.isDebugEnabled()) {
                    log.debug("Checkpointing state of all restoring tasks since {}ms has elapsed (commit interval is {}ms)",
                        elapsedMsSinceLastCommit, commitIntervalMs);
                }

                measureCheckpointLatency(() -> {
                    for (final Task task : updatingTasks.values()) {
                        // do not enforce checkpointing during restoration if its position has not advanced much
                        task.maybeCheckpoint(false);
                    }
                });

                lastCommitMs = now;
            }
        }

        private void measureCheckpointLatency(final Runnable actionToMeasure) {
            final long startMs = time.milliseconds();
            try {
                actionToMeasure.run();
            } finally {
                totalCheckpointLatency += Math.max(0L, time.milliseconds() - startMs);
            }
        }

        private void recordMetrics(final long now, final long totalLatency, final long totalWaitLatency) {
            final long totalRestoreLatency = Math.max(0L, totalLatency - totalWaitLatency - totalCheckpointLatency);

            updaterMetrics.idleRatioSensor.record((double) totalWaitLatency / totalLatency, now);
            updaterMetrics.checkpointRatioSensor.record((double) totalCheckpointLatency / totalLatency, now);

            if (changelogReader.isRestoringActive()) {
                updaterMetrics.activeRestoreRatioSensor.record((double) totalRestoreLatency / totalLatency, now);
                updaterMetrics.standbyRestoreRatioSensor.record(0.0d, now);
            } else {
                updaterMetrics.standbyRestoreRatioSensor.record((double) totalRestoreLatency / totalLatency, now);
                updaterMetrics.activeRestoreRatioSensor.record(0.0d, now);
            }

            totalCheckpointLatency = 0L;
        }
    }

    private final Time time;
    private final Logger log;
    private final String name;
    private final Metrics metrics;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final ChangelogReader changelogReader;
    private final TopologyMetadata topologyMetadata;
    private final Queue<TaskAndAction> tasksAndActions = new LinkedList<>();
    private final Lock tasksAndActionsLock = new ReentrantLock();
    private final Condition tasksAndActionsCondition = tasksAndActionsLock.newCondition();
    private final Queue<StreamTask> restoredActiveTasks = new LinkedList<>();
    private final Lock restoredActiveTasksLock = new ReentrantLock();
    private final Condition restoredActiveTasksCondition = restoredActiveTasksLock.newCondition();
    private final Lock exceptionsAndFailedTasksLock = new ReentrantLock();
    private final Queue<ExceptionAndTask> exceptionsAndFailedTasks = new LinkedList<>();
    private final BlockingQueue<Task> removedTasks = new LinkedBlockingQueue<>();
    private final AtomicBoolean isTopologyResumed = new AtomicBoolean(false);

    private final long commitIntervalMs;
    private long lastCommitMs;

    private StateUpdaterThread stateUpdaterThread = null;

    public DefaultStateUpdater(final String name,
                               final Metrics metrics,
                               final StreamsConfig config,
                               final Consumer<byte[], byte[]> restoreConsumer,
                               final ChangelogReader changelogReader,
                               final TopologyMetadata topologyMetadata,
                               final Time time) {
        this.time = time;
        this.name = name;
        this.metrics = metrics;
        this.restoreConsumer = restoreConsumer;
        this.changelogReader = changelogReader;
        this.topologyMetadata = topologyMetadata;
        this.commitIntervalMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);

        final String logPrefix = String.format("state-updater [%s] ", name);
        final LogContext logContext = new LogContext(logPrefix);
        this.log = logContext.logger(DefaultStateUpdater.class);
    }

    public void start() {
        if (stateUpdaterThread == null) {
            if (!restoredActiveTasks.isEmpty() || !exceptionsAndFailedTasks.isEmpty()) {
                throw new IllegalStateException("State updater started with non-empty output queues. "
                    + BUG_ERROR_MESSAGE);
            }
            stateUpdaterThread = new StateUpdaterThread(name, metrics, changelogReader);
            stateUpdaterThread.start();

            // initialize the last commit as of now to prevent first commit happens immediately
            this.lastCommitMs = time.milliseconds();
        }
    }

    @Override
    public void shutdown(final Duration timeout) {
        if (stateUpdaterThread != null) {
            log.info("Shutting down state updater thread");

            // first set the running flag and then
            // notify the condition in case the thread is waiting on it;
            // note this ordering should not be changed
            stateUpdaterThread.isRunning.set(false);

            tasksAndActionsLock.lock();
            try {
                tasksAndActionsCondition.signalAll();
            } finally {
                tasksAndActionsLock.unlock();
            }

            try {
                stateUpdaterThread.join(timeout.toMillis());
                if (stateUpdaterThread.isAlive()) {
                    throw new StreamsException("State updater thread did not shutdown within the timeout");
                }
                stateUpdaterThread = null;
            } catch (final InterruptedException ignored) {
            }
        }
    }

    @Override
    public void add(final Task task) {
        verifyStateFor(task);

        tasksAndActionsLock.lock();
        try {
            tasksAndActions.add(TaskAndAction.createAddTask(task));
            tasksAndActionsCondition.signalAll();
        } finally {
            tasksAndActionsLock.unlock();
        }
    }

    private void verifyStateFor(final Task task) {
        if (task.isActive() && task.state() != State.RESTORING) {
            throw new IllegalStateException("Active task " + task.id() + " is not in state RESTORING. " + BUG_ERROR_MESSAGE);
        }
        if (!task.isActive() && task.state() != State.RUNNING) {
            throw new IllegalStateException("Standby task " + task.id() + " is not in state RUNNING. " + BUG_ERROR_MESSAGE);
        }
    }

    @Override
    public CompletableFuture<RemovedTaskResult> remove(final TaskId taskId) {
        final CompletableFuture<RemovedTaskResult> future = new CompletableFuture<>();
        tasksAndActionsLock.lock();
        try {
            tasksAndActions.add(TaskAndAction.createRemoveTask(taskId, future));
            tasksAndActionsCondition.signalAll();
        } finally {
            tasksAndActionsLock.unlock();
        }
        return future;
    }

    @Override
    public void signalResume() {
        tasksAndActionsLock.lock();
        try {
            isTopologyResumed.set(true);
            tasksAndActionsCondition.signalAll();
        } finally {
            tasksAndActionsLock.unlock();
        }
    }

    @Override
    public Set<StreamTask> drainRestoredActiveTasks(final Duration timeout) {
        final long timeoutMs = timeout.toMillis();
        final long startTime = time.milliseconds();
        final long deadline = startTime + timeoutMs;
        long now = startTime;
        final Set<StreamTask> result = new HashSet<>();
        try {
            while (now <= deadline && result.isEmpty()) {
                restoredActiveTasksLock.lock();
                try {
                    while (restoredActiveTasks.isEmpty() && now <= deadline) {
                        final boolean elapsed = restoredActiveTasksCondition.await(deadline - now, TimeUnit.MILLISECONDS);
                        now = time.milliseconds();
                    }
                    result.addAll(restoredActiveTasks);
                    restoredActiveTasks.clear();
                } finally {
                    restoredActiveTasksLock.unlock();
                }
                now = time.milliseconds();
            }
            return result;
        } catch (final InterruptedException ignored) {
        }
        return result;
    }

    @Override
    public List<ExceptionAndTask> drainExceptionsAndFailedTasks() {
        final List<ExceptionAndTask> result = new ArrayList<>();
        exceptionsAndFailedTasksLock.lock();
        try {
            result.addAll(exceptionsAndFailedTasks);
            exceptionsAndFailedTasks.clear();
        } finally {
            exceptionsAndFailedTasksLock.unlock();
        }
        return result;
    }

    @Override
    public boolean hasExceptionsAndFailedTasks() {
        exceptionsAndFailedTasksLock.lock();
        try {
            return !exceptionsAndFailedTasks.isEmpty();
        } finally {
            exceptionsAndFailedTasksLock.unlock();
        }
    }

    public Set<StandbyTask> updatingStandbyTasks() {
        return stateUpdaterThread != null
            ? Set.copyOf(stateUpdaterThread.updatingStandbyTasks())
            : Collections.emptySet();
    }

    @Override
    public Set<Task> updatingTasks() {
        return stateUpdaterThread != null
            ? Set.copyOf(stateUpdaterThread.updatingTasks())
            : Collections.emptySet();
    }

    public Set<StreamTask> restoredActiveTasks() {
        restoredActiveTasksLock.lock();
        try {
            return Set.copyOf(restoredActiveTasks);
        } finally {
            restoredActiveTasksLock.unlock();
        }
    }

    public List<ExceptionAndTask> exceptionsAndFailedTasks() {
        exceptionsAndFailedTasksLock.lock();
        try {
            return List.copyOf(exceptionsAndFailedTasks);
        } finally {
            exceptionsAndFailedTasksLock.unlock();
        }
    }

    public Set<Task> removedTasks() {
        return Set.copyOf(removedTasks);
    }

    public Set<Task> pausedTasks() {
        return stateUpdaterThread != null
            ? Set.copyOf(stateUpdaterThread.pausedTasks())
            : Collections.emptySet();
    }

    @Override
    public Set<Task> tasks() {
        return executeWithQueuesLocked(() -> streamOfTasks().map(ReadOnlyTask::new).collect(Collectors.toSet()));
    }

    @Override
    public boolean restoresActiveTasks() {
        return !executeWithQueuesLocked(
            () -> streamOfTasks().filter(Task::isActive).collect(Collectors.toSet())
        ).isEmpty();
    }

    public Set<StreamTask> activeTasks() {
        return executeWithQueuesLocked(
            () -> streamOfTasks().filter(Task::isActive).map(t -> (StreamTask) t).collect(Collectors.toSet())
        );
    }

    @Override
    public Set<StandbyTask> standbyTasks() {
        return executeWithQueuesLocked(
            () -> streamOfTasks().filter(t -> !t.isActive()).map(t -> (StandbyTask) t).collect(Collectors.toSet())
        );
    }

    @Override
    public KafkaFutureImpl<Uuid> restoreConsumerInstanceId(final Duration timeout) {
        return stateUpdaterThread.restoreConsumerInstanceId(timeout);
    }

    public boolean isRunning() {
        return stateUpdaterThread != null && stateUpdaterThread.isRunning.get();
    }

    // used for testing
    boolean isIdle() {
        if (stateUpdaterThread != null) {
            return stateUpdaterThread.isIdle.get();
        }
        return false;
    }

    private <T> Set<T> executeWithQueuesLocked(final Supplier<Set<T>> action) {
        tasksAndActionsLock.lock();
        restoredActiveTasksLock.lock();
        exceptionsAndFailedTasksLock.lock();
        try {
            return action.get();
        } finally {
            exceptionsAndFailedTasksLock.unlock();
            restoredActiveTasksLock.unlock();
            tasksAndActionsLock.unlock();
        }
    }

    private Stream<Task> streamOfTasks() {
        return
            Stream.concat(
                streamOfNonPausedTasks(),
                pausedTasks().stream()
            );
    }

    private Stream<Task> streamOfNonPausedTasks() {
        return
            Stream.concat(
                tasksAndActions.stream()
                    .filter(taskAndAction -> taskAndAction.action() == Action.ADD)
                    .map(TaskAndAction::task),
                Stream.concat(
                    updatingTasks().stream(),
                    Stream.concat(
                        restoredActiveTasks.stream(),
                        Stream.concat(
                            exceptionsAndFailedTasks.stream().map(ExceptionAndTask::task),
                            removedTasks.stream()))));
    }

    private class StateUpdaterMetrics {
        private static final String STATE_LEVEL_GROUP = "stream-state-updater-metrics";

        private static final String IDLE_RATIO_DESCRIPTION = RATIO_DESCRIPTION + "being idle";
        private static final String RESTORE_RATIO_DESCRIPTION = RATIO_DESCRIPTION + "restoring active tasks";
        private static final String UPDATE_RATIO_DESCRIPTION = RATIO_DESCRIPTION + "updating standby tasks";
        private static final String CHECKPOINT_RATIO_DESCRIPTION = RATIO_DESCRIPTION + "checkpointing tasks restored progress";
        private static final String RESTORE_RECORDS_RATE_DESCRIPTION = RATE_DESCRIPTION + "records restored";
        private static final String RESTORE_RATE_DESCRIPTION = RATE_DESCRIPTION + "restore calls triggered";

        private final Sensor restoreSensor;
        private final Sensor idleRatioSensor;
        private final Sensor activeRestoreRatioSensor;
        private final Sensor standbyRestoreRatioSensor;
        private final Sensor checkpointRatioSensor;

        private final Deque<String> allSensorNames = new LinkedList<>();
        private final Deque<MetricName> allMetricNames = new LinkedList<>();

        private StateUpdaterMetrics(final Metrics metrics, final String threadId) {
            final Map<String, String> threadLevelTags = new LinkedHashMap<>();
            threadLevelTags.put(THREAD_ID_TAG, threadId);

            MetricName metricName = metrics.metricName("active-restoring-tasks",
                STATE_LEVEL_GROUP,
                "The number of active tasks currently undergoing restoration",
                threadLevelTags);
            metrics.addMetric(metricName, (config, now) -> stateUpdaterThread != null ?
                stateUpdaterThread.numRestoringActiveTasks() : 0);
            allMetricNames.push(metricName);

            metricName = metrics.metricName("standby-updating-tasks",
                STATE_LEVEL_GROUP,
                "The number of standby tasks currently undergoing state update",
                threadLevelTags);
            metrics.addMetric(metricName, (config, now) -> stateUpdaterThread != null ?
                stateUpdaterThread.numUpdatingStandbyTasks() : 0);
            allMetricNames.push(metricName);

            metricName = metrics.metricName("active-paused-tasks",
                STATE_LEVEL_GROUP,
                "The number of active tasks paused restoring",
                threadLevelTags);
            metrics.addMetric(metricName, (config, now) -> stateUpdaterThread != null ?
                stateUpdaterThread.numPausedActiveTasks() : 0);
            allMetricNames.push(metricName);

            metricName = metrics.metricName("standby-paused-tasks",
                STATE_LEVEL_GROUP,
                "The number of standby tasks paused state update",
                threadLevelTags);
            metrics.addMetric(metricName, (config, now) -> stateUpdaterThread != null ?
                stateUpdaterThread.numPausedStandbyTasks() : 0);
            allMetricNames.push(metricName);

            this.idleRatioSensor = metrics.sensor("idle-ratio", RecordingLevel.INFO);
            this.idleRatioSensor.add(new MetricName("idle-ratio", STATE_LEVEL_GROUP, IDLE_RATIO_DESCRIPTION, threadLevelTags), new Avg());
            allSensorNames.add("idle-ratio");

            this.activeRestoreRatioSensor = metrics.sensor("active-restore-ratio", RecordingLevel.INFO);
            this.activeRestoreRatioSensor.add(new MetricName("active-restore-ratio", STATE_LEVEL_GROUP, RESTORE_RATIO_DESCRIPTION, threadLevelTags), new Avg());
            allSensorNames.add("active-restore-ratio");

            this.standbyRestoreRatioSensor = metrics.sensor("standby-update-ratio", RecordingLevel.INFO);
            this.standbyRestoreRatioSensor.add(new MetricName("standby-update-ratio", STATE_LEVEL_GROUP, UPDATE_RATIO_DESCRIPTION, threadLevelTags), new Avg());
            allSensorNames.add("standby-update-ratio");

            this.checkpointRatioSensor = metrics.sensor("checkpoint-ratio", RecordingLevel.INFO);
            this.checkpointRatioSensor.add(new MetricName("checkpoint-ratio", STATE_LEVEL_GROUP, CHECKPOINT_RATIO_DESCRIPTION, threadLevelTags), new Avg());
            allSensorNames.add("checkpoint-ratio");

            this.restoreSensor = metrics.sensor("restore-records", RecordingLevel.INFO);
            this.restoreSensor.add(new MetricName("restore-records-rate", STATE_LEVEL_GROUP, RESTORE_RECORDS_RATE_DESCRIPTION, threadLevelTags), new Rate());
            this.restoreSensor.add(new MetricName("restore-call-rate", STATE_LEVEL_GROUP, RESTORE_RATE_DESCRIPTION, threadLevelTags), new Rate(new WindowedCount()));
            allSensorNames.add("restore-records");
        }

        void clear() {
            while (!allSensorNames.isEmpty()) {
                metrics.removeSensor(allSensorNames.pop());
            }

            while (!allMetricNames.isEmpty()) {
                metrics.removeMetric(allMetricNames.pop());
            }
        }
    }
}
