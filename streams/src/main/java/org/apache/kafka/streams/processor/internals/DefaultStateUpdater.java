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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.State;
import org.apache.kafka.streams.processor.internals.TaskAndAction.Action;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_DESCRIPTION;

public class DefaultStateUpdater implements StateUpdater {

    private final static String BUG_ERROR_MESSAGE = "This indicates a bug. " +
        "Please report at https://issues.apache.org/jira/projects/KAFKA/issues or to the dev-mailing list (https://kafka.apache.org/contact).";

    private class StateUpdaterThread extends Thread {

        private final Logger log;
        private final StateUpdaterMetrics restoreMetrics;
        private final ChangelogReader changelogReader;
        private final Map<TaskId, Task> updatingTasks = new ConcurrentHashMap<>();
        private final AtomicBoolean isRunning = new AtomicBoolean(true);

        private long totalCheckpointLatency = 0L;

        public StateUpdaterThread(final String name,
                                  final Metrics metrics,
                                  final ChangelogReader changelogReader) {
            super(name);
            this.changelogReader = changelogReader;
            this.restoreMetrics = new StateUpdaterMetrics(metrics, name);

            final String logPrefix = String.format("%s ", name);
            final LogContext logContext = new LogContext(logPrefix);
            log = logContext.logger(DefaultStateUpdater.class);
        }

        public Collection<Task> getUpdatingTasks() {
            return updatingTasks.values();
        }

        public Collection<StandbyTask> getUpdatingStandbyTasks() {
            return updatingTasks.values().stream()
                .filter(t -> !t.isActive())
                .map(t -> (StandbyTask) t)
                .collect(Collectors.toList());
        }

        public Collection<StreamTask> getUpdatingActiveTasks() {
            return updatingTasks.values().stream()
                .filter(Task::isActive)
                .map(t -> (StreamTask) t)
                .collect(Collectors.toList());
        }

        public boolean onlyStandbyTasksLeft() {
            return !updatingTasks.isEmpty() && updatingTasks.values().stream().noneMatch(Task::isActive);
        }

        @Override
        public void run() {
            log.info("State updater thread started");
            try {
                while (isRunning.get()) {
                    try {
                        runOnce();
                    } catch (final InterruptedException interruptedException) {
                        return;
                    }
                }
            } catch (final RuntimeException anyOtherException) {
                handleRuntimeException(anyOtherException);
            } finally {
                clear();
                shutdownGate.countDown();
                log.info("State updater thread shutdown");
            }
        }

        private void runOnce() throws InterruptedException {
            final long totalStartTimeMs = time.milliseconds();

            performActionsOnTasks();
            restoreTasks(totalStartTimeMs);

            final long checkpointStartTimeMs = time.milliseconds();

            maybeCheckpointUpdatingTasks(checkpointStartTimeMs);

            final long waitStartTimeMs = time.milliseconds();
            totalCheckpointLatency += Math.max(0L, waitStartTimeMs - checkpointStartTimeMs);

            waitIfAllChangelogsCompletelyRead();

            final long endTimeMs = time.milliseconds();
            final long totalWaitTime = Math.max(0L, endTimeMs - waitStartTimeMs);
            final long totalTime = Math.max(0L, endTimeMs - totalStartTimeMs);

            recordMetrics(endTimeMs, totalTime, totalWaitTime);
        }

        private void performActionsOnTasks() {
            tasksAndActionsLock.lock();
            try {
                for (final TaskAndAction taskAndAction : getTasksAndActions()) {
                    final Action action = taskAndAction.getAction();
                    switch (action) {
                        case ADD:
                            addTask(taskAndAction.getTask());
                            break;
                        case REMOVE:
                            removeTask(taskAndAction.getTaskId());
                            break;
                        case PAUSE:
                            pauseTask(taskAndAction.getTaskId());
                            break;
                        case RESUME:
                            resumeTask(taskAndAction.getTaskId());
                            break;
                    }
                }
            } finally {
                tasksAndActionsLock.unlock();
            }
        }

        private void restoreTasks(final long now) {
            try {
                final long restored = changelogReader.restore(updatingTasks);

                restoreMetrics.restoreSensor.record(restored, now);
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

        private void handleRuntimeException(final RuntimeException runtimeException) {
            log.error("An unexpected error occurred within the state updater thread: " + runtimeException);
            addToExceptionsAndFailedTasksThenClearUpdatingTasks(new ExceptionAndTasks(new HashSet<>(updatingTasks.values()), runtimeException));
            isRunning.set(false);
        }

        private void handleTaskCorruptedException(final TaskCorruptedException taskCorruptedException) {
            log.info("Encountered task corrupted exception: ", taskCorruptedException);
            final Set<TaskId> corruptedTaskIds = taskCorruptedException.corruptedTasks();
            final Set<Task> corruptedTasks = new HashSet<>();
            for (final TaskId taskId : corruptedTaskIds) {
                final Task corruptedTask = updatingTasks.get(taskId);
                if (corruptedTask == null) {
                    throw new IllegalStateException("Task " + taskId + " is corrupted but is not updating. " + BUG_ERROR_MESSAGE);
                }
                corruptedTasks.add(corruptedTask);
            }
            addToExceptionsAndFailedTasksThenRemoveFromUpdatingTasks(new ExceptionAndTasks(corruptedTasks, taskCorruptedException));
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
            if (!updatingTasks.containsKey(failedTaskId)) {
                throw new IllegalStateException("Task " + failedTaskId + " failed but is not updating. " + BUG_ERROR_MESSAGE);
            }
            final Set<Task> failedTask = new HashSet<>();
            failedTask.add(updatingTasks.get(failedTaskId));
            addToExceptionsAndFailedTasksThenRemoveFromUpdatingTasks(new ExceptionAndTasks(failedTask, streamsException));
        }

        private void handleStreamsExceptionWithoutTask(final StreamsException streamsException) {
            addToExceptionsAndFailedTasksThenClearUpdatingTasks(
                new ExceptionAndTasks(new HashSet<>(updatingTasks.values()), streamsException));
        }

        // It is important to remove the corrupted tasks from the updating tasks after they were added to the
        // failed tasks.
        // This ensures that all tasks are found in DefaultStateUpdater#getTasks().
        private void addToExceptionsAndFailedTasksThenRemoveFromUpdatingTasks(final ExceptionAndTasks exceptionAndTasks) {
            exceptionsAndFailedTasks.add(exceptionAndTasks);
            exceptionAndTasks.getTasks().stream().map(Task::id).forEach(updatingTasks::remove);
            transitToUpdateStandbysIfOnlyStandbysLeft();
        }

        private void addToExceptionsAndFailedTasksThenClearUpdatingTasks(final ExceptionAndTasks exceptionAndTasks) {
            exceptionsAndFailedTasks.add(exceptionAndTasks);
            updatingTasks.clear();
        }

        private void waitIfAllChangelogsCompletelyRead() throws InterruptedException {
            if (isRunning.get() && changelogReader.allChangelogsCompleted()) {
                tasksAndActionsLock.lock();
                try {
                    while (tasksAndActions.isEmpty()) {
                        tasksAndActionsCondition.await();
                    }
                } finally {
                    tasksAndActionsLock.unlock();
                }
            }
        }

        private void clear() {
            tasksAndActionsLock.lock();
            restoredActiveTasksLock.lock();
            try {
                tasksAndActions.clear();
                restoredActiveTasks.clear();
            } finally {
                restoredActiveTasksLock.unlock();
                tasksAndActionsLock.unlock();
            }
            changelogReader.clear();
            updatingTasks.clear();
            restoreMetrics.clear();
        }

        private List<TaskAndAction> getTasksAndActions() {
            final List<TaskAndAction> tasksAndActionsToProcess = new ArrayList<>(tasksAndActions);
            tasksAndActions.clear();
            return tasksAndActionsToProcess;
        }

        private void addTask(final Task task) {
            if (isStateless(task)) {
                addToRestoredTasks((StreamTask) task);
                log.debug("Stateless active task " + task.id() + " was added to the restored tasks of the state updater");
            } else {
                final Task existingTask = updatingTasks.putIfAbsent(task.id(), task);
                if (existingTask != null) {
                    throw new IllegalStateException((existingTask.isActive() ? "Active" : "Standby") + " task " + task.id() + " already exist, " +
                        "should not try to add another " + (task.isActive() ? "active" : "standby") + " task with the same id. " + BUG_ERROR_MESSAGE);
                }

                if (task.isActive()) {
                    log.debug("Stateful active task " + task.id() + " was added to the updating tasks of the state updater");
                    changelogReader.enforceRestoreActive();
                } else {
                    log.debug("Standby task " + task.id() + " was added to the updating tasks of the state updater");
                    if (updatingTasks.size() == 1) {
                        changelogReader.transitToUpdateStandby();
                    }
                }
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
                transitToUpdateStandbysIfOnlyStandbysLeft();
                log.debug((task.isActive() ? "Active" : "Standby")
                    + " task " + task.id() + " was removed from the updating tasks and added to the removed tasks.");
            } else if (pausedTasks.containsKey(taskId)) {
                task = pausedTasks.get(taskId);
                final Collection<TopicPartition> changelogPartitions = task.changelogPartitions();
                changelogReader.unregister(changelogPartitions);
                removedTasks.add(task);
                pausedTasks.remove(taskId);
                log.debug((task.isActive() ? "Active" : "Standby")
                    + " task " + task.id() + " was removed from the paused tasks and added to the removed tasks.");
            } else {
                log.debug("Task " + taskId + " was not removed since it is not updating or paused.");
            }
        }

        private void pauseTask(final TaskId taskId) {
            final Task task = updatingTasks.get(taskId);
            if (task != null) {
                // do not need to unregister changelog partitions for paused tasks
                measureCheckpointLatency(() -> task.maybeCheckpoint(true));
                pausedTasks.put(taskId, task);
                updatingTasks.remove(taskId);
                transitToUpdateStandbysIfOnlyStandbysLeft();
                log.debug((task.isActive() ? "Active" : "Standby")
                    + " task " + task.id() + " was paused from the updating tasks and added to the paused tasks.");
            } else {
                log.debug("Task " + taskId + " was not paused since it is not updating.");
            }
        }

        private void resumeTask(final TaskId taskId) {
            final Task task = pausedTasks.get(taskId);
            if (task != null) {
                updatingTasks.put(taskId, task);
                pausedTasks.remove(taskId);

                if (task.isActive()) {
                    log.debug("Stateful active task " + task.id() + " was resumed to the updating tasks of the state updater");
                    changelogReader.enforceRestoreActive();
                } else {
                    log.debug("Standby task " + task.id() + " was resumed to the updating tasks of the state updater");
                    if (updatingTasks.size() == 1) {
                        changelogReader.transitToUpdateStandby();
                    }
                }
            } else {
                log.debug("Task " + taskId + " was not resumed since it is not paused.");
            }
        }

        private boolean isStateless(final Task task) {
            return task.changelogPartitions().isEmpty() && task.isActive();
        }

        private void maybeCompleteRestoration(final StreamTask task,
                                              final Set<TopicPartition> restoredChangelogs) {
            final Collection<TopicPartition> taskChangelogPartitions = task.changelogPartitions();
            if (restoredChangelogs.containsAll(taskChangelogPartitions)) {
                measureCheckpointLatency(() -> task.maybeCheckpoint(true));
                addToRestoredTasks(task);
                updatingTasks.remove(task.id());
                log.debug("Stateful active task " + task.id() + " completed restoration");
                transitToUpdateStandbysIfOnlyStandbysLeft();
            }
        }

        private void transitToUpdateStandbysIfOnlyStandbysLeft() {
            if (onlyStandbyTasksLeft()) {
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

        private void maybeCheckpointUpdatingTasks(final long now) {
            final long elapsedMsSinceLastCommit = now - lastCommitMs;
            if (elapsedMsSinceLastCommit > commitIntervalMs) {
                if (log.isDebugEnabled()) {
                    log.debug("Checkpointing all restoring tasks since {}ms has elapsed (commit interval is {}ms)",
                        elapsedMsSinceLastCommit, commitIntervalMs);
                }

                for (final Task task : updatingTasks.values()) {
                    // do not enforce checkpointing during restoration if its position has not advanced much
                    task.maybeCheckpoint(false);
                }

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

            restoreMetrics.idleRatioSensor.record((double) totalWaitLatency / totalLatency, now);
            restoreMetrics.restoreRatioSensor.record((double) totalRestoreLatency / totalLatency, now);
            restoreMetrics.checkpointRatioSensor.record((double) totalCheckpointLatency / totalLatency, now);

            totalCheckpointLatency = 0L;
        }
    }

    private final Time time;
    private final Metrics metrics;
    private final ChangelogReader changelogReader;
    private final Queue<TaskAndAction> tasksAndActions = new LinkedList<>();
    private final Lock tasksAndActionsLock = new ReentrantLock();
    private final Condition tasksAndActionsCondition = tasksAndActionsLock.newCondition();
    private final Queue<StreamTask> restoredActiveTasks = new LinkedList<>();
    private final Lock restoredActiveTasksLock = new ReentrantLock();
    private final Condition restoredActiveTasksCondition = restoredActiveTasksLock.newCondition();
    private final BlockingQueue<ExceptionAndTasks> exceptionsAndFailedTasks = new LinkedBlockingQueue<>();
    private final BlockingQueue<Task> removedTasks = new LinkedBlockingQueue<>();
    private final Map<TaskId, Task> pausedTasks = new ConcurrentHashMap<>();

    private final long commitIntervalMs;
    private long lastCommitMs;

    private StateUpdaterThread stateUpdaterThread = null;
    private CountDownLatch shutdownGate;

    public DefaultStateUpdater(final StreamsConfig config,
                               final ChangelogReader changelogReader,
                               final Metrics metrics,
                               final Time time) {
        this.changelogReader = changelogReader;
        this.time = time;
        this.metrics = metrics;
        this.commitIntervalMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
    }

    public void start() {
        if (stateUpdaterThread == null) {
            stateUpdaterThread = new StateUpdaterThread("state-updater", metrics, changelogReader);
            stateUpdaterThread.start();
            shutdownGate = new CountDownLatch(1);

            // initialize the last commit as of now to prevent first commit happens immediately
            this.lastCommitMs = time.milliseconds();
        }
    }

    @Override
    public void shutdown(final Duration timeout) {
        if (stateUpdaterThread != null) {
            stateUpdaterThread.isRunning.set(false);
            stateUpdaterThread.interrupt();
            try {
                if (!shutdownGate.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
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
    public void remove(final TaskId taskId) {
        tasksAndActionsLock.lock();
        try {
            tasksAndActions.add(TaskAndAction.createRemoveTask(taskId));
            tasksAndActionsCondition.signalAll();
        } finally {
            tasksAndActionsLock.unlock();
        }
    }

    @Override
    public void pause(final TaskId taskId) {
        tasksAndActionsLock.lock();
        try {
            tasksAndActions.add(TaskAndAction.createPauseTask(taskId));
            tasksAndActionsCondition.signalAll();
        } finally {
            tasksAndActionsLock.unlock();
        }
    }

    @Override
    public void resume(final TaskId taskId) {
        tasksAndActionsLock.lock();
        try {
            tasksAndActions.add(TaskAndAction.createResumeTask(taskId));
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
    public Set<Task> drainRemovedTasks() {
        final List<Task> result = new ArrayList<>();
        removedTasks.drainTo(result);
        return new HashSet<>(result);
    }

    @Override
    public List<ExceptionAndTasks> drainExceptionsAndFailedTasks() {
        final List<ExceptionAndTasks> result = new ArrayList<>();
        exceptionsAndFailedTasks.drainTo(result);
        return result;
    }

    public Set<StandbyTask> getUpdatingStandbyTasks() {
        return stateUpdaterThread != null
            ? Collections.unmodifiableSet(new HashSet<>(stateUpdaterThread.getUpdatingStandbyTasks()))
            : Collections.emptySet();
    }

    public Set<StreamTask> getUpdatingActiveTasks() {
        return stateUpdaterThread != null
                ? Collections.unmodifiableSet(new HashSet<>(stateUpdaterThread.getUpdatingActiveTasks()))
                : Collections.emptySet();
    }

    public Set<Task> getUpdatingTasks() {
        return stateUpdaterThread != null
            ? Collections.unmodifiableSet(new HashSet<>(stateUpdaterThread.getUpdatingTasks()))
            : Collections.emptySet();
    }

    public Set<StreamTask> getRestoredActiveTasks() {
        restoredActiveTasksLock.lock();
        try {
            return Collections.unmodifiableSet(new HashSet<>(restoredActiveTasks));
        } finally {
            restoredActiveTasksLock.unlock();
        }
    }

    public List<ExceptionAndTasks> getExceptionsAndFailedTasks() {
        return Collections.unmodifiableList(new ArrayList<>(exceptionsAndFailedTasks));
    }

    public Set<Task> getRemovedTasks() {
        return Collections.unmodifiableSet(new HashSet<>(removedTasks));
    }

    public Set<StandbyTask> getPausedStandbyTasks() {
        return Collections.unmodifiableSet(pausedTasks.values().stream()
            .filter(t -> !t.isActive())
            .map(t -> (StandbyTask) t)
            .collect(Collectors.toSet()));
    }

    public Set<StreamTask> getPausedActiveTasks() {
        return Collections.unmodifiableSet(pausedTasks.values().stream()
                .filter(Task::isActive)
                .map(t -> (StreamTask) t)
                .collect(Collectors.toSet()));
    }

    public Set<Task> getPausedTasks() {
        return Collections.unmodifiableSet(new HashSet<>(pausedTasks.values()));
    }

    @Override
    public Set<Task> getTasks() {
        return executeWithQueuesLocked(() -> getStreamOfTasks().collect(Collectors.toSet()));
    }

    @Override
    public Set<StreamTask> getActiveTasks() {
        return executeWithQueuesLocked(
            () -> getStreamOfTasks().filter(Task::isActive).map(t -> (StreamTask) t).collect(Collectors.toSet())
        );
    }

    @Override
    public Set<StandbyTask> getStandbyTasks() {
        return executeWithQueuesLocked(
            () -> getStreamOfTasks().filter(t -> !t.isActive()).map(t -> (StandbyTask) t).collect(Collectors.toSet())
        );
    }

    private <T> Set<T> executeWithQueuesLocked(final Supplier<Set<T>> action) {
        tasksAndActionsLock.lock();
        restoredActiveTasksLock.lock();
        try {
            return action.get();
        } finally {
            restoredActiveTasksLock.unlock();
            tasksAndActionsLock.unlock();
        }
    }

    private Stream<Task> getStreamOfTasks() {
        return
            Stream.concat(
                tasksAndActions.stream()
                    .filter(taskAndAction -> taskAndAction.getAction() == Action.ADD)
                    .map(TaskAndAction::getTask),
                Stream.concat(
                    getUpdatingTasks().stream(),
                    Stream.concat(
                        restoredActiveTasks.stream(),
                        Stream.concat(
                            exceptionsAndFailedTasks.stream().flatMap(exceptionAndTasks -> exceptionAndTasks.getTasks().stream()),
                            Stream.concat(
                                getPausedTasks().stream(),
                                removedTasks.stream())))));
    }

    private class StateUpdaterMetrics {
        private static final String STATE_LEVEL_GROUP = "stream-state-metrics";

        private static final String IDLE_RATIO_DESCRIPTION = RATIO_DESCRIPTION + "being idle";
        private static final String RESTORE_RATIO_DESCRIPTION = RATIO_DESCRIPTION + "restoring tasks";
        private static final String CHECKPOINT_RATIO_DESCRIPTION = RATIO_DESCRIPTION + "checkpointing tasks restored progress";
        private static final String RESTORE_RECORDS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + "records restored";
        private static final String RESTORE_RECORDS_RATE_DESCRIPTION = RATE_DESCRIPTION + "records restored";
        private static final String RESTORE_RATE_DESCRIPTION = RATE_DESCRIPTION + "restore calls triggered";

        private final Sensor restoreSensor;
        private final Sensor idleRatioSensor;
        private final Sensor restoreRatioSensor;
        private final Sensor checkpointRatioSensor;

        private final Deque<String> allSensorNames = new LinkedList<>();
        private final Deque<MetricName> allMetricNames = new LinkedList<>();

        private StateUpdaterMetrics(final Metrics metrics, final String threadId) {
            final Map<String, String> threadLevelTags = StreamsMetricsImpl.threadLevelTagMap(threadId);

            MetricName metricName = metrics.metricName("restoring-active-tasks",
                STATE_LEVEL_GROUP,
                "The number of active tasks currently undergoing restoration",
                threadLevelTags);
            metrics.addMetric(metricName, (config, now) -> getUpdatingActiveTasks().size());
            allMetricNames.push(metricName);

            metricName = metrics.metricName("restoring-standby-tasks",
                STATE_LEVEL_GROUP,
                "The number of active tasks currently undergoing restoration",
                threadLevelTags);
            metrics.addMetric(metricName, (config, now) -> getUpdatingStandbyTasks().size());
            allMetricNames.push(metricName);

            metricName = metrics.metricName("paused-active-tasks",
                STATE_LEVEL_GROUP,
                "The number of active tasks paused restoring",
                threadLevelTags);
            metrics.addMetric(metricName, (config, now) -> getPausedActiveTasks().size());
            allMetricNames.push(metricName);

            metricName = metrics.metricName("paused-standby-tasks",
                STATE_LEVEL_GROUP,
                "The number of standby tasks paused restoring",
                threadLevelTags);
            metrics.addMetric(metricName, (config, now) -> getPausedStandbyTasks().size());
            allMetricNames.push(metricName);

            this.idleRatioSensor = metrics.sensor("idle-ratio", RecordingLevel.INFO);
            this.idleRatioSensor.add(new MetricName("idle-ratio", STATE_LEVEL_GROUP, IDLE_RATIO_DESCRIPTION, threadLevelTags), new Value());
            allSensorNames.add("idle-ratio");

            this.restoreRatioSensor = metrics.sensor("restore-ratio", RecordingLevel.INFO);
            this.restoreRatioSensor.add(new MetricName("restore-ratio", STATE_LEVEL_GROUP, RESTORE_RATIO_DESCRIPTION, threadLevelTags), new Value());
            allSensorNames.add("restore-ratio");

            this.checkpointRatioSensor = metrics.sensor("checkpoint-ratio", RecordingLevel.INFO);
            this.checkpointRatioSensor.add(new MetricName("checkpoint-ratio", STATE_LEVEL_GROUP, CHECKPOINT_RATIO_DESCRIPTION, threadLevelTags), new Value());
            allSensorNames.add("checkpoint-ratio");

            this.restoreSensor = metrics.sensor("restore-records", RecordingLevel.INFO);
            this.restoreSensor.add(new MetricName("restore-records-total", STATE_LEVEL_GROUP, RESTORE_RECORDS_TOTAL_DESCRIPTION, threadLevelTags), new CumulativeSum());
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
