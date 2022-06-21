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

import org.apache.kafka.common.TopicPartition;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DefaultStateUpdater implements StateUpdater {

    private final static String BUG_ERROR_MESSAGE = "This indicates a bug. " +
        "Please report at https://issues.apache.org/jira/projects/KAFKA/issues or to the dev-mailing list (https://kafka.apache.org/contact).";

    private class StateUpdaterThread extends Thread {

        private final ChangelogReader changelogReader;
        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private final Consumer<Set<TopicPartition>> offsetResetter;
        private final Map<TaskId, Task> updatingTasks = new ConcurrentHashMap<>();
        private final Logger log;

        public StateUpdaterThread(final String name,
                                  final ChangelogReader changelogReader,
                                  final Consumer<Set<TopicPartition>> offsetResetter) {
            super(name);
            this.changelogReader = changelogReader;
            this.offsetResetter = offsetResetter;

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
            performActionsOnTasks();
            restoreTasks();
            maybeCheckpointUpdatingTasks(time.milliseconds());
            waitIfAllChangelogsCompletelyRead();
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
                    }
                }
            } finally {
                tasksAndActionsLock.unlock();
            }
        }

        private void restoreTasks() {
            try {
                changelogReader.restore(updatingTasks);
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
            final ExceptionAndTasks exceptionAndTasks = new ExceptionAndTasks(new HashSet<>(updatingTasks.values()), runtimeException);
            updatingTasks.clear();
            exceptionsAndFailedTasks.add(exceptionAndTasks);
            isRunning.set(false);
        }

        private void handleTaskCorruptedException(final TaskCorruptedException taskCorruptedException) {
            log.info("Encountered task corrupted exception: ", taskCorruptedException);
            final Set<TaskId> corruptedTaskIds = taskCorruptedException.corruptedTasks();
            final Set<Task> corruptedTasks = new HashSet<>();
            for (final TaskId taskId : corruptedTaskIds) {
                final Task corruptedTask = updatingTasks.remove(taskId);
                if (corruptedTask == null) {
                    throw new IllegalStateException("Task " + taskId + " is corrupted but is not updating. " + BUG_ERROR_MESSAGE);
                }
                corruptedTasks.add(corruptedTask);
            }
            exceptionsAndFailedTasks.add(new ExceptionAndTasks(corruptedTasks, taskCorruptedException));
        }

        private void handleStreamsException(final StreamsException streamsException) {
            log.info("Encountered streams exception: ", streamsException);
            final ExceptionAndTasks exceptionAndTasks;
            if (streamsException.taskId().isPresent()) {
                exceptionAndTasks = handleStreamsExceptionWithTask(streamsException);
            } else {
                exceptionAndTasks = handleStreamsExceptionWithoutTask(streamsException);
            }
            exceptionsAndFailedTasks.add(exceptionAndTasks);
        }

        private ExceptionAndTasks handleStreamsExceptionWithTask(final StreamsException streamsException) {
            final TaskId failedTaskId = streamsException.taskId().get();
            if (!updatingTasks.containsKey(failedTaskId)) {
                throw new IllegalStateException("Task " + failedTaskId + " failed but is not updating. " + BUG_ERROR_MESSAGE);
            }
            final Set<Task> failedTask = new HashSet<>();
            failedTask.add(updatingTasks.get(failedTaskId));
            updatingTasks.remove(failedTaskId);
            return new ExceptionAndTasks(failedTask, streamsException);
        }

        private ExceptionAndTasks handleStreamsExceptionWithoutTask(final StreamsException streamsException) {
            final ExceptionAndTasks exceptionAndTasks = new ExceptionAndTasks(new HashSet<>(updatingTasks.values()), streamsException);
            updatingTasks.clear();
            return exceptionAndTasks;
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
        }

        private List<TaskAndAction> getTasksAndActions() {
            final List<TaskAndAction> tasksAndActionsToProcess = new ArrayList<>(tasksAndActions);
            tasksAndActions.clear();
            return tasksAndActionsToProcess;
        }

        private void addTask(final Task task) {
            if (isStateless(task)) {
                addTaskToRestoredTasks((StreamTask) task);
                log.debug("Stateless active task " + task.id() + " was added to the restored tasks of the state updater");
            } else {
                updatingTasks.put(task.id(), task);
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
            final Task task = updatingTasks.remove(taskId);
            if (task != null) {
                task.maybeCheckpoint(true);

                final Collection<TopicPartition> changelogPartitions = task.changelogPartitions();
                changelogReader.unregister(changelogPartitions);
                removedTasks.add(task);
                log.debug((task.isActive() ? "Active" : "Standby")
                    + " task " + task.id() + " was removed from the updating tasks and added to the removed tasks.");
            } else {
                log.debug("Task " + taskId + " was not removed since it is not updating.");
            }
        }

        private boolean isStateless(final Task task) {
            return task.changelogPartitions().isEmpty() && task.isActive();
        }

        private void maybeCompleteRestoration(final StreamTask task,
                                              final Set<TopicPartition> restoredChangelogs) {
            final Collection<TopicPartition> taskChangelogPartitions = task.changelogPartitions();
            if (restoredChangelogs.containsAll(taskChangelogPartitions)) {
                task.completeRestoration(offsetResetter);
                task.maybeCheckpoint(true);
                addTaskToRestoredTasks(task);
                updatingTasks.remove(task.id());
                log.debug("Stateful active task " + task.id() + " completed restoration");
                if (onlyStandbyTasksLeft()) {
                    changelogReader.transitToUpdateStandby();
                }
            }
        }

        private void addTaskToRestoredTasks(final StreamTask task) {
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
                    log.debug("Committing all restoring tasks since {}ms has elapsed (commit interval is {}ms)",
                        elapsedMsSinceLastCommit, commitIntervalMs);
                }

                for (final Task task : updatingTasks.values()) {
                    // do not enforce checkpointing during restoration if its position has not advanced much
                    task.maybeCheckpoint(false);
                }

                lastCommitMs = now;
            }
        }
    }

    private final Time time;
    private final ChangelogReader changelogReader;
    private final Consumer<Set<TopicPartition>> offsetResetter;
    private final Queue<TaskAndAction> tasksAndActions = new LinkedList<>();
    private final Lock tasksAndActionsLock = new ReentrantLock();
    private final Condition tasksAndActionsCondition = tasksAndActionsLock.newCondition();
    private final Queue<StreamTask> restoredActiveTasks = new LinkedList<>();
    private final Lock restoredActiveTasksLock = new ReentrantLock();
    private final Condition restoredActiveTasksCondition = restoredActiveTasksLock.newCondition();
    private final BlockingQueue<ExceptionAndTasks> exceptionsAndFailedTasks = new LinkedBlockingQueue<>();
    private final BlockingQueue<Task> removedTasks = new LinkedBlockingQueue<>();
    private CountDownLatch shutdownGate;

    private final long commitIntervalMs;
    private long lastCommitMs;

    private StateUpdaterThread stateUpdaterThread = null;

    public DefaultStateUpdater(final StreamsConfig config,
                               final ChangelogReader changelogReader,
                               final Consumer<Set<TopicPartition>> offsetResetter,
                               final Time time) {
        this.changelogReader = changelogReader;
        this.offsetResetter = offsetResetter;
        this.time = time;

        this.commitIntervalMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
        // initialize the last commit as of now to prevent first commit happens immediately
        this.lastCommitMs = time.milliseconds();
    }

    @Override
    public void add(final Task task) {
        if (stateUpdaterThread == null) {
            stateUpdaterThread = new StateUpdaterThread("state-updater", changelogReader, offsetResetter);
            stateUpdaterThread.start();
            shutdownGate = new CountDownLatch(1);
        }

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
        } catch (final InterruptedException e) {
            // ignore
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
        return Collections.unmodifiableSet(new HashSet<>(stateUpdaterThread.getUpdatingStandbyTasks()));
    }

    public Set<Task> getUpdatingTasks() {
        return Collections.unmodifiableSet(new HashSet<>(stateUpdaterThread.getUpdatingTasks()));
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
}
