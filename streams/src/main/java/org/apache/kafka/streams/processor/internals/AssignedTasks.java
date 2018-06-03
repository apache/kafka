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
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

abstract class AssignedTasks<T extends Task> {
    private final Logger log;
    private final String taskTypeName;
    private final TaskAction<T> commitAction;
    private final Map<TaskId, T> created = new HashMap<>();
    private final Map<TaskId, T> suspended = new HashMap<>();
    private final Map<TaskId, T> restoring = new HashMap<>();
    private final Set<TopicPartition> restoredPartitions = new HashSet<>();
    private final Set<TaskId> previousActiveTasks = new HashSet<>();
    // IQ may access this map.
    final Map<TaskId, T> running = new ConcurrentHashMap<>();
    private final Map<TopicPartition, T> runningByPartition = new HashMap<>();
    final Map<TopicPartition, T> restoringByPartition = new HashMap<>();

    AssignedTasks(final LogContext logContext,
                  final String taskTypeName) {
        this.taskTypeName = taskTypeName;

        this.log = logContext.logger(getClass());

        commitAction = new TaskAction<T>() {
            @Override
            public String name() {
                return "commit";
            }

            @Override
            public void apply(final T task) {
                task.commit();
            }
        };
    }

    void addNewTask(final T task) {
        created.put(task.id(), task);
    }

    /**
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void initializeNewTasks() {
        if (!created.isEmpty()) {
            log.debug("Initializing {}s {}", taskTypeName, created.keySet());
        }
        for (final Iterator<Map.Entry<TaskId, T>> it = created.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<TaskId, T> entry = it.next();
            try {
                if (!entry.getValue().initializeStateStores()) {
                    log.debug("Transitioning {} {} to restoring", taskTypeName, entry.getKey());
                    addToRestoring(entry.getValue());
                } else {
                    transitionToRunning(entry.getValue());
                }
                it.remove();
            } catch (final LockException e) {
                // made this trace as it will spam the logs in the poll loop.
                log.trace("Could not create {} {} due to {}; will retry", taskTypeName, entry.getKey(), e.toString());
            }
        }
    }

    void updateRestored(final Collection<TopicPartition> restored) {
        if (restored.isEmpty()) {
            return;
        }
        log.trace("{} changelog partitions that have completed restoring so far: {}", taskTypeName, restored);
        restoredPartitions.addAll(restored);
        for (final Iterator<Map.Entry<TaskId, T>> it = restoring.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<TaskId, T> entry = it.next();
            final T task = entry.getValue();
            if (restoredPartitions.containsAll(task.changelogPartitions())) {
                transitionToRunning(task);
                it.remove();
                log.trace("{} {} completed restoration as all its changelog partitions {} have been applied to restore state",
                        taskTypeName,
                        task.id(),
                        task.changelogPartitions());
            } else {
                if (log.isTraceEnabled()) {
                    final HashSet<TopicPartition> outstandingPartitions = new HashSet<>(task.changelogPartitions());
                    outstandingPartitions.removeAll(restoredPartitions);
                    log.trace("{} {} cannot resume processing yet since some of its changelog partitions have not completed restoring: {}",
                              taskTypeName,
                              task.id(),
                              outstandingPartitions);
                }
            }
        }
        if (allTasksRunning()) {
            restoredPartitions.clear();
        }
    }

    boolean allTasksRunning() {
        return created.isEmpty()
                && suspended.isEmpty()
                && restoring.isEmpty();
    }

    Collection<T> running() {
        return running.values();
    }

    RuntimeException suspend() {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        log.trace("Suspending running {} {}", taskTypeName, runningTaskIds());
        firstException.compareAndSet(null, suspendTasks(running.values()));
        log.trace("Close restoring {} {}", taskTypeName, restoring.keySet());
        firstException.compareAndSet(null, closeNonRunningTasks(restoring.values()));
        log.trace("Close created {} {}", taskTypeName, created.keySet());
        firstException.compareAndSet(null, closeNonRunningTasks(created.values()));
        previousActiveTasks.clear();
        previousActiveTasks.addAll(running.keySet());
        running.clear();
        restoring.clear();
        created.clear();
        runningByPartition.clear();
        restoringByPartition.clear();
        return firstException.get();
    }

    private RuntimeException closeNonRunningTasks(final Collection<T> tasks) {
        RuntimeException exception = null;
        for (final T task : tasks) {
            try {
                task.close(false, false);
            } catch (final RuntimeException e) {
                log.error("Failed to close {}, {}", taskTypeName, task.id(), e);
                if (exception == null) {
                    exception = e;
                }
            }
        }
        return exception;
    }

    private RuntimeException suspendTasks(final Collection<T> tasks) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        for (final Iterator<T> it = tasks.iterator(); it.hasNext(); ) {
            final T task = it.next();
            try {
                task.suspend();
                suspended.put(task.id(), task);
            } catch (final TaskMigratedException closeAsZombieAndSwallow) {
                // as we suspend a task, we are either shutting down or rebalancing, thus, we swallow and move on
                log.info("Failed to suspend {} {} since it got migrated to another thread already. " +
                        "Closing it as zombie and move on.", taskTypeName, task.id());
                firstException.compareAndSet(null, closeZombieTask(task));
                it.remove();
            } catch (final RuntimeException e) {
                log.error("Suspending {} {} failed due to the following error:", taskTypeName, task.id(), e);
                firstException.compareAndSet(null, e);
                try {
                    task.close(false, false);
                } catch (final RuntimeException f) {
                    log.error("After suspending failed, closing the same {} {} failed again due to the following error:", taskTypeName, task.id(), f);
                }
            }
        }
        return firstException.get();
    }

    RuntimeException closeZombieTask(final T task) {
        try {
            task.close(false, true);
        } catch (final RuntimeException e) {
            log.warn("Failed to close zombie {} {} due to {}; ignore and proceed.", taskTypeName, task.id(), e.toString());
            return e;
        }
        return null;
    }

    boolean hasRunningTasks() {
        return !running.isEmpty();
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    boolean maybeResumeSuspendedTask(final TaskId taskId, final Set<TopicPartition> partitions) {
        if (suspended.containsKey(taskId)) {
            final T task = suspended.get(taskId);
            log.trace("found suspended {} {}", taskTypeName, taskId);
            if (task.partitions().equals(partitions)) {
                suspended.remove(taskId);
                task.resume();
                try {
                    transitionToRunning(task);
                } catch (final TaskMigratedException e) {
                    // we need to catch migration exception internally since this function
                    // is triggered in the rebalance callback
                    log.info("Failed to resume {} {} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.", taskTypeName, task.id());
                    final RuntimeException fatalException = closeZombieTask(task);
                    running.remove(task.id());
                    if (fatalException != null) {
                        throw fatalException;
                    }
                    throw e;
                }
                log.trace("resuming suspended {} {}", taskTypeName, task.id());
                return true;
            } else {
                log.warn("couldn't resume task {} assigned partitions {}, task partitions {}", taskId, partitions, task.partitions());
            }
        }
        return false;
    }

    private void addToRestoring(final T task) {
        restoring.put(task.id(), task);
        for (final TopicPartition topicPartition : task.partitions()) {
            restoringByPartition.put(topicPartition, task);
        }
        for (final TopicPartition topicPartition : task.changelogPartitions()) {
            restoringByPartition.put(topicPartition, task);
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    private void transitionToRunning(final T task) {
        log.debug("transitioning {} {} to running", taskTypeName, task.id());
        running.put(task.id(), task);
        task.initializeTopology();
        for (final TopicPartition topicPartition : task.partitions()) {
            runningByPartition.put(topicPartition, task);
        }
        for (final TopicPartition topicPartition : task.changelogPartitions()) {
            runningByPartition.put(topicPartition, task);
        }
    }

    T runningTaskFor(final TopicPartition partition) {
        return runningByPartition.get(partition);
    }

    Set<TaskId> runningTaskIds() {
        return running.keySet();
    }

    Map<TaskId, T> runningTaskMap() {
        return Collections.unmodifiableMap(running);
    }

    @Override
    public String toString() {
        return toString("");
    }

    public String toString(final String indent) {
        final StringBuilder builder = new StringBuilder();
        describe(builder, running.values(), indent, "Running:");
        describe(builder, suspended.values(), indent, "Suspended:");
        describe(builder, restoring.values(), indent, "Restoring:");
        describe(builder, created.values(), indent, "New:");
        return builder.toString();
    }

    private void describe(final StringBuilder builder,
                          final Collection<T> tasks,
                          final String indent,
                          final String name) {
        builder.append(indent).append(name);
        for (final T t : tasks) {
            builder.append(indent).append(t.toString(indent + "\t\t"));
        }
        builder.append("\n");
    }

    private List<T> allTasks() {
        final List<T> tasks = new ArrayList<>();
        tasks.addAll(running.values());
        tasks.addAll(suspended.values());
        tasks.addAll(restoring.values());
        tasks.addAll(created.values());
        return tasks;
    }

    Collection<T> restoringTasks() {
        return Collections.unmodifiableCollection(restoring.values());
    }

    Set<TaskId> allAssignedTaskIds() {
        final Set<TaskId> taskIds = new HashSet<>();
        taskIds.addAll(running.keySet());
        taskIds.addAll(suspended.keySet());
        taskIds.addAll(restoring.keySet());
        taskIds.addAll(created.keySet());
        return taskIds;
    }

    void clear() {
        runningByPartition.clear();
        restoringByPartition.clear();
        running.clear();
        created.clear();
        suspended.clear();
        restoredPartitions.clear();
    }

    Set<TaskId> previousTaskIds() {
        return previousActiveTasks;
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int commit() {
        applyToRunningTasks(commitAction);
        return running.size();
    }

    void applyToRunningTasks(final TaskAction<T> action) {
        RuntimeException firstException = null;

        for (final Iterator<T> it = running().iterator(); it.hasNext(); ) {
            final T task = it.next();
            try {
                action.apply(task);
            } catch (final TaskMigratedException e) {
                log.info("Failed to commit {} {} since it got migrated to another thread already. " +
                        "Closing it as zombie before triggering a new rebalance.", taskTypeName, task.id());
                final RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null) {
                    throw fatalException;
                }
                it.remove();
                throw e;
            } catch (final RuntimeException t) {
                log.error("Failed to {} {} {} due to the following error:",
                          action.name(),
                          taskTypeName,
                          task.id(),
                          t);
                if (firstException == null) {
                    firstException = t;
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    void closeNonAssignedSuspendedTasks(final Map<TaskId, Set<TopicPartition>> newAssignment) {
        final Iterator<T> standByTaskIterator = suspended.values().iterator();
        while (standByTaskIterator.hasNext()) {
            final T suspendedTask = standByTaskIterator.next();
            if (!newAssignment.containsKey(suspendedTask.id()) || !suspendedTask.partitions().equals(newAssignment.get(suspendedTask.id()))) {
                log.debug("Closing suspended and not re-assigned {} {}", taskTypeName, suspendedTask.id());
                try {
                    suspendedTask.closeSuspended(true, false, null);
                } catch (final Exception e) {
                    log.error("Failed to remove suspended {} {} due to the following error:", taskTypeName, suspendedTask.id(), e);
                } finally {
                    standByTaskIterator.remove();
                }
            }
        }
    }

    void close(final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        for (final T task : allTasks()) {
            try {
                task.close(clean, false);
            } catch (final TaskMigratedException e) {
                log.info("Failed to close {} {} since it got migrated to another thread already. " +
                        "Closing it as zombie and move on.", taskTypeName, task.id());
                firstException.compareAndSet(null, closeZombieTask(task));
            } catch (final RuntimeException t) {
                log.error("Failed while closing {} {} due to the following error:",
                          task.getClass().getSimpleName(),
                          task.id(),
                          t);
                if (clean) {
                    if (!closeUnclean(task)) {
                        firstException.compareAndSet(null, t);
                    }
                } else {
                    firstException.compareAndSet(null, t);
                }
            }
        }

        clear();

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw fatalException;
        }
    }

    private boolean closeUnclean(final T task) {
        log.info("Try to close {} {} unclean.", task.getClass().getSimpleName(), task.id());
        try {
            task.close(false, false);
        } catch (final RuntimeException fatalException) {
            log.error("Failed while closing {} {} due to the following error:",
                task.getClass().getSimpleName(),
                task.id(),
                fatalException);
            return false;
        }

        return true;
    }
}
