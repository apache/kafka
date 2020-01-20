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
    final Logger log;
    final String taskTypeName;
    final Map<TaskId, T> created = new ConcurrentHashMap<>();

    // IQ may access this map.
    final Map<TaskId, T> running = new ConcurrentHashMap<>();
    final Map<TopicPartition, T> runningByPartition = new HashMap<>();

    AssignedTasks(final LogContext logContext,
                  final String taskTypeName) {
        this.taskTypeName = taskTypeName;
        this.log = logContext.logger(getClass());
    }

    void addNewTask(final T task) {
        created.put(task.id(), task);
    }

    /**
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's changelog does not contain the partition
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void initializeNewTasks() {
        if (!created.isEmpty()) {
            log.debug("Initializing {}s {}", taskTypeName, created.keySet());
        }
        for (final Iterator<Map.Entry<TaskId, T>> it = created.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<TaskId, T> entry = it.next();
            try {
                final T task = entry.getValue();
                task.initializeMetadata();

                // don't remove from created until the task has been successfully initialized
                removeTaskFromAllStateMaps(task, created);

                if (!task.initializeStateStores()) {
                    log.debug("Transitioning {} {} to restoring", taskTypeName, entry.getKey());
                    ((AssignedStreamsTasks) this).addTaskToRestoring((StreamTask) task);
                } else {
                    transitionToRunning(task);
                }

                it.remove();
            } catch (final LockException e) {
                // If this is a permanent error, then we could spam the log since this is in the run loop. But, other related
                // messages show up anyway. So keeping in debug for sake of faster discoverability of problem
                log.debug("Could not create {} {} due to {}; will retry", taskTypeName, entry.getKey(), e.toString());
            }
        }
    }

    boolean allTasksRunning() {
        return created.isEmpty();
    }

    Collection<T> running() {
        return running.values();
    }

    void tryCloseZombieTask(final T task) {
        try {
            task.close(false, true);
        } catch (final RuntimeException e) {
            log.warn("Failed to close zombie {} {} due to {}; ignore and proceed.", taskTypeName, task.id(), e.toString());
        }
    }

    boolean hasRunningTasks() {
        return !running.isEmpty();
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void transitionToRunning(final T task) {
        log.debug("Transitioning {} {} to running", taskTypeName, task.id());
        running.put(task.id(), task);
        task.initializeTopology();
        for (final TopicPartition topicPartition : task.partitions()) {
            runningByPartition.put(topicPartition, task);
        }
        for (final TopicPartition topicPartition : task.changelogPartitions()) {
            runningByPartition.put(topicPartition, task);
        }
    }

    /**
     * Removes the passed in task (and its corresponding partitions) from all state maps and sets,
     * except for the one it currently resides in.
     *
     * @param task the task to be removed
     * @param currentStateMap the current state map, which the task should not be removed from
     */
    void removeTaskFromAllStateMaps(final T task, final Map<TaskId, T> currentStateMap) {
        final TaskId id = task.id();
        final Set<TopicPartition> taskPartitions = new HashSet<>(task.partitions());
        taskPartitions.addAll(task.changelogPartitions());

        if (currentStateMap != running) {
            running.remove(id);
            runningByPartition.keySet().removeAll(taskPartitions);
        }
        if (currentStateMap != created) {
            created.remove(id);
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
        describeTasks(builder, running.values(), indent, "Running:");
        describePartitions(builder, runningByPartition.keySet(), indent, "Running Partitions:");
        describeTasks(builder, created.values(), indent, "New:");
        return builder.toString();
    }

    void describeTasks(final StringBuilder builder,
                       final Collection<T> tasks,
                       final String indent,
                       final String name) {
        builder.append(indent).append(name);
        for (final T t : tasks) {
            builder.append(indent).append(t.toString(indent + "\t\t"));
        }
        builder.append("\n");
    }

    void describePartitions(final StringBuilder builder,
                            final Collection<TopicPartition> partitions,
                            final String indent,
                            final String name) {
        builder.append(indent).append(name);
        for (final TopicPartition tp : partitions) {
            builder.append(indent).append(tp.toString());
        }
        builder.append("\n");
    }

    List<T> allTasks() {
        final List<T> tasks = new ArrayList<>();
        tasks.addAll(running.values());
        tasks.addAll(created.values());
        return tasks;
    }

    Set<TaskId> allAssignedTaskIds() {
        final Set<TaskId> taskIds = new HashSet<>();
        taskIds.addAll(running.keySet());
        taskIds.addAll(created.keySet());
        return taskIds;
    }

    void clear() {
        runningByPartition.clear();
        running.clear();
        created.clear();
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int commit() {
        int committed = 0;
        RuntimeException firstException = null;

        for (final T task : running.values()) {
            try {
                if (task.commitNeeded()) {
                    task.commit();
                    committed++;
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to commit {} {} since it got migrated to another thread already. " +
                        "Will trigger a new rebalance and close all tasks as zombies together.", taskTypeName, task.id());
                throw e;
            } catch (final RuntimeException t) {
                log.error("Failed to commit {} {} due to the following error:",
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

        return committed;
    }

    void shutdown(final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        for (final T task: allTasks()) {
            try {
                closeTask(task, clean);
            } catch (final TaskMigratedException e) {
                log.info("Failed to close {} {} since it got migrated to another thread already. " +
                    "Closing it as zombie and move on.", taskTypeName, task.id());
                tryCloseZombieTask(task);
            } catch (final RuntimeException t) {
                log.error("Failed while closing {} {} due to the following error:",
                    task.getClass().getSimpleName(),
                    task.id(),
                    t);
                if (clean) {
                    closeUnclean(task);
                }
                firstException.compareAndSet(null, t);
            }
        }

        clear();

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw fatalException;
        }
    }

    void closeTask(final T task, final boolean clean) {
        task.close(clean, false);
    }

    private void closeUnclean(final T task) {
        log.info("Try to close {} {} unclean.", task.getClass().getSimpleName(), task.id());
        try {
            task.close(false, false);
        } catch (final RuntimeException fatalException) {
            log.error("Failed while closing {} {} due to the following error:",
                task.getClass().getSimpleName(),
                task.id(),
                fatalException);
        }
    }

}
