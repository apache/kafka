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

import org.apache.kafka.common.KafkaException;
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

class AssignedTasks implements RestoringTasks {
    private final Logger log;
    private final String taskTypeName;
    private final TaskAction maybeCommitAction;
    private final TaskAction commitAction;
    private Map<TaskId, Task> created = new HashMap<>();
    private Map<TaskId, Task> suspended = new HashMap<>();
    private Map<TaskId, Task> restoring = new HashMap<>();
    private Set<TopicPartition> restoredPartitions = new HashSet<>();
    private Set<TaskId> previousActiveTasks = new HashSet<>();
    // IQ may access this map.
    private Map<TaskId, Task> running = new ConcurrentHashMap<>();
    private Map<TopicPartition, Task> runningByPartition = new HashMap<>();
    private Map<TopicPartition, Task> restoringByPartition = new HashMap<>();
    private int committed = 0;


    AssignedTasks(final LogContext logContext,
                  final String taskTypeName) {
        this.taskTypeName = taskTypeName;

        this.log = logContext.logger(getClass());

        maybeCommitAction = new TaskAction() {
            @Override
            public String name() {
                return "maybeCommit";
            }

            @Override
            public void apply(final Task task) {
                if (task.commitNeeded()) {
                    committed++;
                    task.commit();
                    if (log.isDebugEnabled()) {
                        log.debug("Committed active task {} per user request in", task.id());
                    }
                }
            }
        };

        commitAction = new TaskAction() {
            @Override
            public String name() {
                return "commit";
            }

            @Override
            public void apply(final Task task) {
                task.commit();
            }
        };
    }

    void addNewTask(final Task task) {
        created.put(task.id(), task);
    }

    Set<TopicPartition> uninitializedPartitions() {
        if (created.isEmpty()) {
            return Collections.emptySet();
        }
        final Set<TopicPartition> partitions = new HashSet<>();
        for (final Map.Entry<TaskId, Task> entry : created.entrySet()) {
            if (entry.getValue().hasStateStores()) {
                partitions.addAll(entry.getValue().partitions());
            }
        }
        return partitions;
    }

    /**
     * @return partitions that are ready to be resumed
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    Set<TopicPartition> initializeNewTasks() {
        final Set<TopicPartition> readyPartitions = new HashSet<>();
        if (!created.isEmpty()) {
            log.debug("Initializing {}s {}", taskTypeName, created.keySet());
        }
        for (final Iterator<Map.Entry<TaskId, Task>> it = created.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<TaskId, Task> entry = it.next();
            try {
                if (!entry.getValue().initialize()) {
                    log.debug("Transitioning {} {} to restoring", taskTypeName, entry.getKey());
                    addToRestoring(entry.getValue());
                } else {
                    transitionToRunning(entry.getValue(), readyPartitions);
                }
                it.remove();
            } catch (final LockException e) {
                // made this trace as it will spam the logs in the poll loop.
                log.trace("Could not create {} {} due to {}; will retry", taskTypeName, entry.getKey(), e.getMessage());
            }
        }
        return readyPartitions;
    }

    Set<TopicPartition> updateRestored(final Collection<TopicPartition> restored) {
        if (restored.isEmpty()) {
            return Collections.emptySet();
        }
        log.trace("{} changelog partitions that have completed restoring so far: {}", taskTypeName, restored);
        final Set<TopicPartition> resume = new HashSet<>();
        restoredPartitions.addAll(restored);
        for (final Iterator<Map.Entry<TaskId, Task>> it = restoring.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<TaskId, Task> entry = it.next();
            final Task task = entry.getValue();
            if (restoredPartitions.containsAll(task.changelogPartitions())) {
                transitionToRunning(task, resume);
                it.remove();
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
        return resume;
    }

    boolean allTasksRunning() {
        return created.isEmpty()
                && suspended.isEmpty()
                && restoring.isEmpty();
    }

    Collection<Task> running() {
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

    private RuntimeException closeNonRunningTasks(final Collection<Task> tasks) {
        RuntimeException exception = null;
        for (final Task task : tasks) {
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

    private RuntimeException suspendTasks(final Collection<Task> tasks) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        for (Iterator<Task> it = tasks.iterator(); it.hasNext(); ) {
            final Task task = it.next();
            try {
                task.suspend();
                suspended.put(task.id(), task);
            } catch (final TaskMigratedException closeAsZombieAndSwallow) {
                // as we suspend a task, we are either shutting down or rebalancing, thus, we swallow and move on
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

    private RuntimeException closeZombieTask(final Task task) {
        log.warn("{} {} got migrated to another thread already. Closing it as zombie.", taskTypeName, task.id());
        try {
            task.close(false, true);
        } catch (final RuntimeException e) {
            log.warn("Failed to close zombie {} {} due to {}; ignore and proceed.", taskTypeName, task.id(), e.getMessage());
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
            final Task task = suspended.get(taskId);
            log.trace("found suspended {} {}", taskTypeName, taskId);
            if (task.partitions().equals(partitions)) {
                suspended.remove(taskId);
                try {
                    task.resume();
                } catch (final TaskMigratedException e) {
                    final RuntimeException fatalException = closeZombieTask(task);
                    if (fatalException != null) {
                        throw fatalException;
                    }
                    suspended.remove(taskId);
                    throw e;
                }
                transitionToRunning(task, new HashSet<TopicPartition>());
                log.trace("resuming suspended {} {}", taskTypeName, task.id());
                return true;
            } else {
                log.warn("couldn't resume task {} assigned partitions {}, task partitions {}", taskId, partitions, task.partitions());
            }
        }
        return false;
    }

    private void addToRestoring(final Task task) {
        restoring.put(task.id(), task);
        for (TopicPartition topicPartition : task.partitions()) {
            restoringByPartition.put(topicPartition, task);
        }
        for (TopicPartition topicPartition : task.changelogPartitions()) {
            restoringByPartition.put(topicPartition, task);
        }
    }

    private void transitionToRunning(final Task task, final Set<TopicPartition> readyPartitions) {
        log.debug("transitioning {} {} to running", taskTypeName, task.id());
        running.put(task.id(), task);
        for (TopicPartition topicPartition : task.partitions()) {
            runningByPartition.put(topicPartition, task);
            if (task.hasStateStores()) {
                readyPartitions.add(topicPartition);
            }
        }
        for (TopicPartition topicPartition : task.changelogPartitions()) {
            runningByPartition.put(topicPartition, task);
        }
    }

    @Override
    public Task restoringTaskFor(final TopicPartition partition) {
        return restoringByPartition.get(partition);
    }

    Task runningTaskFor(final TopicPartition partition) {
        return runningByPartition.get(partition);
    }

    Set<TaskId> runningTaskIds() {
        return running.keySet();
    }

    Map<TaskId, Task> runningTaskMap() {
        return Collections.unmodifiableMap(running);
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
                          final Collection<Task> tasks,
                          final String indent,
                          final String name) {
        builder.append(indent).append(name);
        for (final Task t : tasks) {
            builder.append(indent).append(t.toString(indent + "\t\t"));
        }
        builder.append("\n");
    }

    private List<Task> allTasks() {
        final List<Task> tasks = new ArrayList<>();
        tasks.addAll(running.values());
        tasks.addAll(suspended.values());
        tasks.addAll(restoring.values());
        tasks.addAll(created.values());
        return tasks;
    }

    Collection<Task> restoringTasks() {
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

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int maybeCommit() {
        committed = 0;
        applyToRunningTasks(maybeCommitAction);
        return committed;
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int process() {
        int processed = 0;
        final Iterator<Map.Entry<TaskId, Task>> it = running.entrySet().iterator();
        while (it.hasNext()) {
            final Task task = it.next().getValue();
            try {
                if (task.process()) {
                    processed++;
                }
            } catch (final TaskMigratedException e) {
                final RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null) {
                    throw fatalException;
                }
                it.remove();
                throw e;
            } catch (final RuntimeException e) {
                log.error("Failed to process {} {} due to the following error:", taskTypeName, task.id(), e);
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
        final Iterator<Map.Entry<TaskId, Task>> it = running.entrySet().iterator();
        while (it.hasNext()) {
            final Task task = it.next().getValue();
            try {
                if (task.maybePunctuateStreamTime()) {
                    punctuated++;
                }
                if (task.maybePunctuateSystemTime()) {
                    punctuated++;
                }
            } catch (final TaskMigratedException e) {
                final RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null) {
                    throw fatalException;
                }
                it.remove();
                throw e;
            } catch (final KafkaException e) {
                log.error("Failed to punctuate {} {} due to the following error:", taskTypeName, task.id(), e);
                throw e;
            }
        }
        return punctuated;
    }

    private void applyToRunningTasks(final TaskAction action) {
        RuntimeException firstException = null;

        for (Iterator<Task> it = running().iterator(); it.hasNext(); ) {
            final Task task = it.next();
            try {
                action.apply(task);
            } catch (final TaskMigratedException e) {
                final RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null) {
                    throw fatalException;
                }
                it.remove();
                if (firstException == null) {
                    firstException = e;
                }
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
        final Iterator<Task> standByTaskIterator = suspended.values().iterator();
        while (standByTaskIterator.hasNext()) {
            final Task suspendedTask = standByTaskIterator.next();
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
        for (final Task task : allTasks()) {
            try {
                task.close(clean, false);
            } catch (final TaskMigratedException e) {
                firstException.compareAndSet(null, closeZombieTask(task));
            } catch (final RuntimeException t) {
                firstException.compareAndSet(null, t);
                log.error("Failed while closing {} {} due to the following error:",
                          task.getClass().getSimpleName(),
                          task.id(),
                          t);
                firstException.compareAndSet(null, closeUncleanIfRequired(task, clean));
            }
        }

        clear();

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw fatalException;
        }
    }

    private RuntimeException closeUncleanIfRequired(final Task task,
                                                    final boolean triedToCloseCleanlyPreviously) {
        if (triedToCloseCleanlyPreviously) {
            log.info("Try to close {} {} unclean.", task.getClass().getSimpleName(), task.id());
            try {
                task.close(false, false);
            } catch (final RuntimeException fatalException) {
                log.error("Failed while closing {} {} due to the following error:",
                    task.getClass().getSimpleName(),
                    task.id(),
                    fatalException);
                return fatalException;
            }
        }

        return null;
    }
}
