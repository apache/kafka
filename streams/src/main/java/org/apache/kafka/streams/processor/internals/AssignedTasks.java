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

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

class AssignedTasks {
    private static final Logger log = LoggerFactory.getLogger(AssignedTasks.class);
    private final String logPrefix;
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
    private int committed = 0;


    AssignedTasks(final String logPrefix,
                  final String taskTypeName) {
        this.logPrefix = logPrefix;
        this.taskTypeName = taskTypeName;

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
                        log.debug("{} Committed active task {} per user request in",
                                  logPrefix, task.id());
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

    void initializeNewTasks() {
        if (!created.isEmpty()) {
            log.trace("{} Initializing {}s {}", logPrefix, taskTypeName, created.keySet());
        }
        for (final Iterator<Map.Entry<TaskId, Task>> it = created.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<TaskId, Task> entry = it.next();
            try {
                if (!entry.getValue().initialize()) {
                    log.debug("{} transitioning {} {} to restoring", logPrefix, taskTypeName, entry.getKey());
                    restoring.put(entry.getKey(), entry.getValue());
                } else {
                    transitionToRunning(entry.getValue());
                }
                it.remove();
            } catch (final LockException e) {
                // made this trace as it will spam the logs in the poll loop.
                log.trace("{} Could not create {} {} due to {}; will retry", logPrefix, taskTypeName, entry.getKey(), e.getMessage());
            }
        }
    }

    Set<TopicPartition> updateRestored(final Collection<TopicPartition> restored) {
        if (restored.isEmpty()) {
            return Collections.emptySet();
        }
        log.trace("{} {} partitions restored for {}", logPrefix, taskTypeName, restored);
        final Set<TopicPartition> resume = new HashSet<>();
        restoredPartitions.addAll(restored);
        for (final Iterator<Map.Entry<TaskId, Task>> it = restoring.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<TaskId, Task> entry = it.next();
            Task task = entry.getValue();
            if (restoredPartitions.containsAll(task.changelogPartitions())) {
                transitionToRunning(task);
                resume.addAll(task.partitions());
                it.remove();
            } else {
                if (log.isTraceEnabled()) {
                    final HashSet<TopicPartition> outstandingPartitions = new HashSet<>(task.changelogPartitions());
                    outstandingPartitions.removeAll(restoredPartitions);
                    log.trace("{} partition restoration not complete for {} {} partitions: {}",
                              logPrefix,
                              taskTypeName,
                              task.id(),
                              task.changelogPartitions());
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
        log.trace("{} Suspending running {} {}", logPrefix, taskTypeName, runningTaskIds());
        firstException.compareAndSet(null, suspendTasks(running.values()));
        log.trace("{} Close restoring {} {}", logPrefix, taskTypeName, restoring.keySet());
        firstException.compareAndSet(null, closeNonRunningTasks(restoring.values()));
        log.trace("{} Close created {} {}", logPrefix, taskTypeName, created.keySet());
        firstException.compareAndSet(null, closeNonRunningTasks(created.values()));
        previousActiveTasks.clear();
        previousActiveTasks.addAll(running.keySet());
        running.clear();
        restoring.clear();
        created.clear();
        runningByPartition.clear();
        return firstException.get();
    }

    private RuntimeException closeNonRunningTasks(final Collection<Task> tasks) {
        RuntimeException exception = null;
        for (final Task task : tasks) {
            try {
                task.close(false);
            } catch (final RuntimeException e) {
                log.error("{} Failed to close {}, {}", logPrefix, taskTypeName, task.id(), e);
                if (exception == null) {
                    exception = e;
                }
            }
        }
        return exception;
    }

    private RuntimeException suspendTasks(final Collection<Task> tasks) {
        RuntimeException exception = null;
        for (Iterator<Task> it = tasks.iterator(); it.hasNext(); ) {
            final Task task = it.next();
            try {
                task.suspend();
                suspended.put(task.id(), task);
            } catch (final CommitFailedException e) {
                suspended.put(task.id(), task);
                // commit failed during suspension. Just log it.
                log.warn("{} Failed to commit {} {} state when suspending due to CommitFailedException", logPrefix, taskTypeName, task.id());
            } catch (final ProducerFencedException e) {
                closeZombieTask(task);
                it.remove();
            } catch (final RuntimeException e) {
                log.error("{} Suspending {} {} failed due to the following error:", logPrefix, taskTypeName, task.id(), e);
                try {
                    task.close(false);
                } catch (final Exception f) {
                    log.error("{} After suspending failed, closing the same {} {} failed again due to the following error:", logPrefix, taskTypeName, task.id(), f);
                }
                if (exception == null) {
                    exception = e;
                }
            }
        }
        return exception;
    }

    private void closeZombieTask(final Task task) {
        log.warn("{} Producer of task {} fenced; closing zombie task", logPrefix, task.id());
        try {
            task.close(false);
        } catch (final Exception e) {
            log.warn("{} Failed to close zombie {} due to {}, ignore and proceed", taskTypeName, logPrefix, e);
        }
    }

    boolean hasRunningTasks() {
        return !running.isEmpty();
    }

    boolean maybeResumeSuspendedTask(final TaskId taskId, final Set<TopicPartition> partitions) {
        if (suspended.containsKey(taskId)) {
            final Task task = suspended.get(taskId);
            log.trace("{} found suspended {} {}", logPrefix, taskTypeName, taskId);
            if (task.partitions().equals(partitions)) {
                suspended.remove(taskId);
                task.resume();
                transitionToRunning(task);
                log.trace("{} resuming suspended {} {}", logPrefix, taskTypeName, task.id());
                return true;
            } else {
                log.trace("{} couldn't resume task {} assigned partitions {}, task partitions", logPrefix, taskId, partitions, task.partitions());
            }
        }
        return false;
    }

    private void transitionToRunning(final Task task) {
        log.debug("{} transitioning {} {} to running", logPrefix, taskTypeName, task.id());
        running.put(task.id(), task);
        for (TopicPartition topicPartition : task.partitions()) {
            runningByPartition.put(topicPartition, task);
        }
        for (TopicPartition topicPartition : task.changelogPartitions()) {
            runningByPartition.put(topicPartition, task);
        }
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

    private List<Task> allInitializedTasks() {
        final List<Task> tasks = new ArrayList<>();
        tasks.addAll(running.values());
        tasks.addAll(suspended.values());
        tasks.addAll(restoring.values());
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
        running.clear();
        created.clear();
        suspended.clear();
        restoredPartitions.clear();
    }

    Set<TaskId> previousTaskIds() {
        return previousActiveTasks;
    }

    int commit() {
        applyToRunningTasks(commitAction);
        return running.size();
    }

    int maybeCommit() {
        committed = 0;
        applyToRunningTasks(maybeCommitAction);
        return committed;
    }

    int process() {
        int processed = 0;
        for (final Task task : running.values()) {
            try {
                if (task.process()) {
                    processed++;
                }
            } catch (RuntimeException e) {
                log.error("{} Failed to process {} {} due to the following error:", logPrefix, taskTypeName, task.id(), e);
                throw e;
            }
        }
        return processed;
    }

    int punctuate() {
        int punctuated = 0;
        for (Task task : running.values()) {
            try {
                if (task.maybePunctuateStreamTime()) {
                    punctuated++;
                }
                if (task.maybePunctuateSystemTime()) {
                    punctuated++;
                }
            } catch (KafkaException e) {
                log.error("{} Failed to punctuate {} {} due to the following error:", logPrefix, taskTypeName, task.id(), e);
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
            } catch (final CommitFailedException e) {
                // commit failed. This is already logged inside the task as WARN and we can just log it again here.
                log.warn("{} Failed to commit {} {} during {} state due to CommitFailedException; this task may be no longer owned by the thread", logPrefix, taskTypeName, task.id(), action.name());
            } catch (final ProducerFencedException e) {
                closeZombieTask(task);
                it.remove();
            } catch (final RuntimeException t) {
                log.error("{} Failed to {} {} {} due to the following error:",
                          logPrefix,
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
                log.debug("{} Closing suspended and not re-assigned {} {}", logPrefix, taskTypeName, suspendedTask.id());
                try {
                    suspendedTask.closeSuspended(true, null);
                } catch (final Exception e) {
                    log.error("{} Failed to remove suspended {} {} due to the following error:", logPrefix, taskTypeName, suspendedTask.id(), e);
                } finally {
                    standByTaskIterator.remove();
                }
            }
        }
    }

    void close(final boolean clean) {
        close(allInitializedTasks(), clean);
        close(created.values(), clean);
        clear();
    }

    private void close(final Collection<Task> tasks, final boolean clean) {
        for (final Task task : tasks) {
            try {
                task.close(clean);
            } catch (final Throwable t) {
                log.error("{} Failed while closing {} {} due to the following error:",
                          logPrefix,
                          task.getClass().getSimpleName(),
                          task.id(),
                          t);
            }
        }
    }
}
