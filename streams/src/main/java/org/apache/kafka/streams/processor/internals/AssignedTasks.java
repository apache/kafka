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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class AssignedTasks<T extends AbstractTask> {
    private static final Logger log = LoggerFactory.getLogger(AssignedTasks.class);
    private final String logPrefix;
    private final String taskTypeName;
    private final Time time;
    private Map<TaskId, T> created = new HashMap<>();
    private Map<TaskId, T> suspended = new HashMap<>();
    private Map<TaskId, T> restoring = new HashMap<>();
    private Set<TopicPartition> restoredPartitions = new HashSet<>();
    private Set<TaskId> previousActiveTasks = new HashSet<>();
    // IQ may access this map.
    private Map<TaskId, T> running = new ConcurrentHashMap<>();
    private Map<TopicPartition, T> runningByPartition = new HashMap<>();

    AssignedTasks(final String logPrefix,
                  final String taskTypeName,
                  final Time time) {
        this.logPrefix = logPrefix;
        this.taskTypeName = taskTypeName;
        this.time = time;
    }

    void addNewTask(final T task) {
        log.trace("{} add new {} {}", logPrefix, taskTypeName, task.id());
        created.put(task.id(), task);
    }

    Set<TopicPartition> uninitializedPartitions() {
        if (created.isEmpty()) {
            return Collections.emptySet();
        }
        final Set<TopicPartition> partitions = new HashSet<>();
        for (final Map.Entry<TaskId, T> entry : created.entrySet()) {
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
        for (final Iterator<Map.Entry<TaskId, T>> it = created.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<TaskId, T> entry = it.next();
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
        final Set<TopicPartition> resume = new HashSet<>();
        restoredPartitions.addAll(restored);
        for (final Iterator<Map.Entry<TaskId, T>> it = restoring.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<TaskId, T> entry = it.next();
            T task = entry.getValue();
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

    Collection<T> runningTasks() {
        return running.values();
    }

    RuntimeException suspend() {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        log.trace("{} Suspending running {} {}", logPrefix, taskTypeName, runningTaskIds());
        firstException.compareAndSet(null, suspendTasks(running.values()));
        log.trace("{} Close restoring {} {}", logPrefix, taskTypeName, restoring.keySet());
        firstException.compareAndSet(null, closeTasksUnclean(restoring.values()));
        firstException.compareAndSet(null, closeTasksUnclean(created.values()));
        previousActiveTasks.clear();
        previousActiveTasks.addAll(running.keySet());
        running.clear();
        restoring.clear();
        created.clear();
        runningByPartition.clear();
        return firstException.get();
    }

    private RuntimeException closeTasksUnclean(final Collection<T> tasks) {
        RuntimeException exception = null;
        for (final T task : tasks) {
            try {
                task.close(false, false);
            } catch (final RuntimeException e) {
                log.error("{} Failed to close {}, {}", logPrefix, taskTypeName, task.id, e);
                if (exception == null) {
                    exception = e;
                }
            }
        }
        return exception;
    }

    private RuntimeException suspendTasks(final Collection<T> tasks) {
        RuntimeException exception = null;
        for (Iterator<T> it = tasks.iterator(); it.hasNext(); ) {
            final T task = it.next();
            try {
                task.suspend();
                suspended.put(task.id(), task);
            } catch (final CommitFailedException e) {
                suspended.put(task.id(), task);
                // commit failed during suspension. Just log it.
                log.warn("{} Failed to commit {} {} state when suspending due to CommitFailedException", logPrefix, taskTypeName, task.id);
            } catch (final ProducerFencedException e) {
                closeZombieTask(task);
                it.remove();
            } catch (final RuntimeException e) {
                log.error("{} Suspending {} {} failed due to the following error:", logPrefix, taskTypeName, task.id, e);
                try {
                    task.close(false, false);
                } catch (final Exception f) {
                    log.error("{} After suspending failed, closing the same {} {} failed again due to the following error:", logPrefix, taskTypeName, task.id, f);
                }
                if (exception == null) {
                    exception = e;
                }
            }
        }
        return exception;
    }

    private void closeZombieTask(final T task) {
        log.warn("{} Producer of {} {} fenced; closing zombie task", logPrefix, taskTypeName, task.id);
        try {
            task.close(false, true);
        } catch (final Exception e) {
            log.warn("{} Failed to close zombie {} due to {}, ignore and proceed", taskTypeName, logPrefix, e);
        }
    }

    boolean hasRunningTasks() {
        return !running.isEmpty();
    }

    boolean maybeResumeSuspendedTask(final TaskId taskId, final Set<TopicPartition> partitions) {
        if (suspended.containsKey(taskId)) {
            final T task = suspended.get(taskId);
            if (task.partitions().equals(partitions)) {
                suspended.remove(taskId);
                log.trace("{} Resuming suspended {} {}", logPrefix, taskTypeName, taskId);
                task.resume();
                transitionToRunning(task);
                return true;
            } else {
                log.trace("{} couldn't resume task {} assigned partitions {}, task partitions", logPrefix, taskId, partitions, task.partitions);
            }
        }
        return false;
    }

    private void transitionToRunning(final T task) {
        log.debug("{} transitioning {} {} to running", logPrefix, taskTypeName, task.id());
        running.put(task.id(), task);
        for (TopicPartition topicPartition : task.partitions()) {
            runningByPartition.put(topicPartition, task);
        }
        for (TopicPartition topicPartition : task.changelogPartitions()) {
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

    List<AbstractTask> allInitializedTasks() {
        final List<AbstractTask> tasks = new ArrayList<>();
        tasks.addAll(running.values());
        tasks.addAll(suspended.values());
        tasks.addAll(restoring.values());
        return tasks;
    }

    Collection<T> suspendedTasks() {
        return suspended.values();
    }

    Collection<T> restoringTasks() {
        return Collections.unmodifiableCollection(restoring.values());
    }

    Collection<TaskId> allAssignedTaskIds() {
        final List<TaskId> taskIds = new ArrayList<>();
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

    void commit() {
        final RuntimeException exception = applyToRunningTasks(new TaskAction<T>() {
            @Override
            public String name() {
                return "commit";
            }

            @Override
            public void apply(final T task) {
                task.commit();
            }
        }, false);

        if (exception != null) {
            throw exception;
        }

    }

    int process() {
        final AtomicInteger processed = new AtomicInteger(0);
        applyToRunningTasks(new TaskAction<T>() {
            @Override
            public String name() {
                return "process";
            }

            @Override
            public void apply(final T task) {
                if (task.process()) {
                    processed.incrementAndGet();
                }
            }
        }, true);
        return processed.get();
    }

    void punctuateAndCommit(final Sensor commitTimeSensor, final Sensor punctuateTimeSensor) {
        final Latency latency = new Latency(time.milliseconds());
        final RuntimeException exception = applyToRunningTasks(new TaskAction<T>() {
            String name;

            @Override
            public String name() {
                return name;
            }

            @Override
            public void apply(final T task) {
                name = "punctuate";
                if (task.maybePunctuate()) {
                    punctuateTimeSensor.record(latency.compute(), latency.startTime);
                }
                if (task.commitNeeded()) {
                    name = "commit";
                    long beforeCommitMs = time.milliseconds();
                    task.commit();
                    commitTimeSensor.record(latency.compute(), latency.startTime);
                    if (log.isDebugEnabled()) {
                        log.debug("{} Committed active task {} per user request in {} ms",
                                  logPrefix, task.id(), latency.startTime - beforeCommitMs);
                    }
                }
            }
        }, false);

        if (exception != null) {
            throw exception;
        }
    }

    Collection<TaskId> suspendedTaskIds() {
        return suspended.keySet();
    }

    interface TaskAction<T extends AbstractTask> {
        String name();

        void apply(final T task);
    }

    class Latency {
        private long startTime;

        Latency(final long startTime) {
            this.startTime = startTime;
        }

        private long compute() {
            final long previousTimeMs = startTime;
            startTime = time.milliseconds();
            return Math.max(startTime - previousTimeMs, 0);
        }
    }


    private RuntimeException applyToRunningTasks(final TaskAction<T> action, final boolean throwException) {
        RuntimeException firstException = null;

        for (Iterator<T> it = runningTasks().iterator(); it.hasNext(); ) {
            final T task = it.next();
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
                if (throwException) {
                    throw t;
                }
                if (firstException == null) {
                    firstException = t;
                }
            }
        }

        return firstException;
    }
}
