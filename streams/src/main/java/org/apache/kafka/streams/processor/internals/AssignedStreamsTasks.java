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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class AssignedStreamsTasks extends AssignedTasks<StreamTask> implements RestoringTasks {
    private final Map<TaskId, StreamTask> suspended = new HashMap<>();
    private final Map<TaskId, StreamTask> restoring = new HashMap<>();
    private final Set<TopicPartition> restoredPartitions = new HashSet<>();
    private final Map<TopicPartition, StreamTask> restoringByPartition = new HashMap<>();
    private final Set<TaskId> prevActiveTasks = new HashSet<>();

    AssignedStreamsTasks(final LogContext logContext) {
        super(logContext, "stream task");
    }

    @Override
    public StreamTask restoringTaskFor(final TopicPartition partition) {
        return restoringByPartition.get(partition);
    }

    @Override
    List<StreamTask> allTasks() {
        final List<StreamTask> tasks = super.allTasks();
        tasks.addAll(restoring.values());
        tasks.addAll(suspended.values());
        return tasks;
    }

    @Override
    Set<TaskId> allAssignedTaskIds() {
        final Set<TaskId> taskIds = super.allAssignedTaskIds();
        taskIds.addAll(restoring.keySet());
        taskIds.addAll(suspended.keySet());
        return taskIds;
    }

    @Override
    boolean allTasksRunning() {
        // If we have some tasks that are suspended but others are running, count this as all tasks are running
        // since they will be closed soon anyway (eg if partitions are revoked at beginning of cooperative rebalance)
        return super.allTasksRunning() && restoring.isEmpty() && (suspended.isEmpty() || !running.isEmpty());
    }

    @Override
    void closeTask(final StreamTask task, final boolean clean) {
        if (suspended.containsKey(task.id())) {
            task.closeSuspended(clean, false, null);
        } else {
            task.close(clean, false);
        }
    }
    
    Set<TaskId> suspendedTaskIds() {
        return suspended.keySet();
    }

    Set<TaskId> previousRunningTaskIds() {
        return prevActiveTasks;
    }

    RuntimeException revokeTasks(final Set<TaskId> revokedTasks, final List<TopicPartition> revokedChangelogs) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        final Set<TaskId> revokedRunningTasks = new HashSet<>();
        final Set<TaskId> revokedNonRunningTasks = new HashSet<>();
        final Set<TaskId> revokedRestoringTasks = new HashSet<>();

        // This is used only for eager rebalancing, so we can just clear it and add any/all tasks that were running
        prevActiveTasks.clear();
        prevActiveTasks.addAll(runningTaskIds());

        for (final TaskId task : revokedTasks) {
            if (running.containsKey(task)) {
                revokedRunningTasks.add(task);
            } else if (created.containsKey(task)) {
                revokedNonRunningTasks.add(task);
            } else if (restoring.containsKey(task)) {
                revokedRestoringTasks.add(task);
            } else if (!suspended.containsKey(task)) {
                log.error("Task {} was revoked but cannot be found in the assignment", task);
            }
        }

        firstException.compareAndSet(null, suspendRunningTasks(revokedRunningTasks, revokedChangelogs));
        firstException.compareAndSet(null, closeNonRunningTasks(revokedNonRunningTasks, revokedChangelogs));
        firstException.compareAndSet(null, closeRestoringTasks(revokedRestoringTasks, revokedChangelogs));

        return firstException.get();
    }

    private RuntimeException suspendRunningTasks(final Set<TaskId> revokedRunningTasks, final List<TopicPartition> revokedChangelogs) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        for (final TaskId id : revokedRunningTasks) {
            final StreamTask task = running.get(id);

            try {
                task.suspend();
                suspended.put(id, task);
            } catch (final TaskMigratedException closeAsZombieAndSwallow) {
                // as we suspend a task, we are either shutting down or rebalancing, thus, we swallow and move on
                log.info("Failed to suspend {} {} since it got migrated to another thread already. " +
                    "Closing it as zombie and move on.", taskTypeName, id);
                firstException.compareAndSet(null, closeZombieTask(task));
                prevActiveTasks.remove(id);
            } catch (final RuntimeException e) {
                log.error("Suspending {} {} failed due to the following error:", taskTypeName, id, e);
                firstException.compareAndSet(null, e);
                try {
                    prevActiveTasks.remove(id);
                    task.close(false, false);
                } catch (final RuntimeException f) {
                    log.error(
                        "After suspending failed, closing the same {} {} failed again due to the following error:",
                        taskTypeName, id, f);
                }
            } finally {
                running.remove(id);
                runningByPartition.keySet().removeAll(task.partitions());
                runningByPartition.keySet().removeAll(task.changelogPartitions());
                revokedChangelogs.addAll(task.changelogPartitions());
            }
        }
        log.trace("Suspended running {} {}", taskTypeName, suspended.keySet());
        return firstException.get();
    }

    private RuntimeException closeNonRunningTasks(final Set<TaskId> revokedNonRunningTasks, final List<TopicPartition> revokedChangelogs) {
        final Set<TaskId> closedNonRunningTasks = new HashSet<>();
        RuntimeException exception = null;

        for (final TaskId id : revokedNonRunningTasks) {
            final StreamTask task = created.get(id);

            try {
                task.close(false, false);
            } catch (final RuntimeException e) {
                log.error("Failed to close {}, {}", taskTypeName, task.id(), e);
                if (exception == null) {
                    exception = e;
                }
            } finally {
                created.remove(id);
                revokedChangelogs.addAll(task.changelogPartitions());
                closedNonRunningTasks.add(task.id);
            }
        }
        log.trace("Closed the created but not initialized {} {}", taskTypeName, closedNonRunningTasks);
        return exception;
    }

    RuntimeException closeRestoringTasks(final Set<TaskId> revokedRestoringTasks, final List<TopicPartition> revokedChangelogs) {
        log.trace("Closing restoring stream tasks {}", revokedRestoringTasks);
        RuntimeException exception = null;

        for (final TaskId id : revokedRestoringTasks) {
            final StreamTask task = restoring.get(id);

            try {
                task.closeStateManager(true);
            } catch (final RuntimeException e) {
                log.error("Failed to close restoring task {} due to the following error:", id, e);
                if (exception == null) {
                    exception = e;
                }
            } finally {
                restoring.remove(id);
                for (final TopicPartition tp : task.partitions()) {
                    restoredPartitions.remove(tp);
                    restoringByPartition.remove(tp);
                }
                revokedChangelogs.addAll(task.changelogPartitions());
            }
        }

        return exception;
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    boolean maybeResumeSuspendedTask(final TaskId taskId,
                                     final Set<TopicPartition> partitions,
                                     final List<TopicPartition> changelogsToBeRemovedDueToFailure) {
        if (suspended.containsKey(taskId)) {
            final StreamTask task = suspended.get(taskId);
            log.trace("Found suspended {} {}", taskTypeName, taskId);
            suspended.remove(taskId);

            if (task.partitions().equals(partitions)) {
                task.resume();
                try {
                    transitionToRunning(task);
                } catch (final TaskMigratedException e) {
                    // we need to catch migration exception internally since this function
                    // is triggered in the rebalance callback
                    log.info("Failed to resume {} {} since it got migrated to another thread already. " +
                        "Closing it as zombie before triggering a new rebalance.", taskTypeName, task.id());
                    final RuntimeException fatalException = closeZombieTask(task);
                    running.remove(taskId);

                    if (fatalException != null) {
                        throw fatalException;
                    }
                    throw e;
                }
                log.trace("Resuming suspended {} {}", taskTypeName, task.id());
                return true;
            } else {
                log.warn("Couldn't resume task {} assigned partitions {}, task partitions {}", taskId, partitions, task.partitions());
                task.closeSuspended(true, false, null);
                changelogsToBeRemovedDueToFailure.addAll(task.changelogPartitions());
            }
        }
        return false;
    }

    List<TopicPartition> closeRevokedSuspendedTasks(final Map<TaskId, Set<TopicPartition>> revokedTasks) {
        final List<TopicPartition> revokedChangelogs = new ArrayList<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> revokedTask : revokedTasks.entrySet()) {
            final StreamTask suspendedTask = suspended.get(revokedTask.getKey());
            if (suspendedTask != null) {
                log.debug("Closing suspended and not re-assigned {} {}", taskTypeName, suspendedTask.id());
                try {
                    suspendedTask.closeSuspended(true, false, null);
                } catch (final Exception e) {
                    log.error("Failed to close suspended {} {} due to the following error:", taskTypeName,
                        suspendedTask.id(), e);
                } finally {
                    suspended.remove(suspendedTask.id());
                    revokedChangelogs.addAll(suspendedTask.changelogPartitions());
                }
            }
        }
        return revokedChangelogs;
    }

    void closeZombieTasks() {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        for (final StreamTask task : allTasks()) {
            firstException.compareAndSet(null, closeZombieTask(task));
        }
        clear();

        if (firstException.get() != null) {
            throw firstException.get();
        }
    }

    void updateRestored(final Collection<TopicPartition> restored) {
        if (restored.isEmpty()) {
            return;
        }
        log.trace("Stream task changelog partitions that have completed restoring so far: {}", restored);
        restoredPartitions.addAll(restored);
        for (final Iterator<Map.Entry<TaskId, StreamTask>> it = restoring.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<TaskId, StreamTask> entry = it.next();
            final StreamTask task = entry.getValue();
            if (restoredPartitions.containsAll(task.changelogPartitions())) {
                transitionToRunning(task);
                it.remove();
                log.debug("Stream task {} completed restoration as all its changelog partitions {} have been applied to restore state",
                    task.id(),
                    task.changelogPartitions());
            } else {
                if (log.isTraceEnabled()) {
                    final HashSet<TopicPartition> outstandingPartitions = new HashSet<>(task.changelogPartitions());
                    outstandingPartitions.removeAll(restoredPartitions);
                    log.trace("Stream task {} cannot resume processing yet since some of its changelog partitions have not completed restoring: {}",
                        task.id(),
                        outstandingPartitions);
                }
            }
        }
        if (allTasksRunning()) {
            restoredPartitions.clear();
        }
    }

    void addToRestoring(final StreamTask task) {
        restoring.put(task.id(), task);
        for (final TopicPartition topicPartition : task.partitions()) {
            restoringByPartition.put(topicPartition, task);
        }
        for (final TopicPartition topicPartition : task.changelogPartitions()) {
            restoringByPartition.put(topicPartition, task);
        }
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int maybeCommitPerUserRequested() {
        int committed = 0;
        RuntimeException firstException = null;

        for (final Iterator<StreamTask> it = running().iterator(); it.hasNext(); ) {
            final StreamTask task = it.next();
            try {
                if (task.commitRequested() && task.commitNeeded()) {
                    task.commit();
                    committed++;
                    log.debug("Committed active task {} per user request in", task.id());
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to commit {} since it got migrated to another thread already. " +
                        "Closing it as zombie before triggering a new rebalance.", task.id());
                final RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null) {
                    throw fatalException;
                }
                it.remove();
                throw e;
            } catch (final RuntimeException t) {
                log.error("Failed to commit StreamTask {} due to the following error:",
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

    /**
     * Returns a map of offsets up to which the records can be deleted; this function should only be called
     * after the commit call to make sure all consumed offsets are actually committed as well
     */
    Map<TopicPartition, Long> recordsToDelete() {
        final Map<TopicPartition, Long> recordsToDelete = new HashMap<>();
        for (final StreamTask task : running.values()) {
            recordsToDelete.putAll(task.purgableOffsets());
        }

        return recordsToDelete;
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int process(final long now) {
        int processed = 0;

        final Iterator<Map.Entry<TaskId, StreamTask>> it = running.entrySet().iterator();
        while (it.hasNext()) {
            final StreamTask task = it.next().getValue();
            try {
                if (task.isProcessable(now) && task.process()) {
                    processed++;
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to process stream task {} since it got migrated to another thread already. " +
                        "Closing it as zombie before triggering a new rebalance.", task.id());
                final RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null) {
                    throw fatalException;
                }
                it.remove();
                throw e;
            } catch (final RuntimeException e) {
                log.error("Failed to process stream task {} due to the following error:", task.id(), e);
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
        final Iterator<Map.Entry<TaskId, StreamTask>> it = running.entrySet().iterator();
        while (it.hasNext()) {
            final StreamTask task = it.next().getValue();
            try {
                if (task.maybePunctuateStreamTime()) {
                    punctuated++;
                }
                if (task.maybePunctuateSystemTime()) {
                    punctuated++;
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to punctuate stream task {} since it got migrated to another thread already. " +
                        "Closing it as zombie before triggering a new rebalance.", task.id());
                final RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null) {
                    throw fatalException;
                }
                it.remove();
                throw e;
            } catch (final KafkaException e) {
                log.error("Failed to punctuate stream task {} due to the following error:", task.id(), e);
                throw e;
            }
        }
        return punctuated;
    }

    void clear() {
        super.clear();
        restoring.clear();
        restoringByPartition.clear();
        restoredPartitions.clear();
        suspended.clear();
    }

    public String toString(final String indent) {
        final StringBuilder builder = new StringBuilder();
        builder.append(super.toString(indent));
        describe(builder, restoring.values(), indent, "Restoring:");
        describe(builder, suspended.values(), indent, "Suspended:");
        return builder.toString();
    }

    // the following are for testing only
    Collection<StreamTask> restoringTasks() {
        return Collections.unmodifiableCollection(restoring.values());
    }

    Set<TaskId> restoringTaskIds() {
        return new HashSet<>(restoring.keySet());
    }

}
