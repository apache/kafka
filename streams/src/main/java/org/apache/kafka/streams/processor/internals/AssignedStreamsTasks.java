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
            task.closeSuspended(clean, null);
        } else {
            task.close(clean, false);
        }
    }

    boolean hasRestoringTasks() {
        return !restoring.isEmpty();
    }
    
    Set<TaskId> suspendedTaskIds() {
        return suspended.keySet();
    }

    Set<TaskId> previousRunningTaskIds() {
        return prevActiveTasks;
    }

    RuntimeException suspendOrCloseTasks(final Set<TaskId> revokedTasks,
                                         final List<TopicPartition> revokedTaskChangelogs) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        final Set<TaskId> revokedRunningTasks = new HashSet<>();
        final Set<TaskId> revokedNonRunningTasks = new HashSet<>();
        final Set<TaskId> revokedRestoringTasks = new HashSet<>();

        // This set is used only for eager rebalancing, so we can just clear it and add any/all tasks that were running
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
                log.warn("Task {} was revoked but cannot be found in the assignment, may have been closed due to error", task);
            }
        }

        firstException.compareAndSet(null, suspendRunningTasks(revokedRunningTasks, revokedTaskChangelogs));
        firstException.compareAndSet(null, closeNonRunningTasks(revokedNonRunningTasks, revokedTaskChangelogs));
        firstException.compareAndSet(null, closeRestoringTasks(revokedRestoringTasks, revokedTaskChangelogs));

        return firstException.get();
    }

    private RuntimeException suspendRunningTasks(final Set<TaskId> runningTasksToSuspend,
                                                 final List<TopicPartition> taskChangelogs) {

        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        log.debug("Suspending running {} {}", taskTypeName, running.keySet());

        for (final TaskId id : runningTasksToSuspend) {
            final StreamTask task = running.get(id);

            try {
                task.suspend();
                suspended.put(id, task);
            } catch (final TaskMigratedException closeAsZombieAndSwallow) {
                // swallow and move on since we are rebalancing
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
                taskChangelogs.addAll(task.changelogPartitions());
            }
        }

        log.trace("Successfully suspended the running {} {}", taskTypeName, suspended.keySet());

        return firstException.get();
    }

    private RuntimeException closeNonRunningTasks(final Set<TaskId> nonRunningTasksToClose,
                                                  final List<TopicPartition> closedTaskChangelogs) {
        log.debug("Closing the created but not initialized {} {}", taskTypeName, nonRunningTasksToClose);
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>();

        for (final TaskId id : nonRunningTasksToClose) {
            final StreamTask task = created.get(id);
            firstException.compareAndSet(null, closeNonRunning(false, task, closedTaskChangelogs));
        }

        return firstException.get();
    }

    RuntimeException closeRestoringTasks(final Set<TaskId> restoringTasksToClose,
                                         final List<TopicPartition> closedTaskChangelogs) {
        log.debug("Closing restoring stream tasks {}", restoringTasksToClose);
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>();

        for (final TaskId id : restoringTasksToClose) {
            final StreamTask task = restoring.get(id);
            firstException.compareAndSet(null, closeRestoring(false, task, closedTaskChangelogs));
        }

        return firstException.get();
    }

    private RuntimeException closeRunning(final boolean isZombie,
                                          final StreamTask task,
                                          final List<TopicPartition> closedTaskChangelogs) {
        running.remove(task.id());
        runningByPartition.keySet().removeAll(task.partitions());
        runningByPartition.keySet().removeAll(task.changelogPartitions());
        closedTaskChangelogs.addAll(task.changelogPartitions());

        try {
            final boolean clean = !isZombie;
            task.close(clean, isZombie);
        } catch (final RuntimeException e) {
            log.error("Failed to close {}, {}", taskTypeName, task.id(), e);
            return e;
        }

        return null;
    }

    private RuntimeException closeNonRunning(final boolean isZombie,
                                             final StreamTask task,
                                             final List<TopicPartition> closedTaskChangelogs) {
        created.remove(task.id());
        closedTaskChangelogs.addAll(task.changelogPartitions());

        try {
            task.close(false, isZombie);
        } catch (final RuntimeException e) {
            log.error("Failed to close {}, {}", taskTypeName, task.id(), e);
            return e;
        }

        return null;
    }

    private RuntimeException closeRestoring(final boolean isZombie,
                                            final StreamTask task,
                                            final List<TopicPartition> closedTaskChangelogs) {
        restoring.remove(task.id());
        closedTaskChangelogs.addAll(task.changelogPartitions());
        for (final TopicPartition tp : task.partitions()) {
            restoredPartitions.remove(tp);
            restoringByPartition.remove(tp);
        }

        try {
            final boolean clean = !isZombie;
            task.closeStateManager(clean);
        } catch (final RuntimeException e) {
            log.error("Failed to close restoring task {} due to the following error:", task.id(), e);
            return e;
        }

        return null;
    }

    private RuntimeException closeSuspended(final boolean isZombie,
                                            final StreamTask task) {
        suspended.remove(task.id());

        try {
            final boolean clean = !isZombie;
            task.closeSuspended(clean, null);
        } catch (final RuntimeException e) {
            log.error("Failed to close suspended {} {} due to the following error:", taskTypeName, task.id(), e);
            return e;
        }

        return null;
    }

    RuntimeException closeNotAssignedSuspendedTasks(final Set<TaskId> revokedTasks) {
        log.debug("Closing the revoked active tasks {} {}", taskTypeName, revokedTasks);
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        for (final TaskId revokedTask : revokedTasks) {
            final StreamTask suspendedTask = suspended.get(revokedTask);

            if (suspendedTask != null) {
                firstException.compareAndSet(null, closeSuspended(false, suspendedTask));
            } else {
                log.debug("Revoked task {} could not be found in suspended, may have already been closed", revokedTask);
            }
        }
        return firstException.get();
    }

    RuntimeException closeZombieTasks(final Set<TaskId> lostTasks, final List<TopicPartition> lostTaskChangelogs) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        for (final TaskId id : lostTasks) {
            if (suspended.containsKey(id)) {
                firstException.compareAndSet(null, closeSuspended(true, suspended.get(id)));
            } else if (created.containsKey(id)) {
                firstException.compareAndSet(null, closeNonRunning(true, created.get(id), lostTaskChangelogs));
            } else if (restoring.containsKey(id)) {
                firstException.compareAndSet(null, closeRestoring(true, created.get(id), lostTaskChangelogs));
            } else if (running.containsKey(id)) {
                firstException.compareAndSet(null, closeRunning(true, running.get(id), lostTaskChangelogs));
            } else {
                // task may have already been closed as a zombie and removed from all task maps
            }
        }

        // We always clear the prevActiveTasks and replace with current set of running tasks to encode in subscription
        // We should exclude any tasks that were lost however, they will be counted as standbys for assignment purposes
        prevActiveTasks.clear();
        prevActiveTasks.addAll(running.keySet());

        // With the current rebalance protocol, there should not be any running tasks left as they were all lost
        if (!prevActiveTasks.isEmpty()) {
            log.error("Found still running {} after closing all tasks lost as zombies", taskTypeName);
            firstException.compareAndSet(null, new IllegalStateException("Not all lost tasks were closed as zombies"));
        }
        return firstException.get();
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    boolean maybeResumeSuspendedTask(final TaskId taskId,
                                     final Set<TopicPartition> partitions) {
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
                task.closeSuspended(true, null);
            }
        }
        return false;
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
