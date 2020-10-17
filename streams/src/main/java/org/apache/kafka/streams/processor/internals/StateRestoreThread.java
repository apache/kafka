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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


/**
 * This is the thread responsible for restoring state stores for both active and standby tasks
 */
public class StateRestoreThread extends Thread {

    private final Time time;
    private final Logger log;
    private final ChangelogReader changelogReader;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final LinkedBlockingDeque<TaskItem> taskItemQueue;
    private final AtomicReference<Set<TopicPartition>> completedChangelogs;
    private final LinkedBlockingDeque<TaskCorruptedException> corruptedExceptions;
    private final AtomicReference<RuntimeException> fatalException;

    public boolean isRunning() {
        return isRunning.get();
    }

    public StateRestoreThread(final Time time,
                              final StreamsConfig config,
                              final String threadClientId,
                              final Admin adminClient,
                              final String groupId,
                              final Consumer<byte[], byte[]> restoreConsumer,
                              final StateRestoreListener userStateRestoreListener) {
        this(time, threadClientId, new StoreChangelogReader(time, config, threadClientId,
                adminClient, groupId, restoreConsumer, userStateRestoreListener));
    }

    // for testing only
    public StateRestoreThread(final Time time,
                              final String threadClientId,
                              final ChangelogReader changelogReader) {
        super(threadClientId);

        final String logPrefix = String.format("state-restore-thread [%s] ", threadClientId);
        final LogContext logContext = new LogContext(logPrefix);

        this.time = time;
        this.log = logContext.logger(getClass());
        this.taskItemQueue = new LinkedBlockingDeque<>();
        this.fatalException = new AtomicReference<>();
        this.corruptedExceptions = new LinkedBlockingDeque<>();
        this.completedChangelogs = new AtomicReference<>(Collections.emptySet());

        this.changelogReader = changelogReader;
    }

    private synchronized void waitIfAllChangelogsCompleted() {
        final Set<TopicPartition> allChangelogs = changelogReader.allChangelogs();
        if (allChangelogs.equals(changelogReader.completedChangelogs())) {
            log.debug("All changelogs {} have completed restoration so far, will wait " +
                    "until new changelogs are registered", allChangelogs);

            while (isRunning.get() && taskItemQueue.isEmpty()) {
                try {
                    wait();
                } catch (final InterruptedException e) {
                    // do nothing
                }
            }
        }
    }

    public synchronized void addInitializedTasks(final List<Task> tasks) {
        if (!tasks.isEmpty()) {
            for (final Task task : tasks) {
                taskItemQueue.add(new TaskItem(task, ItemType.CREATE));
            }
            notifyAll();
        }
    }

    public synchronized void addClosedTasks(final List<Task> tasks) {
        if (!tasks.isEmpty()) {
            for (final Task task : tasks) {
                taskItemQueue.add(new TaskItem(task, ItemType.CLOSE));
            }
            notifyAll();
        }
    }

    public Set<TopicPartition> completedChangelogs() {
        return completedChangelogs.get();
    }

    @Override
    public void run() {
        try {
            while (isRunning()) {
                runOnce();
            }
        } catch (final RuntimeException e) {
            log.error("Encountered the following exception while restoring states " +
                    "and the thread is going to shut down: ", e);

            // we would not throw the exception from the restore thread
            // but would need the main thread to get and throw it
            fatalException.set(e);
        } finally {
            // if the thread is exiting due to exception,
            // we would still set its running flag
            isRunning.set(false);

            try {
                changelogReader.clear();
            } catch (final Throwable e) {
                log.error("Failed to close changelog reader due to the following error:", e);
            }

            shutdownLatch.countDown();
        }
    }

    private void updateChangelogReader() {
        // in each iteration hence we should update the changelog reader by unregistering closed tasks first,
        // then registering newly created ones; this is because when a task is recycled / revived it would be treated
        // as a closed-then-created tasks
        final List<TaskItem> items = new ArrayList<>();
        taskItemQueue.drainTo(items);

        for (final TaskItem item : items) {
            // TODO KAFKA-10575: we should consider also call the listener if the
            //                   task is closed but not yet completed restoration
            if (item.type == ItemType.CLOSE) {
                changelogReader.unregister(item.task.changelogPartitions());

                log.info("Unregistered changelogs {} for closing task {}",
                        item.task.changelogPartitions(),
                        item.task.id());
            } else if (item.type == ItemType.CREATE) {
                // we should only convert the state manager type right before re-registering the changelogs
                item.task.stateManager().maybeCompleteTaskTypeConversion();

                final Set<TopicPartition> allChangelogs = changelogReader.allChangelogs();
                for (final TopicPartition partition : item.task.changelogPartitions()) {
                    if (!allChangelogs.contains(partition))
                        changelogReader.register(partition, item.task.stateManager());
                }

                log.info("Registered changelogs {} for created task {}",
                        item.task.changelogPartitions(),
                        item.task.id());
            }
        }
        items.clear();
    }

    private void restoreStoresFromChangelogs() {
        // try to restore some changelogs
        final long startMs = time.milliseconds();
        try {
            final int numRestored = changelogReader.restore();
            // TODO KIP-444: we should record the restoration related metrics including restore-ratio
            log.debug("Restored {} records in {} ms", numRestored, time.milliseconds() - startMs);
        } catch (final TaskCorruptedException e) {
            log.warn("Detected the states of tasks " + e.corruptedTaskWithChangelogs() + " are corrupted. " +
                    "Will unregister the affected changelog partitions for now and let the main thread to handle it.", e);

            // we should remove all changelog partitions of the affected task, not just the corrupted partitions
            // since the main thread could potentially close the whole state manager and wipe state stores;
            // we can still proceed and restore other partitions until the main thread come revived the tasks
            changelogReader.unregister(e.corruptedTaskWithChangelogs()
                    .keySet().stream()
                    .map(changelogReader::changelogsForTask)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()));

            corruptedExceptions.add(e);
        } catch (final StreamsException e) {
            // if we are shutting down, the consumer could throw interrupt exception which can be ignored;
            // otherwise, we re-throw
            if (!(e.getCause() instanceof InterruptException) || isRunning.get()) {
                throw e;
            }
        } catch (final TimeoutException e) {
            log.info("Encountered timeout when restoring states, will retry in the next loop");
        }
    }

    // Visible for testing
    void runOnce() {
        waitIfAllChangelogsCompleted();

        if (!isRunning.get())
            return;

        // first update the changelog reader (un)registering changelogs for (closed) created
        // tasks, this must be done in each iteration so that we can stop fetching for those closed tasks asap
        updateChangelogReader();

        // try to restore some changelogs
        restoreStoresFromChangelogs();

        // finally update completed changelogs
        completedChangelogs.set(changelogReader.completedChangelogs());
    }

    /**
     * Get the next exception from the restore thread if there's any.
     * If there's a fatal exception, return that;
     * Otherwise return the non-fatal task corrupted exception
     *
     * Possible returned exception:
     *
     * * StreamsException (streams fatal)
     * * RuntimeException (non-streams fatal)
     * * TaskCorruptedException (streams non-fatal)
     */
    public RuntimeException pollNextExceptionIfAny() {
        final RuntimeException fatal = fatalException.get();

        if (fatal != null) {
            return fatal;
        } else {
            final List<TaskCorruptedException> exceptions = new ArrayList<>();
            corruptedExceptions.drainTo(exceptions);

            if (exceptions.isEmpty()) {
                return null;
            } else if (exceptions.size() == 1) {
                return exceptions.get(0);
            } else {
                // if there are more exceptions accumulated, try to consolidate them as a single exception
                // for the main thread to re-throw and handle
                final Map<TaskId, Collection<TopicPartition>> taskWithChangelogs = new HashMap<>();
                for (final TaskCorruptedException exception : exceptions) {
                    for (final Map.Entry<TaskId, Collection<TopicPartition>> entry : exception.corruptedTaskWithChangelogs().entrySet()) {
                        if (taskWithChangelogs.containsKey(entry.getKey())) {
                            taskWithChangelogs.get(entry.getKey()).addAll(entry.getValue());
                        } else {
                            taskWithChangelogs.put(entry.getKey(), new HashSet<>(entry.getValue()));
                        }
                    }
                }

                return new TaskCorruptedException(taskWithChangelogs);
            }
        }
    }

    public void shutdown(final long timeoutMs) throws InterruptedException {
        log.info("Shutting down");

        isRunning.set(false);
        interrupt();

        final boolean shutdownComplete = shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);

        if (shutdownComplete) {
            log.info("Shutdown complete");
        } else {
            log.warn("Shutdown timed out after {}", timeoutMs);
        }
    }

    private enum ItemType {
        CREATE,
        CLOSE
    }

    private static class TaskItem {
        private final Task task;
        private final ItemType type;

        private TaskItem(final Task task, final ItemType type) {
            this.task = task;
            this.type = type;
        }
    }


    // testing functions below
    ChangelogReader changelogReader() {
        return changelogReader;
    }

    void setFatalException(final RuntimeException e) {
        fatalException.set(e);
    }

    void addTaskCorruptedException(final TaskCorruptedException e) {
        corruptedExceptions.add(e);
    }

    void setIsRunning(final boolean flag) {
        isRunning.set(flag);
    }
}
