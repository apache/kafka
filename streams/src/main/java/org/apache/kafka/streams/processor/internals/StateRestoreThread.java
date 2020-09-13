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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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

    public boolean isRunning() {
        return isRunning.get();
    }

    public StateRestoreThread(final Time time,
                              final StreamsConfig config,
                              final String threadClientId,
                              final Admin adminClient,
                              final Consumer<byte[], byte[]> mainConsumer,
                              final Consumer<byte[], byte[]> restoreConsumer,
                              final StateRestoreListener userStateRestoreListener) {
        super(threadClientId);

        final String logPrefix = String.format("state-restore-thread [%s] ", threadClientId);
        final LogContext logContext = new LogContext(logPrefix);

        this.time = time;
        this.log = logContext.logger(getClass());
        this.taskItemQueue = new LinkedBlockingDeque<>();
        this.corruptedExceptions = new LinkedBlockingDeque<>();
        this.completedChangelogs = new AtomicReference<>(Collections.emptySet());

        this.changelogReader = new StoreChangelogReader(
            time, config, logContext, adminClient, mainConsumer, restoreConsumer, userStateRestoreListener);
    }

    private synchronized void waitIfAllChangelogsCompleted() {
        final Set<TopicPartition> allChangelogs = changelogReader.allChangelogs();
        if (allChangelogs.equals(changelogReader.completedChangelogs())) {
            log.debug("All changelogs {} have completed restoration so far, will wait " +
                    "until new changelogs are registered", allChangelogs);

            while (taskItemQueue.isEmpty()) {
                try {
                    wait();
                } catch (final InterruptedException e) {
                    // do nothing
                }
            }
        }
    }

    public synchronized void addInitializedTasks(final List<AbstractTask> tasks) {
        if (!tasks.isEmpty()) {
            for (final AbstractTask task: tasks) {
                taskItemQueue.add(new TaskItem(task, ItemType.CREATE));
            }
            notifyAll();
        }
    }

    public synchronized void addClosedTasks(final List<AbstractTask> tasks) {
        if (!tasks.isEmpty()) {
            for (final AbstractTask task: tasks) {
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
                waitIfAllChangelogsCompleted();

                // a task being recycled maybe in both closed and initialized tasks,
                // and hence we should process the closed ones first and then initialized ones
                final List<TaskItem> items = new ArrayList<>();
                taskItemQueue.drainTo(items);

                if (!items.isEmpty()) {
                    for (final TaskItem item : items) {
                        // TODO: we should consider also call the listener if the
                        //       changelog is not yet completed
                        if (item.type == ItemType.CLOSE) {
                            changelogReader.unregister(item.task.changelogPartitions());
                        } else if (item.type == ItemType.CREATE) {
                            for (final TopicPartition partition : item.task.changelogPartitions()) {
                                changelogReader.register(partition, item.task.stateMgr);
                            }
                        }
                    }
                }
                items.clear();

                // try to restore some changelogs
                final long startMs = time.milliseconds();
                try {
                    final int numRestored = changelogReader.restore();
                    // TODO: we should record the restoration related metrics
                    log.info("Restored {} records in {} ms", numRestored, time.milliseconds() - startMs);
                } catch (final TaskCorruptedException e) {
                    log.warn("Detected the states of tasks " + e.corruptedTaskWithChangelogs() + " are corrupted. " +
                            "Will close the task as dirty and re-create and bootstrap from scratch.", e);

                    // remove corrupted partitions form the changelog reader and continue; we can still proceed
                    // and restore other partitions until the main thread come to handle this exception
                    changelogReader.unregister(e.corruptedTaskWithChangelogs().values().stream()
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList()));

                    corruptedExceptions.add(e);
                } catch (final TimeoutException e) {
                    log.info("Encountered timeout when restoring states, will retry in the next loop");
                }

                // finally update completed changelogs
                completedChangelogs.set(changelogReader.completedChangelogs());
            }
        } catch (final Exception e) {
            log.error("Encountered the following exception while restoring states " +
                    "and the thread is going to shut down: ", e);
            throw e;
        } finally {
            try {
                changelogReader.clear();
            } catch (final Throwable e) {
                log.error("Failed to close changelog reader due to the following error:", e);
            }

            shutdownLatch.countDown();
        }
    }

    public TaskCorruptedException nextCorruptedException() {
        return corruptedExceptions.poll();
    }

    public boolean shutdown(final long timeoutMs) throws InterruptedException {
        log.info("Shutting down");

        isRunning.set(false);
        interrupt();

        final boolean ret = shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);

        if (ret) {
            log.info("Shutdown complete");
        } else {
            log.warn("Shutdown timed out after {}", timeoutMs);
        }

        return ret;
    }

    private enum ItemType {
        CREATE,
        CLOSE,
        REVIVE
    }

    private static class TaskItem {
        private final AbstractTask task;
        private final ItemType type;

        private TaskItem(final AbstractTask task, final ItemType type) {
            this.task = task;
            this.type = type;
        }
    }
}
