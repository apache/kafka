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

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


/**
 * This is the thread responsible for restoring state stores for both active and standby tasks
 */
public class StateRestoreThread extends Thread {

    private final Time time;
    private final Logger log;
    private final StreamsConfig config;
    private final ChangelogReader changelogReader;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final ConcurrentLinkedDeque<TaskCorruptedException> corruptedExceptions;

    public boolean isRunning() {
        return isRunning.get();
    }

    public StateRestoreThread(final Time time,
                              final StreamsConfig config,
                              final String threadClientId,
                              final ChangelogReader changelogReader) {
        super(threadClientId);
        this.time = time;
        this.config = config;
        this.changelogReader = changelogReader;
        this.corruptedExceptions = new ConcurrentLinkedDeque<>();

        final String logPrefix = String.format("state-restore-thread [%s] ", threadClientId);
        final LogContext logContext = new LogContext(logPrefix);
        this.log = logContext.logger(getClass());
    }

    @Override
    public void run() {
        try {
            while (isRunning()) {
                final long startMs = time.milliseconds();

                try {
                    // try to restore some changelogs, if there's nothing to restore it would wait inside this call
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
            }
        } catch (final Exception e) {
            log.error("Encountered the following exception while restoring states " +
                    "and the thread is going to shut down: ", e);
            throw e;
        } finally {
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
}
